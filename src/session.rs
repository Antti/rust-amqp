use protocol;
use table::Table;
use table::TableEntry::{FieldTable, Bool, LongString};
use framing::{Frame, FrameType, MethodFrame, ContentHeaderFrame};
use amqp_error::{AMQPResult, AMQPError};
use super::VERSION;

use std::cmp;
use std::io;
use std::collections::HashMap;
use std::net::SocketAddr;

use byteorder::{BigEndian, ReadBytesExt};
use enum_primitive::FromPrimitive;

use futures::{self, Future, BoxFuture, Oneshot, Complete, Poll, finished, done, failed};
use tokio_core::io::{read_exact, write_all};
use tokio_core::io::{TaskIo, TaskIoRead, TaskIoWrite};
use tokio_core::{LoopHandle, TcpStream};

use bytes::{Buf, BlockBuf, MutBuf};


use url::{Url, percent_encoding};

pub const AMQPS_PORT: u16 = 5671;
pub const AMQP_PORT: u16 = 5672;

#[derive(Debug)]
pub enum AMQPScheme {
    AMQP,
    #[cfg(feature = "tls")]
    AMQPS,
}

#[derive(Debug)]
pub struct Options {
    pub host: String,
    pub port: u16,
    pub login: String,
    pub password: String,
    pub vhost: String,
    pub frame_max_limit: u32,
    pub channel_max_limit: u16,
    pub locale: String,
    pub scheme: AMQPScheme,
    pub heartbeat: u16
}

impl Default for Options {
    fn default() -> Options {
        Options {
            host: "127.0.0.1".to_string(),
            port: AMQP_PORT,
            vhost: "/".to_string(),
            login: "guest".to_string(),
            password: "guest".to_string(),
            frame_max_limit: 131072,
            channel_max_limit: 65535,
            heartbeat: 0,
            locale: "en_US".to_string(),
            scheme: AMQPScheme::AMQP,
        }
    }
}

pub struct Channel {
    pub id: u16,
    future_handlers: HashMap<String, Complete<MethodFrame>>,
    content_body: Option<Vec<u8>>,
    content_headers: Option<ContentHeaderFrame>
}

impl Channel {
    pub fn new(channel_id: u16) -> Self {
        Channel { id: channel_id, future_handlers: HashMap::new(), content_body: None, content_headers: None }
    }
    pub fn register_sync_future(&mut self, method_name: String, complete: Complete<MethodFrame>) {
        self.future_handlers.insert(method_name, complete);
    }

    pub fn dispatch(&mut self, frame: Frame) {
        match frame.frame_type {
            FrameType::METHOD => {
                let method_frame = MethodFrame::decode(&frame).unwrap();
                let method_name = method_frame.method_name();
                match method_name {
                    "basic.deliver" => { debug!("Received basic.deliver"); return; },
                    _ => {}
                }
                match self.future_handlers.remove(method_name) {
                    Some(complete) => complete.complete(method_frame),
                    None => { panic!("Unexpected method frame: {} on channel {}", method_name, self.id) }
                }
            },
            FrameType::HEADERS => {
                let content_headers = ContentHeaderFrame::decode(&frame).unwrap();
                self.content_body = Some(Vec::with_capacity(content_headers.body_size as usize));
                self.content_headers = Some(content_headers);
            },
            FrameType::BODY => {
                match self.content_body {
                    Some(ref mut content_body) => {
                        content_body.extend_from_slice(&frame.payload);
                        if let Some(ref content_headers) = self.content_headers {
                            if content_body.len() == content_headers.body_size as usize {
                                // println!("Ready to dispatch content to consumers: {:?}, {}", content_body, String::from_utf8_lossy(content_body));
                                // TODO: Dispatch to consumers
                            }
                        }
                    },
                    None => panic!("Unexpected body frame. Expected headers first")
                }
            },
            FrameType::HEARTBEAT => {}
        }
    }
}

/// Session holds the connection.
/// Every synchronous method creates a future, which will be resolved, when the corresponding response is received.
/// Session receives all the frames and "dispatches" them to resolve futures. The future will be resolved if
/// the messages matches future's expectation, that is the channel is correct and the expected message class & method match.
/// The consumer is treated like a stream, so the session drives a stream and for each resolved message it sends ack/reject/nack.

pub struct Session {
    tcp_stream: TcpStream,
    channel_max_limit: u16,
    frame_max_limit: u32,
    heartbeat: u16,
    channels: HashMap<u16, Channel>,
    frame_read_buf: BlockBuf,
    frame_write_buf: BlockBuf
}

impl Session {
    /// Use `open_url` to create new amqp session from a "amqp url"
    ///
    /// # Arguments
    /// * `url_string`: The format is: `amqp://username:password@host:port/virtual_host`
    ///
    /// Most of the params have their default, so you can just pass this:
    /// `"amqp://localhost//"` and it will connect to rabbitmq server,
    /// running on `localhost` on port `5672`,
    /// with login `"guest"`, password: `"guest"` to vhost `"/"`
    pub fn open_url(handle: LoopHandle, url_string: &str) -> Box<Future<Item=Session, Error=AMQPError>> {
        let options = parse_url(url_string).unwrap();
        Session::new(handle, options)
    }

    /// Initialize new rabbitmq session.
    /// You can use default options:
    /// # Example
    /// ```no_run
    /// use std::default::Default;
    /// use amqp::{Options, Session};
    /// let session = match Session::new(Options { .. Default::default() }){
    ///     Ok(session) => session,
    ///     Err(error) => panic!("Failed openning an amqp session: {:?}", error)
    /// };
    /// ```
    pub fn new(handle: LoopHandle, options: Options) -> BoxFuture<Session, AMQPError> {
        let frame_max_limit = options.frame_max_limit;
        let heartbeat = options.heartbeat;
        let address = resolve(&options.host, options.port);
        let stream = handle.tcp_connect(&address);
        let inited_connection = stream.and_then(|stream|{
            debug!("Initializing connection...");
            write_all(stream, [b'A', b'M', b'Q', b'P', 0, 0, 9, 1])
        });
        inited_connection.map(move |(stream, _)| {
            Session {
                channel_max_limit: 65535,
                tcp_stream: stream,
                frame_max_limit: frame_max_limit,
                heartbeat: heartbeat,
                channels: HashMap::new(),
                frame_read_buf: BlockBuf::default(),
                frame_write_buf: BlockBuf::default()
            }
        }).map_err(From::from).
            //and_then(move |session| session.init(options)).
            boxed()
    }

    pub fn close(self) -> BoxFuture<(), AMQPError> {
        let close = protocol::connection::Close {
                reply_code: 200,
                reply_text: "Bye".to_string(),
                class_id: 0,
                method_id: 0
        };
        self.write_sync_method::<_, protocol::connection::CloseOk>(close, 0).map(|(session, _close_ok)| drop(session)).boxed()
    }

    pub fn open_channel(self, channel_id: u16) -> BoxFuture<(Self, u16), AMQPError> {
        let open_channel = protocol::channel::Open::with_default_values();
        self.write_sync_method::<_, protocol::channel::OpenOk>(open_channel, channel_id).map(move |(mut session, _open_ok)|{
            session.channels.insert(channel_id, Channel::new(channel_id));
            (session, channel_id)
        }).boxed()
    }

    pub fn close_channel(self, channel_id: u16) -> BoxFuture<Self, AMQPError> {
        let close_channel = protocol::channel::Close {
            reply_code: 200,
            reply_text: "Closing channel".into(),
            class_id: 0,
            method_id: 0,
        };
        self.write_sync_method::<_, protocol::channel::CloseOk>(close_channel, channel_id).map(|(session, _)| session).boxed()
    }

    pub fn consume<S>(self, channel_id: u16, queue: S) -> BoxFuture<(Self, protocol::basic::ConsumeOk), AMQPError> where S: Into<String> {
        let consume = protocol::basic::Consume {
            ticket: 0,
            queue: queue.into(),
            consumer_tag: "".into(),
            no_local: true,
            no_ack: true,
            exclusive: false,
            nowait: false,
            arguments: Table::new(),
        };
        self.write_sync_method::<_, protocol::basic::ConsumeOk>(consume, channel_id).boxed()
    }


    pub fn basic_qos(self,
                 channel_id: u16,
                 prefetch_size: u32,
                 prefetch_count: u16,
                 global: bool)
                 -> BoxFuture<(Self, protocol::basic::QosOk), AMQPError> {
        let qos = protocol::basic::Qos {
            prefetch_size: prefetch_size,
            prefetch_count: prefetch_count,
            global: global,
        };
        self.write_sync_method::<_, protocol::basic::QosOk>(qos, channel_id).boxed()
    }

    fn dispatch_frame(&mut self, frame: Frame) -> AMQPResult<()> {
        match frame.channel {
            0 => {
                match frame.frame_type {
                    FrameType::METHOD => {
                        let method_frame = try!(MethodFrame::decode(&frame));
                        match method_frame.method_name() {
                            "connection.start" => {
                                let start_frame = try!(protocol::connection::Start::decode(method_frame));
                                debug!("Received connection.start: {:?}", start_frame);
                                let mut client_properties = Table::new();
                                let mut capabilities = Table::new();
                                capabilities.insert("publisher_confirms".to_owned(), Bool(true));
                                capabilities.insert("consumer_cancel_notify".to_owned(), Bool(true));
                                capabilities.insert("exchange_exchange_bindings".to_owned(), Bool(true));
                                capabilities.insert("basic.nack".to_owned(), Bool(true));
                                capabilities.insert("connection.blocked".to_owned(), Bool(true));
                                capabilities.insert("authentication_failure_close".to_owned(), Bool(true));
                                client_properties.insert("capabilities".to_owned(), FieldTable(capabilities));
                                client_properties.insert("product".to_owned(), LongString("rust-amqp".to_owned()));
                                client_properties.insert("platform".to_owned(), LongString("rust".to_owned()));
                                client_properties.insert("version".to_owned(), LongString(VERSION.to_owned()));
                                client_properties.insert("information".to_owned(),
                                                            LongString("https://github.com/Antti/rust-amqp".to_owned()));

                                let start_ok = protocol::connection::StartOk {
                                    client_properties: client_properties,
                                    mechanism: "PLAIN".to_owned(),
                                    response: format!("\0{}\0{}", "guest", "guest"),
                                    locale: "en_US".to_owned(),
                                };
                                self.write_frame_to_buf(&start_ok.to_frame(0).unwrap());
                                Ok(())
                            },

                            "connection.tune" => {
                                // send tune-ok, send connection.open
                                let tune = try!(protocol::connection::Tune::decode(method_frame));
                                debug!("Received tune request: {:?}", tune);
                                self.channel_max_limit = negotiate(tune.channel_max, self.channel_max_limit);
                                self.frame_max_limit = negotiate(tune.frame_max, self.frame_max_limit);
                                let tune_ok = protocol::connection::TuneOk {
                                    channel_max: self.channel_max_limit,
                                    frame_max: self.frame_max_limit,
                                    heartbeat: self.heartbeat,
                                };
                                debug!("Sending connection.tune-ok: {:?}", tune_ok);
                                self.write_frame_to_buf(&tune_ok.to_frame(0).unwrap());


                                let open = protocol::connection::Open {
                                    virtual_host: percent_decode("/"),
                                    capabilities: "".to_owned(),
                                    insist: false,
                                };

                                debug!("Sending connection.open: {:?}", open);
                                self.write_frame_to_buf(&open.to_frame(0).unwrap());
                                Ok(())
                            },

                            "connection.open-ok" => {
                                // connection initialized, notify something
                                let open_ok = try!(protocol::connection::OpenOk::decode(method_frame));
                                println!("Received connection.open-ok: {:?}", open_ok);
                                Ok(())
                            }

                            "connection.close" => {
                                let close = try!(protocol::connection::Close::decode(method_frame));
                                debug!("Sending connection.close");
                                self.write_frame_to_buf(&protocol::connection::CloseOk.to_frame(0).unwrap());
                                Err(AMQPError::ConnectionClosed(close))
                            },
                            _ => Ok(())
                        }
                    },
                    frame_type => Err(AMQPError::Protocol(format!("Unexpected frame type on channel 0: {:?}", frame_type)))
                }
            }, // handle connection methods
            channel_id => {
                match self.channels.get_mut(&channel_id) {
                    Some(mut channel) => channel.dispatch(frame),
                    None => panic!("Unknown channel {}", channel_id)
                }
                Ok(())
            }
        }
    }

    fn write_frame_to_buf(&mut self, frame: &Frame) {
        self.frame_write_buf.write_slice(&frame.encode().unwrap())
    }

    fn read_frame(self) -> BoxFuture<(Self, Frame), AMQPError> {
        let Session { tcp_stream, frame_read_buf, frame_write_buf, channel_max_limit, frame_max_limit, heartbeat, channels } =  self;
        read_frame(tcp_stream).map(move |(tcp_stream, frame)|
            (Session {
                tcp_stream: tcp_stream,
                frame_read_buf: frame_read_buf,
                frame_write_buf: frame_write_buf,
                channel_max_limit: channel_max_limit,
                frame_max_limit: frame_max_limit,
                heartbeat: heartbeat,
                channels: channels
            }, frame)
        ).boxed()
    }

    fn write_frame(self, frame: Frame) -> BoxFuture<Self, AMQPError> {
        let Session { tcp_stream, frame_read_buf, frame_write_buf, channel_max_limit, frame_max_limit, heartbeat, channels } =  self;
        write_frame(tcp_stream, frame).map(move |tcp_stream|
            Session {
                tcp_stream: tcp_stream,
                frame_read_buf: frame_read_buf,
                frame_write_buf: frame_write_buf,
                channel_max_limit: channel_max_limit,
                frame_max_limit: frame_max_limit,
                heartbeat: heartbeat,
                channels: channels
            }
        ).boxed()
    }

    fn write_sync_method<T, U>(self, method: T, channel_id: u16) -> BoxFuture<(Self, U), AMQPError> where T: Method, U: Method + Send + 'static {
        let session = done(method.to_frame(channel_id)).and_then(|method_frame|{
            self.write_frame(method_frame)
        });
        session.and_then(|session|{
            session.read_frame().and_then(|(session, frame)|{
                let method_frame = done(MethodFrame::decode(&frame));
                let maybe_reply = method_frame.and_then(|method_frame|{
                    done(Method::decode(method_frame) as AMQPResult<U>)
                });
                finished(session).join(maybe_reply)
            })
        }).boxed()
    }

    fn write_sync_method2<T>(self, method: T, channel_id: u16, r_method: String) -> BoxFuture<(Self, Oneshot<MethodFrame>), AMQPError> where T: Method {
        let session = done(method.to_frame(channel_id)).and_then(|method_frame|{
            self.write_frame(method_frame)
        });
        session.map(move |mut session|{
            let (tx, rx) = futures::oneshot();
            {
                let c = session.channels.get_mut(&channel_id).unwrap();
                c.register_sync_future(r_method, tx);
            }
            (session, rx)
        }).boxed()
    }
}

#[derive(Debug)]
enum ConsumeResult {
    Ack { delivery_tag: u64, multiple: bool },
    Nack { delivery_tag: u64, multiple: bool, requeue: bool },
    Reject { delivery_tag: u64, requeue: bool }
}

fn test_consumer(frame: Frame) -> BoxFuture<ConsumeResult, AMQPError> {
    done(Ok(ConsumeResult::Ack{delivery_tag: 25, multiple: false})).boxed()
}

fn read_frame(tcp_stream: TcpStream) -> BoxFuture<(TcpStream, Frame), AMQPError> {
    read_exact(tcp_stream, [0u8; 7]).and_then(|(tcp_stream, header)|{
        let header = &mut &header[..];
        let frame_type_id = header.read_u8().unwrap();
        let channel = header.read_u16::<BigEndian>().unwrap();
        let payload_size = header.read_u32::<BigEndian>().unwrap() as usize;
        let payload_buf = vec![0u8; payload_size+1];

        let frame_type = done(match FrameType::from_u8(frame_type_id) {
            Some(frame_type) => Ok(frame_type),
            None => Err(io::Error::new(io::ErrorKind::Other, format!("Unknown Frame Type: {:X}", frame_type_id)))
        });
        read_exact(tcp_stream, payload_buf).and_then(|(tcp_stream, mut payload)| {
            let frame_end_validated = done(match payload[payload.len()-1] {
                0xCE => {
                    let payload_len = payload.len()-1;
                    payload.truncate(payload_len);
                    Ok(payload)
                },
                _ => Err(io::Error::new(io::ErrorKind::Other, "Frame end error"))
            });
            finished(tcp_stream).join3(frame_end_validated, frame_type)
        }).map(move |(tcp_stream, payload, frame_type)| {
            (tcp_stream, Frame {
                    frame_type: frame_type,
                    channel: channel,
                    payload: payload
            })
        })
    }).map_err(From::from).boxed()
}

impl Future for Session {
    type Item = ();
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        use std::io::Read;
        use bytes::{ReadExt, WriteExt};

        debug!("Session is being polled");
        loop {
            if self.frame_write_buf.len() > 0 {
                debug!("Trying to write write buffer. Write buf size: {}", self.frame_write_buf.len());
                let write_len = try_nb!(self.tcp_stream.write_buf(&mut self.frame_write_buf.buf())); // TODO: Maybe do with after read.
                self.frame_write_buf.shift(write_len);
                debug!("Bytes written. New write buf size: {:?}", self.frame_write_buf.len());
            }

            debug!("Trying to append buffer starting from: {}", self.frame_read_buf.len());
            try_nb!(self.tcp_stream.read_buf(&mut self.frame_read_buf));
            debug!("Bytes read. New read buf size: {}", self.frame_read_buf.len());

            while self.frame_read_buf.len() > 0 {
                debug!("Trying to parse frame. Buf size: {}", self.frame_read_buf.len());
                match try_parse_frame(&mut (self.frame_read_buf)) {
                    Some(frame) => {
                        debug!("Frame parsed. Bytes left in the buffer: {}", self.frame_read_buf.len());
                        if let Err(err) = self.dispatch_frame(frame) {
                            return Poll::Err(err)
                        }
                    },
                    None => { break }
                }
            }
        }

        Poll::NotReady
    }
}


fn try_parse_frame(buf: &mut BlockBuf) -> Option<Frame> {
    use framing::FrameHeader;

    // This panics..
    // if !buf.is_compact() {
    //     buf.compact();
    // }

    if buf.len() > 7 {
        let mut h = [0u8; 7];
        buf.buf().read_slice(&mut h);
        let header = FrameHeader::new(&h);
        if buf.len() < header.payload_size as usize + 8 { // header + payload_size + frame_end
            return None;
        }

        buf.shift(7); // skip the header
        let payload_buf = buf.shift(header.payload_size as usize);
        let frame_end = buf.shift(1);
        if frame_end.buf().bytes()[0] != 0xCE {
            error!("Frame end error");
            return None; // There should be a way to indicate an error;
        }
        Some(Frame {
            frame_type: FrameType::from_u8(header.frame_type_id).unwrap(), //also should return, rather than panicing
            channel: header.channel,
            payload: Vec::from(payload_buf.buf().bytes())
        })
    } else {
        None
    }
}

#[inline]
fn write_frame(tcp_stream: TcpStream, frame: Frame) -> BoxFuture<TcpStream, AMQPError> {
    write_all(tcp_stream, frame.encode().unwrap()).map(|(tcp_stream, _)| tcp_stream).map_err(From::from).boxed()
}

fn negotiate<T: cmp::Ord>(their_value: T, our_value: T) -> T {
    cmp::min(their_value, our_value)
}

fn percent_decode(string: &str) -> String {
    percent_encoding::percent_decode(string.as_bytes()).decode_utf8_lossy().to_string()
}

fn parse_url(url_string: &str) -> AMQPResult<Options> {
    fn clean_vhost(vhost: &str) -> &str {
        match vhost.chars().next() {
            Some('/') => &vhost[1..],
            _ => vhost
        }
    }

    let default: Options = Default::default();

    let url = try!(Url::parse(url_string));
    if url.cannot_be_a_base() {
        return Err(AMQPError::SchemeError("Must have relative scheme".to_string()));
    }

    let vhost = clean_vhost(url.path());
    let host = url.domain().map(|s| s.to_string()).unwrap_or(default.host);
    let login = match url.username() {
        "" => String::from(default.login),
        username => username.to_string()
    };
    let password = url.password().map_or(String::from(default.password), ToString::to_string);
    let (scheme, default_port) = match url.scheme() {
        "amqp" => (AMQPScheme::AMQP, AMQP_PORT),
        #[cfg(feature = "tls")]
        "amqps" => (AMQPScheme::AMQPS, AMQPS_PORT),
        unknown_scheme => {
            return Err(AMQPError::SchemeError(format!("Unknown scheme: {:?}", unknown_scheme)))
        }
    };
    let port = url.port().unwrap_or(default_port);

    Ok(Options {
        host: host.to_string(),
        port: port,
        scheme: scheme,
        login: login,
        password: password,
        vhost: vhost.to_string(),
        ..Default::default()
    })
}

fn resolve(host: &str, port: u16) -> SocketAddr {
    use std::net::ToSocketAddrs;
    let mut addrs = (host, port).to_socket_addrs().unwrap();
    addrs.next().unwrap()
}


#[cfg(test)]
mod test {
    use super::{parse_url, AMQPScheme};

    #[test]
    fn test_full_parse_url() {
        let options = parse_url("amqp://username:password@hostname:12345/vhost").expect("Failed parsing url");
        assert_eq!(options.host, "hostname");
        assert_eq!(options.login, "username");
        assert_eq!(options.password, "password");
        assert_eq!(options.port, 12345);
        assert_eq!(options.vhost, "vhost");
        // assert!(match options.scheme { AMQPScheme::AMQP => true, _ => false });
    }

    #[test]
    fn test_full_parse_url_without_vhost() {
        let options = parse_url("amqp://host").expect("Failed parsing url");
        assert_eq!(options.host, "host");
        assert_eq!(options.vhost, "/");
    }

    #[test]
    fn test_full_parse_url_with_empty_vhost() {
        let options = parse_url("amqp://host/").expect("Failed parsing url");
        assert_eq!(options.host, "host");
        assert_eq!(options.vhost, "");
    }

    #[test]
    fn test_full_parse_url_with_slash_vhost() {
        let options = parse_url("amqp://host//").expect("Failed parsing url");
        assert_eq!(options.host, "host");
        assert_eq!(options.vhost, "/");
    }

    #[test]
    fn test_parse_url_defaults() {
        let options = parse_url("amqp://").expect("Failed parsing url");
        assert_eq!(options.host, "");
        assert_eq!(options.vhost, "");
        assert_eq!(options.login, "guest");
        assert_eq!(options.password, "guest");
        assert_eq!(options.port, 5672);
    }
}
