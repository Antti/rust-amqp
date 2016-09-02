use protocol;
use table::Table;
use table::TableEntry::{FieldTable, Bool, LongString};
use framing::{Frame, FrameType, MethodFrame, ContentHeaderFrame};
use amqp_error::{AMQPResult, AMQPError};
use super::VERSION;

use std::cmp;
use std::io;
use std::mem;
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
    id: u16,
    future_handlers: HashMap<String, Complete<MethodFrame>>,
    content_body: Option<Vec<u8>>,
    content_headers: Option<ContentHeaderFrame>,
    content_method: Option<MethodFrame>
}

impl Channel {
    pub fn new(channel_id: u16) -> Self {
        Channel { id: channel_id, future_handlers: HashMap::new(), content_body: None, content_headers: None, content_method: None }
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
                    "basic.deliver" => {
                        debug!("Received basic.deliver");
                        self.content_method = Some(method_frame);
                    },
                    method_name => {
                        match self.future_handlers.remove(method_name) {
                            Some(complete) => complete.complete(method_frame),
                            None => { panic!("Unexpected method frame: {} on channel {}", method_name, self.id) }
                        }
                    }
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
                                // println!("Ready to dispatch content to consumers: {}", String::from_utf8_lossy(content_body));
                                // TODO: Dispatch self.content_method, self.content_headers, self.content_body
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

// Future that resolves into an inited session
pub struct SessionInitializer {
    connection: Option<Connection>,
    options: Options,
    session_inited: bool
}

impl SessionInitializer {
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
                                    response: format!("\0{}\0{}", self.options.login, self.options.password),
                                    locale: self.options.locale.clone(),
                                };
                                debug!("Sending connection.start-ok: {:?}", start_ok);
                                self.write_frame_to_buf(&start_ok.to_frame(0).unwrap());
                                Ok(())
                            },

                            "connection.tune" => {
                                // send tune-ok, send connection.open
                                let tune = try!(protocol::connection::Tune::decode(method_frame));
                                debug!("Received tune request: {:?}", tune);
                                self.options.channel_max_limit = negotiate(tune.channel_max, self.options.channel_max_limit);
                                self.options.frame_max_limit = negotiate(tune.frame_max, self.options.frame_max_limit);
                                let tune_ok = protocol::connection::TuneOk {
                                    channel_max: self.options.channel_max_limit,
                                    frame_max: self.options.frame_max_limit,
                                    heartbeat: self.options.heartbeat,
                                };
                                debug!("Sending connection.tune-ok: {:?}", tune_ok);
                                self.write_frame_to_buf(&tune_ok.to_frame(0).unwrap());


                                let open = protocol::connection::Open {
                                    virtual_host: percent_decode(&self.options.vhost),
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
                                self.session_inited = true;
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
                let err_msg = format!("Unexpected frame on channel {} during session initialization", channel_id);
                error!("{}", err_msg);
                let close = protocol::connection::Close {
                    reply_code: 500,
                    reply_text: err_msg,
                    class_id: 0,
                    method_id: 0,
                };
                self.write_frame_to_buf(&close.to_frame(0).unwrap());
                Err(AMQPError::Protocol(format!("Unexpected frame type on channel 0: {:?}", channel_id)))
            }
        }
    }

    fn write_frame_to_buf(&mut self, frame: &Frame) {
        match self.connection {
            Some(ref mut connection) => {
                connection.write_slice(&frame.encode().unwrap());
                connection.flush_buffers();
            },
            None => panic!("Connection not found")
        }
    }

}

impl Future for SessionInitializer {
    type Item = Session;
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let frames = match self.connection {
            Some(ref mut connection) => {
                match connection.flush_buffers() {
                    Ok(frames) => frames,
                    Err(e) => return Poll::Err(e)
                }
            },
            None => panic!("Feature was resolved")
        };
        for frame in frames {
            self.dispatch_frame(frame);
        }
        match self.session_inited {
            true => {
                debug!("Session was inited. Resolving future");
                match mem::replace(&mut self.connection, None){
                    Some(connection) => {
                        Poll::Ok(Session {
                            channel_max_limit: self.options.channel_max_limit,
                            connection: connection,
                            frame_max_limit: self.options.frame_max_limit,
                            heartbeat: self.options.heartbeat,
                            channels: HashMap::new()
                        })
                    },
                    None => panic!("Session is gone")
                }
            },
            false => Poll::NotReady
        }
    }
}

pub struct SessionRunner {
    session: Session
}

impl Future for SessionRunner {
    type Item = ();
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        debug!("Polling runner");
        match self.session.flush_buffers() {
            Ok(_) => Poll::NotReady,
            Err(err) => Poll::Err(err)
        }
    }
}

pub struct Connection {
    stream: TcpStream,
    frame_read_buf: BlockBuf,
    frame_write_buf: BlockBuf
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection { stream: stream, frame_read_buf: BlockBuf::default(), frame_write_buf: BlockBuf::default() }
    }

    pub fn flush_buffers(&mut self) -> AMQPResult<Vec<Frame>> {
        self.flush_write_buffer();
        self.fill_read_buffer();
        Ok(self.parse_frames())
    }

    pub fn write_slice(&mut self, slice: &[u8]) {
        self.frame_write_buf.write_slice(slice)
    }

    fn flush_write_buffer(&mut self) -> AMQPResult<()> {
        use bytes::WriteExt;

        debug!("Flushing buffers");
        if self.frame_write_buf.len() > 0 {
            debug!("Trying to write write buffer. Write buf size: {}", self.frame_write_buf.len());
            let write_result = self.stream.write_buf(&mut self.frame_write_buf.buf());
            match write_result {
                Ok(write_len) => {
                    self.frame_write_buf.shift(write_len);
                    debug!("Bytes written. New write buf size: {:?}", self.frame_write_buf.len());
                },
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    debug!("Writing would block");
                },
                Err(e) => return Err(From::from(e))
            }
        }
        Ok(())
    }

    fn fill_read_buffer(&mut self) -> AMQPResult<()> {
        use bytes::ReadExt;

        debug!("Trying to append buffer starting from: {}", self.frame_read_buf.len());
        let read_result = self.stream.read_buf(&mut self.frame_read_buf);
        match read_result {
            Ok(read_len) => {
                debug!("Bytes read. New read buf size: {}", self.frame_read_buf.len());
                Ok(())
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                debug!("Reading would block");
                Ok(())
            },
            Err(e) => Err(From::from(e))
        }
    }

    fn parse_frames(&mut self) -> Vec<Frame> {
        let mut frames = vec![];
        while self.frame_read_buf.len() > 0 {
            match self.parse_frame(){
                Some(frame) => frames.push(frame),
                None => break
            }
        }
        frames
    }

    fn parse_frame(&mut self) -> Option<Frame> {
        debug!("Trying to parse frame. Buf size: {}", self.frame_read_buf.len());
        try_parse_frame(&mut (self.frame_read_buf)).map(|frame|{
            debug!("Frame parsed. Bytes left in the buffer: {}", self.frame_read_buf.len());
            frame
        })
    }
}

impl futures::stream::Stream for Connection {
    type Item = Frame;
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Err(err) = self.flush_write_buffer() {
            return Poll::Err(err)
        }
        if let Err(err) = self.fill_read_buffer() {
            return Poll::Err(err)
        }
        match self.parse_frame() {
            Some(frame) => Poll::Ok(Some(frame)),
            None => Poll::NotReady
        }
    }
}

pub struct SyncMethodFuture<T> {
    oneshot: Oneshot<T>,
    connection: Connection
}

impl <T> Future for SyncMethodFuture<T> {
    type Item = T;
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.connection.flush_buffers();
        Poll::NotReady
    }
}


/// Session holds the connection.
/// Every synchronous method creates a future, which will be resolved, when the corresponding response is received.
/// Session receives all the frames and "dispatches" them to resolve futures. The future will be resolved if
/// the messages matches future's expectation, that is the channel is correct and the expected message class & method match.
/// The consumer is treated like a stream, so the session drives a stream and for each resolved message it sends ack/reject/nack.

pub struct Session {
    connection: Connection,
    channel_max_limit: u16,
    frame_max_limit: u32,
    heartbeat: u16,
    channels: HashMap<u16, Channel>
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
    pub fn open_url(handle: LoopHandle, url_string: &str) -> BoxFuture<Session, AMQPError> {
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
    pub fn new(handle: LoopHandle, options: Options) -> BoxFuture<Self, AMQPError> {
        let address = resolve(&options.host, options.port);
        let stream = handle.tcp_connect(&address);
        let inited_connection = stream.and_then(|stream|{
            debug!("Initializing connection...");
            write_all(stream, [b'A', b'M', b'Q', b'P', 0, 0, 9, 1])
        });
        inited_connection.map_err(From::from).and_then(move |(stream, _)|
            SessionInitializer {
                connection: Some(Connection::new(stream)),
                options: options,
                session_inited: false
            }
        ).boxed()
    }

    // pub fn run(&mut self) -> BoxFuture<(), AMQPError> {
    //     use futures::stream::Stream;

    //     self.connection.map(|frame|{
    //         self.dispatch_frame(frame)
    //     }).into_future().map(|_| () ).map_err(|(err,_)| err ).boxed()
    // }

    // pub fn close(self) -> BoxFuture<(), AMQPError> {
    //     let close = protocol::connection::Close {
    //             reply_code: 200,
    //             reply_text: "Bye".to_string(),
    //             class_id: 0,
    //             method_id: 0
    //     };
    //     self.write_sync_method::<_, protocol::connection::CloseOk>(close, 0).map(|(session, _close_ok)| drop(session)).boxed()
    // }

    pub fn open_channel(&mut self, channel_id: u16) -> BoxFuture<u16, AMQPError> {
        self.channels.insert(channel_id, Channel::new(channel_id));
        let open_channel = protocol::channel::Open::with_default_values();
        self.write_sync_method::<_, protocol::channel::OpenOk>(open_channel, channel_id).map(move |open_ok|{
            channel_id
        }).boxed()
    }

    // pub fn close_channel(self, channel_id: u16) -> BoxFuture<Self, AMQPError> {
    //     let close_channel = protocol::channel::Close {
    //         reply_code: 200,
    //         reply_text: "Closing channel".into(),
    //         class_id: 0,
    //         method_id: 0,
    //     };
    //     self.write_sync_method::<_, protocol::channel::CloseOk>(close_channel, channel_id).map(|(session, _)| session).boxed()
    // }

    pub fn consume<S>(&mut self, channel_id: u16, queue: S) -> BoxFuture<protocol::basic::ConsumeOk, AMQPError> where S: Into<String> {
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
        self.write_sync_method(consume, channel_id)
    }

    // pub fn basic_qos(self,
    //              channel_id: u16,
    //              prefetch_size: u32,
    //              prefetch_count: u16,
    //              global: bool)
    //              -> BoxFuture<(Self, protocol::basic::QosOk), AMQPError> {
    //     let qos = protocol::basic::Qos {
    //         prefetch_size: prefetch_size,
    //         prefetch_count: prefetch_count,
    //         global: global,
    //     };
    //     self.write_sync_method::<_, protocol::basic::QosOk>(qos, channel_id).boxed()
    // }

    pub fn session_runner(self) -> SessionRunner {
        SessionRunner { session: self }
    }

    fn dispatch_frame(&mut self, frame: Frame) -> AMQPResult<()> {
        match frame.channel {
            0 => {
                match frame.frame_type {
                    FrameType::METHOD => {
                        let method_frame = try!(MethodFrame::decode(&frame));
                        match method_frame.method_name() {
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
                debug!("Dispatching frame to channel: {}", channel_id);
                match self.channels.get_mut(&channel_id) {
                    Some(mut channel) => channel.dispatch(frame),
                    None => panic!("Unknown channel {}", channel_id)
                }
                Ok(())
            }
        }
    }

    fn write_frame_to_buf(&mut self, frame: &Frame) {
        self.connection.write_slice(&frame.encode().unwrap());
        self.flush_buffers();
    }

    fn flush_buffers(&mut self) -> AMQPResult<()> {
        self.connection.flush_buffers().and_then(|frames|{
            for frame in frames {
                match self.dispatch_frame(frame) {
                    Err(err) => return Err(err),
                    _ => {}
                }
            }
            Ok(())
        })
    }

    fn write_sync_method<T, U>(&mut self, method: T, channel_id: u16) -> BoxFuture<U, AMQPError> where T: Method, U: Method + Send + 'static {
        self.write_frame_to_buf(&method.to_frame(channel_id).unwrap());
        let (tx, rx) = futures::oneshot();
        {
            let c = self.channels.get_mut(&channel_id).unwrap();
            c.register_sync_future(U::name().to_owned(), tx);
        }
        let oneshot = rx.map_err(|_| AMQPError::Protocol("Oneshot was cancelled".to_owned())).and_then(|method_frame|{
            Method::decode(method_frame)
        });
        // TODO: Keep polling connection and dispatching frames until oneshot was resolved.
        oneshot.boxed()
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
