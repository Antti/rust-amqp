use channel;
use protocol;
use table::Table;
use table::TableEntry::{FieldTable, Bool, LongString};
use framing::{Frame, MethodFrame};
use amqp_error::{AMQPResult, AMQPError};
use protocol::Method;
use super::VERSION;

use std::cmp;
use std::io;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use enum_primitive::FromPrimitive;

use futures::{Future, BoxFuture, finished, done};
use futures_io::{read_exact, write_all};
use futures_io::{TaskIo, TaskIoRead, TaskIoWrite};
use futures_mio::{LoopHandle, TcpStream};

use std::net::SocketAddr;


use url::{Url, percent_encoding};

pub const AMQPS_PORT: u16 = 5671;
pub const AMQP_PORT: u16 = 5672;

const CHANNEL_BUFFER_SIZE: usize = 100;

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
}

impl Default for Options {
    fn default() -> Options {
        Options {
            host: "127.0.0.1".to_string(),
            port: AMQP_PORT,
            vhost: "".to_string(),
            login: "guest".to_string(),
            password: "guest".to_string(),
            frame_max_limit: 131072,
            channel_max_limit: 65535,
            locale: "en_US".to_string(),
            scheme: AMQPScheme::AMQP,
        }
    }
}

pub struct Session {
    pub reader: TaskIoRead<TcpStream>,
    pub writer: TaskIoWrite<TcpStream>,
    channel_max_limit: u16,
    frame_max_limit: u32
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
        let address = resolve(&options.host, options.port);
        let stream = handle.tcp_connect(&address);
        let pair = stream.map(move |tcp_stream| {
            debug!("Connected to: {:?}", address);
            let io = TaskIo::new(tcp_stream);
            io.split()
        });
        let inited_connection = pair.and_then(|(reader, writer)|{
            debug!("Initializing connection...");
            finished(reader).join(write_all(writer, [b'A', b'M', b'Q', b'P', 0, 0, 9, 1]))
        });
        inited_connection.map(move |(reader, (writer, _))| {
            Session {
                channel_max_limit: 65535,
                reader: reader,
                writer: writer,
                frame_max_limit: frame_max_limit
            }
        }).map_err(From::from).and_then(move |session| session.init(options)).boxed()
    }

    fn read_frame(self) -> BoxFuture<(Self, Frame), AMQPError> {
        let Session { reader, writer, channel_max_limit, frame_max_limit } =  self;
        read_frame(reader).map(move |(reader, frame)|
            (Session{ reader: reader, writer: writer, channel_max_limit: channel_max_limit, frame_max_limit: frame_max_limit }, frame)
        ).boxed()
    }

    fn write_frame(self, frame: Frame) -> BoxFuture<Self, AMQPError> {
        let Session { reader, writer, channel_max_limit, frame_max_limit } =  self;
        write_frame(writer, frame).map(move |writer|
            Session{ reader: reader, writer: writer, channel_max_limit: channel_max_limit, frame_max_limit: frame_max_limit }
        ).boxed()
    }

    fn init(self, options: Options) -> BoxFuture<Session, AMQPError> {
        debug!("Starting init session");
        let Options{ login, password, locale, vhost, .. } = options;
        let start_frame = self.read_frame().and_then(|(session, frame)| {
            let method_frame = MethodFrame::decode(&frame).unwrap();
            let start : AMQPResult<protocol::connection::Start> = protocol::Method::decode(method_frame);
            finished(session).join(done(start))
        });
        let session = start_frame.and_then(move |(session, start_frame)|{
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
            client_properties.insert("version".to_owned(), LongString("VERSION".to_owned()));
            client_properties.insert("information".to_owned(),
                                        LongString("https://github.com/Antti/rust-amqp".to_owned()));

            let start_ok = protocol::connection::StartOk {
                client_properties: client_properties,
                mechanism: "PLAIN".to_owned(),
                response: format!("\0{}\0{}", login, password),
                locale: locale,
            };
            session.write_frame(start_ok.to_frame(0).unwrap())
        });

        let session = session.and_then(|session| {
            session.read_frame().and_then(|(session, frame)| {
                let connection_tune_or_close = MethodFrame::decode(&frame).unwrap();
                let tune: AMQPResult<protocol::connection::Tune> = match connection_tune_or_close.method_name() {
                    "connection.tune" => protocol::Method::decode(connection_tune_or_close),
                    "connection.close" => {
                        let close_frame: AMQPResult<protocol::connection::Close> = protocol::Method::decode(connection_tune_or_close);
                        Err(AMQPError::Protocol(format!("Connection was closed: {:?}", close_frame)))
                    }
                    response_method => Err(AMQPError::Protocol(format!("Unexpected response: {}", response_method)))
                };
                finished(session).join(done(tune))
            }).and_then(|(mut session, tune)|{
                debug!("Received tune request: {:?}", tune);
                println!("Received tune request: {:?}", tune);
                session.channel_max_limit = negotiate(tune.channel_max, session.channel_max_limit);
                session.frame_max_limit = negotiate(tune.frame_max, session.frame_max_limit);
                let tune_ok = protocol::connection::TuneOk {
                    channel_max: session.channel_max_limit,
                    frame_max: session.frame_max_limit,
                    heartbeat: 0,
                };
                debug!("Sending connection.tune-ok: {:?}", tune_ok);
                session.write_frame(tune_ok.to_frame(0).unwrap())
            })
        });

        let session = session.and_then(move |session|{
            let open = protocol::connection::Open {
                virtual_host: percent_decode(&vhost),
                capabilities: "".to_owned(),
                insist: false,
            };
            debug!("Sending connection.open: {:?}", open);
            session.write_frame(open.to_frame(0).unwrap()).and_then(|session|{
                session.read_frame().and_then(|(session, frame)|{
                    let open_ok_or_close = MethodFrame::decode(&frame).unwrap();
                    let frame_result = match open_ok_or_close.method_name() {
                        "connection.open-ok" => {
                            let open_ok_frame: AMQPResult<protocol::connection::OpenOk> = protocol::Method::decode(open_ok_or_close);
                            debug!("Connection initialized. conneciton.open-ok recieved: {:?}", open_ok_frame);
                            info!("Session initialized");
                            Ok(())
                        },
                        something_else => {
                            error!("Unexpected method received. Expected connection.open-ok, received: {:?}.", something_else);
                            Err(AMQPError::Protocol("Unexpected frame".to_string()))
                        }
                    };
                    finished(session).join(done(frame_result))
                }).map(|(session, _)| session)
            })
        });

        session.boxed()
    }

    pub fn close(self) -> BoxFuture<(), AMQPError> {
        let close = protocol::connection::Close {
                reply_code: 200,
                reply_text: "Bye".to_string(),
                class_id: 0,
                method_id: 0
        };
        self.write_frame(close.to_frame(0).unwrap()).map(|session| drop(session)).boxed()
    }
}

 fn read_frame(reader: TaskIoRead<TcpStream>) -> BoxFuture<(TaskIoRead<TcpStream>, Frame), AMQPError> {
    let header = [0u8; 7];
    let frame_header = read_exact(reader, header).map(|(reader, header)|{
        let header = &mut &header[..];
        let frame_type_id = header.read_u8().unwrap();
        let channel = header.read_u16::<BigEndian>().unwrap();
        let payload_size = header.read_u32::<BigEndian>().unwrap() as usize;
        (reader, (frame_type_id, channel, payload_size))
    });
    let header_and_payload = frame_header.and_then(|(reader, header)|{
        let payload_buf = vec![0u8; header.2+1];
        finished(header).join(read_exact(reader, payload_buf))
    });

    let header_and_payload = header_and_payload.and_then(|(header, (reader, mut payload))|{
        done(match payload[payload.len()-1] {
            0xCE => {
                let payload_len = payload.len()-1;
                payload.truncate(payload_len);
                Ok((reader, header, payload))
            },
            _ => Err(io::Error::new(io::ErrorKind::Other, "Frame end error"))
        })
    });
    header_and_payload.and_then(|(reader, header, payload)|{
        done(match FrameType::from_u8(header.0) {
            Some(ft) => Ok((reader, Frame {
                frame_type: ft,
                channel: header.1,
                payload: payload
            })),
            None => Err(io::Error::new(io::ErrorKind::Other, "Unknown Frame Type"))
        })
    }).map_err(From::from).boxed()
}

#[inline]
fn write_frame(writer: TaskIoWrite<TcpStream>, frame: Frame) -> BoxFuture<TaskIoWrite<TcpStream>, AMQPError> {
    write_all(writer, frame.encode().unwrap()).map(|(writer, _)| writer).map_err(From::from).boxed()
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
    println!("Resolving {}:{}", host, port);
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
        assert!(match options.scheme { AMQPScheme::AMQP => true, _ => false });
    }

    #[test]
    fn test_full_parse_url_without_vhost() {
        let options = parse_url("amqp://host").expect("Failed parsing url");
        assert_eq!(options.host, "host");
        assert_eq!(options.vhost, "");
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
