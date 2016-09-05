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
use std::cell::RefCell;

use enum_primitive::FromPrimitive;

use futures::{self, Future, BoxFuture, Complete, Poll, Async};
use futures::task::TaskRc;
use tokio_core::io::write_all;
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

pub trait Consumer: Send + 'static {
    fn consume(&mut self, method: protocol::basic::Deliver, headers: protocol::basic::BasicProperties, body: Vec<u8>);
}

pub struct ChannelDispatcher {
    id: u16,
    future_handlers: HashMap<String, Complete<MethodFrame>>,
    consumers: HashMap<String, Box<Consumer>>,
    content_body: Option<Vec<u8>>,
    content_headers: Option<ContentHeaderFrame>,
    content_method: Option<MethodFrame>
}

impl ChannelDispatcher {
    fn new(channel_id: u16) -> Self {
        ChannelDispatcher {
            id: channel_id,
            future_handlers: HashMap::new(),
            content_body: None,
            content_headers: None,
            content_method: None,
            consumers: HashMap::new()
        }
    }

    fn dispatch(&mut self, frame: Frame) {
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
                let ready_to_send = match self.content_body {
                    Some(ref mut content_body) => {
                        content_body.extend_from_slice(&frame.payload);
                        match self.content_headers {
                            Some(ref content_headers) => content_headers.body_size as usize == content_body.len(),
                            None => panic!("Unexpected body frame. Expected headers first")
                        }
                    },
                    None => panic!("Unexpected body frame. Expected headers first")
                };

                if ready_to_send {
                    match mem::replace(&mut self.content_headers, None) {
                        None => {},
                        Some(content_headers) => {
                            let content_method = mem::replace(&mut self.content_method, None).unwrap();
                            let basic_properties = protocol::basic::BasicProperties::decode(content_headers).unwrap();
                            let basic_deliver = protocol::basic::Deliver::decode(content_method).unwrap();

                            match self.consumers.get_mut(&basic_deliver.consumer_tag) {
                                Some(consumer) => {
                                    // TODO: Decide how to execute a consumer. It should not block a loop if it's slow.
                                    // Maybe run it in the separate thread_pool? How to communicate ack/nack/reject then?
                                    consumer.consume(basic_deliver, basic_properties, mem::replace(&mut self.content_body, None).unwrap())
                                },
                                None => error!("No consumer found for tag {}", basic_deliver.consumer_tag)
                            }
                        }
                    }
                }
            },
            FrameType::HEARTBEAT => {
                debug!("Received heartbeat.")
            }
        }
    }

    fn register_sync_future(&mut self, method_name: String, complete: Complete<MethodFrame>) {
        self.future_handlers.insert(method_name, complete);
    }

    fn register_consumer(&mut self, consumer_tag: String, consumer: Box<Consumer>) {
        self.consumers.insert(consumer_tag, consumer);
    }
}

pub type SyncMethodFutureResponse<T> = BoxFuture<(Channel, T), AMQPError>;

pub struct Channel {
    pub id: u16,
    session: SessionRef
}

impl Channel {
    fn new(session: SessionRef, channel_id: u16) -> Self {
        Channel { id: channel_id, session: session }
    }

    fn open_channel(self) -> SyncMethodFutureResponse<protocol::channel::OpenOk> {
        let open_channel = protocol::channel::Open::with_default_values();
        self.write_sync_method(open_channel)
    }

    pub fn close_channel(self) -> SyncMethodFutureResponse<protocol::channel::CloseOk> {
        let close_channel = protocol::channel::Close {
            reply_code: 200,
            reply_text: "Closing channel".into(),
            class_id: 0,
            method_id: 0,
        };
        // TODO: Make sure to remove channel_dispatcher from the session, also return just CloseOK
        self.write_sync_method(close_channel)
    }

    pub fn consume<S, C>(self, queue: S, consumer: C) -> SyncMethodFutureResponse<protocol::basic::ConsumeOk> where S: Into<String>, C: Consumer {
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
        self.write_sync_method::<_, protocol::basic::ConsumeOk>(consume).map(move |(channel, consume_ok)| {
            let consumer_tag = consume_ok.consumer_tag.clone();
            channel.session.with(|session|{
                let mut session = session.borrow_mut();
                session.register_consumer(&channel.id, consumer_tag, Box::new(consumer))
            });
            (channel, consume_ok)
        }).boxed()
    }

    pub fn basic_qos(self,
                 prefetch_size: u32,
                 prefetch_count: u16,
                 global: bool)
                 -> SyncMethodFutureResponse<protocol::basic::QosOk> {
        let qos = protocol::basic::Qos {
            prefetch_size: prefetch_size,
            prefetch_count: prefetch_count,
            global: global,
        };
        self.write_sync_method(qos)
    }


    fn write_sync_method<T, U>(self, method: T) -> SyncMethodFutureResponse<U> where T: Method, U: Method + Send + 'static {
        let (tx, rx) = futures::oneshot();
        self.session.with(|session|{
            let mut session = session.borrow_mut();
            session.write_frame_to_buf(&method.to_frame(self.id).unwrap()); // TODO: return failing future if write failed
            session.register_sync_future(&self.id, U::name().to_owned(), tx);
        });

        let session_clone = self.session.clone();

        let oneshot = rx.map_err(|_| AMQPError::Protocol("Oneshot was cancelled".to_owned())).and_then(|method_frame|{
            Method::decode(method_frame)
        }).map(|response| (self, response));

        SyncMethodFuture { oneshot: oneshot, session: session_clone }.boxed()
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
                                capabilities.insert("connection.blocked".to_owned(), Bool(false));
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
                self.close_session(500, err_msg.clone());
                Err(AMQPError::Protocol(err_msg))
            }
        }
    }

    fn close_session(&mut self, reply_code: u16, reply_text: String) {
        let close = protocol::connection::Close {
            reply_code: reply_code,
            reply_text: reply_text,
            class_id: 0,
            method_id: 0,
        };
        self.write_frame_to_buf(&close.to_frame(0).unwrap());
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
        let mut frames = vec![];
        match self.connection {
            Some(ref mut connection) => {
                match connection.flush_buffers() {
                    Ok(_) => {
                        while connection.frame_read_buf.len() > 0 {
                            match try_parse_frame(&mut connection.frame_read_buf) {
                                Some(frame) => frames.push(frame),
                                None => { break }
                            }
                        }
                    },
                    Err(e) => return Err(e)
                }
            },
            None => panic!("Feature was resolved")
        }
        for frame in frames {
            match self.dispatch_frame(frame) {
                Err(err) => return Err(err),
                _ => {}
            }
        }
        match self.session_inited {
            true => {
                debug!("Session was inited. Resolving future");
                match mem::replace(&mut self.connection, None){
                    Some(connection) => {
                        let Connection { stream, frame_read_buf, frame_write_buf } = connection;
                        Ok(Async::Ready(Session {
                            channel_max_limit: self.options.channel_max_limit,
                            stream: stream,
                            frame_read_buf: frame_read_buf,
                            frame_write_buf: frame_write_buf,
                            frame_max_limit: self.options.frame_max_limit,
                            heartbeat: self.options.heartbeat,
                            channels: HashMap::new()
                        }))
                    },
                    None => panic!("Session is gone")
                }
            },
            false => Ok(Async::NotReady)
        }
    }
}

pub struct SessionRunner {
    session: SessionRef
}

impl Future for SessionRunner {
    type Item = ();
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        debug!("Polling runner");
        self.session.with(|session| {
            let mut session = session.borrow_mut();
            session.read_and_dispatch_all().and_then(|_| session.flush_write_buffer()).map(|_| Async::NotReady)
        })
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

    pub fn flush_buffers(&mut self) -> AMQPResult<()> {
        debug!("Connection::flush_buffers");
        if let Err(err) = self.flush_write_buffer() {
            error!("Error flushing write buffers! {:?}", err);
            return Err(err);
        }
        if let Err(err) = self.fill_read_buffer() {
            error!("Error filling read buffers! {:?}", err);
            return Err(err);
        }
        Ok(())
    }

    pub fn write_slice(&mut self, slice: &[u8]) {
        self.frame_write_buf.write_slice(slice)
    }

    fn flush_write_buffer(&mut self) -> AMQPResult<()> {
        use bytes::WriteExt;

        debug!("Flushing write buffer");
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
                debug!("Read {} bytes. New read buf size: {}", read_len, self.frame_read_buf.len());
                Ok(())
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                debug!("Reading would block");
                Ok(())
            },
            Err(e) => Err(From::from(e))
        }
    }
}

// Drives session until synchronous method was resolved
pub struct SyncMethodFuture<F, T> where F: Future<Item=T, Error=AMQPError> {
    oneshot: F,
    session: SessionRef
}

impl <F, T> Future for SyncMethodFuture<F, T> where F: Future<Item=T, Error=AMQPError> {
    type Item = T;
    type Error = AMQPError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        debug!("SyncMethodFuture polling...");
        let ref mut oneshot = self.oneshot;
        self.session.with(|session| {
            let mut session = session.borrow_mut();
            loop {
                // Maybe our oneshot was already resolved, then just return
                match oneshot.poll() {
                    Ok(Async::NotReady) => {}, // continue
                    other => return other  //resolved or error
                }
                match session.parse_and_dispatch_next_frame() {
                    Ok(Some(_)) => {} // there was a dispatched continue the loop, which will resolve oneshot if the frame was expected
                    Ok(None) => {
                        // buffer did not contain a full frame, read some more
                        match session.fill_read_buffer() {
                            Ok(Async::Ready(_)) => {} // something was read, continue loop
                            Ok(Async::NotReady) => return Ok(Async::NotReady), // would block
                            Err(err) => return Err(err) // pass the error up
                        }
                    },
                    Err(err) => return Err(err) // pass the error up
                }
            }
        })
    }
}

pub struct Client {
    session: SessionRef
}

impl Client {
    pub fn open_url(handle: LoopHandle, url_string: &str) -> BoxFuture<Self, AMQPError> {
        Session::open_url(handle, url_string).map(|session| {
            Client { session: TaskRc::new(RefCell::new(session)) }
        }).boxed()
    }

    // TODO: Should be redone, because it should not use write_sync_method.
    // pub fn close(mut self) -> BoxFuture<protocol::connection::CloseOk, AMQPError> {
    //     let close = protocol::connection::Close {
    //             reply_code: 200,
    //             reply_text: "Bye".to_string(),
    //             class_id: 0,
    //             method_id: 0
    //     };
    //     self.write_sync_method(close, 0).map(|close_ok| { drop(self); close_ok }).boxed()
    // }

    pub fn open_channel(&mut self, channel_id: u16) -> SyncMethodFutureResponse<protocol::channel::OpenOk> {
        let channel_d = ChannelDispatcher::new(channel_id);
        self.session.with(|session| session.borrow_mut().channels.insert(channel_id, channel_d));
        let channel = Channel::new(self.session.clone(), channel_id);
        channel.open_channel()
    }

    pub fn session_runner(self) -> SessionRunner {
        debug!("Creating session runner");
        SessionRunner { session: self.session }
    }
}

type SessionRef = TaskRc<RefCell<Session>>;

/// Session holds the connection.
/// Every synchronous method creates a future, which will be resolved, when the corresponding response is received.
/// Session receives all the frames and "dispatches" them to resolve futures. The future will be resolved if
/// the messages matches future's expectation, that is the channel is correct and the expected message class & method match.
/// The consumer is treated like a stream, so the session drives a stream and for each resolved message it sends ack/reject/nack.

pub struct Session {
    stream: TcpStream,
    frame_read_buf: BlockBuf,
    frame_write_buf: BlockBuf,
    channel_max_limit: u16,
    frame_max_limit: u32,
    heartbeat: u16,
    channels: HashMap<u16, ChannelDispatcher>
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

    fn register_sync_future(&mut self, channel_id: &u16, method_name: String, complete: Complete<MethodFrame>) {
        match self.channels.get_mut(channel_id) {
            Some(channel) => channel.register_sync_future(method_name, complete),
            None => panic!("Channel gone")
        };
    }

    fn register_consumer(&mut self, channel_id: &u16, consumer_tag: String, consumer: Box<Consumer>) {
        match self.channels.get_mut(channel_id) {
            Some(channel) => channel.register_consumer(consumer_tag, consumer),
            None => panic!("Channel gone")
        };
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

    fn write_frame_to_buf(&mut self, frame: &Frame) -> Poll<(), AMQPError> {
        // TODO: Split content frames if they are larger than frame_max_limit
        self.frame_write_buf.write_slice(&frame.encode().unwrap());
        self.flush_write_buffer()
    }

    fn parse_and_dispatch_frames(&mut self) -> AMQPResult<usize>{
        // if !self.frame_read_buf.is_compact() {
        //     self.frame_read_buf.compact();
        // }
        let mut frames_count = 0;
        while self.frame_read_buf.len() > 0 {
            match self.parse_and_dispatch_next_frame() {
                Ok(Some(_)) => { frames_count += 1 },
                Ok(None) => { break },
                Err(err) => return Err(err)
            }
        }
        Ok(frames_count)
    }

    fn parse_and_dispatch_next_frame(&mut self) -> AMQPResult<Option<()>> {
        match try_parse_frame(&mut self.frame_read_buf) {
            Some(frame) => self.dispatch_frame(frame).map(|_| Some(())),
            None => Ok(None)
        }
    }

    // Connection

    fn read_and_dispatch_all(&mut self) -> Poll<(), AMQPError> {
        // There might be something left in the buffer, try to dispatch that before filling in more data
        if let Err(err) = self.parse_and_dispatch_frames() {
            return Err(err)
        }
        loop {
            match self.fill_read_buffer() {
                Ok(Async::Ready(_)) => {
                    if let Err(err) = self.parse_and_dispatch_frames() {
                        return Err(err)
                    }
                },
                Ok(Async::NotReady) => { break },
                Err(err) => return Err(err)
            }
        }
        Ok(Async::NotReady)
    }

    fn flush_write_buffer(&mut self) -> Poll<(), AMQPError> {
        use bytes::WriteExt;

        debug!("Flushing write buffer");
        while self.frame_write_buf.len() > 0{
            debug!("Trying to write write buffer. Write buf size: {}", self.frame_write_buf.len());
            let write_result = self.stream.write_buf(&mut self.frame_write_buf.buf());
            match write_result {
                Ok(write_len) => {
                    self.frame_write_buf.shift(write_len);
                    debug!("Bytes written. New write buf size: {:?}", self.frame_write_buf.len());
                },
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    debug!("Writing would block");
                    return Ok(Async::NotReady)
                },
                Err(e) => return Err(From::from(e))
            }
        }
        Ok(Async::Ready(()))
    }

    fn fill_read_buffer(&mut self) -> Poll<(), AMQPError> {
        use bytes::ReadExt;

        debug!("Trying to append buffer starting from: {}", self.frame_read_buf.len());
        let read_result = self.stream.read_buf(&mut self.frame_read_buf);

        match read_result {
            Ok(read_len) => {
                debug!("Read {} bytes. New read buf size: {}", read_len, self.frame_read_buf.len());
                Ok(Async::Ready(()))
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                debug!("Reading would block");
                Ok(Async::NotReady)
            },
            Err(e) => Err(From::from(e))
        }
    }
}

fn try_parse_frame(buf: &mut BlockBuf) -> Option<Frame> {
    use framing::FrameHeader;

    debug!("Trying to parse frame. Buf size: {}", buf.len());

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
        debug!("Frame parsed. Bytes left in the buffer: {}", buf.len());
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
