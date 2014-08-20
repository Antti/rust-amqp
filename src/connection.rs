use std::io::{IoResult};
use std::io::net::tcp::TcpStream;
use std::default::Default;
use std::cmp;
use framing;
use framing::{Frame, Method};
use protocol;
use table::{FieldTable, Table, Bool, ShortShortInt, ShortShortUint, ShortInt, ShortUint, LongInt, LongUint, LongLongInt, LongLongUint, Float, Double, DecimalValue, LongString, FieldArray, Timestamp};
use std::collections::TreeMap;


pub struct Connection {
    socket: TcpStream
}

pub struct Options <'a>  {
    host: &'a str,
    port: u16,
    login: &'a str,
    password: &'a str,
    vhost: &'a str,
    frame_max_limit: u32,
    channel_max_limit: u16,
    locale: &'a str
}

impl <'a>  Default for Options <'a>  {
    fn default() -> Options <'a>  {
        Options {
            host: "127.0.0.1", port: 5672, vhost: "/",
            login: "guest", password: "guest",
            frame_max_limit: 131072, channel_max_limit: 65535,
            locale: "en_US"
        }
    }
}

impl Connection {
    pub fn open(options: Options) -> IoResult<Connection> {
        let mut socket = try!(TcpStream::connect(options.host, options.port));
        try!(socket.write([b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let mut connection = Connection { socket: socket};

        let frame = connection.read(); //Start

        let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
        let start : protocol::connection::Start = framing::Method::decode(arguments);

        let mut client_properties = TreeMap::new();
        let mut capabilities = TreeMap::new();
        capabilities.insert("publisher_confirms".to_string(), Bool(true));
        capabilities.insert("consumer_cancel_notify".to_string(), Bool(true));
        capabilities.insert("exchange_exchange_bindings".to_string(), Bool(true));
        capabilities.insert("basic.nack".to_string(), Bool(true));
        capabilities.insert("connection.blocked".to_string(), Bool(true));
        capabilities.insert("authentication_failure_close".to_string(), Bool(true));
        client_properties.insert("capabilities".to_string(), FieldTable(capabilities));
        client_properties.insert("product".to_string(), LongString("rust-amqp".to_string()));
        client_properties.insert("platform".to_string(), LongString("rust".to_string()));
        client_properties.insert("version".to_string(), LongString("0.1".to_string()));
        client_properties.insert("information".to_string(), LongString("https://github.com/Antti/rust-amqp".to_string()));

        let start_ok = protocol::connection::StartOk {
            client_properties: client_properties, mechanism: "PLAIN".to_string(),
            response: format!("\0{}\0{}", options.login, options.password), locale: options.locale.to_string()};
        try!(connection.send_method_frame(0, &start_ok));

        let frame = connection.read();//Tune
        let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
        let tune : protocol::connection::Tune = framing::Method::decode(arguments);

        let tune_ok = protocol::connection::TuneOk {
            channel_max: negotiate(tune.channel_max, options.channel_max_limit),
            frame_max: negotiate(tune.frame_max, options.frame_max_limit), heartbeat: 0};
        try!(connection.send_method_frame(0, &tune_ok));

        let open = protocol::connection::Open{virtual_host: options.vhost.to_string(), capabilities: "".to_string(), insist: false };
        try!(connection.send_method_frame(0, &open));

        let frame = connection.read();//Open-ok
        let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
        let open_ok : protocol::connection::OpenOk = framing::Method::decode(arguments);

        Ok(connection)

        //  The client opens a TCP/IP connection to the server and sends a protocol header. This is the only data
        // the client sends that is not formatted as a method.
        //  The server responds with its protocol version and other properties, including a list of the security
        // mechanisms that it supports (the Start method).
        //  The client selects a security mechanism (Start-Ok).
        //  The server starts the authentication process, which uses the SASL challenge-response model. It sends
        // the client a challenge (Secure).
        //  The client sends an authentication response (Secure-Ok). For example using the "plain" mechanism,
        // the response consist of a login name and password.
        // Advanced Message Queuing Protocol Specification v0-9-1 Page 19 of 39 Copyright (c) 2006-2008. All rights reserved. See Notice and License. General Architecture
        //  The server repeats the challenge (Secure) or moves to negotiation, sending a set of parameters such as
        // maximum frame size (Tune).
        //  The client accepts or lowers these parameters (Tune-Ok).
        //  The client formally opens the connection and selects a virtual host (Open).
        //  The server confirms that the virtual host is a valid choice (Open-Ok).
        //  The client now uses the connection as desired
    }
    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        let close = protocol::connection::Close{reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        self.send_method_frame(0, &close).unwrap();

        let frame = self.read();//close-ok
        let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
        let close_ok : protocol::connection::CloseOk = framing::Method::decode(arguments);
        self.socket.close_write().unwrap();
        self.socket.close_read().unwrap();
        //TODO: Need to drop socket somehow (Maybe have an Option<Socket>)
    }

    pub fn write(&mut self, frame: Frame) -> IoResult<()>{
        self.socket.write(frame.encode().as_slice())
    }

    pub fn send_method_frame(&mut self, channel: u16, method: &Method)  -> IoResult<()> {
        println!("Sending method {} to channel {}", method.name(), channel);
        self.write(Frame {frame_type: framing::METHOD, channel: channel, payload: framing::encode_method_frame(method) })
    }

    pub fn read(&mut self) -> IoResult<Frame> {
        let frame = Frame::decode(&mut self.socket);
        if frame.is_ok() {
            let unwrapped = frame.clone().unwrap();
            println!("Received frame: type: {}, channel: {}, size: {}", unwrapped.frame_type, unwrapped.channel, unwrapped.payload.len());
        }
        frame
    }
}

fn negotiate<T : cmp::Ord>(their_value: T, our_value: T) -> T {
    cmp::min(their_value, our_value)
}
