use std::io::{IoResult};
use std::io::net::tcp::TcpStream;
use std::default::Default;
use std::cmp;
use framing;
use framing::Frame;
use channel;
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
    // pub fn from_url(url_string: &str) -> IoResult<Connection> {
    //     let url = URL::parse(url_string);
    //     let opts = Options { host: url.host, port: url.port, login: url.login, password: url.password, vhost: url.path, ..Options::default()};
    //     Connection::open(opts)
    // }
    pub fn open(options: Options) -> IoResult<Connection> {
        let mut socket = try!(TcpStream::connect(options.host, options.port));
        try!(socket.write([b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let mut connection = Connection { socket: socket};

        let frame = connection.read(); //Start

        let method_frame = framing::decode_method_frame(frame.unwrap());
        let start : protocol::connection::Start = match method_frame.method_name(){
            "connection.start" => protocol::Method::decode(method_frame).unwrap(),
            meth => fail!("Unexpected method frame: {}", meth) //In reality you would probably skip the frame and try to read another?
        };
        //  The client selects a security mechanism (Start-Ok).
        //  The server starts the authentication process, which uses the SASL challenge-response model. It sends
        // the client a challenge (Secure).
        //  The client sends an authentication response (Secure-Ok). For example using the "plain" mechanism,
        // the response consist of a login name and password.
        //  The server repeats the challenge (Secure) or moves to negotiation, sending a set of parameters such as

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
        let tune : protocol::connection::Tune = try!(connection.rpc(0, &start_ok, "connection.tune"));

        let tune_ok = protocol::connection::TuneOk {
            channel_max: negotiate(tune.channel_max, options.channel_max_limit),
            frame_max: negotiate(tune.frame_max, options.frame_max_limit), heartbeat: 0};
        try!(connection.send_method_frame(0, &tune_ok));

        let open = protocol::connection::Open{virtual_host: options.vhost.to_string(), capabilities: "".to_string(), insist: false };
        let open_ok : protocol::connection::OpenOk = try!(connection.rpc(0, &open, "connection.open-ok"));

        Ok(connection)
    }

    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        let close = protocol::connection::Close{reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        let close_ok : protocol::connection::CloseOk = self.rpc(0, &close, "connection.close-ok").unwrap();

        self.socket.close_write().unwrap();
        self.socket.close_read().unwrap();
        //TODO: Need to drop socket somehow (Maybe have an Option<Socket>)
    }

    pub fn send_method_frame(&mut self, channel: u16, method: &protocol::Method)  -> IoResult<()> {
        println!("Sending method {} to channel {}", method.name(), channel);
        self.write(Frame {frame_type: framing::METHOD, channel: channel, payload: framing::encode_method_frame(method) })
    }

    pub fn rpc<T: protocol::Method>(&mut self, channel: u16, method: &protocol::Method, expected_reply: &str) -> IoResult<T> {
        let method_frame = try!(self.raw_rpc(channel, method));
        match method_frame.method_name() {
            m_name if m_name == expected_reply => protocol::Method::decode(method_frame),
            m_name => fail!("Unexpected method frame: {}, expected: {}", m_name, expected_reply)
        }
    }

    pub fn raw_rpc(&mut self, channel: u16, method: &protocol::Method) -> IoResult<protocol::MethodFrame> {
        self.send_method_frame(channel, method);
        self.read().map(|frame| framing::decode_method_frame(frame))
    }

    pub fn write(&mut self, frame: Frame) -> IoResult<()>{
        self.socket.write(frame.encode().as_slice())
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
