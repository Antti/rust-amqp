use channel;
use connection;
use protocol;
use table;
use table::{FieldTable, Bool, LongString};
use framing::MethodFrame;

use std::cell::RefCell;
use std::rc::Rc;
use std::io::IoResult;
use std::cmp;
use std::default::Default;


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

pub struct Session {
	pub connection: Rc<RefCell<connection::Connection>>,
	channels: Vec<Rc<channel::Channel>>,
	channel_max_limit: u16
}

impl Session {
    pub fn new(options: Options) -> IoResult<Session> {
    	let connection = try!(connection::Connection::open(options.host, options.port));
    	let mut session = Session {
			connection: Rc::new(RefCell::new(connection)),
			channels: vec!(),
			channel_max_limit: 0
    	};
    	try!(session.init(options))
    	Ok(session)
    }

    fn init(&mut self, options: Options) -> IoResult<()> {
    	let mut connection = self.connection.borrow_mut();
    	connection.frame_max_limit = options.frame_max_limit;
    	let frame = connection.read(); //Start
        let method_frame = MethodFrame::decode(frame.unwrap());
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

        let mut client_properties = table::new();
        let mut capabilities = table::new();
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

        self.channel_max_limit =  negotiate(tune.channel_max, self.channel_max_limit);
        connection.frame_max_limit = negotiate(tune.frame_max, connection.frame_max_limit);
        let tune_ok = protocol::connection::TuneOk {
            channel_max: self.channel_max_limit,
            frame_max: connection.frame_max_limit, heartbeat: 0};
        try!(connection.send_method_frame(0, &tune_ok));

        let open = protocol::connection::Open{virtual_host: options.vhost.to_string(), capabilities: "".to_string(), insist: false };
        let open_ok : protocol::connection::OpenOk = try!(connection.rpc(0, &open, "connection.open-ok"));
        Ok(())
    }

	pub fn open_channel(&mut self, channel: u16) -> IoResult<Rc<channel::Channel>>{
        let meth = protocol::channel::Open {out_of_band: "".to_string()};
        let open_ok : protocol::channel::OpenOk = try!(self.connection.borrow_mut().rpc(channel, &meth, "channel.open-ok"));
        let channel = Rc::new(channel::Channel::new(self.connection.clone(), channel));
        self.channels.push(channel.clone());
        Ok(channel)
    }

    pub fn close(&self, reply_code: u16, reply_text: String) {
    	self.connection.borrow_mut().close(reply_code, reply_text)
    }
}

fn negotiate<T : cmp::Ord>(their_value: T, our_value: T) -> T {
    cmp::min(their_value, our_value)
}
