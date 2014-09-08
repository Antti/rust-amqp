use channel;
use connection::Connection;
use protocol;
use table;
use table::{FieldTable, Bool, LongString};
use framing::{MethodFrame, Frame, BODY};

use std::sync::{Arc, Mutex};
use std::io::IoResult;
use std::cmp;
use std::default::Default;
use std::collections::hashmap::HashMap;
use std::comm::Receiver;
// use url::Url;


pub struct Options <'a>  {
    pub host: &'a str,
    pub port: u16,
    pub login: &'a str,
    pub password: &'a str,
    pub vhost: &'a str,
    pub frame_max_limit: u32,
    pub channel_max_limit: u16,
    pub locale: &'a str
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
	connection: Connection,
	channels: Arc<Mutex<HashMap<u16, Sender<Frame>> >>,
	channel_max_limit: u16,
	channel_zero: channel::Channel,
    sender: Sender<Frame>
}

impl Session {
    // pub fn from_url(url_string: &str) -> IoResult<Session> {
    //     let url = Url::parse(url_string).unwrap();
    //     let vhost = url.serialize_path().expect("vhost not given");
    //     let vhost = vhost.as_slice();
    //     let opts = Options { host: url.domain().expect("host not given"), port: url.port().expect("port not given"),
    //      login: url.username().expect("username not given"), password: url.password().expect("password not given"),
    //      vhost: vhost, ..Default::default()};
    //     Session::new(opts)
    // }

    pub fn new(options: Options) -> IoResult<Session> {
    	let connection = try!(Connection::open(options.host, options.port));
        let (channel_sender, channel_receiver) = channel(); //channel0
        let (session_sender, session_receiver) = channel(); //session sender & receiver
        let channel_zero = channel::Channel::new(0, (session_sender.clone(), channel_receiver));
    	let mut session = Session {
			connection: connection,
			channels: Arc::new(Mutex::new(HashMap::new())),
			channel_max_limit: 0,
			channel_zero: channel_zero,
            sender: session_sender
    	};
        session.channels.lock().insert(0, channel_sender);
        let con1 = session.connection.clone();
        let con2 = session.connection.clone();
        let channels_clone = session.channels.clone();
        spawn( proc(){ Session::reading_loop(con1, channels_clone ) });
        spawn( proc(){ Session::writing_loop(con2, session_receiver ) });

    	try!(session.init(options))
    	Ok(session)
    }

    fn init(&mut self, options: Options) -> IoResult<()> {
	    let frame = self.channel_zero.read(); //Start
        let method_frame = MethodFrame::decode(frame);
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
        let tune : protocol::connection::Tune = try!(self.channel_zero.rpc(&start_ok, "connection.tune"));

        self.channel_max_limit =  negotiate(tune.channel_max, self.channel_max_limit);
    	self.connection.frame_max_limit = negotiate(tune.frame_max, options.frame_max_limit);
    	let frame_max_limit = self.connection.frame_max_limit;
        let tune_ok = protocol::connection::TuneOk {
            channel_max: self.channel_max_limit,
            frame_max: frame_max_limit, heartbeat: 0};
        self.channel_zero.send_method_frame(&tune_ok);

        let open = protocol::connection::Open{virtual_host: options.vhost.to_string(), capabilities: "".to_string(), insist: false };
        let open_ok : protocol::connection::OpenOk = try!(self.channel_zero.rpc(&open, "connection.open-ok"));
        Ok(())
    }

	pub fn open_channel(&mut self, channel_id: u16) -> IoResult<channel::Channel> {
        let (sender, receiver) = channel();
        let channel = channel::Channel::new(channel_id, (self.sender.clone(), receiver));
        self.channels.lock().insert(channel_id, sender);
        try!(channel.open());
        Ok(channel)
    }

    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        let close = protocol::connection::Close {reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        let close_ok : protocol::connection::CloseOk = self.channel_zero.rpc(&close, "connection.close-ok").unwrap();
        self.connection.close();
    }

    pub fn reading_loop(mut connection: Connection, channels: Arc<Mutex<HashMap<u16, Sender<Frame>>>>) -> () {
        loop {
            let frame = match connection.read() {
                Ok(frame) => frame,
                Err(some_err) => {println!("Error in reading loop: {}", some_err); break} //Notify session somehow. It should stop now.
            };
            let chans = channels.lock();
            let ref target_channel = (*chans)[frame.channel];
            target_channel.send(frame);
            // match frame.frame_type {
            //     framing::METHOD => {},
            //     framing::HEADERS => {},
            //     framing::BODY => {},
            //     framing::HEARTBEAT => {}
            // }
            // Handle heartbeats
            // Dispatch frame to the given channel.
        }
    }

    pub fn writing_loop(mut connection: Connection, receiver: Receiver<Frame>) {
        loop {
            let res = receiver.recv_opt();
            match res {
                Ok(frame) => {
                    match frame.frame_type {
                        BODY => {
                            //TODO: Check if need to include frame header + end octet into calculation. (9 bytes extra)
                            for content_frame in split_content_into_frames(frame.payload, 13107).move_iter() {
                                connection.write(Frame { frame_type: frame.frame_type, channel: frame.channel, payload: content_frame}).unwrap();
                            }
                        },
                        _ => {connection.write(frame).unwrap();}
                    }
                },
                Err(_) => break //Notify session somehow... (but it's probably dead already)
            }
        }
    }
}

fn negotiate<T : cmp::Ord>(their_value: T, our_value: T) -> T {
    cmp::min(their_value, our_value)
}

fn split_content_into_frames(content: Vec<u8>, frame_limit: uint) -> Vec<Vec<u8>> {
    assert!(frame_limit > 0, "Can't have frame_max_limit == 0");
    let mut content_frames = vec!();
    let mut current_pos = 0;
    while current_pos < content.len() {
        let new_pos = current_pos + cmp::min(content.len() - current_pos, frame_limit);
        content_frames.push(content.slice(current_pos, new_pos).into_vec());
        current_pos = new_pos;
    }
    content_frames
}


#[test]
fn test_split_content_into_frames() {
    let content = vec!(1,2,3,4,5,6,7,8,9,10);
    let frames = split_content_into_frames(content, 3);
    assert_eq!(frames, vec!(vec!(1, 2, 3), vec!(4, 5, 6), vec!(7, 8, 9), vec!(10)));
}
