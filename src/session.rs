use channel;
use connection::Connection;
use protocol::{self, MethodFrame};
use table;
use table::TableEntry::{FieldTable, Bool, LongString};
use framing::{Frame, FrameType};
use amqp_error::{AMQPResult, AMQPError};

use std::sync::{Arc, Mutex};
use std::cmp;
use std::default::Default;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{JoinGuard, Thread};

use url::{UrlParser, SchemeType};

const CHANNEL_BUFFER_SIZE :uint = 100;


#[derive(Show)]
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
	channels: Arc<Mutex<HashMap<u16, SyncSender<Frame>> >>,
	channel_max_limit: u16,
	channel_zero: channel::Channel,
    sender: SyncSender<Frame>,
    #[allow(dead_code)]
    reading_loop: JoinGuard<()>,
    #[allow(dead_code)]
    writing_loop: JoinGuard<()>
}

impl Session {
    /// Use `open_url` to create new amqp session from a "amqp url"
    ///
    /// # Arguments
    /// * `url_string`: The format is: `amqp://username:password@host:port/virtual_host`
    ///
    /// Most of the params have their default, so you can just pass this:
    /// `"amqp://localhost/"` and it will connect to rabbitmq server, running on `localhost` on port `65535`,
    /// with login `"guest"`, password: `"guest"` to vhost `"/"`
    pub fn open_url(url_string: &str) -> AMQPResult<Session> {
        let default: Options = Default::default();
        let mut url_parser = UrlParser::new();
        url_parser.scheme_type_mapper(scheme_type_mapper);
        let url = url_parser.parse(url_string).unwrap();
        let vhost = url.serialize_path().unwrap_or(default.vhost.to_string());
        let host  = url.domain().unwrap_or(default.host);
        let port = url.port().unwrap_or(default.port);
        let login = url.username().and_then(|username| match username.as_slice(){ "" => None, _ => Some(username)} ).unwrap_or(default.login);
        let password = url.password().unwrap_or(default.password);
        let opts = Options { host: host, port: port,
         login: login, password: password,
         vhost: vhost.as_slice(), ..Default::default()};
        Session::new(opts)
    }

    /// Initialize new rabbitmq session.
    /// You can use default options:
    /// # Example
    /// ```no_run
    /// use std::default::Default;
    /// use amqp::session::{Options, Session};
    /// let session = match Session::new(Options { .. Default::default() }){
    ///     Ok(session) => session,
    ///     Err(error) => panic!("Failed openning an amqp session: {}", error)
    /// };
    /// ```
    pub fn new(options: Options) -> AMQPResult<Session> {
    	let connection = try!(Connection::open(options.host, options.port));
        let (channel_sender, channel_receiver) = sync_channel(CHANNEL_BUFFER_SIZE); //channel0
        let (session_sender, session_receiver) = sync_channel(CHANNEL_BUFFER_SIZE); //session sender & receiver
        let channels = Arc::new(Mutex::new(HashMap::new()));
        let channel_zero = channel::Channel::new(0, (session_sender.clone(), channel_receiver));
        try!(channels.lock().map_err(|_| AMQPError::SyncError)).insert(0, channel_sender);
        let con1 = connection.clone();
        let con2 = connection.clone();
        let channels_clone = channels.clone();
        let reading_loop = Thread::spawn( move || Session::reading_loop(con1, channels_clone ) );
        let writing_loop = Thread::spawn( move || Session::writing_loop(con2, session_receiver ) );
        let mut session = Session {
            connection: connection,
            channels: channels,
            channel_max_limit: 0,
            channel_zero: channel_zero,
            sender: session_sender,
            reading_loop: reading_loop,
            writing_loop: writing_loop
        };
        try!(session.init(options));
    	Ok(session)
    }

    fn init(&mut self, options: Options) -> AMQPResult<()> {
        debug!("Starting init session");
	    let frame = self.channel_zero.read(); //Start
        let method_frame = MethodFrame::decode(frame);
        let start : protocol::connection::Start = match method_frame.method_name(){
            "connection.start" => protocol::Method::decode(method_frame).unwrap(),
            meth => panic!("Unexpected method frame: {}", meth) //In reality you would probably skip the frame and try to read another?
        };
        debug!("Received connection.start: {}", start);
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

        debug!("Sending connection.start-ok");
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
        let _ : protocol::connection::OpenOk = try!(self.channel_zero.rpc(&open, "connection.open-ok"));
        Ok(())
    }

    /// `open_channel` will open a new amqp channel:
    /// # Arguments
    ///
    /// * `channel_id` - channel number
    ///
    /// # Exmaple
    /// ```no_run
    /// use std::default::Default;
    /// use amqp::session::{Options, Session};
    /// let mut session =  Session::new(Options { .. Default::default() }).unwrap();
    /// let channel = match session.open_channel(1){
    ///     Ok(channel) => channel,
    ///     Err(error) => panic!("Failed openning channel: {}", error)
    /// };
    /// ```
	pub fn open_channel(&mut self, channel_id: u16) -> AMQPResult<channel::Channel> {
        debug!("Openning channel: {}", channel_id);
        let (sender, receiver) = sync_channel(CHANNEL_BUFFER_SIZE);
        let channel = channel::Channel::new(channel_id, (self.sender.clone(), receiver));
        try!(self.channels.lock().map_err(|_| AMQPError::SyncError)).insert(channel_id, sender);
        try!(channel.open());
        Ok(channel)
    }

    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        debug!("Closing session: reply_code: {}, reply_text: {}", reply_code, reply_text);
        let close = protocol::connection::Close {reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        let _ : protocol::connection::CloseOk = self.channel_zero.rpc(&close, "connection.close-ok").unwrap();
        self.connection.close();
    }

    pub fn reading_loop(mut connection: Connection, channels: Arc<Mutex<HashMap<u16, SyncSender<Frame>>>>) -> () {
        debug!("Starting reading loop");
        loop {
            let frame = match connection.read() {
                Ok(frame) => frame,
                Err(some_err) => {debug!("Error in reading loop: {}", some_err); break} //Notify session somehow. It should stop now.
            };
            let chans = channels.lock().unwrap();
            let ref target_channel = (*chans)[frame.channel];
            target_channel.send(frame).ok().expect("Error sending packet");
            // match frame.frame_type {
            //     framing::METHOD => {},
            //     framing::HEADERS => {},
            //     framing::BODY => {},
            //     framing::HEARTBEAT => {}
            // }
            // Handle heartbeats
        }
        debug!("Exiting reading loop");
    }

    pub fn writing_loop(mut connection: Connection, receiver: Receiver<Frame>) {
        debug!("Starting writing loop");
        loop {
            match receiver.recv() {
                Ok(frame) => {
                    match frame.frame_type {
                        FrameType::BODY => {
                            //TODO: Check if need to include frame header + end octet into calculation. (9 bytes extra)
                            for content_frame in split_content_into_frames(frame.payload, 13107).into_iter() {
                                connection.write(Frame { frame_type: frame.frame_type, channel: frame.channel, payload: content_frame}).unwrap();
                            }
                        },
                        _ => {connection.write(frame).unwrap();}
                    }
                },
                Err(_) => break //Notify session somehow... (but it's probably dead already)
            }
        }
        debug!("Exiting writing loop");
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
        content_frames.push(content.slice(current_pos, new_pos).to_vec());
        current_pos = new_pos;
    }
    content_frames
}

fn scheme_type_mapper(scheme: &str) -> SchemeType {
    match scheme{
        "amqp" => SchemeType::Relative(5672),
        _ => {panic!("Uknown scheme: {}", scheme)}
    }
}

#[test]
fn test_split_content_into_frames() {
    let content = vec!(1,2,3,4,5,6,7,8,9,10);
    let frames = split_content_into_frames(content, 3);
    assert_eq!(frames, vec!(vec!(1, 2, 3), vec!(4, 5, 6), vec!(7, 8, 9), vec!(10)));
}
