use channel;
use connection;
use connection::Options;
use protocol;
use std::cell::RefCell;
use std::rc::Rc;
use std::io::IoResult;

pub struct Session {
	pub connection: Rc<RefCell<connection::Connection>>,
	channels: Vec<Rc<channel::Channel>>
}

impl Session {
	pub fn open_channel(&mut self, channel: u16) -> IoResult<Rc<channel::Channel>>{
        let meth = protocol::channel::Open {out_of_band: "".to_string()};
        let open_ok : protocol::channel::OpenOk = try!(self.connection.borrow_mut().rpc(channel, &meth, "channel.open-ok"));
        let channel = Rc::new(channel::Channel::new(self.connection.clone(), channel));
        self.channels.push(channel.clone());
        Ok(channel)
    }

    pub fn new(options: Options) -> IoResult<Session> {
    	let connection = try!(connection::Connection::open(options));
    	Ok(
    		Session{
    			connection: Rc::new(RefCell::new(connection)),
    			channels: vec!()
    		}
    	)
    }

    pub fn close(&self, reply_code: u16, reply_text: String) {
    	self.connection.borrow_mut().close(reply_code, reply_text)
    }
}
