use std::io::{IoResult, IoError, EndOfFile};
use std::cmp;
use std::comm::{Sender, Receiver};

use framing;
use framing::{ContentHeaderFrame, MethodFrame, Frame};
use table::Table;
use protocol;
use protocol::channel;
use protocol::basic;

pub struct Channel {
	chan: (Sender<Frame>, Receiver<Frame>),
	pub id: u16
}

impl Channel {
	pub fn new(id: u16, chan: (Sender<Frame>, Receiver<Frame>)) -> Channel {
		Channel{id: id, chan: chan}
	}

	pub fn open(&self) -> IoResult<protocol::channel::OpenOk> {
		let meth = protocol::channel::Open {out_of_band: "".to_string()};
        self.rpc(&meth, "channel.open-ok")
	}
	pub fn close(&self, reply_code: u16, reply_text: &str) {
		let close = &channel::Close {reply_code: reply_code, reply_text: reply_text.to_string(), class_id: 0, method_id: 0};
		let reply: channel::CloseOk = self.rpc(close, "channel.close-ok").unwrap();
	}

	// This should probably read from some frames buffer
	pub fn read(&self) -> Frame {
		self.chan.ref1().recv()
	}

	fn write(&self, frame: Frame) {
		self.chan.ref0().send(frame)
	}

	pub fn send_method_frame(&self, method: &protocol::Method) {
        println!("Sending method {} to channel {}", method.name(), self.id);
        self.write(Frame {frame_type: framing::METHOD, channel: self.id, payload: MethodFrame::encode_method(method) })
    }

    pub fn rpc<T: protocol::Method>(&self, method: &protocol::Method, expected_reply: &str) -> IoResult<T> {
        let method_frame = self.raw_rpc(method);
        match method_frame.method_name() {
            m_name if m_name == expected_reply => protocol::Method::decode(method_frame),
            m_name => fail!("Unexpected method frame: {}, expected: {}", m_name, expected_reply)
        }
    }

    pub fn raw_rpc(&self, method: &protocol::Method) -> MethodFrame {
        self.send_method_frame(method);
        MethodFrame::decode(self.read())
    }

	pub fn read_headers(&self) -> IoResult<ContentHeaderFrame> {
		ContentHeaderFrame::decode(self.read())
	}

	pub fn read_body(&self, size: u64) -> IoResult<Vec<u8>> {
    	let mut body = Vec::with_capacity(size as uint);
		while body.len() < size as uint {
    		body = body.append(self.read().payload.as_slice())
    	}
    	Ok(body)
	}

	pub fn basic_publish(&self, ticket: u16, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
						 properties: basic::BasicProperties, content: Vec<u8>) {
		let publish = &protocol::basic::Publish {
			ticket: ticket, exchange: exchange.to_string(),
			routing_key: routing_key.to_string(), mandatory: mandatory, immediate: immediate};
		let properties_flags = properties.flags();
		let content_header = ContentHeaderFrame { content_class: 60, weight: 0, body_size: content.len() as u64,
			properties_flags: properties_flags, properties: properties.encode() };
		let content_header_frame = framing::Frame {frame_type: framing::HEADERS, channel: self.id,
			payload: content_header.encode() };

		//TODO: Check if need to include frame header + end octet into calculation. (9 bytes extra)
		let content_frames = Channel::split_content_into_frames(content, self.get_frame_max_limit() as uint);
		self.send_method_frame(publish);
		self.write(content_header_frame);

		for content_frame in content_frames.move_iter() {
			self.write(framing::Frame { frame_type: framing::BODY, channel: self.id, payload: content_frame});
		}
	}

	pub fn basic_get(&self, ticket: u16, queue: &str, no_ack: bool) -> IoResult<(protocol::basic::BasicProperties, Vec<u8>)> {
  		let get = &basic::Get{ ticket: ticket, queue: queue.to_string(), no_ack: no_ack };
  		let method_frame = self.raw_rpc(get);
  		match method_frame.method_name() {
  			"basic.get-ok" => {
			    let reply: basic::GetOk = try!(protocol::Method::decode(method_frame));
			    let headers = try!(self.read_headers());
			    let properties = try!(basic::BasicProperties::decode(headers.clone()));
			    let body = try!(self.read_body(headers.body_size));
			    Ok((properties, body))
  			}
  			"basic.get-empty" => return Err(IoError{kind: EndOfFile, desc: "The queue is empty", detail: None}),
  			method => fail!(format!("Not expected method: {}", method))
  		}
	}

	pub fn exchange_declare(&self, ticket: u16, exchange: &str, _type: &str, passive: bool, durable: bool,
		auto_delete: bool, internal: bool, nowait: bool, arguments: Table) -> IoResult<protocol::exchange::DeclareOk> {
		let declare = protocol::exchange::Declare {
			ticket: ticket, exchange: exchange.to_string(), _type: _type.to_string(), passive: passive, durable: durable,
			auto_delete: auto_delete, internal: internal, nowait: nowait, arguments: arguments
		};
		self.rpc(&declare,"exchange.declare-ok")
    }

	pub fn queue_declare(&self, ticket: u16, queue: &str, passive: bool, durable: bool, exclusive: bool,
		auto_delete: bool, nowait: bool, arguments: Table) -> IoResult<protocol::queue::DeclareOk> {
		let declare = protocol::queue::Declare {
			ticket: ticket, queue: queue.to_string(), passive: passive, durable: durable, exclusive: exclusive,
			auto_delete: auto_delete, nowait: nowait, arguments: arguments
		};
		self.rpc(&declare, "queue.declare-ok")
	}

	pub fn queue_bind(&self, ticket: u16, queue: &str, exchange: &str, routing_key: &str, nowait: bool, arguments: Table) -> IoResult<protocol::queue::BindOk> {
		let bind = protocol::queue::Bind {
			ticket: ticket, queue: queue.to_string(), exchange: exchange.to_string(),
			routing_key: routing_key.to_string(), nowait: nowait, arguments: arguments
		};
		self.rpc(&bind, "queue.bind-ok")
	}

	fn split_content_into_frames(content: Vec<u8>, frame_limit: uint) -> Vec<Vec<u8>> {
		let mut content_frames = vec!();
		let mut current_pos = 0;
		while current_pos < content.len() {
			let new_pos = current_pos + cmp::min(content.len() - current_pos, frame_limit);
			content_frames.push(content.slice(current_pos, new_pos).into_vec());
			current_pos = new_pos;
		}
		content_frames
	}

	fn get_frame_max_limit(&self) -> u32 {
		500u32
		// let connection = self.connection.borrow();
		// assert!(connection.frame_max_limit > 0, "Can't have frame_max_limit == 0");
		// connection.frame_max_limit
	}
}


#[test]
fn test_split_content_into_frames() {
	let content = vec!(1,2,3,4,5,6,7,8,9,10);
	let frames = Channel::split_content_into_frames(content, 3);
	assert_eq!(frames, vec!(vec!(1, 2, 3), vec!(4, 5, 6), vec!(7, 8, 9), vec!(10)));
}