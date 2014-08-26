use std::cell::RefCell;
use std::rc::Rc;
use std::io::{IoResult, IoError, EndOfFile};
use std::cmp;

use connection;
use framing;
use protocol;
use protocol::channel;
use protocol::basic;

pub struct Channel{
	connection: Rc<RefCell<connection::Connection>>,
	pub id: u16
}

impl Channel {
	pub fn new(connection: Rc<RefCell<connection::Connection>>, id: u16) -> Channel{
		Channel{connection: connection, id: id}
	}

	pub fn close(&self, reply_code: u16, reply_text: &str) {
		let close = &channel::Close {reply_code: reply_code, reply_text: reply_text.to_string(), class_id: 0, method_id: 0};
		let reply: channel::CloseOk = self.rpc(close, "channel.close-ok").unwrap();
	}

	pub fn rpc<T: protocol::Method>(&self, method: &protocol::Method, expected_reply: &str) -> IoResult<T> {
		let mut connection = self.connection.borrow_mut();
		connection.rpc(self.id, method, expected_reply)
	}

	pub fn raw_rpc(&self, method: &protocol::Method) -> IoResult<protocol::MethodFrame> {
		let mut connection = self.connection.borrow_mut();
		connection.raw_rpc(self.id, method)
	}

	pub fn read_headers(&self) -> IoResult<protocol::ContentHeaderFrame> {
		let mut connection = self.connection.borrow_mut();
		let frame = try!(connection.read());
		framing::decode_content_header_frame(frame)
	}

	pub fn read_body(&self, size: u64) -> IoResult<Vec<u8>> {
		let mut connection = self.connection.borrow_mut();
    	let mut body = Vec::with_capacity(size as uint);
		while body.len() < size as uint {
    		body = body.append(try!(connection.read()).payload.as_slice())
    	}
    	Ok(body)
	}

	pub fn basic_publish(&self, ticket: u16, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
						 properties: basic::BasicProperties, content: Vec<u8>) {
		let mut connection = self.connection.borrow_mut();
		let publish = &protocol::basic::Publish {
			ticket: ticket, exchange: exchange.to_string(),
			routing_key: routing_key.to_string(), mandatory: mandatory, immediate: immediate};
		let properties_flags = properties.flags();
		let content_header = protocol::ContentHeaderFrame { content_class: 60, weight: 0, body_size: content.len() as u64,
			properties_flags: properties_flags, properties: properties.encode() };
		let content_header_frame = framing::Frame {frame_type: framing::HEADERS, channel: self.id,
			payload: framing::encode_content_header_frame(&content_header) };

		//TODO: Check if need to include frame header + end octet into calculation. (9 bytes extra)
		let content_frames = Channel::split_content_into_frames(content, connection.frame_max_limit as uint);

		connection.send_method_frame(self.id, publish);
		connection.write(content_header_frame);

		for content_frame in content_frames.move_iter() {
			connection.write(framing::Frame { frame_type: framing::BODY, channel: self.id, payload: content_frame});
		}
	}

	pub fn basic_get(&self, ticket: u16, queue: &str, no_ack: bool) -> IoResult<(protocol::basic::BasicProperties, Vec<u8>)> {
  		let get = &basic::Get{ ticket: ticket, queue: queue.to_string(), no_ack: no_ack };
  		let method_frame = try!(self.raw_rpc(get));
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

	// exchangeDeclare
	// queueDeclare
	// queueBind
}


#[test]
fn test_split_content_into_frames() {
	let content = vec!(1,2,3,4,5,6,7,8,9,10);
	let frames = Channel::split_content_into_frames(content, 3);
	assert_eq!(frames, vec!(vec!(1, 2, 3), vec!(4, 5, 6), vec!(7, 8, 9), vec!(10)));
}