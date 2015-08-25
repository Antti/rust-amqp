use amqp_error::{AMQPResult, AMQPError};
use std::sync::mpsc::Receiver;

use framing::{ContentHeaderFrame, Frame, FrameType};
use table::Table;
use protocol;
use protocol::{MethodFrame, channel, basic};
use protocol::basic::BasicProperties;
use connection::Connection;
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

pub trait Consumer : Send {
    fn handle_delivery(&mut self, channel: &mut Channel, method: basic::Deliver, headers: BasicProperties, body: Vec<u8>);
}

pub type ConsumerCallBackFn = fn(channel: &mut Channel, method: basic::Deliver, headers: BasicProperties, body: Vec<u8>);

impl Consumer for ConsumerCallBackFn {
    fn handle_delivery(&mut self, channel: &mut Channel, method: basic::Deliver, headers: BasicProperties, body: Vec<u8>) {
        self(channel, method, headers, body);
    }
}

pub struct Channel {
    pub id: u16,
    pub consumers: Rc<RefCell<HashMap<String, Box<Consumer>>>>,
    receiver: Receiver<AMQPResult<Frame>>,
    pub connection: Connection //TODO: Make private
}

unsafe impl Send for Channel {}

impl Channel {
    pub fn new(id: u16, receiver: Receiver<AMQPResult<Frame>>, connection: Connection) -> Channel {
        Channel{ id: id, receiver: receiver, consumers: Rc::new(RefCell::new(HashMap::new())), connection: connection }
    }

    pub fn open(&mut self) -> AMQPResult<protocol::channel::OpenOk> {
        let meth = protocol::channel::Open {out_of_band: "".to_string()};
        self.rpc(&meth, "channel.open-ok")
    }
    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        let close = &channel::Close {reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        let _: channel::CloseOk = self.rpc(close, "channel.close-ok").ok().expect("Error closing connection");
    }

    pub fn read(&self) -> AMQPResult<Frame> {
        self.receiver.recv().ok().expect("Error reading packet from channel")
    }

    pub fn write(&mut self, frame: Frame) {
        self.connection.write(frame).ok().expect("Error writing packet to connection");
    }

    pub fn send_method_frame<T>(&mut self, method: &T)  where T: protocol::Method {
        debug!("Sending method {} to channel {}", method.name(), self.id);
        let id = self.id;
        self.write(Frame {frame_type: FrameType::METHOD, channel: id, payload: MethodFrame::encode_method(method) })
    }

    pub fn rpc<T, U>(&mut self, method: &U, expected_reply: &str) -> AMQPResult<T> where T: protocol::Method, U: protocol::Method {
        let method_frame = try!(self.raw_rpc(method));
        match method_frame.method_name() {
            m_name if m_name == expected_reply => protocol::Method::decode(method_frame),
            m_name => Err(AMQPError::Protocol(format!("Unexpected method frame: {}, expected: {}", m_name, expected_reply)))
        }
    }

    pub fn raw_rpc<T>(&mut self, method: &T) -> AMQPResult<MethodFrame>  where T: protocol::Method {
        self.send_method_frame(method);
        Ok(MethodFrame::decode(try!(self.read()))) // TODO: Probably decode can fail as well
    }

    pub fn read_headers(&mut self) -> AMQPResult<ContentHeaderFrame> {
        ContentHeaderFrame::decode(try!(self.read()))
    }

    pub fn read_body(&mut self, size: u64) -> AMQPResult<Vec<u8>> {
        let mut body = Vec::with_capacity(size as usize);
        while body.len() < size as usize {
            body.extend(try!(self.read()).payload.into_iter())
        }
        Ok(body)
    }

    pub fn exchange_declare(&mut self, exchange: &str, _type: &str, passive: bool, durable: bool,
        auto_delete: bool, internal: bool, nowait: bool, arguments: Table) -> AMQPResult<protocol::exchange::DeclareOk> {
        let declare = protocol::exchange::Declare {
            ticket: 0, exchange: exchange.to_string(), _type: _type.to_string(), passive: passive, durable: durable,
            auto_delete: auto_delete, internal: internal, nowait: nowait, arguments: arguments
        };
        self.rpc(&declare,"exchange.declare-ok")
    }

    pub fn exchange_bind(&mut self, destination: &str, source: &str,
                         routing_key: &str, arguments: Table) -> AMQPResult<protocol::exchange::BindOk> {
        let bind = protocol::exchange::Bind {
            ticket: 0, destination: destination.to_string(), source: source.to_string(),
            routing_key:routing_key.to_string(), nowait: false, arguments: arguments
        };
        self.rpc(&bind, "exchange.bind-ok")
    }

    pub fn queue_declare(&mut self, queue: &str, passive: bool, durable: bool, exclusive: bool,
        auto_delete: bool, nowait: bool, arguments: Table) -> AMQPResult<protocol::queue::DeclareOk> {
        let declare = protocol::queue::Declare {
            ticket: 0, queue: queue.to_string(), passive: passive, durable: durable, exclusive: exclusive,
            auto_delete: auto_delete, nowait: nowait, arguments: arguments
        };
        self.rpc(&declare, "queue.declare-ok")
    }

    pub fn queue_bind(&mut self, queue: &str, exchange: &str, routing_key: &str, nowait: bool, arguments: Table) -> AMQPResult<protocol::queue::BindOk> {
        let bind = protocol::queue::Bind {
            ticket: 0, queue: queue.to_string(), exchange: exchange.to_string(),
            routing_key: routing_key.to_string(), nowait: nowait, arguments: arguments
        };
        self.rpc(&bind, "queue.bind-ok")
    }
}
