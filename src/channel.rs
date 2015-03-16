use amqp_error::AMQPResult;
use std::sync::mpsc::{SyncSender, Receiver};

use framing::{ContentHeaderFrame, Frame, FrameType};
use table::Table;
use protocol;
use protocol::{MethodFrame, channel, basic};
use protocol::basic::BasicProperties;
use connection::Connection;
use std::collections::HashMap;

pub type ConsumerCallback = fn(channel: &mut Channel, method: basic::Deliver, headers: BasicProperties, body: Vec<u8>);

pub struct Channel {
    pub id: u16,
    pub consumers: HashMap<String, ConsumerCallback>,
    receiver: Receiver<Frame>,
    pub connection: Connection //TODO: Make private
}

impl Channel {
    pub fn new(id: u16, receiver: Receiver<Frame>, connection: Connection) -> Channel {
        Channel{id: id, receiver: receiver, consumers: HashMap::new(), connection: connection}
    }

    pub fn open(&mut self) -> AMQPResult<protocol::channel::OpenOk> {
        let meth = protocol::channel::Open {out_of_band: "".to_string()};
        self.rpc(&meth, "channel.open-ok")
    }
    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        let close = &channel::Close {reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        let _: channel::CloseOk = self.rpc(close, "channel.close-ok").ok().unwrap();
    }

    pub fn read(&self) -> Frame {
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
        let method_frame = self.raw_rpc(method);
        match method_frame.method_name() {
            m_name if m_name == expected_reply => protocol::Method::decode(method_frame),
            m_name => panic!("Unexpected method frame: {}, expected: {}", m_name, expected_reply)
        }
    }

    pub fn raw_rpc<T>(&mut self, method: &T) -> MethodFrame  where T: protocol::Method {
        self.send_method_frame(method);
        MethodFrame::decode(self.read())
    }

    pub fn read_headers(&mut self) -> AMQPResult<ContentHeaderFrame> {
        ContentHeaderFrame::decode(self.read())
    }

    pub fn read_body(&mut self, size: u64) -> AMQPResult<Vec<u8>> {
        let mut body = Vec::with_capacity(size as usize);
        while body.len() < size as usize {
            body.extend(self.read().payload.into_iter())
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
