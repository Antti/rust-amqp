use amqp_error::AMQPResult;
use std::comm::{SyncSender, Receiver};

use framing::{ContentHeaderFrame, Frame, FrameType};
use table::Table;
use protocol;
use protocol::{MethodFrame, channel, basic};
use protocol::basic::BasicProperties;
use std::collections::HashMap;

pub type ConsumerCallback = fn(channel: &Channel, method: basic::Deliver, headers: BasicProperties, body: Vec<u8>);

pub struct Channel {
    pub id: u16,
    pub consumers: HashMap<String, ConsumerCallback>,
    chan: (SyncSender<Frame>, Receiver<Frame>)
}

impl Channel {
    pub fn new(id: u16, chan: (SyncSender<Frame>, Receiver<Frame>)) -> Channel {
        Channel{id: id, chan: chan, consumers: HashMap::new()}
    }

    pub fn open(&self) -> AMQPResult<protocol::channel::OpenOk> {
        let meth = protocol::channel::Open {out_of_band: "".to_string()};
        self.rpc(&meth, "channel.open-ok")
    }
    pub fn close(&self, reply_code: u16, reply_text: &str) {
        let close = &channel::Close {reply_code: reply_code, reply_text: reply_text.to_string(), class_id: 0, method_id: 0};
        let _: channel::CloseOk = self.rpc(close, "channel.close-ok").unwrap();
    }

    pub fn read(&self) -> Frame {
        self.chan.ref1().recv()
    }

    pub fn write(&self, frame: Frame) {
        self.chan.ref0().send(frame)
    }

    pub fn send_method_frame<T>(&self, method: &T)  where T: protocol::Method {
        debug!("Sending method {} to channel {}", method.name(), self.id);
        self.write(Frame {frame_type: FrameType::METHOD, channel: self.id, payload: MethodFrame::encode_method(method) })
    }

    pub fn rpc<T, U>(&self, method: &U, expected_reply: &str) -> AMQPResult<T> where T: protocol::Method, U: protocol::Method {
        let method_frame = self.raw_rpc(method);
        match method_frame.method_name() {
            m_name if m_name == expected_reply => protocol::Method::decode(method_frame),
            m_name => panic!("Unexpected method frame: {}, expected: {}", m_name, expected_reply)
        }
    }

    pub fn raw_rpc<T>(&self, method: &T) -> MethodFrame  where T: protocol::Method {
        self.send_method_frame(method);
        MethodFrame::decode(self.read())
    }

    pub fn read_headers(&self) -> AMQPResult<ContentHeaderFrame> {
        ContentHeaderFrame::decode(self.read())
    }

    pub fn read_body(&self, size: u64) -> AMQPResult<Vec<u8>> {
        let mut body = Vec::with_capacity(size as uint);
        while body.len() < size as uint {
            body.extend(self.read().payload.into_iter())
        }
        Ok(body)
    }

    pub fn exchange_declare(&self, exchange: &str, _type: &str, passive: bool, durable: bool,
        auto_delete: bool, internal: bool, nowait: bool, arguments: Table) -> AMQPResult<protocol::exchange::DeclareOk> {
        let declare = protocol::exchange::Declare {
            ticket: 0, exchange: exchange.to_string(), _type: _type.to_string(), passive: passive, durable: durable,
            auto_delete: auto_delete, internal: internal, nowait: nowait, arguments: arguments
        };
        self.rpc(&declare,"exchange.declare-ok")
    }

    pub fn queue_declare(&self, queue: &str, passive: bool, durable: bool, exclusive: bool,
        auto_delete: bool, nowait: bool, arguments: Table) -> AMQPResult<protocol::queue::DeclareOk> {
        let declare = protocol::queue::Declare {
            ticket: 0, queue: queue.to_string(), passive: passive, durable: durable, exclusive: exclusive,
            auto_delete: auto_delete, nowait: nowait, arguments: arguments
        };
        self.rpc(&declare, "queue.declare-ok")
    }

    pub fn queue_bind(&self, queue: &str, exchange: &str, routing_key: &str, nowait: bool, arguments: Table) -> AMQPResult<protocol::queue::BindOk> {
        let bind = protocol::queue::Bind {
            ticket: 0, queue: queue.to_string(), exchange: exchange.to_string(),
            routing_key: routing_key.to_string(), nowait: nowait, arguments: arguments
        };
        self.rpc(&bind, "queue.bind-ok")
    }
}
