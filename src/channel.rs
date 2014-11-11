use std::io::{IoResult, IoError, EndOfFile};
use std::comm::{Sender, Receiver};

use framing;
use framing::{ContentHeaderFrame, Frame};
use table::Table;
use protocol;
use protocol::{MethodFrame, channel, basic};
use protocol::basic::BasicProperties;

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

    pub fn read(&self) -> Frame {
        self.chan.ref1().recv()
    }

    fn write(&self, frame: Frame) {
        self.chan.ref0().send(frame)
    }

    pub fn send_method_frame<T>(&self, method: &T)  where T: protocol::Method {
        debug!("Sending method {} to channel {}", method.name(), self.id);
        self.write(Frame {frame_type: framing::METHOD, channel: self.id, payload: MethodFrame::encode_method(method) })
    }

    pub fn rpc<T, U>(&self, method: &U, expected_reply: &str) -> IoResult<T> where T: protocol::Method, U: protocol::Method {
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

    pub fn read_headers(&self) -> IoResult<ContentHeaderFrame> {
        ContentHeaderFrame::decode(self.read())
    }

    pub fn read_body(&self, size: u64) -> IoResult<Vec<u8>> {
        let mut body = Vec::with_capacity(size as uint);
        while body.len() < size as uint {
            body.extend(self.read().payload.into_iter())
        }
        Ok(body)
    }

    pub fn basic_publish(&self, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
                         properties: BasicProperties, content: Vec<u8>) {
        let publish = &protocol::basic::Publish {
            ticket: 0, exchange: exchange.to_string(),
            routing_key: routing_key.to_string(), mandatory: mandatory, immediate: immediate};
        let properties_flags = properties.flags();
        let content_header = ContentHeaderFrame { content_class: 60, weight: 0, body_size: content.len() as u64,
            properties_flags: properties_flags, properties: properties.encode() };
        let content_header_frame = framing::Frame {frame_type: framing::HEADERS, channel: self.id,
            payload: content_header.encode() };
        let content_frame = framing::Frame { frame_type: framing::BODY, channel: self.id, payload: content};

        self.send_method_frame(publish);
        self.write(content_header_frame);
        self.write(content_frame);
    }

    pub fn basic_get(&self, queue: &str, no_ack: bool) -> IoResult<(BasicProperties, Vec<u8>, basic::GetOk)> {
        let get = &basic::Get{ ticket: 0, queue: queue.to_string(), no_ack: no_ack };
        let method_frame = self.raw_rpc(get);
        match method_frame.method_name() {
            "basic.get-ok" => {
                let reply: basic::GetOk = try!(protocol::Method::decode(method_frame));
                let headers = try!(self.read_headers());
                let body = try!(self.read_body(headers.body_size));
                let properties = try!(BasicProperties::decode(headers));
                Ok((properties, body, reply))
            }
            "basic.get-empty" => return Err(IoError{kind: EndOfFile, desc: "The queue is empty", detail: None}),
            method => panic!(format!("Not expected method: {}", method))
        }
    }

    pub fn basic_ack(&self, delivery_tag: u64, multiple: bool) {
        self.send_method_frame(&protocol::basic::Ack{delivery_tag: delivery_tag, multiple: multiple});
    }

    // Rabbitmq specific
    pub fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) {
        self.send_method_frame(&protocol::basic::Nack{delivery_tag: delivery_tag, multiple: multiple, requeue: requeue});
    }

    pub fn basic_reject(&self, delivery_tag: u64, requeue: bool) {
        self.send_method_frame(&protocol::basic::Reject{delivery_tag: delivery_tag, requeue: requeue});
    }

    pub fn exchange_declare(&self, exchange: &str, _type: &str, passive: bool, durable: bool,
        auto_delete: bool, internal: bool, nowait: bool, arguments: Table) -> IoResult<protocol::exchange::DeclareOk> {
        let declare = protocol::exchange::Declare {
            ticket: 0, exchange: exchange.to_string(), _type: _type.to_string(), passive: passive, durable: durable,
            auto_delete: auto_delete, internal: internal, nowait: nowait, arguments: arguments
        };
        self.rpc(&declare,"exchange.declare-ok")
    }

    pub fn queue_declare(&self, queue: &str, passive: bool, durable: bool, exclusive: bool,
        auto_delete: bool, nowait: bool, arguments: Table) -> IoResult<protocol::queue::DeclareOk> {
        let declare = protocol::queue::Declare {
            ticket: 0, queue: queue.to_string(), passive: passive, durable: durable, exclusive: exclusive,
            auto_delete: auto_delete, nowait: nowait, arguments: arguments
        };
        self.rpc(&declare, "queue.declare-ok")
    }

    pub fn queue_bind(&self, queue: &str, exchange: &str, routing_key: &str, nowait: bool, arguments: Table) -> IoResult<protocol::queue::BindOk> {
        let bind = protocol::queue::Bind {
            ticket: 0, queue: queue.to_string(), exchange: exchange.to_string(),
            routing_key: routing_key.to_string(), nowait: nowait, arguments: arguments
        };
        self.rpc(&bind, "queue.bind-ok")
    }
}
