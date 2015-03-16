use channel::Channel;
use channel::ConsumerCallback;
use table::Table;
use framing::{ContentHeaderFrame, FrameType, Frame};
use protocol::{MethodFrame, basic, Method};
use protocol::basic::{BasicProperties, GetOk, Consume, ConsumeOk, Deliver, Publish, Ack, Nack, Reject};
use amqp_error::AMQPError;

pub struct GetIterator <'a> {
    queue: &'a str,
    no_ack: bool,
    channel: &'a mut Channel
}

pub trait Basic <'a> {
    fn basic_get(&'a mut self, queue: &'a str, no_ack: bool) -> GetIterator<'a>;
    fn basic_consume(&mut self, callback: ConsumerCallback,
    queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool,
    exclusive: bool, nowait: bool, arguments: Table) -> String;
    fn start_consuming(&mut self);
    fn basic_publish(&mut self, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
                         properties: BasicProperties, content: Vec<u8>);
    fn basic_ack(&mut self, delivery_tag: u64, multiple: bool);
    fn basic_nack(&mut self, delivery_tag: u64, multiple: bool, requeue: bool);
    fn basic_reject(&mut self, delivery_tag: u64, requeue: bool);
}

#[derive(Debug)]
pub struct GetResult {
    pub reply: GetOk,
    pub headers: BasicProperties,
    pub body: Vec<u8>
}

impl <'a> Iterator for GetIterator<'a > {
    type Item = GetResult;

    fn next(&mut self) -> Option<GetResult> {
        let get = &basic::Get{ ticket: 0, queue: self.queue.to_string(), no_ack: self.no_ack };
        let ref mut channel = self.channel;
        let method_frame = channel.raw_rpc(get);
        match method_frame.method_name() {
            "basic.get-ok" => {
                let reply: basic::GetOk = Method::decode(method_frame).ok().unwrap();
                let headers = channel.read_headers().ok().unwrap();
                let body = channel.read_body(headers.body_size).ok().unwrap();
                let properties = BasicProperties::decode(headers).ok().unwrap();
                Some(GetResult {headers: properties, reply: reply, body: body})
            }
            "basic.get-empty" => None,
            method => panic!(format!("Not expected method: {}", method))
        }
    }
}

impl <'a> Basic<'a> for Channel {
    fn basic_get(&'a mut self, queue: &'a str, no_ack: bool) -> GetIterator<'a> {
        GetIterator { channel: self, queue: queue, no_ack: no_ack }
    }

    fn basic_consume(&mut self, callback: ConsumerCallback,
        queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool,
        exclusive: bool, nowait: bool, arguments: Table) -> String {
        let consume = &Consume {
            ticket: 0, queue: queue.to_string(), consumer_tag: consumer_tag.to_string(),
            no_local: no_local, no_ack: no_ack, exclusive: exclusive, nowait: nowait, arguments: arguments
        };
        let reply: ConsumeOk = self.rpc(consume, "basic.consume-ok").ok().unwrap();
        self.consumers.insert(reply.consumer_tag.clone(), callback);
        reply.consumer_tag
    }

    // Will run the infinate loop, which will receive frames on the given channel & call consumers.
    fn start_consuming(&mut self) {
        loop {
          let frame = self.read();
                match frame.frame_type {
                    FrameType::METHOD => {
                        let method_frame = MethodFrame::decode(frame);
                        match method_frame.method_name() {
                            "basic.deliver" => {
                                let deliver_method : Deliver = Method::decode(method_frame).ok().unwrap();
                                let headers = self.read_headers().ok().unwrap();
                                let body = self.read_body(headers.body_size).ok().unwrap();
                                let properties = BasicProperties::decode(headers).ok().unwrap();
                                let consumer = self.consumers.get(&deliver_method.consumer_tag).map(|&x| x);
                                match consumer {
                                    Some(callback) => (callback)(self, deliver_method, properties, body),
                                    None => {error!("Received deliver frame for the unknown consumer: {}", deliver_method.consumer_tag)}
                                };
                            }
                            _ => {} //TODO: Handle other callbacks as well.
                        }
                    }
                    _ => {}
                }
        }
    }


    fn basic_publish(&mut self, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
                         properties: BasicProperties, content: Vec<u8>) {
        let publish = &Publish {
            ticket: 0, exchange: exchange.to_string(),
            routing_key: routing_key.to_string(), mandatory: mandatory, immediate: immediate};
        let properties_flags = properties.flags();
        let content_header = ContentHeaderFrame { content_class: 60, weight: 0, body_size: content.len() as u64,
            properties_flags: properties_flags, properties: properties.encode() };
        let content_header_frame = Frame {frame_type: FrameType::HEADERS, channel: self.id,
            payload: content_header.encode() };
        let content_frame = Frame { frame_type: FrameType::BODY, channel: self.id, payload: content};

        self.send_method_frame(publish);
        self.write(content_header_frame);
        self.write(content_frame);
    }

    fn basic_ack(&mut self, delivery_tag: u64, multiple: bool) {
        self.send_method_frame(&Ack{delivery_tag: delivery_tag, multiple: multiple});
    }

    // Rabbitmq specific
    fn basic_nack(&mut self, delivery_tag: u64, multiple: bool, requeue: bool) {
        self.send_method_frame(&Nack{delivery_tag: delivery_tag, multiple: multiple, requeue: requeue});
    }

    fn basic_reject(&mut self, delivery_tag: u64, requeue: bool) {
        self.send_method_frame(&Reject{delivery_tag: delivery_tag, requeue: requeue});
    }

}

