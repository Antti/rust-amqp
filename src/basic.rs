use channel::Channel;
use channel::ConsumerCallback;
use table::Table;
use framing;
use protocol::{MethodFrame, Method};
use protocol::basic::{BasicProperties, GetOk, Consume, ConsumeOk, Deliver};

pub struct GetIterator <'a> {
    queue: &'a str,
    no_ack: bool,
    channel: &'a Channel
}

#[deriving(Show)]
pub struct GetResult {
    pub reply: GetOk,
    pub headers: BasicProperties,
    pub body: Vec<u8>
}

impl <'a> Iterator<GetResult> for GetIterator<'a > {
    fn next(&mut self) -> Option<GetResult> {
        match self.channel.basic_get(self.queue, self.no_ack) {
            Ok((reply, basic_properties, body)) => Some(GetResult {headers: basic_properties, reply: reply, body: body}),
            Err(_) => None
        }
    }
}

pub fn get<'a >(channel: &'a Channel, queue: &'a str, no_ack: bool) -> GetIterator<'a> {
    GetIterator { channel: channel, queue: queue, no_ack: no_ack }
}


pub fn basic_consume(channel: &mut Channel, callback: ConsumerCallback,
    queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool,
    exclusive: bool, nowait: bool, arguments: Table) -> String{
    let consume = &Consume {
        ticket: 0, queue: queue.to_string(), consumer_tag: consumer_tag.to_string(),
        no_local: no_local, no_ack: no_ack, exclusive: exclusive, nowait: nowait, arguments: arguments
    };
    let reply: ConsumeOk = channel.rpc(consume, "basic.consume-ok").unwrap();
    channel.consumers.insert(reply.consumer_tag.clone(), callback);
    reply.consumer_tag
}

pub fn start_consuming(channel: &Channel) {
    loop {
      let frame = channel.read();
            match frame.frame_type {
                framing::METHOD => {
                    let method_frame = MethodFrame::decode(frame.clone());
                    match method_frame.method_name() {
                        "basic.deliver" => {
                            let deliver_method : Deliver = Method::decode(method_frame).unwrap();
                            let headers = channel.read_headers().unwrap();
                            let body = channel.read_body(headers.body_size).unwrap();
                            let properties = BasicProperties::decode(headers).unwrap();
                            channel.consumers[deliver_method.consumer_tag](channel, deliver_method, properties, body);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
    }
}