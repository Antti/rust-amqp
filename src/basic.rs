use channel::Channel;
use protocol::basic::{BasicProperties};
use protocol::basic::GetOk;

pub struct GetIterator <'a> {
    queue: &'a str,
    no_ack: bool,
    channel: &'a Channel
}

#[deriving(Show)]
pub struct GetResult {
    pub headers: BasicProperties,
    pub reply: GetOk,
    pub body: Vec<u8>
}

impl <'a> Iterator<GetResult> for GetIterator<'a > {
    fn next(&mut self) -> Option<GetResult> {
        match self.channel.basic_get(self.queue, self.no_ack) {
            Ok((basic_properties, body, reply)) => Some(GetResult {headers: basic_properties, reply: reply, body: body}),
            Err(_) => None
        }
    }
}

pub fn get<'a >(channel: &'a Channel, queue: &'a str, no_ack: bool) -> GetIterator<'a> {
    GetIterator { channel: channel, queue: queue, no_ack: no_ack }
}