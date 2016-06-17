use channel::Channel;
use channel::Consumer;
use table::Table;
use protocol::{basic, Method};
use protocol::basic::{BasicProperties, GetOk, QosOk, CancelOk};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use amqp_error::{AMQPResult, AMQPError};

pub enum AckAction {
    Ack(u64),
    Nack(u64, bool),
    Reject(u64, bool),
}

pub struct GetIterator<'a> {
    queue: &'a str,
    no_ack: bool,
    channel: &'a mut Channel,
    ack_receiver: Receiver<AckAction>,
    ack_sender: SyncSender<AckAction>,
}

pub trait Basic<'a> {
    fn basic_get(&'a mut self, queue: &'a str, no_ack: bool) -> GetIterator<'a>;
    fn basic_consume<T, S>(&mut self,
                           callback: T,
                           queue: S,
                           consumer_tag: S,
                           no_local: bool,
                           no_ack: bool,
                           exclusive: bool,
                           nowait: bool,
                           arguments: Table)
                           -> AMQPResult<String>
        where T: Consumer + 'static,
              S: Into<String>;
    fn basic_publish<S>(&mut self,
                        exchange: S,
                        routing_key: S,
                        mandatory: bool,
                        immediate: bool,
                        properties: BasicProperties,
                        content: Vec<u8>)
                        -> AMQPResult<()>
        where S: Into<String>;
    fn basic_ack(&mut self, delivery_tag: u64, multiple: bool) -> AMQPResult<()>;
    fn basic_nack(&mut self, delivery_tag: u64, multiple: bool, requeue: bool) -> AMQPResult<()>;
    fn basic_reject(&mut self, delivery_tag: u64, requeue: bool) -> AMQPResult<()>;
    fn basic_prefetch(&mut self, prefetch_count: u16) -> AMQPResult<QosOk>;
    fn basic_qos(&mut self,
                 prefetch_size: u32,
                 prefetch_count: u16,
                 global: bool)
                 -> AMQPResult<QosOk>;
    fn basic_cancel(&mut self, consumer_tag: String, no_wait: bool) -> AMQPResult<CancelOk>;
}

// #[derive(Debug)]
pub struct GetResult {
    pub reply: GetOk,
    pub headers: BasicProperties,
    pub body: Vec<u8>,
    ack_sender: SyncSender<AckAction>,
}

impl<'a> Iterator for GetIterator<'a> {
    type Item = GetResult;

    #[allow(unused_must_use)]
    fn next(&mut self) -> Option<Self::Item> {
        self.ack_message();
        let get = &basic::Get {
            ticket: 0,
            queue: self.queue.to_owned(),
            no_ack: self.no_ack,
        };
        let method_frame_result = self.channel.raw_rpc(get);
        let method_frame = match method_frame_result {
            Ok(m) => m,
            Err(_) => {
                return None;
            }
        };
        match method_frame.method_name() {
            "basic.get-ok" => {
                let reply: basic::GetOk = Method::decode(method_frame).ok().unwrap();
                let headers = self.channel.read_headers().ok().unwrap();
                let body = self.channel.read_body(headers.body_size).ok().unwrap();
                let properties = BasicProperties::decode(headers).ok().unwrap();
                Some(GetResult {
                    headers: properties,
                    reply: reply,
                    body: body,
                    ack_sender: self.ack_sender.clone(),
                })
            }
            "basic.get-empty" => None,
            "basic.return" => None, // TODO: Handle different return methods differently
            method => {
                debug!("Unexpected method: {}", method);
                None
            }
        }
    }
}

/// Will acknowledge

impl<'a> Drop for GetIterator<'a> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.ack_message();
    }
}

impl<'a> GetIterator<'a> {
    pub fn new(channel: &'a mut Channel, queue: &'a str, no_ack: bool) -> Self {
        let (tx, rx) = sync_channel::<AckAction>(1);
        GetIterator {
            channel: channel,
            queue: queue,
            no_ack: no_ack,
            ack_receiver: rx,
            ack_sender: tx,
        }
    }
    fn ack_message(&mut self) -> AMQPResult<()> {
        match self.ack_receiver.try_recv() {
            Ok(ack_action) => {
                match ack_action {
                    AckAction::Ack(delivery_tag) => {
                        try!(self.channel.basic_ack(delivery_tag, false))
                    }
                    AckAction::Nack(delivery_tag, requeue) => {
                        try!(self.channel.basic_nack(delivery_tag, false, requeue))
                    }
                    AckAction::Reject(delivery_tag, requeue) => {
                        try!(self.channel.basic_reject(delivery_tag, requeue))
                    }
                }
            }
            Err(_) => return Err(AMQPError::QueueEmpty),
        }
        Ok(())
    }
}

impl GetResult {
    pub fn ack(&self) {
        self.ack_sender.try_send(AckAction::Ack(self.reply.delivery_tag)).unwrap_or(());
    }
    pub fn nack(&self, requeue: bool) {
        self.ack_sender.try_send(AckAction::Nack(self.reply.delivery_tag, requeue)).unwrap_or(());
    }
    pub fn reject(&self, requeue: bool) {
        self.ack_sender.try_send(AckAction::Reject(self.reply.delivery_tag, requeue)).unwrap_or(());
    }
}
