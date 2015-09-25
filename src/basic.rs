use channel::Channel;
use channel::Consumer;
use table::Table;
use framing::{ContentHeaderFrame, FrameType, Frame};
use protocol::{MethodFrame, basic, Method};
use protocol::basic::{BasicProperties, GetOk, Consume, ConsumeOk, Deliver, Publish, Ack, Nack, Reject, Qos, QosOk, Cancel, CancelOk};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use amqp_error::{AMQPResult, AMQPError};

enum AckAction {
    Ack(u64),
    Nack(u64, bool),
    Reject(u64, bool)
}

pub struct GetIterator <'a> {
    queue: &'a str,
    no_ack: bool,
    channel: &'a mut Channel,
    ack_receiver: Receiver<AckAction>,
    ack_sender: SyncSender<AckAction>
}

pub trait Basic <'a> {
    fn basic_get(&'a mut self, queue: &'a str, no_ack: bool) -> GetIterator<'a>;
    fn basic_consume<T>(&mut self, callback: T,
    queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool,
    exclusive: bool, nowait: bool, arguments: Table) -> AMQPResult<String> where T: Consumer;
    fn start_consuming(&mut self);
    fn basic_publish(&mut self, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
                         properties: BasicProperties, content: Vec<u8>) -> AMQPResult<()>;
    fn basic_ack(&mut self, delivery_tag: u64, multiple: bool) -> AMQPResult<()>;
    fn basic_nack(&mut self, delivery_tag: u64, multiple: bool, requeue: bool) -> AMQPResult<()>;
    fn basic_reject(&mut self, delivery_tag: u64, requeue: bool) -> AMQPResult<()>;
    fn basic_prefetch(&mut self, prefetch_count: u16) -> AMQPResult<QosOk>;
    fn basic_qos(&mut self, prefetch_size: u32, prefetch_count: u16, global: bool) -> AMQPResult<QosOk>;
    fn basic_cancel(&mut self, consumer_tag: String, no_wait: bool) -> AMQPResult<CancelOk>;
}

// #[derive(Debug)]
pub struct GetResult {
    pub reply: GetOk,
    pub headers: BasicProperties,
    pub body: Vec<u8>,
    ack_sender: SyncSender<AckAction>
}

impl <'a> Iterator for GetIterator<'a > {
    type Item = GetResult;

    #[allow(unused_must_use)]
    fn next(&mut self) -> Option<Self::Item> {
        self.ack_message();
        let get = &basic::Get{ ticket: 0, queue: self.queue.to_string(), no_ack: self.no_ack };
        let method_frame_result = self.channel.raw_rpc(get);
        let method_frame = match method_frame_result {
            Ok(m) => m,
            Err(_) => { return None }
        };
        match method_frame.method_name() {
            "basic.get-ok" => {
                let reply: basic::GetOk = Method::decode(method_frame).ok().unwrap();
                let headers = self.channel.read_headers().ok().unwrap();
                let body = self.channel.read_body(headers.body_size).ok().unwrap();
                let properties = BasicProperties::decode(headers).ok().unwrap();
                Some(GetResult {headers: properties, reply: reply, body: body, ack_sender: self.ack_sender.clone()})
            }
            "basic.get-empty" => None,
            method => { debug!("Unexpected method: {}", method); return None }
        }
    }
}

/// Will acknowledge

impl <'a> Drop for GetIterator <'a> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.ack_message();
    }
}

impl <'a> GetIterator <'a> {
    fn ack_message(&mut self) -> AMQPResult<()> {
        match self.ack_receiver.try_recv(){
            Ok(ack_action) => match ack_action {
                            AckAction::Ack(delivery_tag) => try!(self.channel.basic_ack(delivery_tag, false)),
                            AckAction::Nack(delivery_tag, requeue) => try!(self.channel.basic_nack(delivery_tag, false, requeue)),
                            AckAction::Reject(delivery_tag, requeue) => try!(self.channel.basic_reject(delivery_tag, requeue))
            },
            Err(_) => return Err(AMQPError::QueueEmpty)
        };
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

impl <'a> Basic<'a> for Channel {

    /// Returns a basic iterator.
    /// # Example
    /// ```no_run
    /// use std::default::Default;
    /// use amqp::session::{Options, Session};
    /// use amqp::basic::Basic;
    /// let mut session = match Session::new(Options { .. Default::default() }){
    ///     Ok(session) => session,
    ///     Err(error) => panic!("Failed openning an amqp session: {:?}", error)
    /// };
    /// let mut channel = session.open_channel(1).ok().expect("Can not open a channel");
    /// for get_result in channel.basic_get("my queue", false) {
    ///     println!("Headers: {:?}", get_result.headers);
    ///     println!("Reply: {:?}", get_result.reply);
    ///     println!("Body: {:?}", String::from_utf8_lossy(&get_result.body));
    ///     get_result.ack();
    /// }
    /// ```
    ///
    fn basic_get(&'a mut self, queue: &'a str, no_ack: bool) -> GetIterator<'a> {
        let (tx, rx) = sync_channel::<AckAction>(1);
        GetIterator { channel: self, queue: queue, no_ack: no_ack, ack_receiver: rx, ack_sender: tx }
    }

    fn basic_consume<T>(&mut self, callback: T,
        queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool,
        exclusive: bool, nowait: bool, arguments: Table) -> AMQPResult<String> where T: Consumer + 'static {
        let consume = &Consume {
            ticket: 0, queue: queue.to_string(), consumer_tag: consumer_tag.to_string(),
            no_local: no_local, no_ack: no_ack, exclusive: exclusive, nowait: nowait, arguments: arguments
        };
        let reply: ConsumeOk = try!(self.rpc(consume, "basic.consume-ok"));
        self.consumers.borrow_mut().insert(reply.consumer_tag.clone(), Box::new(callback));
        Ok(reply.consumer_tag)
    }

    // Will run the infinite loop, which will receive frames on the given channel & call consumers.
    fn start_consuming(&mut self) {
        loop {
            if let Err(err) = try_consume(self) {
                error!("Error consuming {:?}", err);
                return;
            }
        }
    }

    fn basic_publish(&mut self, exchange: &str, routing_key: &str, mandatory: bool, immediate: bool,
                         properties: BasicProperties, content: Vec<u8>) -> AMQPResult<()> {
        let publish = &Publish {
            ticket: 0, exchange: exchange.to_string(),
            routing_key: routing_key.to_string(), mandatory: mandatory, immediate: immediate};
        let properties_flags = properties.flags();
        let content_header = ContentHeaderFrame { content_class: 60, weight: 0, body_size: content.len() as u64,
            properties_flags: properties_flags, properties: try!(properties.encode()) };
        let content_header_frame = Frame {frame_type: FrameType::HEADERS, channel: self.id,
            payload: try!(content_header.encode()) };
        let content_frame = Frame { frame_type: FrameType::BODY, channel: self.id, payload: content};

        try!(self.send_method_frame(publish));
        try!(self.write(content_header_frame));
        try!(self.write(content_frame));
        Ok(())
    }

    fn basic_ack(&mut self, delivery_tag: u64, multiple: bool) -> AMQPResult<()> {
        self.send_method_frame(&Ack{delivery_tag: delivery_tag, multiple: multiple})
    }

    // Rabbitmq specific
    fn basic_nack(&mut self, delivery_tag: u64, multiple: bool, requeue: bool) -> AMQPResult<()> {
        self.send_method_frame(&Nack{delivery_tag: delivery_tag, multiple: multiple, requeue: requeue})
    }

    fn basic_reject(&mut self, delivery_tag: u64, requeue: bool) -> AMQPResult<()> {
        self.send_method_frame(&Reject{delivery_tag: delivery_tag, requeue: requeue})
    }

    fn basic_prefetch(&mut self, prefetch_count:u16 ) -> AMQPResult<QosOk> {
        self.basic_qos(0, prefetch_count, false)
    }

    fn basic_qos(&mut self, prefetch_size: u32, prefetch_count: u16, global: bool) -> AMQPResult<QosOk> {
        let qos=&Qos{prefetch_size: prefetch_size,
                     prefetch_count: prefetch_count,
                     global: global};
        self.rpc(qos, "basic.qos-ok")
    }

    fn basic_cancel(&mut self, consumer_tag: String, no_wait: bool) -> AMQPResult<CancelOk>{
        let cancel = &Cancel{ consumer_tag: consumer_tag, nowait: no_wait };
        self.rpc(cancel, "basic.cancel-ok")
    }
}

fn try_consume(channel : &mut Channel) -> AMQPResult<()> {
    let frame = try!(channel.read());
    match frame.frame_type {
        FrameType::METHOD => {
            let method_frame = try!(MethodFrame::decode(frame));
            match method_frame.method_name() {
                "basic.deliver" => {
                    let deliver_method : Deliver = try!(Method::decode(method_frame));
                    let headers = try!(channel.read_headers());
                    let body = try!(channel.read_body(headers.body_size));
                    let properties = try!(BasicProperties::decode(headers));
                    let conss1 = channel.consumers.clone();
                    let mut conss = conss1.borrow_mut();
                    let cons = conss.get_mut(&deliver_method.consumer_tag);
                    match cons {
                        Some(mut consumer) => {
                            consumer.handle_delivery(channel, deliver_method, properties, body);
                        }
                        None => { error!("Received deliver frame for the unknown consumer: {}", deliver_method.consumer_tag) }
                    };
                }
                _ => {} //TODO: Handle other methods as well.
            }
        }
        _ => {} // TODO: Handle other frames
    }
    Ok(())
}
