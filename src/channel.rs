use amqp_error::{AMQPResult, AMQPError};

use framing::{MethodFrame, ContentHeaderFrame, Frame, FrameType};
use table::Table;
use protocol;
// use basic::{Basic, GetIterator};
use protocol::{channel, basic};
use protocol::basic::BasicProperties;
use protocol::basic::{Consume, ConsumeOk, Deliver, Publish, Ack, Nack, Reject, Qos, QosOk, Cancel,
                      CancelOk};
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use method::Method;

use futures::{Future, BoxFuture, finished, done};
use futures::stream::{Sender, Receiver};

pub trait Consumer: Send {
    fn handle_delivery(&mut self,
                       channel: &mut Channel,
                       method: basic::Deliver,
                       headers: BasicProperties,
                       body: Vec<u8>);
}

pub type ConsumerCallBackFn = fn(channel: &mut Channel,
                                 method: basic::Deliver,
                                 headers: BasicProperties,
                                 body: Vec<u8>);

impl Consumer for ConsumerCallBackFn {
    fn handle_delivery(&mut self,
                       channel: &mut Channel,
                       method: basic::Deliver,
                       headers: BasicProperties,
                       body: Vec<u8>) {
        self(channel, method, headers, body);
    }
}

impl<T> Consumer for Box<T>
    where T: FnMut(&mut Channel, basic::Deliver, BasicProperties, Vec<u8>) + Send
{
    fn handle_delivery(&mut self,
                       channel: &mut Channel,
                       method: basic::Deliver,
                       headers: BasicProperties,
                       body: Vec<u8>) {
        self(channel, method, headers, body);
    }
}

pub struct Channel {
    id: u16,
    consumers: Rc<RefCell<HashMap<String, Box<Consumer>>>>,
    receiver: Receiver<Frame, AMQPError>
}

impl Channel {
    pub fn new(id: u16, receiver: Receiver<Frame, AMQPError>) -> Channel {
        Channel {
            id: id,
            consumers: Rc::new(RefCell::new(HashMap::new())),
            receiver: receiver
        }
    }

    fn rpc<T, U>(&self, &T) -> BoxFuture<U, AMQPError> where T: Method, U: Method {
        self.receiver
    }

    // fn read_headers(&self, session: Session) -> BoxFuture<(Session, ContentHeaderFrame), AMQPError> {
    //     session.read_frame().and_then(|(session, frame)|{
    //         finished(session).join(done(ContentHeaderFrame::decode(frame)))
    //     }).boxed()
    // }

    fn read_body(&mut self, size: u64) -> AMQPResult<Vec<u8>> {
        let mut body = Vec::with_capacity(size as usize);
        while body.len() < size as usize {
            body.extend(try!(self.read()).payload.into_iter())
        }
        Ok(body)
    }

    pub fn exchange_declare<S>(&mut self,
                               exchange: S,
                               _type: S,
                               passive: bool,
                               durable: bool,
                               auto_delete: bool,
                               internal: bool,
                               nowait: bool,
                               arguments: Table)
                               -> AMQPResult<protocol::exchange::DeclareOk>
        where S: Into<String>
    {
        let declare = protocol::exchange::Declare {
            ticket: 0,
            exchange: exchange.into(),
            _type: _type.into(),
            passive: passive,
            durable: durable,
            auto_delete: auto_delete,
            internal: internal,
            nowait: nowait,
            arguments: arguments,
        };
        self.rpc(&declare, "exchange.declare-ok")
    }

    pub fn exchange_bind<S>(&mut self,
                            destination: S,
                            source: S,
                            routing_key: S,
                            arguments: Table)
                            -> AMQPResult<protocol::exchange::BindOk>
        where S: Into<String>
    {
        let bind = protocol::exchange::Bind {
            ticket: 0,
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments: arguments,
        };
        self.rpc(&bind, "exchange.bind-ok")
    }

    pub fn queue_declare<S>(&mut self,
                            queue: S,
                            passive: bool,
                            durable: bool,
                            exclusive: bool,
                            auto_delete: bool,
                            nowait: bool,
                            arguments: Table)
                            -> AMQPResult<protocol::queue::DeclareOk>
        where S: Into<String>
    {
        let declare = protocol::queue::Declare {
            ticket: 0,
            queue: queue.into(),
            passive: passive,
            durable: durable,
            exclusive: exclusive,
            auto_delete: auto_delete,
            nowait: nowait,
            arguments: arguments,
        };
        self.rpc(&declare, "queue.declare-ok")
    }

    pub fn queue_bind<S>(&mut self,
                         queue: S,
                         exchange: S,
                         routing_key: S,
                         nowait: bool,
                         arguments: Table)
                         -> AMQPResult<protocol::queue::BindOk>
        where S: Into<String>
    {
        let bind = protocol::queue::Bind {
            ticket: 0,
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            nowait: nowait,
            arguments: arguments,
        };
        self.rpc(&bind, "queue.bind-ok")
    }

    fn try_consume(&mut self, frame: Frame) -> AMQPResult<Option<Frame>> {
        match frame.frame_type {
            FrameType::METHOD => {
                let method_frame = try!(MethodFrame::decode(&frame));
                match method_frame.method_name() {
                    "basic.deliver" => {
                        let deliver_method: Deliver = try!(Method::decode(method_frame));
                        let headers = try!(self.read_headers());
                        let body = try!(self.read_body(headers.body_size));
                        let properties = try!(BasicProperties::decode(headers));
                        let conss1 = self.consumers.clone();
                        let mut conss = conss1.borrow_mut();
                        let cons = conss.get_mut(&deliver_method.consumer_tag);
                        match cons {
                            Some(mut consumer) => {
                                consumer.handle_delivery(self, deliver_method, properties, body);
                                Ok(None)
                            }
                            None => {
                                error!("Received deliver frame for the unknown consumer: {}",
                                       deliver_method.consumer_tag);
                                Ok(None)
                            }
                        }
                    }
                    // connection:blocked
                    // connection:unblocked
                    // TODO: Handle other methods as well (basic.ack, basic.nack)
                    _ => Ok(Some(frame)),
                }
            }
            _ => {
                // Pass on all other types of frames
                Ok(Some(frame))
            }
        }
    }
}


// impl<'a> Basic<'a> for Channel {
//     /// Returns a basic iterator.
//     /// # Example
//     /// ```no_run
//     /// use std::default::Default;
//     /// use amqp::{Options, Session, Basic};
//     /// let mut session = match Session::new(Options { .. Default::default() }){
//     ///     Ok(session) => session,
//     ///     Err(error) => panic!("Failed openning an amqp session: {:?}", error)
//     /// };
//     /// let mut channel = session.open_channel(1).ok().expect("Can not open a channel");
//     /// for get_result in channel.basic_get("my queue", false) {
//     ///     println!("Headers: {:?}", get_result.headers);
//     ///     println!("Reply: {:?}", get_result.reply);
//     ///     println!("Body: {:?}", String::from_utf8_lossy(&get_result.body));
//     ///     get_result.ack();
//     /// }
//     /// ```
//     ///
//     fn basic_get(&'a mut self, queue: &'a str, no_ack: bool) -> GetIterator<'a> {
//         GetIterator::new(self, queue, no_ack)
//     }

//     fn basic_consume<T, S>(&mut self,
//                            callback: T,
//                            queue: S,
//                            consumer_tag: S,
//                            no_local: bool,
//                            no_ack: bool,
//                            exclusive: bool,
//                            nowait: bool,
//                            arguments: Table)
//                            -> AMQPResult<String>
//         where T: Consumer + 'static,
//               S: Into<String>
//     {
//         let consume = &Consume {
//             ticket: 0,
//             queue: queue.into(),
//             consumer_tag: consumer_tag.into(),
//             no_local: no_local,
//             no_ack: no_ack,
//             exclusive: exclusive,
//             nowait: nowait,
//             arguments: arguments,
//         };
//         let reply: ConsumeOk = try!(self.rpc(consume, "basic.consume-ok"));
//         self.consumers.borrow_mut().insert(reply.consumer_tag.clone(), Box::new(callback));
//         Ok(reply.consumer_tag)
//     }

//     fn basic_publish<S>(&mut self,
//                         exchange: S,
//                         routing_key: S,
//                         mandatory: bool,
//                         immediate: bool,
//                         properties: BasicProperties,
//                         content: Vec<u8>)
//                         -> AMQPResult<()>
//         where S: Into<String>
//     {
//         let publish = &Publish {
//             ticket: 0,
//             exchange: exchange.into(),
//             routing_key: routing_key.into(),
//             mandatory: mandatory,
//             immediate: immediate,
//         };
//         let properties_flags = properties.flags();
//         let content_header = ContentHeaderFrame {
//             content_class: 60,
//             weight: 0,
//             body_size: content.len() as u64,
//             properties_flags: properties_flags,
//             properties: try!(properties.encode()),
//         };
//         let content_header_frame = Frame {
//             frame_type: FrameType::HEADERS,
//             channel: self.id,
//             payload: try!(content_header.encode()),
//         };
//         let content_frame = Frame {
//             frame_type: FrameType::BODY,
//             channel: self.id,
//             payload: content,
//         };

//         try!(self.send_method_frame(publish));
//         try!(self.write(content_header_frame));
//         try!(self.write(content_frame));
//         Ok(())
//     }

//     fn basic_ack(&mut self, delivery_tag: u64, multiple: bool) -> AMQPResult<()> {
//         self.send_method_frame(&Ack {
//             delivery_tag: delivery_tag,
//             multiple: multiple,
//         })
//     }

//     // Rabbitmq specific
//     fn basic_nack(&mut self, delivery_tag: u64, multiple: bool, requeue: bool) -> AMQPResult<()> {
//         self.send_method_frame(&Nack {
//             delivery_tag: delivery_tag,
//             multiple: multiple,
//             requeue: requeue,
//         })
//     }

//     fn basic_reject(&mut self, delivery_tag: u64, requeue: bool) -> AMQPResult<()> {
//         self.send_method_frame(&Reject {
//             delivery_tag: delivery_tag,
//             requeue: requeue,
//         })
//     }

//     fn basic_prefetch(&mut self, prefetch_count: u16) -> AMQPResult<QosOk> {
//         self.basic_qos(0, prefetch_count, false)
//     }

//     fn basic_qos(&mut self,
//                  prefetch_size: u32,
//                  prefetch_count: u16,
//                  global: bool)
//                  -> AMQPResult<QosOk> {
//         let qos = &Qos {
//             prefetch_size: prefetch_size,
//             prefetch_count: prefetch_count,
//             global: global,
//         };
//         self.rpc(qos, "basic.qos-ok")
//     }

//     fn basic_cancel(&mut self, consumer_tag: String, no_wait: bool) -> AMQPResult<CancelOk> {
//         let cancel = &Cancel {
//             consumer_tag: consumer_tag,
//             nowait: no_wait,
//         };
//         self.rpc(cancel, "basic.cancel-ok")
//     }
// }
