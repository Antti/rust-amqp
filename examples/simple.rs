extern crate amqp;

use amqp::session::Options;
use amqp::session::Session;
use amqp::protocol;
use amqp::table;
use amqp::basic;
use std::default::Default;
//table types:
//use table::{FieldTable, Table, Bool, ShortShortInt, ShortShortUint, ShortInt, ShortUint, LongInt, LongUint, LongLongInt, LongLongUint, Float, Double, DecimalValue, LongString, FieldArray, Timestamp};

fn main() {
    let mut session = Session::new(Options{.. Default::default()}).unwrap();
    let channel = session.open_channel(1).unwrap();
    println!("Openned channel: {}", channel.id);

    let queue_name = "test_queue";
    //ticket: u16, queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare(queue_name, true, true, false, false, false, table::new());
    println!("Queue declare: {}", queue_declare);
    for get_result in basic::get(&channel, queue_name, false) {
        println!("Headers: {}", get_result.headers);
        println!("Reply: {}", get_result.reply);
        println!("Body: {}", String::from_utf8_lossy(get_result.body.as_slice()));
        channel.basic_ack(get_result.reply.delivery_tag, false);
    }

    channel.basic_publish("", queue_name, true, false,
        protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
        (b"Hello from rust!").to_vec());
    channel.close(200, "Bye");
    session.close(200, "Good Bye".to_string());
}