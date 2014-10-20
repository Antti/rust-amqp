extern crate amqp;

use amqp::session::Options;
use amqp::session::Session;
use amqp::protocol;
use amqp::table;
use std::default::Default;
//table types:
//use table::{FieldTable, Table, Bool, ShortShortInt, ShortShortUint, ShortInt, ShortUint, LongInt, LongUint, LongLongInt, LongLongUint, Float, Double, DecimalValue, LongString, FieldArray, Timestamp};

fn main() {
    let mut session = Session::new(Options{.. Default::default()}).unwrap();
    let channel = session.open_channel(1).unwrap();
    println!("Openned channel: {}", channel.id);

    let queue_name = "test_queue";
    //ticket: u16, queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare(0, queue_name, true, true, false, false, false, table::new());
    println!("Queue declare: {}", queue_declare);
    loop {
        match channel.basic_get(0, queue_name, true){
            Ok((headers, body)) => {
                println!("Headers: {}", headers);
                println!("Body: {}", String::from_utf8_lossy(body.as_slice()));
            },
            Err(err) => {println!("Basic get error: {}", err); break;}
        }
    }

    channel.basic_publish(0, "", queue_name, true, true,
        protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
        (b"Hello from rust!").to_vec());
    channel.close(200, "Bye");
    session.close(200, "Good Bye".to_string());
}