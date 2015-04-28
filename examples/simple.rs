extern crate amqp;

use amqp::session::Session;
use amqp::protocol;
use amqp::table;
use amqp::basic::Basic;
use amqp::channel::Channel;
use std::default::Default;
//table types:
//use table::{FieldTable, Table, Bool, ShortShortInt, ShortShortUint, ShortInt, ShortUint, LongInt, LongUint, LongLongInt, LongLongUint, Float, Double, DecimalValue, LongString, FieldArray, Timestamp};

fn consumer_function(channel: &mut Channel, deliver: protocol::basic::Deliver, headers: protocol::basic::BasicProperties, body: Vec<u8>){
    println!("Got a delivery:");
    println!("Deliver info: {:?}", deliver);
    println!("Content headers: {:?}", headers);
    println!("Content body: {:?}", body);
    channel.basic_ack(deliver.delivery_tag, false);
}

fn main() {
    let mut session = Session::open_url("amqp://localhost/").ok().unwrap();
    let mut channel = session.open_channel(1).ok().unwrap();
    println!("Openned channel: {}", channel.id);

    let queue_name = "test_queue";
    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare(queue_name, false, true, false, false, false, table::new());
    println!("Queue declare: {:?}", queue_declare);
    for get_result in channel.basic_get(queue_name, true) {
        println!("Headers: {:?}", get_result.headers);
        println!("Reply: {:?}", get_result.reply);
        println!("Body: {:?}", String::from_utf8_lossy(&get_result.body));
        // channel.basic_ack(get_result.reply.delivery_tag, false);
    }

    //queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool, exclusive: bool, nowait: bool, arguments: Table
    println!("Declaring consumer...");
    let consumer_name = channel.basic_consume(consumer_function, queue_name, "", false, false, false, false, table::new());
    println!("Starting consumer {:?}", consumer_name);
    channel.start_consuming();

    channel.basic_publish("", queue_name, true, false,
        protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
        (b"Hello from rust!").to_vec());
    channel.close(200, "Bye".to_string());
    session.close(200, "Good Bye".to_string());
}
