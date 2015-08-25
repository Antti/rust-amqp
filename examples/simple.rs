extern crate amqp;
extern crate env_logger;

use amqp::session::Session;
use amqp::protocol;
use amqp::table;
use amqp::basic::Basic;
use amqp::channel::{Channel, ConsumerCallBackFn};
use std::default::Default;
use std::thread;

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
    env_logger::init().unwrap();
    let amqp_url = "amqp://guest:guest@127.0.0.1//";
    let mut session = match Session::open_url(amqp_url) {
        Ok(session) => session,
        Err(error) => panic!("Can't create session: {:?}", error)
    };
    let mut channel = session.open_channel(1).ok().expect("Can't open channel");
    println!("Openned channel: {}", channel.id);

    let queue_name = "test_queue";
    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare(queue_name, false, true, false, false, false, table::new());
    println!("Queue declare: {:?}", queue_declare);
    for get_result in channel.basic_get(queue_name, false) {
        println!("Headers: {:?}", get_result.headers);
        println!("Reply: {:?}", get_result.reply);
        println!("Body: {:?}", String::from_utf8_lossy(&get_result.body));
        get_result.ack();
    }

    //queue: &str, consumer_tag: &str, no_local: bool, no_ack: bool, exclusive: bool, nowait: bool, arguments: Table
    println!("Declaring consumer...");
    let consumer_name = channel.basic_consume(consumer_function as ConsumerCallBackFn, queue_name, "", false, false, false, false, table::new());
    println!("Starting consumer {:?}", consumer_name);

    let consumers_thread = thread::spawn(move || {
        channel.start_consuming();
        channel
    });

    // There is currently no way to stop the consumers, so we infinitely join thread.
    let mut channel = consumers_thread.join().ok().expect("Can't get channel from consumer thread");

    channel.basic_publish("", queue_name, true, false,
        protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
        (b"Hello from rust!").to_vec());
    channel.close(200, "Bye".to_string());
    session.close(200, "Good Bye".to_string());
}
