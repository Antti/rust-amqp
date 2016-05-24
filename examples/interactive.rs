extern crate amqp;
extern crate env_logger;

use amqp::{Session, Table, Basic, protocol};
use std::default::Default;
use std::io;

fn main() {
    env_logger::init().unwrap();
    let mut amqp_url = String::new();
    println!("Please input the AMQP endpoint to use: ");
    io::stdin().read_line(&mut amqp_url).ok().expect("Unable to read URL!");
    let mut session = match Session::open_url(&amqp_url[..]) {
        Ok(session) => session,
        Err(error) => panic!("Can't create session: {:?}", error)
    };
    let mut channel = session.open_channel(1).ok().expect("Can't open channel!");

    let queue_name = "test_queue";
    channel.queue_declare(queue_name, false, true, false, false, false, Table::new()).ok().expect("Unable to declare queue!");

    println!("Type some text here below, it will be sent into the '{}' queue. Hit Enter when done.", queue_name);
    let mut input_data = String::new();
    io::stdin().read_line(&mut input_data).ok().expect("Unable to read input data!");

    println!("Sending data...\n");
    channel.basic_publish("", queue_name, true, false,
        protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
        input_data.trim_right().to_string().into_bytes()).unwrap();

     for get_result in channel.basic_get(queue_name, false) {
        println!("Received: {:?}", String::from_utf8_lossy(&get_result.body));
        get_result.ack();
    }

    println!("Queue is now empty, quitting...");
    channel.close(200, "Bye");
    session.close(200, "Good Bye");
}
