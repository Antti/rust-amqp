extern crate amqp;
extern crate env_logger;

use amqp::session::Options;
use amqp::session::Session;
use amqp::protocol;
use amqp::table;
use amqp::basic::Basic;
use std::default::Default;

extern "C" {
  fn signal(sig: u32, cb: extern fn(u32));
}
extern fn interrupt(_:u32) {
  unsafe {
      stop_loop = true;
  }
}

static mut stop_loop : bool = false;

fn main() {
    env_logger::init().unwrap();
    let mut session = Session::new(Options{.. Default::default()}).ok().expect("Can't create session");
    let mut channel = session.open_channel(1).ok().expect("Can't open channel");
    println!("Openned channel: {:?}", channel.id);

    let queue_name = "test_queue";
    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare(queue_name, false, true, false, false, false, table::new());
    println!("Queue declare: {:?}", queue_declare);

    unsafe {
      signal(2, interrupt);
    }

    loop {
        channel.basic_publish("", queue_name, true, false,
            protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
            (b"Hello from rust!").to_vec());
        unsafe {
            if stop_loop {
                break;
            }
        }
    }
    println!("Stopping producer");
    channel.close(200, "Bye".to_string());
    session.close(200, "Good Bye".to_string());
}
