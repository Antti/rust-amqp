extern crate amqp;
extern crate env_logger;

use amqp::{Session, Options, Table, Basic, TableEntry, protocol};
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
    let mut session = Session::new(Options{vhost: "/", .. Default::default()}).ok().expect("Can't create session");
    let mut channel = session.open_channel(1).ok().expect("Can't open channel");
    println!("Openned channel: {:?}", channel.id);

    let queue_name = "test_queue";
    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare(queue_name, false, true, false, false, false, Table::new());
    println!("Queue declare: {:?}", queue_declare);

    unsafe {
      signal(2, interrupt);
    }

    loop {
        let mut headers = Table::new();
        let field_array = vec![TableEntry::LongString("Foo".to_owned()), TableEntry::LongString("Bar".to_owned())];
        headers.insert("foo".to_owned(), TableEntry::LongString("Foo".to_owned()));
        headers.insert("field array test".to_owned(), TableEntry::FieldArray(field_array));
        let properties = protocol::basic::BasicProperties { content_type: Some("text".to_owned()), headers: Some(headers), ..Default::default() };
        channel.basic_publish("", queue_name, true, false,
            properties,
            (b"Hello from rust!").to_vec()).ok().expect("Failed publishing");
        unsafe {
            if stop_loop {
                break;
            }
        }
    }
    println!("Stopping producer");
    channel.close(200, "Bye");
    session.close(200, "Good Bye");
}
