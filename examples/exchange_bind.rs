extern crate amqp;
extern crate env_logger;

use amqp::{Session, Options, Table};
use std::default::Default;


fn main() {
    env_logger::init().unwrap();
    let mut session = Session::new(Options{vhost: "/", .. Default::default()}).ok().expect("Can't create session");
    let mut channel = session.open_channel(1).ok().expect("Can't open channel");
    println!("Openned channel: {:?}", channel.id);

    let exchange1 = "test_exchange";
    let exchange2 = "test_exchange2";
    let exchange_type = "topic";

    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let exchange_declare1 = channel.exchange_declare(exchange1, exchange_type,
                                                     false, true, false, false, false, Table::new());

    println!("Exchange declare: {:?}", exchange_declare1);
    let exchange_declare2 = channel.exchange_declare(exchange2, exchange_type,
                                                     false, true, false, false, false, Table::new());
    println!("Exchange declare: {:?}", exchange_declare2);

    let bind_reply = channel.exchange_bind(exchange1, exchange2, "#", Table::new());
    println!("Exchange bind: {:?}", bind_reply);


    channel.close(200, "Bye".to_string());
    session.close(200, "Good Bye".to_string());
}
