extern crate amqp;

use amqp::session::Options;
use amqp::session::Session;
use amqp::table;
use std::default::Default;


fn main() {
    let mut session = Session::new(Options{.. Default::default()}).ok().unwrap();
    let mut channel = session.open_channel(1).ok().unwrap();
    println!("Openned channel: {:?}", channel.id);

    let exchange1 = "test_exchange";
    let exchange2 = "test_exchange2";
    let exchange_type = "topic";

    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let exchange_declare1 = channel.exchange_declare(exchange1, exchange_type,
                                                     false, true, false, false, false, table::new());

    println!("Exchange declare: {:?}", exchange_declare1);
    let exchange_declare2 = channel.exchange_declare(exchange2, exchange_type,
                                                     false, true, false, false, false, table::new());
    println!("Exchange declare: {:?}", exchange_declare2);

    let bind_reply = channel.exchange_bind(exchange1, exchange2, "#", table::new());
    println!("Exchange bind: {:?}", bind_reply);


    channel.close(200, "Bye".to_string());
    session.close(200, "Good Bye".to_string());
}
