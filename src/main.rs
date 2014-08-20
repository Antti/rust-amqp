extern crate amqp;

use amqp::connection::{Connection, Options};
use std::default::Default;

fn main() {
    let mut connection = Connection::open(Options{.. Default::default()}).unwrap();
    connection.close(200, "Good Bye".to_string());
}