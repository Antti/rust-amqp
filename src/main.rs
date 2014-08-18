extern crate amqp;

use amqp::connection::Connection;
// use amqp::framing;
// use amqp::framing::Method;
// use amqp::table::{FieldTable, Table, Bool, ShortShortInt, ShortShortUint, ShortInt, ShortUint, LongInt, LongUint, LongLongInt, LongLongUint, Float, Double, DecimalValue, LongString, FieldArray, Timestamp};
// use amqp::protocol::connection;
// use std::collections::TreeMap;

fn main() {
    let mut connection = Connection::open("localhost", 5672, "guest", "guest", "/").unwrap();
    connection.close(200, "Good Bye".to_string());
}