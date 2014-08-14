extern crate amqp;

use amqp::connection::Connection;
use amqp::framing;
use amqp::framing::Method;
use amqp::framing::{Bool, ShortShortInt, ShortShortUint, ShortInt, ShortUint, LongInt, LongUint, LongLongInt, LongLongUint, Float, Double, DecimalValue, LongString, FieldArray, Timestamp, FieldTable};
use amqp::protocol::connection;
use std::collections::TreeMap;

fn main() {
    let mut connection = Connection::open("localhost", 5672).unwrap();
    let frame = connection.read(); //Start

    let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
    let start : amqp::protocol::connection::Start = framing::Method::decode(arguments);
    println!("{}", start);
    // println!("{}", start.server_properties.find(&"product".to_string()));


    let mut client_properties = TreeMap::new();
    let mut capabilities = TreeMap::new();
    capabilities.insert("publisher_confirms".to_string(), Bool(true));
    capabilities.insert("consumer_cancel_notify".to_string(), Bool(true));
    capabilities.insert("exchange_exchange_bindings".to_string(), Bool(true));
    capabilities.insert("basic.nack".to_string(), Bool(true));
    capabilities.insert("connection.blocked".to_string(), Bool(true));
    capabilities.insert("authentication_failure_close".to_string(), Bool(true));
    client_properties.insert("capabilities".to_string(), FieldTable(capabilities));
    client_properties.insert("product".to_string(), LongString("rust-amqp".to_string()));
    client_properties.insert("platform".to_string(), LongString("rust".to_string()));
    client_properties.insert("version".to_string(), LongString("0.1".to_string()));
    client_properties.insert("information".to_string(), LongString("http://github.com".to_string()));

    let start_ok = connection::StartOk {
        client_properties: client_properties, mechanism: "PLAIN".to_string(),
        response: "\0guest\0guest".to_string(), locale: "en_US".to_string()};
    connection.send_method_frame(0, &start_ok);

    let frame = connection.read();//Tune
    let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
    let tune : amqp::protocol::connection::Tune = framing::Method::decode(arguments);

    let tune_ok = connection::TuneOk {channel_max: tune.channel_max,frame_max: tune.frame_max, heartbeat: 0};
    connection.send_method_frame(0, &tune_ok);

    let open = connection::Open{virtual_host: "/".to_string(), capabilities: "".to_string(), insist: false };
    connection.send_method_frame(0, &open);

    let frame = connection.read();//Open-ok
    let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
    let open_ok : amqp::protocol::connection::OpenOk = framing::Method::decode(arguments);


    let close = connection::Close{reply_code: 200, reply_text: "Good bye".to_string(), class_id: 0, method_id: 0};
    connection.send_method_frame(0, &close);

    let frame = connection.read();//close-ok
    let (class_id, method_id, arguments) = framing::decode_method_frame(&frame.unwrap());
    let close_ok : amqp::protocol::connection::CloseOk = framing::Method::decode(arguments);

}