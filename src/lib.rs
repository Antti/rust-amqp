/*!

#rust-amqp
[![Build Status](https://travis-ci.org/Antti/rust-amqp.svg)](https://travis-ci.org/Antti/rust-amqp)

AMQ protocol implementation in pure rust.

> Note:
> The project is still in very early stages of development,
> it implements all the protocol parsing, but not all the protocol methods are wrapped/easy to use.
> Expect the API to be changed in the future.

## What it currently can do:
* Connect to server
* Open/close channels
* Declare queues/exchanges
* All the methods from the Basic class are implemented, including get, publish, ack, nack, reject, consume. So you can send/receive messages.

Have a look at the examples in examples folder.


### Connecting to the server & openning channel:
>Note: Currently it can't connect using TLS connections.

```ignore
    use amqp::session::Session;
    use amqp::table;

    let mut session = Session::open_url("amqp://localhost/").unwrap();
    let mut channel = session.open_channel(1).unwrap();
```

### Declaring queue:
```ignore
    use amqp::table;
    //The arguments come in following order:
    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declare = channel.queue_declare("my_queue_name", false, true, false, false, false, table::new());
```

### Publishing message:
```ignore
    channel.basic_publish("", "my_queue_name", true, false,
    amqp::protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()}, (b"Hello from rust!").to_vec());
```

This will send message: "Hello from rust!" to the queue named "my_queue_name".

The messages have type of Vec<u8>, so if you want to send string, first you must convert it to Vec<u8>.

## Development notes:

The methods encoding/decoding code is generated using codegen.rb & amqp-rabbitmq-0.9.1.json spec.

To generate a new spec, run:

```sh
make
```

To build project, use cargo:

```sh
cargo build
```

To build examples:
```sh
cargo test
```
*/

extern crate collect;
extern crate url;
#[macro_use]
extern crate log;

pub mod connection;
pub mod channel;
pub mod framing;
pub mod table;
pub mod protocol;
pub mod session;
pub mod basic;
pub mod amqp_error;

