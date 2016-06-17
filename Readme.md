#rust-amqp [![Build Status](https://travis-ci.org/Antti/rust-amqp.svg)](https://travis-ci.org/Antti/rust-amqp)

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

Have a look at the examples in `examples/` folder.

### Connecting to the server & openning channel:

```rust
extern crate amqp;
use amqp::Session;

let mut session = Session::open_url("amqp://localhost/").unwrap();
let mut channel = session.open_channel(1).unwrap();
```

> Note: This library supports TLS connections, via OpenSSL.
> However, this is an optional feature that is enabled by default but can be disabled at build-time (via `cargo --no-default-features` on the command-line, or with `default-features = false` in your `Cargo.toml`).

### Declaring queue:
```rust
//The arguments come in following order:
//queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
let queue_declare = channel.queue_declare("my_queue_name", false, true, false, false, false, Table::new());
```

### Publishing message:
```rust
channel.basic_publish("", "my_queue_name", true, false,
    protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()}, (b"Hello from rust!").to_vec());
```

This will send message: "Hello from rust!" to the queue named "my_queue_name".

The messages have type of `Vec<u8>`, so if you want to send string, first you must convert it to `Vec<u8>`.


## Known issues:

* We don't handle frames sent to channel 0 after the session was established.
* Asynchronous methods are not handled properly see #18.
* There are still few places where we call `unwrap`, not handling error responses properly.

## Missing things:

* There's no facility to re-establish connection
* No heartbeats support
* Not all the amqp methods are implemented in a rust interface. You can still call them manually.


## Development notes:

The methods encoding/decoding code is generated using codegen.rb & amqp-rabbitmq-0.9.1.json spec.

You need to have rustfmt installed to generate protocol.rs
To generate a new spec, run:

```sh
make
```

To build the project and run the testsuite, use cargo:

```sh
cargo build
cargo test
```

Moreover, there are several source examples to show library usage, and an interactive one to quickly test the library.
They can be run directly from cargo:

```sh
cargo run --example interactive
```

## OpenSSL

On MacOS X If you have problems with OpenSSL during complication, regarding missing headers or linkage, try:

```
brew install openssl
export OPENSSL_INCLUDE_DIR=`brew --prefix openssl`/include
export OPENSSL_LIB_DIR=`brew --prefix openssl`/lib

cargo clean
cargo test
```

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
