#rust-amqp

AMQ protocol implementation in pure rust.

[![Build Status](https://travis-ci.org/Antti/rust-amqp.svg)](https://travis-ci.org/Antti/rust-amqp)

> *** Note: ***

> The project is still in very early stages of development,
> it implements the most of the protocol (decoding/encoding frames), but is not very easy to use.
> Expect the API to be changed in the future.

### What it currently can do:
* Connect to server
* Open/close channels
* Declare queues
* Recieve (basic get) and publish messages.

Have a look at an example: examples/simple.rs

### Development notes:

The methods encoding/decoding code is generated using codegen.rb & amqp-rabbitmq-0.9.1.json spec.

To generate a new spec, run:

```
make
```

To build project, use cargo:

```
cargo build
```

This will build libamqp library & example client: simple
