rust-amqp
=========

AMQ protocol implementation in pure rust.

The project is still in very early stages of development,
though it's capable of decoding & encoding method, data, headers frames.

It can connect to the server, post & receive messages.

### You can actually use this library in your project, but expect the API to be changed in the future.

### Have a look at an example: src/main.rs

The methods encoding/decoding code is generated using codegen.rb & amqp-rabbitmq-0.9.1.json spec.

To generate a new spec, run:

```
make
```

To build project, use cargo:

```
cargo build
```

This will build libamqp library & example client: rust-amqp.
