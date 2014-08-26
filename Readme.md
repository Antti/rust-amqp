rust-amqp
=========

AMQ protocol implementation in pure rust.

The project is still in very early stages of development,
though it's capable of decoding & encoding method frames, decoding & encoding methods.


## It's not recommended to use this library for now.

It can do full start/start-ok, tune/tune-ok, open/open-ok, close/close-ok cycle :).

The structure of files, methods & structs is not final, it's more like a prototype.

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
