# 0.0.12

Re-exported types from sub-modules to a crate level.
Made most of the internals private. Made public only necessary things.

Upgrading, in most cases requires removing preceding module name before struct:
```rust
use amqp::session::Session;
use amqp::basic::Basic;
use amqp::channel::Channel;
```
will become something like:
```rust
use amqp::{Session, Basic, Channel};
```
The only things where you still use modules of are `protocol` and `table`.
