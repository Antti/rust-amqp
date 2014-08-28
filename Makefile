all: src/protocol.rs

src/protocol.rs: codegen.rb codegen.erb amqp-rabbitmq-0.9.1.json
	ruby codegen.rb > src/protocol.rs
