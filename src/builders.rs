use basic::Basic;
use amqp_error::AMQPResult;
use amq_proto::Table;
use channel::{Channel, Consumer};
use protocol;

pub struct ConsumeBuilder<T>
    where T: Consumer + 'static
{
    _callback: T,
    _queue_name: String,
    _tag: String,
    _no_local: bool,
    _no_ack: bool,
    _exclusive: bool,
    _nowait: bool,
    _arguments: Table,
}

impl <T: Consumer + 'static> ConsumeBuilder<T>
{
    pub fn new<S>(callback: T, name: S) -> ConsumeBuilder<T>
        where S: Into<String>
    {
        ConsumeBuilder {
            _callback: callback,
            _queue_name: name.into(),
            _tag: "".into(),
            _no_local: false,
            _no_ack: false,
            _exclusive: false,
            _nowait: false,
            _arguments: Table::new(),
        }
    }

    pub fn tag<S>(mut self, tag: S) -> ConsumeBuilder<T>
        where S: Into<String>
    {
        self._tag = tag.into();
        self
    }

    pub fn no_local(mut self) -> ConsumeBuilder<T> {
        self._no_local = true;
        self
    }

    pub fn no_ack(mut self) -> ConsumeBuilder<T> {
        self._no_ack = true;
        self
    }

    pub fn exclusive(mut self) -> ConsumeBuilder<T> {
        self._exclusive = true;
        self
    }

    pub fn nowait(mut self) -> ConsumeBuilder<T> {
        self._nowait = true;
        self
    }

    pub fn basic_consume(self, channel: &mut Channel) -> AMQPResult<String> {
        channel.basic_consume(
            self._callback,
            self._queue_name,
            self._tag,
            self._no_local,
            self._no_ack,
            self._exclusive,
            self._nowait,
            self._arguments)
    }
}

pub struct QueueBuilder {
    _name: String,
    _passive: bool,
    _durable: bool,
    _exclusive: bool,
    _auto_delete: bool,
    _nowait: bool,
    _arguments: Table,
}

impl QueueBuilder {
    pub fn named<S>(name: S) -> QueueBuilder
        where S: Into<String> {
        QueueBuilder {
            _name: name.into(),
            _passive: false,
            _durable: false,
            _exclusive: false,
            _auto_delete: false,
            _nowait: false,
            _arguments: Table::new()
        }
    }

    pub fn passive(mut self) -> QueueBuilder {
        self._passive = true;
        self
    }

    pub fn durable(mut self) -> QueueBuilder {
        self._durable = true;
        self
    }

    pub fn exclusive(mut self) -> QueueBuilder {
        self._exclusive = true;
        self
    }

    pub fn auto_delete(mut self) -> QueueBuilder {
        self._auto_delete = true;
        self
    }

    pub fn nowait(mut self) -> QueueBuilder {
        self._nowait = true;
        self
    }

    pub fn arguments(mut self, table: Table) -> QueueBuilder {
        self._arguments = table;
        self
    }

    pub fn declare(&self, channel: &mut Channel) ->
        AMQPResult<protocol::queue::DeclareOk> {
        channel.queue_declare(
            self._name.clone(),
            self._passive,
            self._durable,
            self._exclusive,
            self._auto_delete,
            self._nowait,
            self._arguments.clone())
    }
}
