extern crate amqp;
extern crate env_logger;

use amqp::{Client, Future};

// 1. Api must be asynchronous
// 2. Channels should be sendable, but not sync.


fn main() {
    drop(env_logger::init().unwrap());
    let mut lp = amqp::Loop::new().unwrap();
    let client = Client::open_url(lp.handle(), "amqp://127.0.0.1//");
    let done = client.and_then(|mut client|{
        println!("Trying to open channel");
        let channel1 = client.open_channel(1).and_then(|channel|{
            channel.with(|channel|{
                let mut channel = channel.borrow_mut();
                println!("Opened channel {:?}", channel.id);
                channel.consume("test_queue").map(|consume_ok|{
                    println!("Consume ok: {:?}", consume_ok);
                })
            })
        });
        client.session_runner().join(channel1)
    });
    // let session = session.open_channel(1).and_then(|(session, channel)| {
    //     println!("Opened channel: {}", channel);
    //     // exchange_declare, queue_declare, bind
    //     // declare consumer.
    //     let qos = session.basic_qos(channel, 0, 1000, true);
    //     let session = qos.and_then(move |(session, _qos_ok)| session.consume(channel, "test_queue".to_string() ).map(|(session, ok)| { println!("{:?}", ok); session }));
    // });
    match lp.run(done) {
        Ok(_) => {},
        Err(err) => { println!("Session was closed because: {:?}", err) }
    };
}

#[cfg(mock)]
fn mock_api(){
    drop(env_logger::init().unwrap());
    let mut lp = amqp::Loop::new().unwrap();
    let session = Session::open_url(lp.handle(), "amqp://127.0.0.1//");
    let session = session.and_then(|session|{
        // Running sync methods on channel will consume channel, returning Future<(Channel, response), AMQPError>
        // This way we can be sure that there will be no race conditions on the channel.
        // All consumer futures are polled when possible and the result is sent to the channel.
        let consumer1 = session.open_channel(1).and_then(|channel|{
            let queue = channel.queue_declare("queue");
            queue.and_then(|(channel, queue)|{
                queue.consume(consumer_object)
            })
        }); // TODO: Check if we need to return ConsumerFinished future?

        // Session on the other hand allows you to open and run many channels concurently.
        let consumer2 = session.open_channel(2).and_then(|channel|{
            channel.queue("other_queue").and_then(|(channel, queue)| queue.consume(|a,b,c, channel| { }))
        });

        let consumer3 = session.open_channel(3).and_then(|channel|{
            producer.produce(channel)
        });

        // Run all the channel promises;
        lp.run(consumer1);
        lp.run(consumer2);
        lp.run(consumer3);

        // Run session to drive consumers
        session.session_runner()
    });
    lp.run(session);
}