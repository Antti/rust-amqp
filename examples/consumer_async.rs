extern crate amqp;
extern crate env_logger;

use amqp::{Session, Future};

fn main() {
    drop(env_logger::init().unwrap());
    let mut lp = amqp::Loop::new().unwrap();
    let session = Session::open_url(lp.handle(), "amqp://localhost//");
    let session = session.map(|session| { println!("Session initialized!"); session } );
    let session = session.and_then(|session| {
        session.open_channel(1).and_then(|(session, channel)| {
            println!("Opened channel: {}", channel);
            // exchange_declare, queue_declare, bind
            // declare consumer.
            // let session = session.consume(channel, "queue", callback);
            // session.and_then(|session| session.run_consumers());
            let session = session.consume(channel, "test_queue".to_string() ).map(|(session, ok)| { println!("{:?}", ok); session });
            session.and_then(move |session|
                session.run().and_then(move |session|{
                    session.close_channel(channel)
                })
            )
        })
    });
    let done = session.and_then(|session| session.close()).map(|_| println!("Session closed."));
    match lp.run(done) {
        Ok(_) => {},
        Err(err) => { println!("Session was closed because: {:?}", err) }
    };
}
