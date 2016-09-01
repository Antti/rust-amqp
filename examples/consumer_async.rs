extern crate amqp;
extern crate env_logger;

use amqp::{Session, Future};

fn main() {
    drop(env_logger::init().unwrap());
    let mut lp = amqp::Loop::new().unwrap();
    let session = Session::open_url(lp.handle(), "amqp://127.0.0.1//");
    let session = lp.run(session).unwrap(); // connect
    // TODO: wait for connection.open-ok
    // let session = session.open_channel(1).and_then(|(session, channel)| {
    //     println!("Opened channel: {}", channel);
    //     // exchange_declare, queue_declare, bind
    //     // declare consumer.
    //     let qos = session.basic_qos(channel, 0, 1000, true);
    //     let session = qos.and_then(move |(session, _qos_ok)| session.consume(channel, "test_queue".to_string() ).map(|(session, ok)| { println!("{:?}", ok); session }));
    //     session.and_then(|session| session)
    // });
    let done = session;
    match lp.run(done) {
        Ok(_) => {},
        Err(err) => { println!("Session was closed because: {:?}", err) }
    };
}
