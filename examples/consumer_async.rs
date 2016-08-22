extern crate amqp;
extern crate env_logger;

use amqp::{Session, Future};

fn main() {
    drop(env_logger::init().unwrap());
    let mut lp = amqp::Loop::new().unwrap();
    let session = Session::open_url(lp.handle(), "amqp://localhost//");
    let session = session.map(|session| { println!("Session initialized!"); session } );
    let done = session.and_then(|session| session.close()).map(|_| println!("Session closed."));
    lp.run(done).unwrap();

    // let channel = session.and_then(|session|
    //     session.open_channel(1).and_then(|(session, channel)|{
    //     })
    // );

    // let queue = channel.and_then(|chan|
    //     chan.queue_declare("queue")
    // );

    // let consumer1 = queue.and_then(|queue|
    //     channel.basic_consume(queue.name).foreach(|msg|{
    //         //do stuff
    //     })
    // );
}
