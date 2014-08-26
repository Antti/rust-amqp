extern crate amqp;

use amqp::connection::Options;
use amqp::session::Session;
use std::default::Default;

fn main() {
    let mut session = Session::new(Options{.. Default::default()}).unwrap();
    let channel = session.open_channel(1).unwrap();
    println!("Openned channel: {}", channel.id);

    let queue_name = "fast_response_reviews";
    loop {
    	match channel.basic_get(0, queue_name, true){
    		Ok((headers, body)) => {
			    println!("Headers: {}", headers);
			    println!("Body: {}", String::from_utf8_lossy(body.as_slice()));
    		},
    		Err(err) => {println!("Basic get error: {}", err); break;}
    	}
    }
    channel.close(200, "Bye");
    session.close(200, "Good Bye".to_string());
}