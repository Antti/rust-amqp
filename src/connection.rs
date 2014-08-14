use std::io::{IoResult};
use std::io::net::tcp::TcpStream;
use framing;
use framing::{Frame, Method};

pub struct Connection {
	socket: TcpStream
}

impl Connection {
	pub fn open(host: &str, port: u16) -> IoResult<Connection> {
		let mut socket = try!(TcpStream::connect(host, port));
		try!(socket.write([b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
		Ok(Connection { socket: socket})
		//  The client opens a TCP/IP connection to the server and sends a protocol header. This is the only data
		// the client sends that is not formatted as a method.
		//  The server responds with its protocol version and other properties, including a list of the security
		// mechanisms that it supports (the Start method).
		//  The client selects a security mechanism (Start-Ok).
		//  The server starts the authentication process, which uses the SASL challenge-response model. It sends
		// the client a challenge (Secure).
		//  The client sends an authentication response (Secure-Ok). For example using the "plain" mechanism,
		// the response consist of a login name and password.
		// Advanced Message Queuing Protocol Specification v0-9-1 Page 19 of 39 Copyright (c) 2006-2008. All rights reserved. See Notice and License. General Architecture
		//  The server repeats the challenge (Secure) or moves to negotiation, sending a set of parameters such as
		// maximum frame size (Tune).
		//  The client accepts or lowers these parameters (Tune-Ok).
		//  The client formally opens the connection and selects a virtual host (Open).
		//  The server confirms that the virtual host is a valid choice (Open-Ok).
		//  The client now uses the connection as desired
	}
	pub fn close(&mut self) {
		// self.write();
		//  One peer (client or server) ends the connection (Close).
		//  The other peer hand-shakes the connection end (Close-Ok).
		//  The server and the client close their socket connection.
	}

	pub fn write(&mut self, frame: Frame) -> IoResult<()>{
		self.socket.write(frame.encode().as_slice())
	}

	pub fn send_method_frame(&mut self, channel: u16, method: &Method)  -> IoResult<()> {
		println!("Sending method {} to channel {}", method.name(), channel);
		self.write(Frame {frame_type: framing::METHOD, channel: channel, payload: framing::encode_method_frame(method) })
	}

	pub fn read(&mut self) -> IoResult<Frame> {
		let frame = Frame::decode(&mut self.socket);
		if frame.is_ok(){
			let unwrapped = frame.clone().unwrap();
			println!("Received frame: type: {}, channel: {}, size: {}", unwrapped.frame_type, unwrapped.channel, unwrapped.payload.len());
		}
		frame
	}
}
