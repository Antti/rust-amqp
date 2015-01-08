use amqp_error::AMQPResult;
use std::io::net::tcp::TcpStream;
use framing::Frame;
use std::error::FromError;

#[derive(Clone)]
pub struct Connection {
    socket: TcpStream,
    pub frame_max_limit: u32
}

impl Connection {
    pub fn open(host: &str, port: u16) -> AMQPResult<Connection> {
        let mut socket = try!(TcpStream::connect((host, port)));
        try!(socket.write(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let connection = Connection { socket: socket, frame_max_limit: 131072 };
        Ok(connection)
    }

    pub fn close(&mut self) {
        self.socket.close_write().unwrap();
        self.socket.close_read().unwrap();
    }

    pub fn write(&mut self, frame: Frame) -> AMQPResult<()>{
        self.socket.write(frame.encode().as_slice()).map_err(|err| FromError::from_error(err))
    }

    pub fn read(&mut self) -> AMQPResult<Frame> {
        Frame::decode(&mut self.socket)
    }

}

