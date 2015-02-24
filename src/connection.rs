use amqp_error::AMQPResult;
use std::old_io::TcpStream;
use std::io::Write;
use framing::Frame;

#[derive(Clone)]
pub struct Connection {
    socket: TcpStream,
    pub frame_max_limit: u32
}

impl Connection {
    pub fn open(host: &str, port: u16) -> AMQPResult<Connection> {
        let mut socket = try!(TcpStream::connect((host, port)));
        try!(socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let connection = Connection { socket: socket, frame_max_limit: 131072 };
        Ok(connection)
    }

    pub fn write(&mut self, frame: Frame) -> AMQPResult<()>{
        Ok(try!(self.socket.write_all(&frame.encode())))
    }

    pub fn read(&mut self) -> AMQPResult<Frame> {
        Frame::decode(&mut self.socket)
    }

}
