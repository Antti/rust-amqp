use std::io::{IoResult};
use std::io::net::tcp::TcpStream;
use framing::Frame;

#[deriving(Clone)]
pub struct Connection {
    socket: TcpStream,
    pub frame_max_limit: u32
}

impl Connection {
    pub fn open(host: &str, port: u16) -> IoResult<Connection> {
        let mut socket = try!(TcpStream::connect(host, port));
        try!(socket.write([b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let connection = Connection { socket: socket, frame_max_limit: 131072 };
        Ok(connection)
    }

    pub fn close(&mut self) {
        self.socket.close_write().unwrap();
        self.socket.close_read().unwrap();
        //TODO: Need to drop socket somehow (Maybe have an Option<Socket>)
    }

    pub fn write(&mut self, frame: Frame) -> IoResult<()>{
        self.socket.write(frame.encode().as_slice())
    }

    pub fn read(&mut self) -> IoResult<Frame> {
        Frame::decode(&mut self.socket)
    }

}

