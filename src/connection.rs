use std::io::{IoResult};
use std::io::net::tcp::TcpStream;
use framing;
use framing::{Frame, MethodFrame};
use protocol;

pub struct Connection {
    socket: TcpStream,
    pub frame_max_limit: u32
}

impl Connection {
    // pub fn from_url(url_string: &str) -> IoResult<Connection> {
    //     let url = URL::parse(url_string);
    //     let opts = Options { host: url.host, port: url.port, login: url.login, password: url.password, vhost: url.path, ..Options::default()};
    //     Connection::open(opts)
    // }

    pub fn open(host: &str, port: u16) -> IoResult<Connection> {
        let mut socket = try!(TcpStream::connect(host, port));
        try!(socket.write([b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let connection = Connection { socket: socket, frame_max_limit: 131072 };
        Ok(connection)
    }

    pub fn close(&mut self, reply_code: u16, reply_text: String) {
        let close = protocol::connection::Close{reply_code: reply_code, reply_text: reply_text, class_id: 0, method_id: 0};
        let close_ok : protocol::connection::CloseOk = self.rpc(0, &close, "connection.close-ok").unwrap();

        self.socket.close_write().unwrap();
        self.socket.close_read().unwrap();
        //TODO: Need to drop socket somehow (Maybe have an Option<Socket>)
    }

    pub fn send_method_frame(&mut self, channel: u16, method: &protocol::Method)  -> IoResult<()> {
        println!("Sending method {} to channel {}", method.name(), channel);
        self.write(Frame {frame_type: framing::METHOD, channel: channel, payload: MethodFrame::encode_method(method) })
    }

    pub fn rpc<T: protocol::Method>(&mut self, channel: u16, method: &protocol::Method, expected_reply: &str) -> IoResult<T> {
        let method_frame = try!(self.raw_rpc(channel, method));
        match method_frame.method_name() {
            m_name if m_name == expected_reply => protocol::Method::decode(method_frame),
            m_name => fail!("Unexpected method frame: {}, expected: {}", m_name, expected_reply)
        }
    }

    pub fn raw_rpc(&mut self, channel: u16, method: &protocol::Method) -> IoResult<MethodFrame> {
        self.send_method_frame(channel, method).unwrap();
        self.read().map(|frame| MethodFrame::decode(frame))
    }

    pub fn write(&mut self, frame: Frame) -> IoResult<()>{
        self.socket.write(frame.encode().as_slice())
    }

    pub fn read(&mut self) -> IoResult<Frame> {
        Frame::decode(&mut self.socket)
    }

    // pub fn reading_loop(&mut self){
    //     loop {
    //         let frame = self.read();
    //         // Handle heartbeats
    //         // Dispatch frame to the given channel.
    //     }
    // }
}

