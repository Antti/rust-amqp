use std::old_io::TcpStream;
use std::io::Write;
use std::cmp;

use amqp_error::AMQPResult;
use framing::{Frame, FrameType};

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
        match frame.frame_type {
            FrameType::BODY => {
                //TODO: Check if need to include frame header + end octet into calculation. (9 bytes extra)
                for content_frame in split_content_into_frames(frame.payload, self.frame_max_limit).into_iter() {
                    try!(self.write_frame(Frame { frame_type: frame.frame_type, channel: frame.channel, payload: content_frame}))
                }
                Ok(())
            },
            _ => self.write_frame(frame)
        }
    }

    pub fn read(&mut self) -> AMQPResult<Frame> {
        Frame::decode(&mut self.socket)
    }

    fn write_frame(&mut self, frame: Frame) -> AMQPResult<()>{
        Ok(try!(self.socket.write_all(&frame.encode())))
    }

}


fn split_content_into_frames(content: Vec<u8>, frame_limit: u32) -> Vec<Vec<u8>> {
    assert!(frame_limit > 0, "Can't have frame_max_limit == 0");
    let mut content_frames = vec!();
    let mut current_pos = 0;
    while current_pos < content.len() {
        let new_pos = current_pos + cmp::min(content.len() - current_pos, frame_limit as usize);
        content_frames.push(content[current_pos .. new_pos].to_vec());
        current_pos = new_pos;
    }
    content_frames
}


#[test]
fn test_split_content_into_frames() {
    let content = vec!(1,2,3,4,5,6,7,8,9,10);
    let frames = split_content_into_frames(content, 3);
    assert_eq!(frames, vec!(vec!(1, 2, 3), vec!(4, 5, 6), vec!(7, 8, 9), vec!(10)));
}