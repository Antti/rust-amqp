use std::net::TcpStream;
use std::io::Write;
use std::cmp;
use std::time;

#[cfg(feature = "tls")]
use openssl::ssl::{SslConnectorBuilder, SslMethod, SslStream};

use amqp_error::AMQPResult;
use amq_proto::{Frame, FrameType, FramePayload};

enum AMQPStream {
    Cleartext(TcpStream),
    #[cfg(feature = "tls")]
    Tls(SslStream<TcpStream>),
}

pub struct Connection {
    socket: AMQPStream,
    pub frame_max_limit: u32,
}

impl Connection {
    #[cfg(feature = "tls")]
    pub fn open_tls(host: &str, port: u16) -> AMQPResult<Connection> {
        let socket = try!(TcpStream::connect((host, port)));
        socket.set_read_timeout(Some(time::Duration::from_millis(75)));
        let connector = SslConnectorBuilder::new(SslMethod::tls()).unwrap().build();

        let mut tls_socket = try!(connector.connect(host, socket));

        try!(init_connection(&mut tls_socket));
        Ok(Connection {
            socket: AMQPStream::Tls(tls_socket),
            frame_max_limit: 131072,
        })
    }

    pub fn open(host: &str, port: u16) -> AMQPResult<Connection> {
        let mut socket = try!(TcpStream::connect((host, port)));
        try!(init_connection(&mut socket));
        Ok(Connection {
            socket: AMQPStream::Cleartext(socket),
            frame_max_limit: 131072,
        })
    }


    pub fn write(&mut self, frame: Frame) -> AMQPResult<()> {
        match frame.frame_type {
            FrameType::BODY => {
                // TODO: Check if need to include frame header + end octet into calculation. (9
                // bytes extra)
                let frame_type = frame.frame_type;
                let channel = frame.channel;
                for content_frame in split_content_into_frames(frame.payload.into_inner(),
                                                               self.frame_max_limit)
                    .into_iter() {
                    try!(self.write_frame(Frame {
                        frame_type: frame_type,
                        channel: channel,
                        payload: FramePayload::new(content_frame),
                    }))
                }
                Ok(())
            }
            _ => self.write_frame(frame),
        }
    }

    pub fn read(&mut self) -> AMQPResult<Frame> {
        match self.socket {
            AMQPStream::Cleartext(ref mut stream) => {
                Frame::decode(stream).map_err(From::from)
            },
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref mut stream) => {
                Frame::decode(stream).map_err(From::from)
            }
        }
    }

    fn write_frame(&mut self, frame: Frame) -> AMQPResult<()> {
        match self.socket {
            AMQPStream::Cleartext(ref mut stream) => {
                Ok(try!(stream.write_all(&try!(frame.encode()))))
            }
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref mut stream) => {
                Ok(try!(stream.write_all(&try!(frame.encode()))))
            }

        }
    }
}

fn init_connection<T>(stream: &mut T) -> AMQPResult<()>
    where T: Write
{
    stream.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]).map_err(From::from)
}

fn split_content_into_frames(content: Vec<u8>, frame_limit: u32) -> Vec<Vec<u8>> {
    assert!(frame_limit > 0, "Can't have frame_max_limit == 0");
    let mut content_frames = vec![];
    let mut current_pos = 0;
    while current_pos < content.len() {
        let new_pos = current_pos + cmp::min(content.len() - current_pos, frame_limit as usize);
        content_frames.push(content[current_pos..new_pos].to_vec());
        current_pos = new_pos;
    }
    content_frames
}


#[test]
fn test_split_content_into_frames() {
    let content = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let frames = split_content_into_frames(content, 3);
    assert_eq!(frames,
               vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9], vec![10]]);
}
