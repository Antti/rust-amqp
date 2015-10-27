use std::net::TcpStream;
use std::io::Write;
use std::cmp;

#[cfg(feature = "tls")]
use openssl::ssl::{SslContext, SslMethod, SslStream};

#[cfg(not(feature = "tls"))]
use url;

#[cfg(not(feature = "tls"))]
use amqp_error::AMQPError;

use amqp_error::AMQPResult;
use framing::{Frame, FrameType};

enum AMQPStream {
    Cleartext(TcpStream),
    #[cfg(feature = "tls")]
    Tls(SslStream<TcpStream>),
}

pub struct Connection {
    socket: AMQPStream,
    pub frame_max_limit: u32,
}

impl Clone for Connection {
    fn clone(&self) -> Connection {
        match self.socket {
            AMQPStream::Cleartext(ref stream) => Connection {
                socket: AMQPStream::Cleartext(stream.try_clone().unwrap()),
                frame_max_limit: self.frame_max_limit,
            },
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref stream) => Connection {
                socket: AMQPStream::Tls(stream.try_clone().unwrap()),
                frame_max_limit: self.frame_max_limit,
            },
        }
    }
}

impl Connection {
    #[cfg(feature = "tls")]
    pub fn open(host: &str, port: u16, use_tls: bool) -> AMQPResult<Connection> {
        let connection;
        let mut socket = try!(TcpStream::connect((host, port)));
        if use_tls {
            let ctx = try!(SslContext::new(SslMethod::Sslv23));
            let mut tls_socket = try!(SslStream::connect(&ctx, socket));
            try!(tls_socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
            connection = Connection {
                socket: AMQPStream::Tls(tls_socket),
                frame_max_limit: 131072,
            };
        } else {
            try!(socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
            connection = Connection {
                socket: AMQPStream::Cleartext(socket),
                frame_max_limit: 131072,
            };
        }
        Ok(connection)
    }

    #[cfg(not(feature = "tls"))]
    pub fn open(host: &str, port: u16, use_tls: bool) -> AMQPResult<Connection> {
        if use_tls {
            return Err(AMQPError::UrlParseError(url::ParseError::InvalidScheme));
        }
        let mut socket = try!(TcpStream::connect((host, port)));
        try!(socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]));
        let connection = Connection {
            socket: AMQPStream::Cleartext(socket),
            frame_max_limit: 131072,
        };
        Ok(connection)
    }


    pub fn write(&mut self, frame: Frame) -> AMQPResult<()> {
        match frame.frame_type {
            FrameType::BODY => {
                // TODO: Check if need to include frame header + end octet into calculation. (9
                // bytes extra)
                for content_frame in split_content_into_frames(frame.payload,
                                                               self.frame_max_limit)
                                         .into_iter() {
                    try!(self.write_frame(Frame {
                        frame_type: frame.frame_type,
                        channel: frame.channel,
                        payload: content_frame,
                    }))
                }
                Ok(())
            }
            _ => self.write_frame(frame),
        }
    }

    pub fn read(&mut self) -> AMQPResult<Frame> {
        match self.socket {
            AMQPStream::Cleartext(ref mut stream) => Frame::decode(stream),
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref mut stream) => Frame::decode(stream),
        }
    }

    fn write_frame(&mut self, frame: Frame) -> AMQPResult<()> {
        match self.socket {
            AMQPStream::Cleartext(ref mut stream) =>
                Ok(try!(stream.write_all(&try!(frame.encode())))),
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref mut stream) => Ok(try!(stream.write_all(&try!(frame.encode())))),
        }
    }

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
