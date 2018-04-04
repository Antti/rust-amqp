use std::net::TcpStream;
use std::io::Write;
use std::sync::Arc;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::RwLock;
use std::cmp;

#[cfg(feature = "tls")]
use openssl::ssl::{SslMethod, SslStream, SslConnectorBuilder};

use amqp_error::AMQPResult;
use amqp_error::AMQPError;
use amq_proto::{Frame, FrameType, FramePayload};

#[derive(Clone)]
enum AMQPStream {
    Cleartext(Arc<RwLock<TcpStream>>),
    #[cfg(feature = "tls")]
    Tls(Arc<RwLock<SslStream<TcpStream>>>),
}

pub struct Connection {
    socket: AMQPStream,
    pub frame_max_limit: u32,
}

impl Clone for Connection {
    fn clone(&self) -> Connection {
        Connection {
            socket: self.socket.clone(),
            frame_max_limit: self.frame_max_limit,
        }
    }
}

impl Connection {
    #[cfg(feature = "tls")]
    pub fn open_tls(host: &str, port: u16) -> AMQPResult<Connection> {
        let socket = TcpStream::connect((host, port))?;
        let connector = match SslConnectorBuilder::new(SslMethod::tls()) {
            Ok(builder) => builder.build(),
            Err(_) => return Err(AMQPError::Protocol(String::from("Unable to connect to the provided host")))
        };
        let mut tls_socket = match connector.danger_connect_without_providing_domain_for_certificate_verification_and_server_name_indication(socket) {
            Ok(tls) => tls,
            Err(_) => return Err(AMQPError::Protocol(String::from("Unable to connect to the provided host")))
        };
        init_connection(&mut tls_socket)?;
        Ok(Connection {
            socket: AMQPStream::Tls(Arc::new(RwLock::new(tls_socket))),
            frame_max_limit: 131072,
        })
    }

    pub fn open(host: &str, port: u16) -> AMQPResult<Connection> {
        let mut socket = TcpStream::connect((host, port))?;
        init_connection(&mut socket)?;
        Ok(Connection {
            socket: AMQPStream::Cleartext(Arc::new(RwLock::new(socket))),
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
                    self.write_frame(Frame {
                        frame_type: frame_type,
                        channel: channel,
                        payload: FramePayload::new(content_frame),
                    })?
                }
                Ok(())
            }
            _ => self.write_frame(frame),
        }
    }

    pub fn read(&mut self) -> AMQPResult<Frame> {
        match self.socket {
            AMQPStream::Cleartext(ref mut stream) => {
                let mut s = match stream.write() {
                    Ok(s) => s,
                    _ => return Err(AMQPError::FramingError(String::from("unable to access stream context"))),
                };
                let mut tcp_s: &TcpStream = s.deref();
                Frame::decode(&mut tcp_s).map_err(From::from)
            },
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref mut stream) => {
                let mut s = match stream.write() {
                    Ok(s) => s,
                    _ => return Err(AMQPError::FramingError(String::from("unable to access stream context"))),
                };
                let mut tls_s : &mut SslStream<TcpStream> = s.deref_mut();
                Frame::decode(tls_s).map_err(From::from)
            },
        }
    }

    fn write_frame(&mut self, frame: Frame) -> AMQPResult<()> {
        match self.socket {
            AMQPStream::Cleartext(ref mut stream) => {
                let mut s = match stream.write() {
                    Ok(s) => s,
                    _ => return Err(AMQPError::FramingError(String::from("unable to access stream context"))),
                };
                Ok(s.write_all(&frame.encode()?)?)
            }
            #[cfg(feature = "tls")]
            AMQPStream::Tls(ref mut stream) => {
                let mut s = match stream.write() {
                    Ok(s) => s,
                    _ => return Err(AMQPError::FramingError(String::from("unable to access stream context"))),
                };
                Ok(s.write_all(&frame.encode()?)?)
            },
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
