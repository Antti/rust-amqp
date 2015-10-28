use std::convert::From;
use std::{io, error, fmt};
use byteorder;
use url;

#[cfg(feature = "tls")]
use openssl;
#[cfg(feature = "tls")]
use std::error::Error;

#[derive(Debug, Clone)]
pub enum AMQPError {
    IoError(io::ErrorKind),
    DecodeError(&'static str),
    Protocol(String),
    SchemeError(String),
    UrlParseError(url::ParseError),
    ByteOrderError,
    QueueEmpty,
    SyncError,
}

impl fmt::Display for AMQPError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AMQP Error: {}", error::Error::description(self))
    }
}

impl error::Error for AMQPError {
    fn description(&self) -> &str {
        match self {
            &AMQPError::IoError(_) => "IoError",
            &AMQPError::DecodeError(_) => "Protocol decoding error",
            &AMQPError::Protocol(_) => "Protocol level error",
            &AMQPError::SchemeError(_) => "Invalid scheme",
            &AMQPError::UrlParseError(_) => "URL parsing error",
            &AMQPError::ByteOrderError => "ByteOrderError",
            &AMQPError::QueueEmpty => "Queue is empty",
            &AMQPError::SyncError => "Synchronisation error",
        }
    }
}

pub type AMQPResult<T> = Result<T, AMQPError>;

impl From<io::Error> for AMQPError {
    fn from(err: io::Error) -> AMQPError {
        AMQPError::IoError(err.kind())
    }
}

impl From<byteorder::Error> for AMQPError {
    fn from(_err: byteorder::Error) -> AMQPError {
        AMQPError::ByteOrderError
    }
}

impl From<url::ParseError> for AMQPError {
    fn from(err: url::ParseError) -> AMQPError {
        AMQPError::UrlParseError(err)
    }
}

#[cfg(feature = "tls")]
impl From<openssl::ssl::error::SslError> for AMQPError {
    fn from(err: openssl::ssl::error::SslError) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}
