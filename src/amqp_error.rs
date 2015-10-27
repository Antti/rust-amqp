use std::convert::From;
use std::io;
use byteorder;
use url;

#[cfg(feature = "tls")]
use openssl;
#[cfg(feature = "tls")]
use std::error::Error;

#[derive(Debug, Clone)]
pub enum AMQPError {
    AMQPIoError(io::ErrorKind),
    ByteOrderError,
    DecodeError(&'static str),
    EncodeError,
    QueueEmpty,
    Protocol(String),
    SyncError,
    UrlParseError(url::ParseError),
}

pub type AMQPResult<T> = Result<T, AMQPError>;

impl From<io::Error> for AMQPError {
    fn from(err: io::Error) -> AMQPError {
        AMQPError::AMQPIoError(err.kind())
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
