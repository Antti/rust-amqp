use std::convert::From;
use std::{io, error, fmt};
use url;
use amq_proto;

#[cfg(feature = "tls")]
use openssl;

#[derive(Debug, Clone)]
pub enum AMQPError {
    IoError(io::ErrorKind),
    DecodeError(&'static str),
    Protocol(String),
    SchemeError(String),
    UrlParseError(url::ParseError),
    QueueEmpty,
    SyncError,
    FramingError(String),
    VHostError,
}

impl fmt::Display for AMQPError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AMQP Error: {}", error::Error::description(self))
    }
}

impl error::Error for AMQPError {
    fn description<'a>(&'a self) -> &'a str {
        match *self {
            AMQPError::IoError(_) => "IoError",
            AMQPError::DecodeError(err) => err,
            AMQPError::Protocol(ref err) => err,
            AMQPError::SchemeError(ref err) => err,
            AMQPError::UrlParseError(_) => "URL parsing error",
            AMQPError::QueueEmpty => "Queue is empty",
            AMQPError::SyncError => "Synchronisation error",
            AMQPError::FramingError(ref err) => err,
            AMQPError::VHostError => "Access to vhost is denied for a current user",
        }
    }
}

pub type AMQPResult<T> = Result<T, AMQPError>;

impl From<io::Error> for AMQPError {
    fn from(err: io::Error) -> AMQPError {
        AMQPError::IoError(err.kind())
    }
}

impl <T> From<::std::sync::PoisonError<T>> for AMQPError {
    fn from(_ : ::std::sync::PoisonError<T>) -> AMQPError {
        AMQPError::SyncError
    }
}

impl From<url::ParseError> for AMQPError {
    fn from(err: url::ParseError) -> AMQPError {
        AMQPError::UrlParseError(err)
    }
}

impl From<amq_proto::Error> for AMQPError {
    fn from(err: amq_proto::Error) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}

#[cfg(feature = "tls")]
impl From<openssl::ssl::Error> for AMQPError {
    fn from(err: openssl::ssl::Error) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}

#[cfg(feature = "tls")]
impl<T: fmt::Debug> From<openssl::ssl::HandshakeError<T>> for AMQPError {
    fn from(err: openssl::ssl::HandshakeError<T>) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}

#[cfg(feature = "tls")]
impl From<openssl::error::ErrorStack> for AMQPError {
    fn from(err: openssl::error::ErrorStack) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}
