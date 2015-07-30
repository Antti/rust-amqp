use std::convert::From;
use std::io;
use byteorder;
use url;

#[derive(Debug)]
pub enum AMQPError {
    AMQPIoError(io::Error),
    AMQPIoError2(byteorder::Error),
    DecodeError(&'static str),
    EncodeError,
    QueueEmpty,
    Protocol(String),
    SyncError,
    UrlParseError(url::ParseError)
}

pub type AMQPResult<T> = Result<T, AMQPError>;

impl From<io::Error> for AMQPError {
    fn from(err: io::Error) -> AMQPError {
        AMQPError::AMQPIoError(err)
    }
}

impl From<byteorder::Error> for AMQPError {
    fn from(err: byteorder::Error) -> AMQPError {
        AMQPError::AMQPIoError2(err)
    }
}

impl From<url::ParseError> for AMQPError {
    fn from(err: url::ParseError) -> AMQPError {
        AMQPError::UrlParseError(err)
    }
}
