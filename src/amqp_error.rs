use std::error::FromError;
use std::io::IoError;

#[deriving(Show)]
pub enum AMQPError {
    AMQPIoError(IoError),
    DecodeError(&'static str),
    EncodeError,
    QueueEmpty
}

pub type AMQPResult<T> =  Result<T, AMQPError>;

impl FromError<IoError> for AMQPError {
    fn from_error(err: IoError) -> AMQPError {
        AMQPError::AMQPIoError(err)
    }
}
