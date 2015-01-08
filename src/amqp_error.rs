use std::error::FromError;
use std::io::IoError;

#[derive(Show)]
pub enum AMQPError {
    AMQPIoError(IoError),
    DecodeError(&'static str),
    EncodeError,
    QueueEmpty,
    SyncError
}

pub type AMQPResult<T> =  Result<T, AMQPError>;

impl FromError<IoError> for AMQPError {
    fn from_error(err: IoError) -> AMQPError {
        AMQPError::AMQPIoError(err)
    }
}
