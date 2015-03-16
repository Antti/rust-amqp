use std::error::FromError;
use std::io;
use std::old_io::IoError;
// use std::sync::poison::PoisonError;
// use std::sync::mutex::MutexGuard;
// use std::collections::hash::map::HashMap;

#[derive(Debug)]
pub enum AMQPError {
    AMQPIoError(io::Error),
    AMQPOldIoError(IoError),
    DecodeError(&'static str),
    EncodeError,
    QueueEmpty,
    Protocol(String),
    SyncError
}

pub type AMQPResult<T> = Result<T, AMQPError>;

impl FromError<io::Error> for AMQPError {
    fn from_error(err: io::Error) -> AMQPError {
        AMQPError::AMQPIoError(err)
    }
}

impl FromError<IoError> for AMQPError {
    fn from_error(err: IoError) -> AMQPError {
        AMQPError::AMQPOldIoError(err)
    }
}
//
// impl <'a, T, U> FromError<PoisonError<MutexGuard<'a, HashMap<T, U>>>> for AMQPError {
//     fn from_error(err: PoisonError<MutexGuard<'a, HashMap<T, U>>>) -> AMQPError {
//         AMQPError::SyncError
//     }
// }
