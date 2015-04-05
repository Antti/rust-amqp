use std::convert::From;
use std::io;
use byteorder;
// use std::sync::poison::PoisonError;
// use std::sync::mutex::MutexGuard;
// use std::collections::hash::map::HashMap;

#[derive(Debug)]
pub enum AMQPError {
    AMQPIoError(io::Error),
    AMQPIoError2(byteorder::Error),
    DecodeError(&'static str),
    EncodeError,
    QueueEmpty,
    Protocol(String),
    SyncError
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

//
// impl <'a, T, U> FromError<PoisonError<MutexGuard<'a, HashMap<T, U>>>> for AMQPError {
//     fn from_error(err: PoisonError<MutexGuard<'a, HashMap<T, U>>>) -> AMQPError {
//         AMQPError::SyncError
//     }
// }
