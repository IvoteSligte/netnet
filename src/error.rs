use std::io;
use std::sync::{Mutex, mpsc};
use std::thread::JoinHandle;

use crate::{MAX_PACKET_SIZE, spsc};

pub type ThreadHandle = Mutex<Option<JoinHandle<Result<()>>>>;

/// Returns [None] if the thread's result has already been extracted.
pub fn take_thread_error(handle: &ThreadHandle) -> Option<Error> {
    let join_handle: JoinHandle<_> = (&mut *handle.lock().unwrap()).take()?;
    assert!(join_handle.is_finished());
    let result: Result<()> = join_handle.join().unwrap();
    Some(result.unwrap_err())
}

#[derive(Debug)]
pub enum Error {
    /// Failed to write to socket or read from socket
    Io(io::Error),
    /// Packet may not be larger than MAX_PACKET_BODY_SIZE bytes.
    PacketTooLarge(usize),
    /// Failed to deserialize UDP packet to [Packet](crate::Packet)
    Deserialize(std::array::TryFromSliceError),
    /// Internal spsc channel closed
    ChannelClosed,
    /// [Receiver::recv_timeout] request timed out.
    Timeout,
    /// Stopped due to stop signal sent by user
    Stopped,
    /// Called Receiver::recv after an error was previously returned
    RecvAfterError,
    /// Called Sender::send after an error was previously returned    
    SendAfterError,
}

impl Error {
    pub fn io_kind(&self) -> Option<io::ErrorKind> {
        match self {
            Error::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<mpsc::RecvError> for Error {
    fn from(_value: mpsc::RecvError) -> Self {
        Self::Stopped
    }
}

impl From<spsc::RecvError> for Error {
    fn from(_value: spsc::RecvError) -> Self {
        Self::Stopped
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(value: std::array::TryFromSliceError) -> Self {
        Self::Deserialize(value)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(error) => error.fmt(f),
            Error::Stopped => f.write_str("Stop signaled"),
            Error::ChannelClosed => f.write_str("Mspc channel closed"),
            Error::Deserialize(error) => write!(f, "Failed to deserialize Packet: {error}"),
            Error::Timeout => f.write_str("Call to Receiver::recv_timeout timed out"),
            Error::PacketTooLarge(size) => {
                write!(f, "Packet is too large: {size} > {MAX_PACKET_SIZE}")
            }
            Error::RecvAfterError => {
                f.write_str("Called Receiver::recv after it previously returned an error")
            }
            Error::SendAfterError => {
                f.write_str("Called Sender::send after it previously returned an error")
            }
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
