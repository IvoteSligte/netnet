use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("Packet may not be larger than MAX_PACKET_SIZE bytes")]
    PacketTooLarge(usize),

    #[error("Internal communication channel closed unexpectedly")]
    ChannelClosed,

    #[error("[Receiver::recv_timeout] request timed out")]
    Timeout,

    #[error("Stopped due to stop signal sent by user")]
    Stopped,

    #[error("Called Receiver::recv after an error was previously returned")]
    RecvAfterError,

    #[error("Called Sender::send after an error was previously returned")]
    SendAfterError,

    #[error(transparent)]
    Rustls(#[from] rustls::Error),

    // #[error(transparent)]
    // Quinn(#[from] quinn::),
}

pub type Result<T> = std::result::Result<T, Error>;
