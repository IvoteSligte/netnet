use std::{
    io,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
};

use log::{debug, info};
pub use wincode::{SchemaRead, SchemaWrite};

mod message;
mod packet;
pub use packet::{Receiver, Sender};

// TODO: reliability mechanism
// TODO: encryption
// TODO: periodic connectivity check (i.e. keepalive packets)
// TODO: buffering? probably necessary to get smooth audio

pub type TimeStamp = chrono::DateTime<chrono::Utc>;
pub use chrono::TimeDelta;

pub const CONNECT_MAGIC: &[u8] = b"MxBAyWf2kwXYia5oAhaBebK6hLgNoMd4u3y8GCbQA8c=";

// Creates a timestamp for the current time, rounded to the nearest microsecond
// so that sender and receiver timestamps are exactly equal.
pub fn now() -> TimeStamp {
    from_micros(to_micros(chrono::Utc::now()))
}

pub fn now_micros() -> i64 {
    to_micros(now())
}

pub fn to_micros(timestamp: TimeStamp) -> i64 {
    timestamp.timestamp_micros()
}

pub fn from_micros(micros: i64) -> TimeStamp {
    TimeStamp::from_timestamp_micros(micros).unwrap()
}

pub fn since(timestamp: TimeStamp) -> TimeDelta {
    now() - timestamp
}

pub fn since_micros(micros: i64) -> TimeDelta {
    since(from_micros(micros))
}

pub fn create_client(
    connect_to: impl ToSocketAddrs,
    max_latency: TimeDelta,
    stop: Signal,
) -> io::Result<(packet::Sender, packet::Receiver)> {
    let socket = UdpSocket::bind("[::]:0")?;
    socket.connect(connect_to)?;
    debug!("Set client socket connection to server");
    let mut buf = [0u8; CONNECT_MAGIC.len()];
    loop {
        debug!("Sending MAGIC to server");
        socket.send(CONNECT_MAGIC)?;
        debug!("Waiting for MAGIC from server");
        let num_read = socket.recv(&mut buf)?;
        if num_read == CONNECT_MAGIC.len() && &buf == CONNECT_MAGIC {
            info!("Client connected to server");
            break;
        }
        debug!("Non-MAGIC packet received as client");
    }
    let sender = packet::Sender::new(socket.try_clone()?, max_latency, stop.clone());
    let receiver = packet::Receiver::new(socket, max_latency, stop);
    Ok((sender, receiver))
}

pub fn create_server(
    port: u16,
    max_latency: TimeDelta,
    stop: Signal,
) -> io::Result<(packet::Sender, packet::Receiver)> {
    let socket = UdpSocket::bind(("::", port))?;
    debug!("Bound server to port {port}");
    let mut buf = [0u8; CONNECT_MAGIC.len()];
    debug!("Waiting for MAGIC from client");
    let client_addr = loop {
        let (num_read, client_addr) = socket.recv_from(&mut buf)?;
        if &buf[..num_read] != CONNECT_MAGIC {
            debug!("Non-MAGIC packet received as server");
        }
        break client_addr;
    };
    info!("Server received MAGIC from client");
    socket.connect(client_addr)?;
    debug!("Set server socket connection");
    debug!("Waiting for ACK_MAGIC from client");
    loop {
        socket.send(CONNECT_MAGIC)?;
        let num_read = socket.recv(&mut buf)?;
        if &buf[..num_read] == ACK_MAGIC {
            break;
        }
        if &buf[..num_read] == CONNECT_MAGIC {
            continue;
        }
        return Err(io::Error::other("Expected ACK_MAGIC from client"));
    }

    let sender = packet::Sender::new(socket.try_clone()?, max_latency, stop.clone());
    let receiver = packet::Receiver::new(socket, max_latency, stop);
    Ok((sender, receiver))
}

type ThreadHandle = Mutex<Option<JoinHandle<crate::Result<()>>>>;

/// Returns [None] if the thread's result has already been extracted.
fn take_thread_error(handle: &ThreadHandle) -> Option<crate::Error> {
    let join_handle: JoinHandle<_> = (&mut *handle.lock().unwrap()).take()?;
    assert!(join_handle.is_finished());
    let result: crate::Result<()> = join_handle.join().unwrap();
    Some(result.unwrap_err())
}

#[derive(Debug)]
pub enum Error {
    /// Failed to write to socket or read from socket
    Io(io::Error),
    /// Failed to serialize packet or message
    Serialize(wincode::WriteError),
    /// Failed to deserialize packet or message    
    Deserialize(wincode::ReadError),
    /// Internal mpsc channel closed
    ChannelClosed,
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

impl From<wincode::WriteError> for Error {
    fn from(value: wincode::WriteError) -> Self {
        Self::Serialize(value)
    }
}

impl From<wincode::ReadError> for Error {
    fn from(value: wincode::ReadError) -> Self {
        Self::Deserialize(value)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(error) => error.fmt(f),
            Error::Serialize(error) => error.fmt(f),
            Error::Deserialize(error) => error.fmt(f),
            Error::Stopped => f.write_str("Stop signaled"),
            Error::ChannelClosed => f.write_str("Mspc channel closed"),
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

#[derive(Default, Clone)]
pub struct Signal {
    value: Arc<AtomicBool>,
}

impl Signal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self) {
        self.value.store(true, Ordering::Release);
    }

    pub fn get(&self) -> bool {
        self.value.load(Ordering::Acquire)
    }
}

pub(crate) struct RunningAverage {
    value: f32,
    samples: f32,
    convergence_window: f32,
}

impl RunningAverage {
    pub fn new(convergence_window: f32) -> Self {
        assert!(convergence_window >= 1.0);
        Self {
            value: 0.0,
            samples: 1.0,
            convergence_window,
        }
    }

    pub fn update(&mut self, sample: f32) {
        let weight = 1.0 / self.samples;
        self.value = self.value * (1.0 - weight) + sample * weight;
        self.samples = f32::min(self.samples + 1.0, self.convergence_window);
    }

    pub fn get(&self) -> f32 {
        self.value
    }
}
