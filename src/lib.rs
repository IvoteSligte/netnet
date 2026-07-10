use std::{
    io,
    net::{SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub use wincode::{SchemaRead, SchemaWrite};

mod reliable;
mod message;
mod packet;

// TODO: reliability mechanism
// TODO: encryption
// TODO: periodic connectivity check (i.e. keepalive packets)
// TODO: buffering? probably necessary to get smooth audio

pub type TimeStamp = chrono::DateTime<chrono::Utc>;
pub use chrono::TimeDelta;

// Creates a timestamp for the current time, rounded to the nearest microsecond
// so that sender and receiver timestamps are exactly equal.
pub fn now() -> TimeStamp {
    TimeStamp::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap()
}

pub fn create_stream_from_socket(
    socket: UdpSocket,
    connect_to: SocketAddr,
    max_latency: Duration,
    stop: Signal,
) -> io::Result<(packet::Sender, packet::Receiver)> {
    packet::create_stream(socket, stop)
}

pub fn create_stream(
    port: u16,
    connect_to: SocketAddr,
    max_latency: Duration,
    stop: Signal,
) -> io::Result<(packet::Sender, packet::Receiver)> {
    create_stream_from_socket(
        UdpSocket::bind(("::", port))?,
        connect_to,
        max_latency,
        stop,
    )
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Stopped,
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

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(error) => error.fmt(f),
            Error::Stopped => f.write_str("Stop signaled"),
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
