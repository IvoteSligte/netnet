use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use log::{info, trace};
pub use wincode::{SchemaRead, SchemaWrite};

pub mod error;
pub mod packet;
pub mod receiver;
pub mod sender;

pub use error::{Error, Result};
pub use packet::*;
pub use receiver::Receiver;
pub use sender::Sender;

// TODO: reliability mechanism
// TODO: encryption
// TODO: periodic connectivity check (i.e. keepalive packets)
// TODO: buffering? probably necessary to get smooth audio

pub type TimeStamp = chrono::DateTime<chrono::Utc>;
pub type TimeDelta = chrono::TimeDelta;

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

pub(crate) fn is_timeout(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::TimedOut
}

pub fn create_client(
    connect_to: impl ToSocketAddrs,
    max_latency: TimeDelta,
    stop: Signal,
) -> io::Result<(Sender, Receiver)> {
    let socket = UdpSocket::bind("[::]:0")?;
    let connected = Signal::new();
    let receiver = Receiver::new(
        socket.try_clone()?,
        max_latency,
        connected.clone(),
        stop.clone(),
    );
    let sender = Sender::new(socket, connect_to, max_latency, connected, stop)?;
    Ok((sender, receiver))
}

pub fn create_server(
    port: u16,
    max_latency: TimeDelta,
    stop: Signal,
) -> io::Result<(Sender, Receiver)> {
    let socket = UdpSocket::bind(("::", port))?;
    info!("Bound server to port {port}");

    let connected = Signal::new();
    let receiver = Receiver::new(        socket.try_clone()?, max_latency, connected.clone(), stop.clone());
    while !receiver.is_connected() {
        trace!("Server waiting for client connection");
        continue;
    }
    info!("Server connected to client");
    let peer_addr = socket.peer_addr()?;
    let sender = Sender::new(
        socket,
        peer_addr,
        max_latency,
        connected,
        stop,
    )?;
    Ok((sender, receiver))
}

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

    pub fn clear(&self) {
        self.value.store(false, Ordering::Release);
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
