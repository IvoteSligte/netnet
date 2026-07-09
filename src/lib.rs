use std::{
    io,
    net::{SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use wincode::{SchemaReadOwned, config::DefaultConfig};

pub use wincode::{SchemaRead, SchemaWrite};

pub mod hole_punch;
pub mod receiver;
pub mod sender;
pub use receiver::Receiver;
pub use sender::Sender;

// TODO: reliability mechanism
// TODO: encryption
// TODO: periodic connectivity check (i.e. keepalive packets)
// TODO: buffering? probably necessary to get smooth audio

// Fixed constants
const MAX_MESSAGE_SIZE: usize = 65507;
const MESSAGE_HEADER_SIZE: usize = 8 + 4 + 2 + 2; // the encoded size of MessageHeader in bytes
const MAX_MESSAGE_BODY_SIZE: usize = MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE;

// Can be adjusted
const RECV_BUFFER_CAP: usize = 200; // max number of messages in the receive buffer
const PACKET_MAP_CAP: usize = 10; // max number of packets in the receiver packet map
const SEND_SLEEP_DURATION: Duration = Duration::from_micros(200);
// How frequently the receiver checks if the connection should be closed
const READ_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    HolePunch(stunclient::Error),
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

impl From<stunclient::Error> for Error {
    fn from(value: stunclient::Error) -> Self {
        Self::HolePunch(value)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(error) => error.fmt(f),
            Error::HolePunch(error) => write!(f, "Hole-punch failed: {error}"),
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

// NOTE: make sure there is no implicit padding to prevent encoding/decoding mismatches
#[derive(Debug, SchemaRead, SchemaWrite)]
struct MessageHeader {
    packet_timestamp: i64,
    packet_id: u32,
    message_id: u16,
    last_message_in_packet: u16,
}

struct PacketInfo {
    timestamp: i64,
    id: u32,
    bytes: Vec<u8>,
    found: Vec<bool>,
    num_found: usize,
}

pub type TimeStamp = chrono::DateTime<chrono::Utc>;

pub fn now() -> TimeStamp {
    chrono::Utc::now()
}

/// Trait automatically implemented for all types implementing [Send], [wincode::SchemaReadOwned], and [wincode::SchemaWrite]
pub trait Packet:
    SchemaReadOwned<DefaultConfig, Dst = Self> + SchemaWrite<DefaultConfig, Src = Self> + Send + 'static
{
}

impl<P> Packet for P where
    P: SchemaReadOwned<DefaultConfig, Dst = Self>
        + SchemaWrite<DefaultConfig, Src = Self>
        + Send
        + 'static
{
}

pub use hole_punch::create_stream_using_hole_punch;

pub fn create_stream_from_socket<P: Packet>(
    socket: UdpSocket,
    connect_to: SocketAddr,
    max_latency: Duration,
    stop: Signal,
) -> io::Result<(Sender<P>, Receiver)> {
    socket.connect(connect_to)?;
    Ok((
        Sender::new(socket.try_clone()?),
        Receiver::new(socket, stop, max_latency),
    ))
}

pub fn create_stream<P: Packet>(
    port: u16,
    connect_to: SocketAddr,
    max_latency: Duration,
    stop: Signal,
) -> io::Result<(Sender<P>, Receiver)> {
    create_stream_from_socket(
        UdpSocket::bind(("::", port))?,
        connect_to,
        max_latency,
        stop,
    )
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
