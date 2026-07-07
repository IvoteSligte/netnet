use chrono::{DateTime, Utc};
use receiver::Receiver;
use std::{
    io,
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use wincode::{SchemaReadOwned, config::DefaultConfig};

pub use wincode::{SchemaRead, SchemaWrite};

mod receiver;
mod sender;

// TODO: stop signal for sender/receiver threads and streams
// TODO: reliability mechanism
// TODO: encryption

// Fixed constants
const MAX_MESSAGE_SIZE: usize = 65507;
const MESSAGE_HEADER_SIZE: usize = 8 + 4 + 2 + 2; // the encoded size of MessageHeader in bytes
const MAX_MESSAGE_BODY_SIZE: usize = MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE;

// Can be adjusted
const MAX_LATENCY_MS: f32 = 100.0;
const RECV_BUFFER_CAP: usize = 200; // max number of messages in the receive buffer
const PACKET_MAP_CAP: usize = 10; // max number of packets in the receiver packet map
const SEND_SLEEP_DURATION: Duration = Duration::from_micros(200);

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

// TODO: periodic connectivity check (i.e. keepalive packets)
// TODO: buffering? probably necessary to get smooth audio
pub struct PacketStream<P> {
    sender: mpsc::Sender<P>,
    receiver: Arc<Mutex<Receiver>>,
}

// Prevents P: Clone requirement
impl<P> Clone for PacketStream<P> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

impl<P> PacketStream<P> {
    // TODO: separate `send` thread
    pub fn new(port: u16, connect_to: SocketAddr) -> io::Result<Self>
    where
        P: SchemaWrite<DefaultConfig, Src = P> + Send + 'static,
    {
        let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))?;
        socket.connect(connect_to)?;
        Ok(Self {
            sender: sender::spawn_thread(socket.try_clone()?),
            receiver: Receiver::new(socket),
        })
    }

    pub fn send(&self, packet: P) {
        self.sender.send(packet).unwrap();
    }

    /// Receives a packet, panicking if stop has been signaled.
    pub fn recv(&self) -> anyhow::Result<(P, DateTime<Utc>)>
    where
        P: SchemaReadOwned<DefaultConfig, Dst = P>,
    {
        loop {
            match self.receiver.lock().unwrap().recv_non_blocking() {
                Ok(Some(packet)) => return Ok(packet),
                Ok(None) => continue,
                Err(err) => return Err(err),
            }
        }
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
