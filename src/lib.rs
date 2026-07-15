use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use log::info;

pub mod error;
pub mod packet;
pub mod receiver;
pub mod sender;
pub mod spsc;

pub use error::{Error, Result};
pub use packet::*;
pub use receiver::Receiver;
pub use sender::Sender;

// TODO: encryption
// TODO: buffering? probably necessary to get smooth audio

pub(crate) fn is_timeout(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::TimedOut
}

pub fn create_client(
    connect_to: impl ToSocketAddrs,
    stop: Signal,
    label: Option<&'static str>,
) -> io::Result<(Sender, Receiver)> {
    let socket = UdpSocket::bind("[::]:0")?;
    info!("Bound client to random port");
    let connected = Signal::new();
    let receiver = Receiver::new(socket.try_clone()?, connected.clone(), stop.clone(), label)?;
    info!("Created receiver for client");
    let sender = Sender::new(socket, connect_to, connected, stop, label)?;
    info!("Created sender for client");
    Ok((sender, receiver))
}

/// Use [Receiver::accept] to get a [Sender] for the connection as soon as a client connects.
pub fn create_server(port: u16, stop: Signal, label: Option<&'static str>) -> io::Result<Receiver> {
    let socket = UdpSocket::bind(("::", port))?;
    info!("Bound server to port {port}");

    let connected = Signal::new();
    let receiver = Receiver::new(socket.try_clone()?, connected.clone(), stop.clone(), label)?;
    info!("Created receiver for server");
    Ok(receiver)
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
