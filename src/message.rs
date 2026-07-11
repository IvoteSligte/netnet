use std::{
    io,
    net::UdpSocket,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use log::info;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    Error, Signal, ThreadHandle, TimeDelta, TimeStamp, since, since_micros, take_thread_error,
};

pub const MAX_SIZE: usize = 65507;
pub const HEADER_SIZE: usize = 8 + 4 + 2 + 2; // the encoded size of MessageHeader in bytes
pub const MAX_BODY_SIZE: usize = MAX_SIZE - HEADER_SIZE;

#[derive(Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct Message {
    pub packet_timestamp: i64,
    pub packet_id: u32,
    pub id: u16,
    pub last_message_in_packet: u16,
    pub body: Vec<u8>,
}

const CONNECT: Message = Message {
    packet_timestamp: i64::MAX,
    packet_id: u32::MAX,
    id: u16::MAX,
    last_message_in_packet: 0,
    body: Vec::new(),
};

const KEEPALIVE: Message = Message {
    packet_timestamp: i64::MAX,
    packet_id: u32::MAX,
    id: u16::MAX - 1,
    last_message_in_packet: 0,
    body: Vec::new(),
};

#[derive(Clone)]
pub struct Sender {
    channel: mpsc::Sender<Message>,
    thread_handle: Arc<ThreadHandle>,
}

impl Sender {
    pub fn new(socket: UdpSocket, max_latency: TimeDelta, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = mpsc::channel::<Message>();
        let thread_handle = std::thread::spawn(move || {
            let mut last_sent_at = TimeStamp::UNIX_EPOCH;
            while !stop.get() {
                let message = match channel_receiver.recv_timeout(Duration::from_micros(100)) {
                    Ok(message) if since_micros(message.packet_timestamp) < max_latency => message,
                    Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                        if since(last_sent_at) > TimeDelta::microseconds(200) {
                            KEEPALIVE
                        } else {
                            continue;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Err(Error::ChannelClosed),
                };
                if since_micros(message.packet_timestamp) > max_latency {
                    continue;
                }
                let bytes = wincode::serialize(&message)?;
                socket.send(&bytes)?;
                last_sent_at = crate::now();
            }
            Ok(())
        });
        Self {
            channel: channel_sender,
            thread_handle: Arc::new(Mutex::new(Some(thread_handle))),
        }
    }

    pub fn send(&self, message: Message) -> crate::Result<()> {
        self.channel
            .send(message)
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::SendAfterError))
    }

    pub fn send_connect(&self) -> crate::Result<()> {
        self.send(CONNECT)
    }

    pub fn send_keepalive(&self) -> crate::Result<()> {
        self.send(KEEPALIVE)
    }
}

fn is_timeout(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::TimedOut
}

pub struct Receiver {
    channel: mpsc::Receiver<Message>,
    thread_handle: ThreadHandle,
}

impl Receiver {
    pub fn new(socket: UdpSocket, max_latency: TimeDelta, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = std::sync::mpsc::channel();
        let connected = Arc::new(AtomicBool::new(false));
        let thread_handle = std::thread::spawn(move || {
            socket.set_read_timeout(Some(Duration::from_micros(200)))?;
            let mut buf = vec![0u8; MAX_SIZE];
            while !stop.get() {
                if !connected.load(Ordering::Acquire) {
                    match socket.recv_from(&mut buf) {
                        Ok((num_read, peer_addr)) => {
                            let message: Message = wincode::deserialize(&buf[..num_read])?;
                            if message == CONNECT {
                                socket.connect(peer_addr)?;
                                connected.store(true, Ordering::Release);
                                info!("Connected to peer");
                            }
                            continue;
                        }
                        Err(ref err) if is_timeout(err) => continue,
                        Err(err) => return Err(err.into()),
                    }
                }
                // TODO: set connected = false after no messages for a while
                match socket.recv(&mut buf) {
                    Ok(num_read) => {
                        let message: Message = wincode::deserialize(&buf[..num_read])?;
                        if since_micros(message.packet_timestamp) > max_latency {
                            continue;
                        }
                        channel_sender.send(message).unwrap();
                    }
                    Err(ref err) if is_timeout(err) => continue,
                    Err(err) => return Err(err.into()),
                }
                let num_read = socket.recv(&mut buf)?;
                let message: Message = wincode::deserialize(&buf[..num_read])?;
                channel_sender.send(message).unwrap();
            }
            Ok(())
        });
        Self {
            channel: channel_receiver,
            thread_handle: Mutex::new(Some(thread_handle)),
        }
    }

    pub fn recv(&self) -> crate::Result<Message> {
        self.channel
            .recv()
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::RecvAfterError))
    }
}
