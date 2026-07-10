use std::{
    io,
    net::UdpSocket,
    sync::{Arc, mpsc},
    thread::JoinHandle,
    time::Duration,
};

use wincode::{SchemaRead, SchemaWrite};

use crate::Signal;

pub const MAX_SIZE: usize = 65507;
pub const HEADER_SIZE: usize = 8 + 4 + 2 + 2; // the encoded size of MessageHeader in bytes
pub const MAX_BODY_SIZE: usize = MAX_SIZE - HEADER_SIZE;

#[derive(Clone, SchemaRead, SchemaWrite)]
pub struct Message {
    pub packet_timestamp: i64,
    pub packet_id: u32,
    pub id: u16,
    pub last_message_in_packet: u16,
    pub body: Vec<u8>,
}

#[derive(Clone)]
pub struct Sender {
    channel: mpsc::Sender<Message>,
    thread_handle: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl Sender {
    pub fn new(socket: UdpSocket, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = mpsc::channel();
        let thread_handle = std::thread::spawn(move || {
            socket.set_write_timeout(Some(Duration::from_millis(200)))?;
            while !stop.get() {
                let message = channel_receiver.recv_timeout(Duration::from_micros(100))?;
                let bytes = wincode::serialize(&message)?;
                socket.send(&bytes)?;
            }
            Ok(())
        });
        Self {
            channel: channel_sender,
            thread_handle: Arc::new(thread_handle),
        }
    }

    pub fn send(&self, message: Message) {
        self.channel.send(message).unwrap()
    }
}

fn is_timeout(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::TimedOut
}

pub struct Receiver {
    stop: Signal,
    channel: mpsc::Receiver<Message>,
    thread_handle: JoinHandle<anyhow::Result<()>>,
}

impl Receiver {
    pub fn new(socket: UdpSocket, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = std::sync::mpsc::channel();
        let stop2 = stop.clone();
        let thread_handle = std::thread::spawn(move || {
            socket.set_read_timeout(Some(Duration::from_micros(200)))?;
            let mut buf = vec![0u8; MAX_SIZE];
            while !stop2.get() {
                match socket.recv(&mut buf) {
                    Ok(num_read) => channel_sender.send(wincode::deserialize(&buf[..num_read])?)?,
                    Err(ref err) if is_timeout(err) => continue,
                    Err(err) => return Err(err.into()),
                }
                let num_read = socket.recv(&mut buf)?;
                let message: Message = wincode::deserialize(&buf[..num_read])?;
                channel_sender.send(message)?;
            }
            Ok(())
        });
        Self {
            stop,
            channel: channel_receiver,
            thread_handle,
        }
    }

    pub fn recv(&self) -> Result<Message, mpsc::RecvError> {
        self.channel.recv()
    }
}
