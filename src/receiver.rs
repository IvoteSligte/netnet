use std::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
    time::Duration,
};

use log::{info, warn};

use crate::{
    CONNECT_PACKET, MAX_PACKET_SIZE, Packet, Signal, TimeDelta, TimeStamp,
    error::{Error, Result, ThreadHandle, take_thread_error},
    is_timeout, since, since_micros,
};

fn try_connect(socket: &UdpSocket, buf: &mut [u8]) -> Result<bool> {
    match socket.recv_from(buf) {
        Ok((num_read, peer_addr)) => {
            let packet: Packet = wincode::deserialize(&buf[..num_read])?;
            if packet == CONNECT_PACKET {
                socket.connect(peer_addr)?;
                info!("Connected to peer");
                return Ok(true);
            }
            Ok(false)
        }
        Err(ref err) if is_timeout(err) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

pub struct Receiver {
    channel: mpsc::Receiver<Packet>,
    thread_handle: ThreadHandle,
    connected: Signal,
}

impl Receiver {
    pub fn new(socket: UdpSocket, max_latency: TimeDelta, connected: Signal, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = std::sync::mpsc::channel();
        let connected2 = connected.clone();
        let thread_handle = std::thread::spawn(move || {
            socket.set_read_timeout(Some(Duration::from_micros(200)))?;
            let mut last_received_at = TimeStamp::UNIX_EPOCH;
            let mut buf = vec![0u8; MAX_PACKET_SIZE];
            while !stop.get() {
                if !connected2.get() {
                    if !try_connect(&socket, &mut buf)? {
                        continue;
                    }
                    connected2.set();
                }
                let num_read = match socket.recv(&mut buf) {
                    Ok(num_read) => num_read,
                    Err(ref err) if is_timeout(err) => {
                        if since(last_received_at) > TimeDelta::seconds(5) {
                            warn!("Disconnected from peer after no packets received for 5 seconds");
                            connected2.clear();
                        }
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                last_received_at = crate::now();
                let packet: Packet = wincode::deserialize(&buf[..num_read])?;
                if since_micros(packet.timestamp) > max_latency {
                    continue;
                }
                channel_sender.send(packet).unwrap();
            }
            Ok(())
        });
        Self {
            channel: channel_receiver,
            thread_handle: Mutex::new(Some(thread_handle)),
            connected,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.get()
    }

    pub fn recv(&self) -> crate::Result<Packet> {
        self.channel
            .recv()
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::RecvAfterError))
    }
}
