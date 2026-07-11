use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use crate::{
    CONNECT_PACKET, Packet, Signal, TimeDelta, TimeStamp,
    error::{Error, Result, ThreadHandle, take_thread_error},
    since, since_micros,
};

#[derive(Clone)]
pub struct Sender {
    channel: mpsc::Sender<Packet>,
    thread_handle: Arc<ThreadHandle>,
}

impl Sender {
    pub fn new(
        socket: UdpSocket,
        peer_addr: impl ToSocketAddrs,
        max_latency: TimeDelta,
        connected: Signal,
        stop: Signal,
    ) -> io::Result<Self> {
        // Makes the socket aware of the peer address and checks its validity. Does not send any info to the peer.
        // In the rest of the code, being "connected" means that the peer is also aware of the connection.
        socket.connect(peer_addr)?;
        let (channel_sender, channel_receiver) = mpsc::channel::<Packet>();
        let thread_handle = std::thread::spawn(move || {
            let mut last_sent_at = TimeStamp::UNIX_EPOCH;
            while !stop.get() {
                if !connected.get() {
                    std::thread::sleep(Duration::from_millis(10));
                }
                let packet = match channel_receiver.recv_timeout(Duration::from_micros(100)) {
                    Ok(packet) if since_micros(packet.timestamp) < max_latency => packet,
                    Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                        if since(last_sent_at) > TimeDelta::microseconds(200) {
                            CONNECT_PACKET
                        } else {
                            continue;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Err(Error::ChannelClosed),
                };
                let bytes = wincode::serialize(&packet)?;
                socket.send(&bytes)?;
                last_sent_at = crate::now();
            }
            Ok(())
        });
        Ok(Self {
            channel: channel_sender,
            thread_handle: Arc::new(Mutex::new(Some(thread_handle))),
        })
    }

    pub fn send(&self, packet: Vec<u8>) -> Result<()> {
        self.channel
            .send(Packet {
                timestamp: crate::now_micros(),
                body: packet,
            })
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::SendAfterError))
    }
}
