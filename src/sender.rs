use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use log::{debug, trace, warn};

use crate::{
    CONNECT_PACKET, KEEPALIVE_PACKET, MAX_PACKET_SIZE, MAX_UDP_PACKET_SIZE, Packet, Signal, TimeDelta, error::{Error, Result, ThreadHandle, take_thread_error}, since, since_micros
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
        // label used in the logs
        label: Option<&'static str>,
    ) -> io::Result<Self> {
        let label = label.unwrap_or("(unknown)");
        // Makes the socket aware of the peer address and checks its validity. Does not send any info to the peer.
        // In the rest of the code, being "connected" means that the peer is also aware of the connection.
        socket.connect(peer_addr)?;
        debug!("{label}: Socket connected");
        let (channel_sender, channel_receiver) = mpsc::channel::<Packet>();
        let thread_handle = std::thread::spawn(move || {
            let mut last_sent_at = crate::now();
            while !stop.get() {
                let packet = {
                    if !connected.get() {
                        trace!("{label}: Sending CONNECT packet");
                        CONNECT_PACKET
                    } else {
                        match channel_receiver.recv_timeout(Duration::from_micros(100)) {
                            Ok(packet) if since_micros(packet.timestamp) < max_latency => packet,
                            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                                if since(last_sent_at) > TimeDelta::milliseconds(500) {
                                    trace!("{label}: Sending KEEPALIVE packet");
                                    KEEPALIVE_PACKET
                                } else {
                                    continue;
                                }
                            }
                            Err(mpsc::RecvTimeoutError::Disconnected) => {
                                warn!("{label}: Sender channel closed");
                                return Err(Error::ChannelClosed);
                            }
                        }
                    }
                };
                debug!("Sending {} byte packet", packet.body.len());
                let bytes = packet.to_bytes();
                debug_assert!(bytes.len() <= MAX_UDP_PACKET_SIZE);
                socket.send(&bytes)?;
                last_sent_at = crate::now();
                // prevent flooding the OS UDP buffer
                std::thread::sleep(Duration::from_micros(200));
            }
            Err(Error::Stopped)
        });
        Ok(Self {
            channel: channel_sender,
            thread_handle: Arc::new(Mutex::new(Some(thread_handle))),
        })
    }

    /// `packet` may not be larger than [MAX_PACKET_BODY_SIZE] bytes.
    pub fn send(&self, packet: Vec<u8>) -> Result<()> {
        if packet.len() > MAX_PACKET_SIZE {
            return Err(Error::PacketTooLarge(packet.len()));
        }
        self.channel
            .send(Packet {
                timestamp: crate::now_micros(),
                body: packet,
            })
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::SendAfterError))
    }
}
