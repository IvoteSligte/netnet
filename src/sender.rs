use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use log::{debug, error, trace, warn};

use crate::{
    CONNECT_PACKET, KEEPALIVE_PACKET, MAX_PACKET_SIZE, MAX_UDP_PACKET_SIZE, Packet, Signal,
    error::{Error, Result, ThreadHandle, take_thread_error},
    spsc,
};

pub struct Sender {
    channel: spsc::Sender<Vec<u8>>,
    thread_handle: Arc<ThreadHandle>,
    label: &'static str,
}

impl Sender {
    pub fn new(
        socket: UdpSocket,
        peer_addr: impl ToSocketAddrs,
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
        let (channel_sender, channel_receiver) = spsc::channel::<Vec<u8>>(100);
        let thread_handle = std::thread::spawn(move || {
            let mut last_sent_at = Instant::now();
            let mut packet_id = 0;
            while !stop.get() {
                let packet = match channel_receiver.recv_timeout(Duration::from_micros(100)) {
                    Ok(body) => {
                        let packet = Packet {
                            id: packet_id,
                            body,
                        };
                        packet_id += 1;
                        packet
                    }
                    Err(spsc::RecvTimeoutError::Timeout) => {
                        if !connected.get() {
                            trace!("{label}: Sending CONNECT packet");
                            CONNECT_PACKET
                            // TODO: The client sends CONNECT to the server every 100 micros, which
                            //     allows the server to be aware of the client near-instantly.
                            //     However, the client only becomes aware of the connection when
                            //     it receives a KEEPALIVE packet from the server, which is sent
                            //     much less frequently, causing an unnecessary delay when connecting.
                        } else if (Instant::now() - last_sent_at) > Duration::from_millis(500) {
                            trace!("{label}: Sending KEEPALIVE packet");
                            KEEPALIVE_PACKET
                        } else {
                            continue;
                        }
                    }
                    Err(spsc::RecvTimeoutError::Disconnected) => {
                        warn!("{label}: Sender channel closed");
                        return Err(Error::ChannelClosed);
                    }
                };
                // only report non-control packets with the debug log level to prevent log spam
                if packet.body.len() > 0 {
                    debug!("Sending {} byte packet", packet.body.len());
                }
                let bytes = packet.to_bytes();
                assert!(bytes.len() <= MAX_UDP_PACKET_SIZE);
                socket.send(&bytes)?;
                last_sent_at = Instant::now();
                // prevent flooding the OS UDP buffer
                std::thread::sleep(Duration::from_micros(200));
            }
            Err(Error::Stopped)
        });
        Ok(Self {
            channel: channel_sender,
            thread_handle: Arc::new(Mutex::new(Some(thread_handle))),
            label,
        })
    }

    /// `packet` may not be larger than [MAX_PACKET_BODY_SIZE] bytes.
    pub fn send(&self, packet: Vec<u8>) -> Result<()> {
        if packet.len() > MAX_PACKET_SIZE {
            return Err(Error::PacketTooLarge(packet.len()));
        }
        let label = self.label;
        self.channel.send(packet).map_err(|_| {
            error!("{label}: Thread disconnected unexpectedly");
            take_thread_error(&self.thread_handle).unwrap_or(Error::SendAfterError)
        })?;
        debug!(
            "{label}: Sending packet in channel (total queued: {})",
            self.channel.queue_len()
        );
        Ok(())
    }
}
