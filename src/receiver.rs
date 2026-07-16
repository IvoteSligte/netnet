use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use log::{debug, error, info, trace, warn};

use crate::{
    CONNECT_PACKET, KEEPALIVE_PACKET, MAX_UDP_PACKET_SIZE, Packet, Sender, Signal,
    average::RunningAverage,
    error::{Error, Result, ThreadHandle, take_thread_error},
    is_timeout, spsc,
};

fn recv(
    socket: &UdpSocket,
    buf: &mut [u8],
    connected: &Signal,
    label: &'static str,
) -> io::Result<usize> {
    if !connected.get() {
        trace!("{label}: Waiting for connection");
    }
    let num_read = if socket.peer_addr().is_ok() {
        socket.recv(buf)?
    } else {
        let (num_read, peer_addr) = socket.recv_from(buf)?;
        socket.connect(peer_addr)?;
        num_read
    };
    if !connected.get() {
        connected.set();
        info!("{label}: Connected to peer");
    }
    Ok(num_read)
}

// TODO: out-of-order buffer and filter
// TODO: deduplication filter

pub struct Receiver {
    channel: spsc::Receiver<Vec<u8>>,
    thread_handle: ThreadHandle,
    socket: UdpSocket,
    connected: Signal,
    stop: Signal,
    label: &'static str,
}

impl Receiver {
    pub fn new(
        socket: UdpSocket,
        connected: Signal,
        stop: Signal,
        // label used in the logs
        label: Option<&'static str>,
    ) -> io::Result<Self> {
        let label = label.unwrap_or("(unknown)");
        let (channel_sender, channel_receiver) = spsc::channel::<Vec<u8>>(100);
        let socket2 = socket.try_clone()?;
        let connected2 = connected.clone();
        let stop2 = stop.clone();
        let thread_handle = std::thread::spawn(move || {
            socket2.set_read_timeout(Some(Duration::from_micros(200)))?;
            let mut last_received_at = Instant::now();
            let mut min_packet_id = 0u64;
            let mut network_packet_loss = RunningAverage::new(1000.0);
            let mut channel_packet_loss = RunningAverage::new(1000.0);
            let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
            while !stop2.get() {
                let num_read = match recv(&socket2, &mut buf, &connected2, label) {
                    Ok(num_read) => num_read,
                    Err(ref err) if is_timeout(err) => {
                        if connected2.get()
                            && (Instant::now() - last_received_at) > Duration::from_secs(5)
                        {
                            warn!(
                                "{label}: Disconnected from peer after no packets received for 5 seconds"
                            );
                            connected2.clear();
                        }
                        continue;
                    }
                    Err(ref err)
                        if cfg!(target_os = "windows") && err.raw_os_error() == Some(997) =>
                    {
                        warn!("Ignoring Windows error 997 received when reading from socket");
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                last_received_at = Instant::now();
                let packet = Packet::from_bytes(&buf[..num_read])?;
                if packet == CONNECT_PACKET {
                    trace!("{label}: Received CONNECT packet");
                    continue;
                }
                if packet == KEEPALIVE_PACKET {
                    trace!("{label}: Received KEEPALIVE packet");
                    continue;
                }
                debug!(
                    "{label}: Received {} byte packet (id: {})",
                    packet.body.len(),
                    packet.id
                );
                if packet.id < min_packet_id {
                    // not changing network_packet_loss here to prevent double counting
                    debug!("{label}: Dropping out-of-order packet (id: {})", packet.id);
                    continue;
                } else if packet.id == min_packet_id + 1 {
                    debug!(
                        "{label}: Skipping packet (id: {}, recent loss: {:.3}%)",
                        packet.id,
                        network_packet_loss.update(1.0) * 100.0
                    )
                } else if packet.id > min_packet_id {
                    debug!(
                        "{label}: Missed {} packets (ids: {}-{}, recent loss: {:.3}%)",
                        packet.id - min_packet_id,
                        min_packet_id,
                        packet.id - 1,
                        network_packet_loss.update((packet.id - min_packet_id) as _) * 100.0
                    )
                } else {
                    network_packet_loss.update(0.0);
                }
                min_packet_id = packet.id + 1;
                if channel_sender.is_full() {}
                let old_packet = channel_sender
                    .send(packet.body)
                    .map_err(|_| Error::ChannelClosed)?;
                if old_packet.is_some() {
                    warn!(
                        "{label}: Wrote to full channel, resulting in packet loss (recent: {:.3}%)",
                        channel_packet_loss.update(1.0) * 100.0
                    );
                } else {
                    channel_packet_loss.update(0.0);
                }
                debug!(
                    "{label}: Sending packet in channel (total queued: {})",
                    channel_sender.queue_len()
                );
            }
            Err(Error::Stopped)
        });
        Ok(Self {
            channel: channel_receiver,
            thread_handle: Some(thread_handle),
            socket,
            connected,
            stop,
            label,
        })
    }

    pub fn is_connected(&self) -> bool {
        self.connected.get()
    }

    pub fn connected_signal(&self) -> Signal {
        self.connected.clone()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let addr = self.socket.peer_addr()?;
        if self.is_connected() {
            Ok(addr)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Peer disconnected",
            ))
        }
    }

    pub fn create_sender(&self) -> io::Result<Sender> {
        Sender::new(
            self.socket.try_clone()?,
            self.peer_addr()?,
            self.connected.clone(),
            self.stop.clone(),
            Some(self.label),
        )
    }

    pub fn accept(&self) -> Result<Sender> {
        let label = self.label;
        while !self.stop.get() && !self.is_connected() {
            trace!("{label}: Listening for connection");
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }
        if self.stop.get() {
            return Err(Error::Stopped);
        }
        info!("{label}: Connection accepted");
        Ok(self.create_sender()?)
    }

    fn recv_maybe_timeout(&mut self, timeout: Option<Duration>) -> Result<Vec<u8>> {
        // TODO: use a fixed-size queue instead of variable-size channel to prevent infinitely growing memory
        loop {
            let received = match timeout {
                Some(timeout) => match self.channel.recv_timeout(timeout) {
                    Ok(ok) => Ok(ok),
                    Err(spsc::RecvTimeoutError::Timeout) => return Err(Error::Timeout),
                    Err(spsc::RecvTimeoutError::Disconnected) => Err(spsc::RecvError),
                },
                None => self.channel.recv(),
            };
            let label = self.label;
            return received.map_err(|_| {
                error!("{label}: Thread disconnected unexpectedly");
                take_thread_error(&mut self.thread_handle).unwrap_or(Error::RecvAfterError)
            });
        }
    }

    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<u8>> {
        self.recv_maybe_timeout(Some(timeout))
    }

    pub fn recv(&mut self) -> Result<Vec<u8>> {
        self.recv_maybe_timeout(None)
    }
}
