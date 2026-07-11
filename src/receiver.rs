use std::{
    io,
    net::{SocketAddr, UdpSocket},
    sync::{Mutex, mpsc},
    time::Duration,
};

use log::{debug, info, trace, warn};

use crate::{
    CONNECT_PACKET, KEEPALIVE_PACKET, MAX_PACKET_SIZE, MAX_UDP_PACKET_SIZE, Packet, Sender, Signal, TimeDelta, error::{Error, Result, ThreadHandle, take_thread_error}, is_timeout, since, since_micros
};

fn recv(
    socket: &UdpSocket,
    buf: &mut [u8],
    connected: &Signal,
    label: &'static str,
) -> io::Result<usize> {
    if connected.get() {
        let num_read = socket.recv(buf)?;
        Ok(num_read)
    } else {
        trace!("{label}: Waiting for connection");
        let (num_read, peer_addr) = socket.recv_from(buf)?;
        socket.connect(peer_addr)?;
        connected.set();
        info!("{label}: Connected to peer");
        Ok(num_read)
    }
}

// TODO: out-of-order buffer and filter

pub struct Receiver {
    channel: mpsc::Receiver<Packet>,
    thread_handle: ThreadHandle,
    socket: UdpSocket,
    max_latency: TimeDelta,
    connected: Signal,
    stop: Signal,
    label: &'static str,
}

impl Receiver {
    pub fn new(
        socket: UdpSocket,
        max_latency: TimeDelta,
        connected: Signal,
        stop: Signal,
        // label used in the logs
        label: Option<&'static str>,
    ) -> io::Result<Self> {
        let label = label.unwrap_or("(unknown)");
        let (channel_sender, channel_receiver) = std::sync::mpsc::channel();
        let socket2 = socket.try_clone()?;
        let connected2 = connected.clone();
        let stop2 = stop.clone();
        let thread_handle = std::thread::spawn(move || {
            socket2.set_read_timeout(Some(Duration::from_micros(200)))?;
            let mut last_received_at = crate::now();
            let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
            while !stop2.get() {
                let num_read = match recv(&socket2, &mut buf, &connected2, label) {
                    Ok(num_read) => num_read,
                    Err(ref err) if is_timeout(err) => {
                        if connected2.get() && since(last_received_at) > TimeDelta::seconds(5) {
                            warn!(
                                "{label}: Disconnected from peer after no packets received for 5 seconds"
                            );
                            connected2.clear();
                        }
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                last_received_at = crate::now();
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
                    "{label}: Received {} byte packet (latency: {:.2}ms)",
                    packet.body.len(),
                    since_micros(packet.timestamp).num_microseconds().unwrap() as f32 / 1000.0
                );
                if since_micros(packet.timestamp) > max_latency {
                    debug!("{label}: Dropping packet due to latency");
                    continue;
                }
                channel_sender.send(packet).unwrap();
            }
            Err(Error::Stopped)
        });
        Ok(Self {
            channel: channel_receiver,
            thread_handle: Mutex::new(Some(thread_handle)),
            socket,
            max_latency,
            connected,
            stop,
            label,
        })
    }

    pub fn is_connected(&self) -> bool {
        self.connected.get()
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
            self.max_latency,
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

    pub fn recv(&self) -> Result<Packet> {
        self.channel
            .recv()
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::RecvAfterError))
    }
}
