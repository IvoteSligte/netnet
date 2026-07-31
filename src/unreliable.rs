use std::{
    collections::VecDeque,
    io::{self, Write},
    iter,
    time::{Duration, Instant},
};

use bytes::Bytes;
use log::{debug, info, trace, warn};
use thiserror::Error;

const HEADER_SIZE: usize = 8 + 4 + 4 + 1;

#[derive(Debug)]
struct Header {
    pub packet_index: u64,
    pub fragment_index: u32,
    pub total_fragments: u32,
    pub stream_id: u8,
}

impl Header {
    pub fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        let mut num_bytes = 0;
        num_bytes += writer.write(&self.packet_index.to_le_bytes())?;
        num_bytes += writer.write(&self.fragment_index.to_le_bytes())?;
        num_bytes += writer.write(&self.total_fragments.to_le_bytes())?;
        num_bytes += writer.write(&[self.stream_id])?;
        assert_eq!(num_bytes, HEADER_SIZE);
        Ok(())
    }

    pub fn read_from(buf: &[u8]) -> io::Result<Self> {
        Ok(Self {
            packet_index: u64::from_le_bytes(*buf[0..8].as_array().unwrap()),
            fragment_index: u32::from_le_bytes(*buf[8..12].as_array().unwrap()),
            total_fragments: u32::from_le_bytes(*buf[12..16].as_array().unwrap()),
            stream_id: buf[16],
        })
    }
}

pub struct UnreliableSender {
    _endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    stream_id: u8,
    packet_index: u64,
    fragments_per_second: fps_ticker::Fps,
    packets_per_second: fps_ticker::Fps,
}

impl UnreliableSender {
    pub fn new(endpoint: quinn::Endpoint, conn: quinn::Connection, stream_id: u8) -> Self {
        Self {
            _endpoint: endpoint,
            conn,
            stream_id,
            packet_index: 0,
            fragments_per_second: Default::default(),
            packets_per_second: Default::default(),
        }
    }

    pub fn max_fragment_size(&self) -> usize {
        self.conn.max_datagram_size().unwrap()
    }

    /// There is no restriction on the size of [bytes], however,
    /// the chance of the receiver receiving the packet in its entirety decreases with size.
    pub fn send(&mut self, bytes: &[u8]) -> Result<(), quinn::SendDatagramError> {
        let chunk_size = self.conn.max_datagram_size().unwrap() - HEADER_SIZE;
        let total_fragments: u32 = bytes.len().div_ceil(chunk_size).try_into().unwrap();

        for (fragment_bytes, fragment_index) in bytes.chunks(chunk_size).zip(0u32..) {
            let mut fragment = Vec::with_capacity(HEADER_SIZE + fragment_bytes.len());
            let header = Header {
                packet_index: self.packet_index,
                fragment_index,
                total_fragments,
                stream_id: self.stream_id,
            };
            self.fragments_per_second.tick();
            trace!(
                "Sending packet {} fragment {}/{} ({} bytes, {:.0}/s)",
                self.packet_index,
                fragment_index,
                total_fragments,
                fragment_bytes.len(),
                self.fragments_per_second.avg()
            );
            header.write_to(&mut fragment).unwrap();
            fragment.extend_from_slice(fragment_bytes);
            self.conn.send_datagram(fragment.into()).unwrap();
        }
        self.packet_index += 1;
        self.packets_per_second.tick();
        trace!(
            "Sent all fragments for packet {} ({:.0} packet/s)",
            self.packet_index,
            self.packets_per_second.avg()
        );
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum RecvTimeoutError {
    #[error("Receive timed out")]
    Timeout,
    #[error("Disconnected")]
    Disconnected,
}

struct RecvStatistics {
    fragments_per_second: fps_ticker::Fps,
    packets_per_second: fps_ticker::Fps,
    last_100_packets: VecDeque<u64>,
    last_statistics_logged_at: Instant,
}

impl RecvStatistics {
    pub fn new() -> Self {
        Self {
            fragments_per_second: Default::default(),
            packets_per_second: Default::default(),
            last_100_packets: VecDeque::with_capacity(100),
            last_statistics_logged_at: Instant::now(),
        }
    }

    pub fn fragments_per_second(&self) -> f64 {
        self.fragments_per_second.avg()
    }

    pub fn packets_per_second(&self) -> f64 {
        self.packets_per_second.avg()
    }

    pub fn received_fragment(&mut self) {
        self.fragments_per_second.tick();
    }

    pub fn received_packet(&mut self, packet_index: u64) {
        let Self {
            packets_per_second,
            last_100_packets,
            last_statistics_logged_at,
            ..
        } = self;
        packets_per_second.tick();
        if last_100_packets.len() >= 100 {
            last_100_packets.pop_front();
        }
        last_100_packets.push_back(packet_index);
        let now = Instant::now();
        if now - *last_statistics_logged_at > Duration::from_secs(1) {
            let first_packet = if last_100_packets.len() < 100 {
                0
            } else {
                last_100_packets[0]
            };
            let last_packet = last_100_packets.back().unwrap();
            // TODO: also report ping/RTT (conn.stats().path.rtt)
            info!(
                "Recent packet loss: {:.1}%",
                1.0 - (last_packet - first_packet) as f32 / last_100_packets.len() as f32
            );
            *last_statistics_logged_at = now;
        }
    }
}

pub(crate) fn spawn_unreliable_receivers(
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
) -> Vec<tokio::sync::mpsc::Receiver<Bytes>> {
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..u8::MAX)
        .map(|_| tokio::sync::mpsc::channel(100))
        .unzip();
    tokio::task::spawn(async move {
        loop {
            let bytes = conn.read_datagram().await.unwrap();
            let header = Header::read_from(&bytes).unwrap();
            if let Err(_) = senders[header.stream_id as usize].send(bytes).await {
                warn!("Receiver channel closed");
                break;
            }
        }
        drop(endpoint);
    });
    receivers
}

pub struct UnreliableReceiver {
    channel: tokio::sync::mpsc::Receiver<Bytes>,
    current_packet_index: u64,
    fragment_map: Vec<Vec<u8>>,
    num_fragments_found: u32,
    statistics: RecvStatistics,
}

impl UnreliableReceiver {
    pub fn new(channel: tokio::sync::mpsc::Receiver<Bytes>) -> Self {
        Self {
            channel,
            current_packet_index: 0,
            fragment_map: Vec::with_capacity(100),
            num_fragments_found: 0,
            statistics: RecvStatistics::new(),
        }
    }

    /// Returns `None` if the connection is closed
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        let Self {
            channel,
            current_packet_index,
            fragment_map,
            num_fragments_found,
            statistics,
        } = self;
        loop {
            let fragment = channel.recv().await?;
            if fragment.len() < HEADER_SIZE {
                warn!("Received fragment without header");
                continue;
            }
            let header = Header::read_from(&fragment).unwrap();
            let Header {
                packet_index,
                fragment_index,
                total_fragments,
                stream_id: _,
            } = header;
            let fragment_bytes = &fragment[HEADER_SIZE..];

            statistics.received_fragment();
            trace!(
                "Received packet {} fragment {}/{} ({} bytes, {:.0}/s)",
                packet_index,
                fragment_index,
                total_fragments,
                fragment_bytes.len(),
                statistics.fragments_per_second()
            );
            if fragment_index >= total_fragments {
                warn!("Invalid fragment header: fragment_index >= total_fragments");
                continue;
            }
            if packet_index > *current_packet_index {
                debug!(
                    "Skipping incomplete packet {} with {}/{} fragments",
                    current_packet_index, num_fragments_found, total_fragments
                );
            }
            if packet_index > *current_packet_index || *num_fragments_found == 0 {
                *num_fragments_found = 0;
                fragment_map.clear();
                fragment_map.extend(iter::repeat(Vec::new()).take(total_fragments as _));
                *current_packet_index = packet_index;
            }
            debug_assert!(fragment_index < total_fragments);
            debug_assert!(total_fragments as usize == fragment_map.len());

            if !fragment_map[fragment_index as usize].is_empty() {
                trace!("Duplicate fragment {}", fragment_index);
                continue;
            }
            fragment_map[fragment_index as usize] = fragment_bytes.to_vec();
            *num_fragments_found += 1;
            if *num_fragments_found < total_fragments {
                continue;
            }
            *current_packet_index = packet_index + 1;
            *num_fragments_found = 0;

            statistics.received_packet(packet_index);
            trace!(
                "Gathered all {} fragments for packet {} ({:.2} packet/s)",
                total_fragments,
                packet_index,
                statistics.packets_per_second()
            );
            let packet_bytes = fragment_map.iter().flatten().copied().collect::<Vec<u8>>();
            return Some(packet_bytes);
        }
    }
}
