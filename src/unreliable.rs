use std::{
    collections::VecDeque,
    io, iter,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use log::{debug, info, trace, warn};
use thiserror::Error;

const HEADER_SIZE: usize = 8 + 4 + 4 + 2 + 1;

#[derive(Debug)]
struct Header {
    pub packet_index: u64,
    pub fragment_index: u32,
    pub total_fragments: u32,
    pub max_fragment_size: u16,
    pub stream_id: u8,
}

impl Header {
    pub fn write_to(&self, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.packet_index);
        bytes.put_u32_le(self.fragment_index);
        bytes.put_u32_le(self.total_fragments);
        bytes.put_u16_le(self.max_fragment_size);
        bytes.put_u8(self.stream_id);
    }

    pub fn read_from(buf: &[u8]) -> io::Result<Self> {
        Ok(Self {
            packet_index: u64::from_le_bytes(*buf[0..8].as_array().unwrap()),
            fragment_index: u32::from_le_bytes(*buf[8..12].as_array().unwrap()),
            total_fragments: u32::from_le_bytes(*buf[12..16].as_array().unwrap()),
            max_fragment_size: u16::from_le_bytes(*buf[16..18].as_array().unwrap()),
            stream_id: buf[18],
        })
    }
}

struct Alloc {
    last_used: Instant,
    bytes: Bytes,
}

impl Alloc {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_unique(&self) -> bool {
        self.bytes.is_unique()
    }
}

struct SendStatistics {
    stream_id: u8,
    packets_in_last_second: Vec<(Instant, usize)>,
    fragments_per_second: fps_ticker::Fps,
    packets_per_second: fps_ticker::Fps,
    last_logged_at: Instant,
}

impl SendStatistics {
    pub fn new(stream_id: u8) -> Self {
        Self {
            stream_id,
            packets_in_last_second: Vec::with_capacity(100),
            fragments_per_second: Default::default(),
            packets_per_second: Default::default(),
            last_logged_at: Instant::now(),
        }
    }

    pub fn fragments_per_second(&self) -> f64 {
        self.fragments_per_second.avg()
    }

    pub fn packets_per_second(&self) -> f64 {
        self.packets_per_second.avg()
    }

    pub fn sent_fragment(&mut self) {
        self.fragments_per_second.tick();
    }

    pub fn sent_packet(&mut self, packet_size: usize) {
        let Self {
            stream_id,
            packets_in_last_second,
            packets_per_second,
            last_logged_at,
            ..
        } = self;
        packets_per_second.tick();
        let now = Instant::now();
        packets_in_last_second.retain(|(sent_at, _)| now - *sent_at < Duration::from_secs(1));
        packets_in_last_second.push((now, packet_size));
        if now - *last_logged_at > Duration::from_secs(1) {
            *last_logged_at = now;
            let bytes_in_last_second: usize =
                packets_in_last_second.iter().map(|(_, size)| *size).sum();
            info!("{stream_id}: bytes/s: {bytes_in_last_second}");
        }
    }
}

pub struct UnreliableSender {
    _endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    stream_id: u8,
    packet_index: u64,
    alloc_pool: Vec<Alloc>,
    stats: SendStatistics,
}

impl UnreliableSender {
    pub fn new(endpoint: quinn::Endpoint, conn: quinn::Connection, stream_id: u8) -> Self {
        Self {
            _endpoint: endpoint,
            conn,
            stream_id,
            packet_index: 0,
            alloc_pool: Vec::with_capacity(100),
            stats: SendStatistics::new(stream_id),
        }
    }

    pub fn max_fragment_size(&self) -> usize {
        self.conn.max_datagram_size().unwrap()
    }

    fn get_alloc(&mut self, size: usize) -> BytesMut {
        match self
            .alloc_pool
            .iter()
            .position(|alloc| alloc.is_unique() && alloc.len() >= size)
        {
            Some(index) => {
                let mut alloc = self
                    .alloc_pool
                    .swap_remove(index)
                    .bytes
                    .try_into_mut()
                    .unwrap();
                alloc.clear();
                alloc
            }
            None => {
                debug!(
                    "Created new allocation of size {size} ({} in pool)",
                    self.alloc_pool.len()
                );
                // Not sure why, but without something to remove old allocations,
                // the pool keeps growing seemingly forever.
                // Perhaps a crate that quinn depends on also checks for allocation
                // uniqueness in a pool (because quinn itself does not seem to).
                let now = Instant::now();
                self.alloc_pool
                    .retain(|alloc| now - alloc.last_used < Duration::from_secs(5));
                BytesMut::with_capacity(size)
            }
        }
    }

    /// There is no restriction on the size of [bytes], however,
    /// the chance of the receiver receiving the packet in its entirety decreases with size.
    pub fn send(&mut self, bytes: &[u8]) -> Result<(), quinn::SendDatagramError> {
        let max_fragment_size = (self.conn.max_datagram_size().unwrap() - HEADER_SIZE) as u16;
        let total_fragments: u32 = bytes
            .len()
            .div_ceil(max_fragment_size as _)
            .try_into()
            .unwrap();
        for (fragment_bytes, fragment_index) in bytes.chunks(max_fragment_size as _).zip(0u32..) {
            let fragment_size = HEADER_SIZE + fragment_bytes.len();
            let mut fragment = self.get_alloc(fragment_size);
            let header = Header {
                packet_index: self.packet_index,
                fragment_index,
                total_fragments,
                max_fragment_size,
                stream_id: self.stream_id,
            };
            self.stats.sent_fragment();
            trace!(
                "Sending packet {} fragment {}/{} ({} bytes, {:.0}/s)",
                self.packet_index,
                fragment_index,
                total_fragments,
                fragment_bytes.len(),
                self.stats.fragments_per_second()
            );
            header.write_to(&mut fragment);
            fragment.extend_from_slice(fragment_bytes);
            // The fragment's backing memory is frozen and pushed to the pool,
            // so that it can be reused as soon as quinn is done with it.
            let fragment = fragment.freeze();
            self.conn.send_datagram(fragment.clone()).unwrap();
            self.alloc_pool.push(Alloc {
                last_used: Instant::now(),
                bytes: fragment,
            });
        }
        self.packet_index += 1;
        trace!(
            "Sent all fragments for packet {} ({:.0} packet/s)",
            self.packet_index,
            self.stats.packets_per_second()
        );
        self.stats
            .sent_packet(total_fragments as usize * HEADER_SIZE + bytes.len());
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
    last_logged_at: Instant,
}

impl RecvStatistics {
    pub fn new() -> Self {
        Self {
            fragments_per_second: Default::default(),
            packets_per_second: Default::default(),
            last_100_packets: VecDeque::with_capacity(100),
            last_logged_at: Instant::now(),
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
            last_logged_at: last_statistics_logged_at,
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
    packet_bytes: Vec<u8>,
    fragment_mask: Vec<bool>,
    num_fragments_found: u32,
    statistics: RecvStatistics,
}

impl UnreliableReceiver {
    pub fn new(channel: tokio::sync::mpsc::Receiver<Bytes>) -> Self {
        Self {
            channel,
            current_packet_index: 0,
            packet_bytes: Vec::with_capacity(10_000),
            fragment_mask: Vec::with_capacity(100),
            num_fragments_found: 0,
            statistics: RecvStatistics::new(),
        }
    }

    /// Returns `None` if the connection is closed
    pub async fn recv(&mut self) -> Option<&[u8]> {
        let Self {
            channel,
            current_packet_index,
            packet_bytes,
            fragment_mask,
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
                max_fragment_size,
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
                packet_bytes.clear();
                packet_bytes.extend(
                    iter::repeat(0u8).take(total_fragments as usize * max_fragment_size as usize),
                );
                for _ in fragment_mask.len()..total_fragments as usize {
                    fragment_mask.push(false);
                }
                fragment_mask.fill(false);
                *current_packet_index = packet_index;
            }
            if fragment_mask[fragment_index as usize] {
                trace!("Duplicate fragment {}", fragment_index);
                continue;
            }
            if fragment_index + 1 == total_fragments {
                // Truncate the size of packet_bytes from a generous guess to the actual bounds.
                let len = (total_fragments - 1) as usize * max_fragment_size as usize
                    + fragment_bytes.len();
                packet_bytes.truncate(len);
            }
            fragment_mask[fragment_index as usize] = true;
            {
                let start = fragment_index as usize * max_fragment_size as usize;
                let end = start + fragment_bytes.len();
                packet_bytes[start..end].copy_from_slice(fragment_bytes);
            }
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
            return Some(packet_bytes.as_slice());
        }
    }
}
