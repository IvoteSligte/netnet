use std::{
    iter,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use log::{debug, info, trace, warn};
use quinn::Connection;
use thiserror::Error;
use tokio::sync::mpsc;

const HEADER_SIZE: usize = 8 + 4 + 4;

pub struct UnreliableSender {
    conn: Arc<Connection>,
    packet_index: u64,
}

impl UnreliableSender {
    pub fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            packet_index: 0,
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
            let mut fragment = Vec::with_capacity(fragment_bytes.len() + HEADER_SIZE);
            fragment.extend_from_slice(&self.packet_index.to_le_bytes());
            fragment.extend_from_slice(&fragment_index.to_le_bytes());
            fragment.extend_from_slice(&total_fragments.to_le_bytes());
            fragment.extend_from_slice(fragment_bytes);
            self.packet_index += 1;
            self.conn.send_datagram(fragment.into()).unwrap();
        }
        Ok(())
    }
}

async fn receiver_task(
    conn: Arc<Connection>,
    channel: tokio::sync::mpsc::Sender<Bytes>,
) -> Result<(), quinn::ConnectionError> {
    loop {
        let bytes = conn.read_datagram().await?;
        if let Err(_) = channel.send(bytes).await {
            info!("Receiver channel closed");
            break Ok(());
        }
    }
}

#[derive(Error, Debug)]
pub enum RecvTimeoutError {
    #[error("Receive timed out")]
    Timeout,
    #[error("Disconnected")]
    Disconnected,
}

pub struct UnreliableReceiver {
    channel: tokio::sync::mpsc::Receiver<Bytes>,
    fragments_per_second: fps_ticker::Fps,
    packets_per_second: fps_ticker::Fps,
    current_packet_index: u64,
    fragment_map: Vec<Vec<u8>>,
    num_fragments_found: u32,
}

impl UnreliableReceiver {
    pub fn new(conn: Arc<Connection>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        tokio::task::spawn(receiver_task(conn, sender));
        Self {
            channel: receiver,
            fragments_per_second: Default::default(),
            packets_per_second: Default::default(),
            current_packet_index: 0,
            fragment_map: Vec::with_capacity(100),
            num_fragments_found: 0,
        }
    }

    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Vec<u8>, RecvTimeoutError> {
        let Self {
            channel,
            fragments_per_second,
            packets_per_second,
            current_packet_index,
            fragment_map,
            num_fragments_found,
        } = self;
        let start = Instant::now();

        while Instant::now() - start < timeout {
            let fragment = match channel.try_recv() {
                Ok(f) => f,
                Err(mpsc::error::TryRecvError::Empty) => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(RecvTimeoutError::Disconnected);
                }
            };
            if fragment.len() >= HEADER_SIZE {
                warn!("Received fragment without header");
                continue;
            }
            let packet_index = u64::from_le_bytes(*fragment[0..8].as_array().unwrap());
            let fragment_index = u32::from_le_bytes(*fragment[8..12].as_array().unwrap());
            let total_fragments = u32::from_le_bytes(*fragment[12..16].as_array().unwrap());
            let fragment_bytes = &fragment[16..];

            fragments_per_second.tick();
            trace!(
                "Received packet {} fragment {}/{} ({:.0}/s)",
                packet_index,
                fragment_index,
                total_fragments,
                fragments_per_second.avg()
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
            *current_packet_index += 1;
            *num_fragments_found = 0;
            packets_per_second.tick();
            trace!(
                "Gathered all {} fragments for packet {} ({:.2}/s)",
                total_fragments,
                packet_index,
                packets_per_second.avg()
            );
            let packet_bytes = fragment_map.iter().flatten().copied().collect::<Vec<u8>>();
            return Ok(packet_bytes);
        }
        Err(RecvTimeoutError::Timeout)
    }

    pub fn recv(&mut self) -> Option<Vec<u8>> {
        self.recv_timeout(Duration::MAX).ok()
    }
}
