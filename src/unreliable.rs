use std::{
    io::{self, Write},
    iter,
    sync::Arc,
};

use bytes::Bytes;
use log::{debug, info, trace, warn};
use quinn::Connection;
use thiserror::Error;

const HEADER_SIZE: usize = 8 + 4 + 4;

#[derive(Debug)]
struct Header {
    pub packet_index: u64,
    pub fragment_index: u32,
    pub total_fragments: u32,
}

impl Header {
    pub fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        let mut num_bytes = 0;
        num_bytes += writer.write(&self.packet_index.to_le_bytes())?;
        num_bytes += writer.write(&self.fragment_index.to_le_bytes())?;
        num_bytes += writer.write(&self.total_fragments.to_le_bytes())?;
        assert_eq!(num_bytes, HEADER_SIZE);
        Ok(())
    }

    pub fn read_from(buf: &[u8]) -> io::Result<Self> {
        Ok(Self {
            packet_index: u64::from_le_bytes(*buf[0..8].as_array().unwrap()),
            fragment_index: u32::from_le_bytes(*buf[8..12].as_array().unwrap()),
            total_fragments: u32::from_le_bytes(*buf[12..16].as_array().unwrap()),
        })
    }
}

pub struct UnreliableSender {
    conn: Arc<Connection>,
    packet_index: u64,
    fragments_per_second: fps_ticker::Fps,
    packets_per_second: fps_ticker::Fps,
}

impl UnreliableSender {
    pub fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
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
            self.packet_index += 1;
            self.conn.send_datagram(fragment.into()).unwrap();
        }
        self.packets_per_second.tick();
        trace!(
            "Sent all fragments for packet {} ({:.0} packet/s)",
            self.packet_index,
            self.packets_per_second.avg()
        );
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

    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        let Self {
            channel,
            fragments_per_second,
            packets_per_second,
            current_packet_index,
            fragment_map,
            num_fragments_found,
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
            } = header;
            let fragment_bytes = &fragment[HEADER_SIZE..];

            fragments_per_second.tick();
            trace!(
                "Received packet {} fragment {}/{} ({} bytes, {:.0}/s)",
                packet_index,
                fragment_index,
                total_fragments,
                fragment_bytes.len(),
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
                "Gathered all {} fragments for packet {} ({:.2} packet/s)",
                total_fragments,
                packet_index,
                packets_per_second.avg()
            );
            let packet_bytes = fragment_map.iter().flatten().copied().collect::<Vec<u8>>();
            return Some(packet_bytes);
        }
    }
}
