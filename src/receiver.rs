use chrono::{DateTime, Utc};
use log::{debug, trace};
use std::{
    collections::VecDeque,
    net::UdpSocket,
    sync::{Arc, Mutex},
};
use wincode::{SchemaReadOwned, config::DefaultConfig};

use crate::*;

fn spawn_receiver_thread(socket: UdpSocket, receiver: Arc<Mutex<Receiver>>) {
    std::thread::spawn(move || {
        let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
        loop {
            socket.recv(&mut buf).unwrap();
            let receiver = &mut *receiver.lock().unwrap();
            let (header, body_bytes) = split_message(&buf);
            if receiver.queue.len() >= RECV_BUFFER_CAP {
                // remove oldest message
                receiver.queue.pop_front();
                receiver.survival_rate.update(0.0);
                trace!(
                    "Dropped message {} of packet {} due to full buffer ({:.0}% survival rate)",
                    header.message_id,
                    header.packet_id,
                    receiver.survival_rate.get() * 100.0
                );
            }
            let mut index = 0;
            for (h, _) in &receiver.queue {
                if h.packet_timestamp >= header.packet_timestamp {
                    break;
                }
                index += 1;
            }
            receiver.queue.insert(index, (header, body_bytes.to_vec()));
        }
    });
}

pub struct Receiver {
    /// Survival rate of messages received (equal to 1.0 - drop_rate)
    survival_rate: RunningAverage,
    /// Fixed-size queue of received messages, sorted by timestamp
    /// (header, body_bytes)
    queue: VecDeque<(MessageHeader, Vec<u8>)>,
    /// Sorted by timestamp
    packet_map: VecDeque<PacketInfo>,
    /// ID of the last complete packet received
    last_packet_id: u32,
    /// Timestamp of the last complete packet received
    last_packet_timestamp: i64,
}

impl Receiver {
    pub fn new(socket: UdpSocket) -> Arc<Mutex<Self>> {
        let receiver = Arc::new(Mutex::new(Receiver {
            survival_rate: RunningAverage::new(10000.0),
            queue: VecDeque::with_capacity(RECV_BUFFER_CAP),
            packet_map: VecDeque::with_capacity(PACKET_MAP_CAP),
            last_packet_id: 0,
            last_packet_timestamp: 0,
        }));
        spawn_receiver_thread(socket, receiver.clone());
        receiver
    }

    /// Returns `Ok(None)` if no (complete) packet has been read.
    pub fn recv_non_blocking<'de, P: SchemaReadOwned<DefaultConfig, Dst = P>>(
        &mut self,
    ) -> anyhow::Result<Option<(P, DateTime<Utc>)>> {
        let Some((header, body_bytes)) = self.queue.pop_back() else {
            return Ok(None);
        };
        if header.packet_timestamp < self.last_packet_timestamp {
            trace!(
                "Dropped out-of-order message {} for packet {}",
                header.message_id, header.packet_id
            );
            self.survival_rate.update(0.0);
            return Ok(None);
        }
        let now = Utc::now().timestamp_micros();
        let latency = (now - header.packet_timestamp) as f32 / 1000.0;
        if latency > MAX_LATENCY_MS {
            trace!(
                "Dropped message {} for packet {} with {:.2}ms latency",
                header.message_id, header.packet_id, latency
            );
            self.survival_rate.update(0.0);
            return Ok(None);
        }
        let (packet_index, info) = match self
            .packet_map
            .binary_search_by_key(&header.packet_timestamp, |info| info.timestamp)
        {
            Ok(index) => (index, &mut self.packet_map[index]),
            Err(index) => {
                let num_messages_in_packet = header.last_message_in_packet as usize + 1;
                let info = PacketInfo {
                    timestamp: header.packet_timestamp,
                    id: header.packet_id,
                    bytes: vec![0u8; num_messages_in_packet * MAX_MESSAGE_BODY_SIZE],
                    found: vec![false; num_messages_in_packet],
                    num_found: 0,
                };
                if self.packet_map.len() >= PACKET_MAP_CAP {
                    self.packet_map.pop_front();
                }
                self.packet_map.insert(index, info);
                (index, &mut self.packet_map[index])
            }
        };
        self.survival_rate.update(1.0);
        let message_id = header.message_id as usize;
        if !info.found[message_id] {
            info.found[message_id] = true;
            info.num_found += 1;
            trace!(
                "Received new message {} for packet {} ({}/{}, {:.2}ms latency, {:.0}% survival rate)",
                message_id,
                header.packet_id,
                info.num_found,
                header.last_message_in_packet + 1,
                (Utc::now().timestamp_micros() - header.packet_timestamp) as f32 / 1000.0,
                self.survival_rate.get() * 100.0,
            );
            let last_message_in_packet = header.last_message_in_packet as usize;
            if message_id == last_message_in_packet {
                // truncate the vector to the true size of the packet
                info.bytes
                    .truncate(last_message_in_packet * MAX_MESSAGE_BODY_SIZE + body_bytes.len());
            }
            let start = message_id * MAX_MESSAGE_BODY_SIZE;
            let end = start + body_bytes.len();
            info.bytes[start..end].copy_from_slice(&body_bytes);

            if info.num_found >= info.found.len() {
                let info = self.packet_map.remove(packet_index).unwrap();
                debug!(
                    "Received packet {} with {} byte body ({} messages)",
                    header.packet_id,
                    info.bytes.len(),
                    last_message_in_packet + 1
                );
                drop_skipped_packets(header.packet_id, self.last_packet_id, &mut self.packet_map);
                let packet = wincode::deserialize(&info.bytes)?;
                self.last_packet_timestamp = header.packet_timestamp;
                self.last_packet_id = header.packet_id;
                return Ok(Some((
                    packet,
                    DateTime::from_timestamp_micros(info.timestamp).unwrap(),
                )));
            }
        }
        Ok(None)
    }
}

fn split_message(message: &[u8]) -> (MessageHeader, &[u8]) {
    let header: MessageHeader = wincode::deserialize(&message[0..MESSAGE_HEADER_SIZE]).unwrap();
    let body = &message[MESSAGE_HEADER_SIZE..];
    (header, body)
}

// Takes into account the fact that packet_id can wrap around from u32::MAX to 0
fn drop_skipped_packets(
    packet_id: u32,
    last_packet_id: u32,
    packet_map: &mut VecDeque<PacketInfo>,
) {
    let num_missed = packet_id.wrapping_sub(last_packet_id).saturating_sub(1);
    if num_missed == 0 {
        return;
    }
    if num_missed > 5 {
        debug!(
            "Packets {} to {} skipped",
            last_packet_id.wrapping_add(1),
            packet_id.wrapping_sub(1)
        );
    }
    for i in 1..=num_missed {
        let id = last_packet_id.wrapping_add(i);
        let info = packet_map.get(0).unwrap();
        if info.id == id {
            debug!(
                "Packet {} skipped ({}/{})",
                info.id,
                info.num_found,
                info.found.len()
            );
            packet_map.pop_front();
        } else {
            debug!("Packet {} skipped (0/unknown)", info.id);
        }
    }
}
