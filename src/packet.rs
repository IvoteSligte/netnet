use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::{Mutex, mpsc},
    time::Duration,
};

use chrono::TimeDelta;
use log::debug;
use wincode::{SchemaRead, SchemaReadOwned, SchemaWrite, config::DefaultConfig};

use crate::{
    Error, Signal, ThreadHandle, TimeStamp, from_micros,
    message::{self, Message},
    since_micros, take_thread_error,
};

const MAX_PACKET_LATENCY: TimeDelta = TimeDelta::milliseconds(100);

#[derive(SchemaWrite, SchemaRead)]
struct Packet {
    timestamp: i64,
    body: Vec<u8>,
}

pub struct Sender {
    channel: mpsc::Sender<Packet>,
    thread_handle: ThreadHandle,
}

impl Sender {
    /// `socket` is assumed to be connected
    pub fn new(socket: UdpSocket, max_latency: TimeDelta, stop: Signal) -> Self {
        let message_sender = message::Sender::new(socket, max_latency, stop.clone());
        let (channel_sender, channel_receiver) = mpsc::channel::<Packet>();
        let thread_handle = std::thread::spawn(move || {
            let mut packet_id = 0u32;
            while !stop.get() {
                let packet = match channel_receiver.recv_timeout(Duration::from_micros(10)) {
                    Ok(packet) => packet,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Err(Error::ChannelClosed),
                };
                let chunks = packet.body.chunks(message::MAX_BODY_SIZE);
                assert!(chunks.len() < u16::MAX as _);
                let num_messages = chunks.len() as u16;
                for (chunk, id) in chunks.zip(0u16..) {
                    let message = Message {
                        packet_timestamp: packet.timestamp,
                        packet_id,
                        id,
                        last_message_in_packet: num_messages - 1,
                        body: chunk.to_vec(),
                    };
                    message_sender.send(message).unwrap();
                }
                packet_id = packet_id.wrapping_add(1);
            }
            Ok(())
        });
        Self {
            channel: channel_sender,
            thread_handle: Mutex::new(Some(thread_handle)),
        }
    }
    pub fn send<P>(&self, packet: &P) -> crate::Result<()>
    where
        P: SchemaWrite<DefaultConfig, Src = P> + Send + 'static,
    {
        let bytes = wincode::serialize(packet).unwrap();
        self.channel
            .send(Packet {
                timestamp: crate::now_micros(),
                body: bytes,
            })
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::SendAfterError))
    }
}

struct PartialPacket {
    timestamp: TimeStamp,
    found: Vec<bool>,
    num_found: usize,
    data: Vec<u8>,
}

pub struct Receiver {
    channel: mpsc::Receiver<Packet>,
    thread_handle: ThreadHandle,
}

impl Receiver {
    /// `socket` is assumed to be connected
    pub fn new(socket: UdpSocket, max_latency: TimeDelta, stop: Signal) -> Self {
        let message_receiver = message::Receiver::new(socket, max_latency, stop.clone());
        let (channel_sender, channel_receiver) = mpsc::channel();
        let thread_handle = std::thread::spawn(move || {
            let mut map = HashMap::<u32, PartialPacket>::with_capacity(100);

            while !stop.get() {
                let message = message_receiver.recv()?;
                if since_micros(message.packet_timestamp) > max_latency {
                    continue;
                }
                let num_messages = message.last_message_in_packet as usize + 1;
                let packet = map
                    .entry(message.packet_id)
                    .or_insert_with(|| PartialPacket {
                        timestamp: from_micros(message.packet_timestamp),
                        found: vec![false; num_messages],
                        num_found: 0,
                        data: vec![0u8; num_messages * crate::message::MAX_BODY_SIZE],
                    });
                if std::mem::replace(&mut packet.found[message.id as usize], true) {
                    debug!(
                        "Duplicate message {} received for packet {}",
                        message.id, message.packet_id
                    );
                    continue;
                }
                let start = crate::message::MAX_BODY_SIZE * message.id as usize;
                let end = start + message.body.len();
                packet.num_found += 1;
                packet.data[start..end].copy_from_slice(&message.body);
                if packet.num_found >= num_messages {
                    let body = std::mem::take(&mut packet.data);
                    channel_sender
                        .send(Packet {
                            timestamp: message.packet_timestamp,
                            body,
                        })
                        .unwrap();
                }
                if map.len() > 1000 {
                    let now = crate::now();
                    // TODO: retain reliable packets for way longer
                    map.retain(|_, packet| (now - packet.timestamp) <= MAX_PACKET_LATENCY);
                }
            }
            Ok(())
        });
        Self {
            channel: channel_receiver,
            thread_handle: Mutex::new(Some(thread_handle)),
        }
    }

    pub fn recv<P>(&self) -> crate::Result<(P, TimeStamp)>
    where
        P: SchemaReadOwned<DefaultConfig, Dst = P>,
    {
        let packet = self
            .channel
            .recv()
            .map_err(|_| take_thread_error(&self.thread_handle).unwrap_or(Error::RecvAfterError))?;
        Ok((
            wincode::deserialize(&packet.body)?,
            from_micros(packet.timestamp),
        ))
    }
}
