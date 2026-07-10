use std::{collections::HashMap, io, net::UdpSocket, sync::mpsc, thread::JoinHandle, time::Duration};

use chrono::TimeDelta;
use log::debug;
use wincode::{SchemaRead, SchemaReadOwned, SchemaWrite, config::DefaultConfig};

use crate::{
    Signal, TimeStamp,
    message::{self, Message},
    now, reliable,
};

const MAX_PACKET_LATENCY: TimeDelta = TimeDelta::milliseconds(100);

#[derive(SchemaWrite, SchemaRead)]
struct Packet {
    timestamp: i64,
    body: Vec<u8>,
}

pub struct Sender {
    channel: mpsc::Sender<(Packet, bool)>,
    thread_handle: JoinHandle<anyhow::Result<()>>,
}

impl Sender {
    fn new(mut message_sender: reliable::Stream, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = mpsc::channel();
        let thread_handle = std::thread::spawn(move || {
            let mut packet_id = 0u32;
            while !stop.get() {
                let (packet, reliable): (Packet, bool) =
                    match channel_receiver.recv_timeout(Duration::from_micros(100)) {
                        Ok(p) => p,
                        Err(ref err) if err == &mpsc::RecvTimeoutError::Timeout => continue,
                        Err(err) => return Err(err.into()),
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
                    if reliable {
                        message_sender.send_reliable(message);
                    } else {
                        message_sender.send_unreliable(message);
                    }
                }
                packet_id = packet_id.wrapping_add(1);
            }
            Ok(())
        });
        Self {
            channel: channel_sender,
            thread_handle,
        }
    }
    pub fn send<P>(&self, packet: &P, reliable: bool)
    where
        P: SchemaWrite<DefaultConfig, Src = P> + Send + 'static,
    {
        let bytes = wincode::serialize(packet).unwrap();
        self.channel
            .send((
                Packet {
                    timestamp: now().timestamp_micros(),
                    body: bytes,
                },
                reliable,
            ))
            .unwrap();
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
    thread_handle: JoinHandle<anyhow::Result<()>>,
}

impl Receiver {
    fn new(mut message_receiver: reliable::Stream, stop: Signal) -> Self {
        let (channel_sender, channel_receiver) = mpsc::channel();
        let thread_handle = std::thread::spawn(move || {
            let mut map = HashMap::<u32, PartialPacket>::with_capacity(100);

            while !stop.get() {
                let message = message_receiver.recv()?;
                let num_messages = message.last_message_in_packet as usize + 1;
                let packet = map
                    .entry(message.packet_id)
                    .or_insert_with(|| PartialPacket {
                        timestamp: TimeStamp::from_timestamp_micros(message.packet_timestamp)
                            .unwrap(),
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
            thread_handle,
        }
    }

    pub fn recv<P>(&mut self) -> anyhow::Result<(P, TimeStamp)>
    where
        P: SchemaReadOwned<DefaultConfig, Dst = P>,
    {
        let packet = self.channel.recv()?;
        Ok((
            wincode::deserialize(&packet.body)?,
            TimeStamp::from_timestamp_micros(packet.timestamp).unwrap(),
        ))
    }
}

pub fn create_stream(socket: UdpSocket, stop: Signal) -> io::Result<(Sender, Receiver)> {
    let (reliable_sender, reliable_receiver) =
        reliable::create_stream(socket.try_clone()?, stop.clone())?;
    let sender = Sender::new(reliable_sender, stop.clone());
    let receiver = Receiver::new(reliable_receiver, stop);
    Ok((sender, receiver))
}
