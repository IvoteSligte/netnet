use std::{
    collections::{HashMap, hash_map::Entry},
    io,
    net::UdpSocket,
    sync::{
        Arc, Weak,
        mpsc::{self, RecvTimeoutError},
    },
    thread::JoinHandle,
    time::Duration,
};

use chrono::TimeDelta;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    Signal, TimeStamp,
    message::{self, Message},
};

pub struct UnreliableSender(message::Sender);

impl UnreliableSender {
    pub fn send(&self, message: Message) {
        self.0.send(Wrapper::unreliable(message))
    }
}

// (packet ID, message ID)
type Id = (u32, u16);

struct ReliableMessage {
    inner: Message,
    last_sent: TimeStamp,
    acked: bool,
}

#[derive(SchemaRead, SchemaWrite)]
enum Wrapper {
    Reliable(Vec<u8>),
    Unreliable(Vec<u8>),
    Ack(Id),
    Ack2(Id),
}

impl Wrapper {
    pub fn unreliable(message: Message) -> Message {
        Message {
            body: wincode::serialize(&Wrapper::Unreliable(message.body)).unwrap(),
            ..message
        }
    }

    pub fn reliable(message: Message) -> Message {
        Message {
            body: wincode::serialize(&Wrapper::Reliable(message.body)).unwrap(),
            ..message
        }
    }

    pub fn ack(packet_id: u32, message_id: Id) -> Message {
        Message {
            packet_timestamp: crate::now().timestamp_micros(),
            packet_id,
            id: 0,
            last_message_in_packet: 0,
            body: wincode::serialize(&Wrapper::Ack(message_id)).unwrap(),
        }
    }

    pub fn ack2(packet_id: u32, message_id: Id) -> Message {
        Message {
            packet_timestamp: crate::now().timestamp_micros(),
            packet_id,
            id: 0,
            last_message_in_packet: 0,
            body: wincode::serialize(&Wrapper::Ack2(message_id)).unwrap(),
        }
    }
}

fn spawn_repeater_thread(
    message_sender: message::Sender,
    stop: Signal,
) -> (
    mpsc::Sender<(Message, Weak<()>)>,
    JoinHandle<anyhow::Result<()>>,
) {
    let (channel_sender, channel_receiver) = mpsc::channel::<(Message, Weak<()>)>();
    let thread_handle = std::thread::spawn(move || {
        let mut repeat_list = Vec::with_capacity(100);
        while !stop.get() {
            let start = crate::now();
            while crate::now() - start < TimeDelta::milliseconds(10) {
                match channel_receiver.recv_timeout(Duration::from_micros(10)) {
                    Ok((message, drop_signal)) => {
                        if drop_signal.strong_count() > 0 {
                            let now = crate::now();
                            message_sender.send(message.clone());
                            repeat_list.push((now, message, drop_signal));
                        }
                    }
                    Err(err) if err == RecvTimeoutError::Timeout => break,
                    Err(err) => return Err(err.into()),
                }
            }
            repeat_list.retain_mut(|(last_time_sent, message, drop_signal)| {
                if drop_signal.strong_count() == 0 {
                    return false;
                }
                let now = crate::now();
                if (now - *last_time_sent) > TimeDelta::milliseconds(100) {
                    *last_time_sent = now;
                    message_sender.send(message.clone());
                }
                true
            });
        }
        Ok(())
    });
    (channel_sender, thread_handle)
}

pub struct Stream {
    repeat_message_sender: mpsc::Sender<(Message, Weak<()>)>,
    message_sender: message::Sender,
    message_receiver: message::Receiver,
    repeat_map: HashMap<Id, Arc<()>>,
    thread_handle: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl Stream {
    pub fn new(socket: UdpSocket, stop: Signal) -> io::Result<Self> {
        let message_sender = message::Sender::new(socket.try_clone()?, stop.clone());
        let (channel_sender, thread_handle) =
            spawn_repeater_thread(message_sender.clone(), stop.clone());
        Ok(Self {
            repeat_message_sender: channel_sender,
            message_sender,
            message_receiver: message::Receiver::new(socket, stop),
            repeat_map: HashMap::with_capacity(100),
            thread_handle: Arc::new(thread_handle),
        })
    }

    pub fn clone_unreliable_sender(&self) -> UnreliableSender {
        UnreliableSender(self.message_sender.clone())
    }

    pub fn send_reliable(&mut self, message: Message) {
        let message = Wrapper::reliable(message);
        let drop_signal = Arc::new(());
        self.repeat_message_sender
            .send((message.clone(), Arc::downgrade(&drop_signal)))
            .unwrap();
        self.repeat_map.insert((message.packet_id, message.id), drop_signal);
    }

    pub fn send_unreliable(&self, message: Message) {
        self.message_sender.send(Wrapper::unreliable(message));
    }

    pub fn recv(&mut self, mut get_new_packet_id: impl FnMut() -> u32) -> anyhow::Result<Message> {
        loop {
            let message: Message = self.message_receiver.recv()?;
            match wincode::deserialize::<Wrapper>(&message.body)? {
                Wrapper::Unreliable(data) => {
                    return Ok(Message {
                        body: data,
                        ..message
                    });
                }
                Wrapper::Reliable(data) => {
                    let id = (message.packet_id, message.id);
                    match self.repeat_map.entry(id) {
                        Entry::Occupied(_) => continue,
                        Entry::Vacant(vacant_entry) => {
                            let drop_signal = Arc::new(());
                            let ack = Wrapper::ack(get_new_packet_id(), id);
                            self.repeat_message_sender
                                .send((ack, Arc::downgrade(&drop_signal)))
                                .unwrap();
                            vacant_entry.insert(drop_signal);
                            return Ok(Message {
                                body: data,
                                ..message
                            });
                        }
                    }
                }
                // Simply send an ACK2 for every received ACK.
                // Once the peer has received the ACK2, it will stop sending ACKs.
                Wrapper::Ack(id) => {
                    self.repeat_map.remove(&id);
                    self.message_sender
                        .send(Wrapper::ack2(get_new_packet_id(), id));
                }
                // NOTE: If ACK2 is received before another (delayed) Reliable message,
                // then the repeated ACK-sending cycle starts again. This is very unlikely,
                // so I'm not sure if it should be fixed.
                Wrapper::Ack2(id) => {
                    self.repeat_map.remove(&id);
                }
            }
        }
    }
}
