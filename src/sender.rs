use chrono::Utc;
use log::{debug, trace};
use std::{
    net::UdpSocket,
    sync::mpsc,
    time::{Duration, Instant},
};

use crate::*;

pub fn spawn_thread<P: Packet>(socket: UdpSocket) -> mpsc::Sender<(P, TimeStamp)> {
    // TODO: this channel has no upper bound on size. probably want to use a bounded channel instead?
    let (sender, receiver) = mpsc::channel::<(P, TimeStamp)>();

    std::thread::spawn(move || {
        let mut message_buf = vec![0u8; MAX_MESSAGE_SIZE];
        let mut packet_id = 0u32;
        let mut wait_start = Instant::now();
        let mut avg_wait_duration = RunningAverage::new(1000.0);
        for (packet, packet_timestamp) in receiver {
            let packet_timestamp = packet_timestamp.timestamp_micros();
            let wait_duration = (Instant::now() - wait_start).as_micros() as f32 / 1000.0;
            avg_wait_duration.update(wait_duration);
            debug!(
                "Spent {:.2}ms waiting since last packet ({:.2}ms on average)",
                wait_duration,
                avg_wait_duration.get(),
            );
            // NOTE: very large packets (>4MB) can cause a wincode PreallocationSizeLimit error
            let data = wincode::serialize(&packet).unwrap();
            let num_messages = data.len().div_ceil(MAX_MESSAGE_BODY_SIZE);
            debug!(
                "Sending packet {} with {} body bytes ({} messages)",
                packet_id,
                data.len(),
                num_messages
            );
            let mut sleep_duration = Duration::default();
            for id in 0..num_messages {
                let start = id as usize * MAX_MESSAGE_BODY_SIZE;
                let end = (start + MAX_MESSAGE_BODY_SIZE).min(data.len());
                let body_bytes = &data[start..end];
                let header = MessageHeader {
                    packet_timestamp,
                    packet_id,
                    message_id: id as _,
                    last_message_in_packet: (num_messages - 1).try_into().unwrap(),
                };
                let bytes = &mut message_buf;
                bytes.clear();
                bytes.extend(wincode::serialize(&header).unwrap());
                bytes.extend(body_bytes);
                trace!(
                    "Sending {} byte message for packet {} ({}/{})",
                    bytes.len(),
                    packet_id,
                    id + 1,
                    num_messages,
                );
                socket.send(bytes).unwrap();
                let before_sleep = Instant::now();
                std::thread::sleep(SEND_SLEEP_DURATION);
                sleep_duration += Instant::now() - before_sleep;
            }
            let duration = (Utc::now().timestamp_micros() - packet_timestamp) as f32 / 1000.0;
            let sleep_duration = sleep_duration.as_micros() as f32 / 1000.0;
            debug!(
                "Sending packet {} took {:.2}ms ({:.2}ms processing, {:.2}ms sleeping)",
                packet_id,
                duration,
                duration - sleep_duration,
                sleep_duration,
            );
            packet_id = packet_id.wrapping_add(1);
            wait_start = Instant::now();
        }
    });
    sender
}

#[derive(Debug)]
pub struct Sender<P: Send>(mpsc::Sender<(P, TimeStamp)>);

impl<P: Send> Clone for Sender<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P: Packet> Sender<P> {
    pub fn new(socket: UdpSocket) -> Self {
        Self(spawn_thread(socket))
    }

    pub fn send(&self, packet: P) -> TimeStamp {
        // Creates a timestamp for the current time, rounded to the nearest microsecond
        // so that sender and receiver timestamps are exactly equal.
        let timestamp = TimeStamp::from_timestamp_micros(now().timestamp_micros()).unwrap();
        self.0.send((packet, timestamp)).unwrap();
        timestamp
    }
}
