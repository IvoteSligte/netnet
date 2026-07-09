use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use netnet::*;

const PORT_SENDER: u16 = 8082;
const PORT_RECEIVER: u16 = 8083;
const DURATION: Duration = Duration::from_secs(3);
const PACKETS_PER_SEC: u64 = 30;
const PACKET_SIZE: usize = 3_141_592;
const MAX_LATENCY: Duration = Duration::from_secs(1000);

const PACKET_COOLDOWN: Duration = Duration::from_nanos(1_000_000_000 / PACKETS_PER_SEC);

fn create_stream(port: u16, port_other: u16, stop: Signal) -> (Sender<Vec<u8>>, Receiver) {
    netnet::create_stream(
        port,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port_other),
        MAX_LATENCY,
        stop,
    )
    .unwrap()
}

fn spawn_receiver(stop: Signal) -> JoinHandle<Vec<TimeStamp>> {
    std::thread::spawn(move || {
        let (_, mut receiver) = create_stream(PORT_RECEIVER, PORT_SENDER, stop);
        let mut received = Vec::new();
        loop {
            let (_packet, timestamp) = match receiver.recv::<Vec<u8>>() {
                Ok(t) => t,
                Err(Error::Stopped) => break,
                Err(err) => unreachable!("Receive error: {err}"),
            };
            println!(
                "Received packet {} (ID) latency: {:.2}ms",
                timestamp.timestamp_micros(),
                (netnet::now() - timestamp).num_microseconds().unwrap() as f32 / 1000.0
            );
            received.push(timestamp);
        }
        println!("Finished receiving packets");
        received
    })
}

fn spawn_sender(stop_receiver: Signal) -> JoinHandle<Vec<TimeStamp>> {
    let start = Instant::now();

    std::thread::spawn(move || {
        let mut packet = vec![0u8; PACKET_SIZE];
        let (sender, _) = create_stream(PORT_SENDER, PORT_RECEIVER, Signal::new());

        let mut sent = Vec::new();
        let mut last_packet_instant = Instant::now();

        while (Instant::now() - start) < DURATION {
            rand::fill(&mut packet);
            let timestamp = sender.send(packet.clone());
            println!("Sent packet {} (ID)", timestamp.timestamp_micros());
            sent.push(timestamp);
            let now = Instant::now();
            if (now - last_packet_instant) < PACKET_COOLDOWN {
                std::thread::sleep(now - last_packet_instant);
            }
            last_packet_instant = now;
        }
        println!("Waiting a few seconds, hoping that the receiver receives all the packets...");
        std::thread::sleep(Duration::from_secs(1));
        stop_receiver.set(); // stop receiver
        println!("Finished sending packets");
        sent
    })
}

// run with RUST_LOG=trace for full logs
fn main() {
    pretty_env_logger::init();

    let stop = Signal::new();
    let send_handle = spawn_sender(stop.clone());
    let receive_handle = spawn_receiver(stop);
    let (sent, received) = (send_handle.join().unwrap(), receive_handle.join().unwrap());
    println!("Sent {} packets.", sent.len());
    println!("Received {} packets.", received.len());
    if sent != received {
        let sent = HashSet::<&TimeStamp>::from_iter(&sent);
        let received = HashSet::from_iter(&received);
        let missed = sent.difference(&received);
        for timestamp in missed {
            println!(
                "Receiver missed packet {} (ID)",
                timestamp.timestamp_micros()
            );
        }
    }
    assert!(sent == received);
}
