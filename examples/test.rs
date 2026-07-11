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
const MAX_LATENCY: TimeDelta = TimeDelta::seconds(1000);

const PACKET_COOLDOWN: Duration = Duration::from_nanos(1_000_000_000 / PACKETS_PER_SEC);

fn spawn_receiver(stop: Signal) -> JoinHandle<usize> {
    std::thread::spawn(move || {
        let (_, receiver) = netnet::create_client(("::", PORT_RECEIVER), MAX_LATENCY, stop).unwrap();
        let mut num_received = 0;
        loop {
            let (_packet, timestamp) = match receiver.recv::<Vec<u8>>() {
                Ok(t) => t,
                Err(Error::Stopped) => break,
                Err(err) => unreachable!("Receive error: {err}"),
            };
            println!(
                "Received packet (latency: {:.2}ms)",
                (netnet::now() - timestamp).num_microseconds().unwrap() as f32 / 1000.0
            );
            num_received += 1;
        }
        println!("Finished receiving packets");
        num_received
    })
}

fn spawn_sender(stop_receiver: Signal) -> JoinHandle<usize> {
    let start = Instant::now();

    std::thread::spawn(move || {
        let mut packet = vec![0u8; PACKET_SIZE];
        let (sender, _) = netnet::create_server(PORT_SENDER, MAX_LATENCY, Signal::new()).unwrap();

        let mut last_packet_instant = Instant::now();

        let mut num_sent = 0;
        while (Instant::now() - start) < DURATION {
            rand::fill(&mut packet);
            sender.send(&packet).unwrap();
            println!("Sent packet");
            num_sent += 1;
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
        num_sent
    })
}

// run with RUST_LOG=trace for full logs
fn main() {
    pretty_env_logger::init();

    let stop = Signal::new();
    let send_handle = spawn_sender(stop.clone());
    let receive_handle = spawn_receiver(stop);
    let (num_sent, num_received) = (send_handle.join().unwrap(), receive_handle.join().unwrap());
    println!("Sent {} packets.", num_sent);
    println!("Received {} packets.", num_received);
}
