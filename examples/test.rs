use std::{
    thread::JoinHandle,
    time::{Duration, Instant},
};

use netnet::*;

const PORT_SENDER: u16 = 8082;
const DURATION: Duration = Duration::from_secs(3);
const PACKET_SIZE: usize = 31_592; // arbitrary
const MAX_LATENCY: TimeDelta = TimeDelta::seconds(1000);

fn spawn_receiver(stop: Signal) -> JoinHandle<usize> {
    std::thread::spawn(move || {
        let (sender, receiver) =
            netnet::create_client(("::", PORT_SENDER), MAX_LATENCY, stop, Some("REC")).unwrap();
        let mut num_received = 0;
        loop {
            let packet = match receiver.recv() {
                Ok(t) => t,
                Err(Error::Stopped) => break,
                Err(err) => panic!("Receive error: {err}"),
            };
            println!(
                "Received {} byte packet (latency: {:.2}ms)",
                packet.body.len(),
                netnet::latency_micros(packet.timestamp)
            );
            num_received += 1;
        }
        println!("Finished receiving packets");
        // sender must be kept alive until now to maintain the connection
        drop(sender);
        num_received
    })
}

fn spawn_sender(stop_receiver: Signal) -> JoinHandle<usize> {
    let start = Instant::now();
    let receiver =
        netnet::create_server(PORT_SENDER, MAX_LATENCY, Signal::new(), Some("SEN")).unwrap();

    std::thread::spawn(move || {
        let sender = receiver.accept().unwrap();
        let mut num_sent = 0;

        while (Instant::now() - start) < DURATION {
            let mut packet = vec![0u8; PACKET_SIZE];
            rand::fill(&mut packet);
            sender.send(packet).unwrap();
            num_sent += 1;
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
