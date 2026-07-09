use std::{
    io::{self, Read, Write},
    net::TcpStream,
    time::Duration,
};

use log::{debug, trace, warn};
use stunclient::StunClient;

use crate::*;

// TODO: encrypt addresses sent over TCP

fn send_public_ip(tcp: &mut TcpStream, public_ip: &SocketAddr) -> io::Result<()> {
    let public_ip = public_ip.to_string();
    let mut bytes = [0u8; 100];
    bytes[0] = b'>';
    bytes[1..99].as_mut().write(public_ip.as_bytes())?;
    bytes[1 + public_ip.len()] = b'<';
    debug!("Writing own public IP to TCP stream");
    tcp.write(&bytes[..1 + public_ip.len() + 1])?;
    debug!("Waiting for write to finish");
    tcp.flush()?;
    Ok(())
}

fn safe_bytes_to_string(bytes: &[u8]) -> String {
    String::from_iter(bytes.iter().copied().map(|b| {
        let c: char = b.into();
        if c.is_ascii_graphic() { c } else { '?' }
    }))
}

fn recv_peer_public_ip(tcp: &mut TcpStream) -> io::Result<SocketAddr> {
    debug!("Waiting for peer public IP");
    let mut bytes = [0u8; 100];
    let mut len = tcp.read(&mut bytes)?;
    if bytes[0] != b'>' {
        return Err(io::Error::other("expected opening address delimiter '>'"));
    }
    trace!(
        "Read {len} bytes: `{}`",
        safe_bytes_to_string(&bytes[..len])
    );
    while !bytes[..len].contains(&b'<') {
        len += tcp.read(&mut bytes[len..])?;
        trace!(
            "Read {len} bytes total: `{}`",
            safe_bytes_to_string(&bytes[..len])
        );
    }
    let end = bytes.iter().position(|b| *b == b'<').unwrap();
    let str = str::from_utf8(&bytes[1..end]).map_err(io::Error::other)?;
    str.parse()
        .map_err(io::Error::other)
        .inspect_err(|_| warn!("Failed to parse peer address `{str}`"))
}

/// Creates a bidirectional packet stream through UDP hole-punch, sending the public IP address over a TCP connection.
/// Make sure the stream is empty on both ends before passing it to this function.
/// `TcpStream::flush` ensures no bytes are left in the queue.
pub fn create_stream_using_hole_punch<P: Packet>(
    tcp: &mut TcpStream,
    max_latency: Duration,
    stop: Signal,
) -> crate::Result<(Sender<P>, Receiver)> {
    debug!("Binding UDP socket");
    // TODO: IPv6
    let udp = UdpSocket::bind("0.0.0.0:0")?;

    debug!("Querying own public IP");
    // TODO: add other stun servers
    let stun = StunClient::with_google_stun_server();
    let public_ip = stun.query_external_address(&udp)?;

    debug!("Exchanging public IP with peer");
    send_public_ip(tcp, &public_ip)?;
    let peer_public_ip = recv_peer_public_ip(tcp)?;

    debug!("Creating stream from UDP socket");
    trace!("Connecting to peer at {}", now());

    dbg!(public_ip, peer_public_ip);

    udp.connect(peer_public_ip)?;

    trace!("Enforcing connection to peer at {}", now());
    debug!("Enforcing connection: sending CONTROL");
    udp.send(b"CONTROL")?;
    debug!("Enforcing connection: receiving CONTROL");
    let mut buf = [0u8; 100];
    let num_read = loop {
        match udp.recv(&mut buf) {
            Ok(ok) => break ok,
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err.into()),
        }
    };
    if &buf[..num_read] != b"CONTROL" {
        return Err(
            io::Error::new(io::ErrorKind::ConnectionRefused, "UDP connection failed").into(),
        );
    }
    Ok(create_stream_from_socket(
        udp.try_clone().unwrap(),
        peer_public_ip,
        max_latency,
        stop,
    )?)
}
