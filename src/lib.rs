use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

use anyhow::{anyhow, bail};
pub use error::{Error, Result};
use insecure::SkipServerVerification;
use log::info;
use quinn::{
    ClientConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

mod average;
pub mod error;
mod insecure;
pub mod reliable;
pub mod unreliable;

pub use reliable::{ReliableReceiver, ReliableSender};
use unreliable::spawn_unreliable_receivers;
pub use unreliable::{RecvTimeoutError, UnreliableReceiver, UnreliableSender};

const SERVER_NAME: &str = "netnet-server";
const PROTOCOL_NAME: &str = "netnet-protocol";

pub use quinn::ConnectionError;

// TODO: make all streams unidirectional?

type QuinnStream = (quinn::SendStream, quinn::RecvStream);

/// Create using [create_client] or [create_server]
pub struct Connection {
    _endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    control_stream: QuinnStream,
    reliable_stream_ids: HashSet<u8>,
    unreliable_receivers: HashMap<u8, UnreliableReceiver>,
}

impl Connection {
    pub(crate) fn new(
        endpoint: quinn::Endpoint,
        conn: quinn::Connection,
        control_stream: QuinnStream,
    ) -> Self {
        let receivers = spawn_unreliable_receivers(endpoint.clone(), conn.clone())
            .into_iter()
            .map(|channel| UnreliableReceiver::new(channel));

        Self {
            _endpoint: endpoint,
            conn,
            control_stream,
            reliable_stream_ids: HashSet::from_iter(0u8..u8::MAX),
            unreliable_receivers: (0u8..).zip(receivers).collect(),
        }
    }

    /// Creates a reliable, bidirectional stream
    pub async fn create_reliable_stream(
        &mut self,
        id: u8,
    ) -> anyhow::Result<(ReliableSender, ReliableReceiver)> {
        info!("Creating reliable stream with ID {id}");
        if !self.reliable_stream_ids.remove(&id) {
            return Err(anyhow!("Reliable stream ID {id} is already taken"));
        }
        let (mut sender, receiver) = self.conn.open_bi().await?;
        sender.write(std::slice::from_ref(&id)).await?;
        Ok((ReliableSender(sender), ReliableReceiver(receiver)))
    }

    /// Returns stream ID and the stream sender/receiver pair
    pub async fn accept_reliable_stream(
        &mut self,
    ) -> anyhow::Result<(u8, ReliableSender, ReliableReceiver)> {
        info!("Accepting reliable stream");
        if self.reliable_stream_ids.is_empty() {
            return Err(anyhow!("All streams are taken"));
        }
        let (sender, mut receiver) = self.conn.accept_bi().await?;
        let mut id = 0u8;
        receiver.read(std::slice::from_mut(&mut id)).await?;
        info!("Accepted reliable stream with ID {id}");
        Ok((id, ReliableSender(sender), ReliableReceiver(receiver)))
    }

    /// Creates an unreliable, bidirectional stream
    pub async fn create_unreliable_stream(
        &mut self,
        id: u8,
    ) -> anyhow::Result<(UnreliableSender, UnreliableReceiver)> {
        info!("Creating unreliable stream with ID {id}");
        let receiver = self
            .unreliable_receivers
            .remove(&id)
            .ok_or_else(|| anyhow!("Unreliable stream ID {id} is already taken"))?;
        let sender = UnreliableSender::new(self._endpoint.clone(), self.conn.clone(), id);
        self.control_stream.0.write(&[id]).await?;
        Ok((sender, receiver))
    }

    /// Returns stream ID and the stream sender/receiver pair
    pub async fn accept_unreliable_stream(
        &mut self,
    ) -> anyhow::Result<(u8, UnreliableSender, UnreliableReceiver)> {
        info!("Accepting unreliable stream");
        if self.unreliable_receivers.is_empty() {
            return Err(anyhow!("All streams are taken"));
        }
        let mut id = 0u8;
        self.control_stream
            .1
            .read_exact(std::slice::from_mut(&mut id))
            .await?;
        match self.unreliable_receivers.remove(&id) {
            Some(receiver) => {
                let sender = UnreliableSender::new(self._endpoint.clone(), self.conn.clone(), id);
                info!("Accepted unreliable stream with ID {id}");
                return Ok((id, sender, receiver));
            }
            None => bail!("Accepted duplicate stream with ID {id}"),
        }
    }

    /// Closes the connection with a `NO_ERROR` error code and the given `reason`.
    /// Consider using a byte string `b"Reason"` to provide a human-readable reason.
    pub fn close(&self, reason: &[u8]) {
        self.conn.close(quinn::VarInt::from_u32(0x0), reason);
    }

    /// Should not be used unless absolutely necessary
    pub fn inner(&self) -> &quinn::Connection {
        &self.conn
    }
}

// TODO: reorder buffer? probably necessary to get smooth audio
//       based on experimentation, only having a buffer for fragments of large packets is sufficient for video
// TODO: forward error correction

// TODO: allow loading cert and key from file
fn generate_self_signed_cert() -> anyhow::Result<(CertificateDer<'static>, PrivateKeyDer<'static>)>
{
    // FIXME: switch to non-localhost?
    let cert = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    Ok((cert_der, key))
}

/// Arbitrary number used to indicate control stream initialization.
/// A value that is unlikely to occur in random data was chosen.
const CONTROL_STREAM_INIT: u8 = 177;

/// Requires a Tokio runtime (even to run the sync code)
pub fn create_client(
    server_addr: SocketAddr,
) -> anyhow::Result<impl Future<Output = anyhow::Result<Connection>>> {
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        // TEMP: only for debugging
        .with_custom_certificate_verifier(SkipServerVerification::new())
        // TEMP: only for debugging
        .with_no_client_auth();
    crypto.alpn_protocols = vec![PROTOCOL_NAME.into()];
    // TODO: set transportconfig::keep_alive_interval
    let config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));

    info!("Creating client endpoint");
    let mut endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))?;

    endpoint.set_default_client_config(config);

    info!("Finished creating client endpoint");
    Ok(async move {
        info!("Connecting to server");
        let connecting = endpoint.connect(server_addr, SERVER_NAME)?;
        let conn = connecting.await?;
        let mut control_stream = conn.accept_bi().await?;
        let mut buf = [0];
        control_stream.1.read_exact(&mut buf).await?;
        assert_eq!(buf[0], CONTROL_STREAM_INIT);
        Ok(Connection::new(endpoint, conn, control_stream))
    })
}

/// Requires a Tokio runtime (even to run the sync code)
pub fn create_server(
    port: u16,
) -> anyhow::Result<impl Future<Output = anyhow::Result<Connection>>> {
    info!("Generating certificate");
    let (cert, key) = generate_self_signed_cert()?;

    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    crypto.alpn_protocols = vec![PROTOCOL_NAME.into()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    info!("Creating server endpoint");
    let endpoint = quinn::Endpoint::server(
        server_config,
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port),
    )?;
    info!("Finished creating server endpoint");
    Ok(async move {
        info!("Accepting incoming connections");
        let incoming = endpoint
            .accept()
            .await
            .ok_or(anyhow!("Connection closed while waiting for client"))?;
        info!("Accepted connection");
        let conn = incoming.await?;
        let mut control_stream = conn.open_bi().await?;
        // Something must be written to open the stream
        control_stream.0.write(&[CONTROL_STREAM_INIT]).await?;
        info!("Created control stream");
        Ok(Connection::new(endpoint, conn, control_stream))
    })
}
