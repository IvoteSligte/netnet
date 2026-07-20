use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

use anyhow::anyhow;
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
pub use unreliable::{RecvTimeoutError, UnreliableReceiver, UnreliableSender};

const SERVER_NAME: &str = "netnet-server";
const PROTOCOL_NAME: &str = "netnet-protocol";

pub use quinn::ConnectionError;

/// Create using [create_client] or [create_server]
pub struct Connection {
    pub unreliable_sender: UnreliableSender,
    pub unreliable_receiver: UnreliableReceiver,
    conn: Arc<quinn::Connection>,
}

impl Connection {
    pub(crate) fn new(conn: quinn::Connection) -> Self {
        let conn = Arc::new(conn);
        Self {
            unreliable_sender: UnreliableSender::new(conn.clone()),
            unreliable_receiver: UnreliableReceiver::new(conn.clone()),
            conn,
        }
    }

    /// Creates a reliable, bidirectional stream
    pub async fn create_reliable_stream(
        &self,
        id: u8,
    ) -> anyhow::Result<(ReliableSender, ReliableReceiver)> {
        let (mut sender, receiver) = self.conn.open_bi().await?;
        sender.write(std::slice::from_ref(&id)).await?;
        Ok((ReliableSender(sender), ReliableReceiver(receiver)))
    }

    /// Returns stream ID and the stream sender/receiver pair
    pub async fn accept_reliable_stream(
        &self,
    ) -> anyhow::Result<(u8, ReliableSender, ReliableReceiver)> {
        let (sender, mut receiver) = self.conn.accept_bi().await?;
        let mut id = 0u8;
        receiver.read(std::slice::from_mut(&mut id)).await?;
        Ok((id, ReliableSender(sender), ReliableReceiver(receiver)))
    }

    /// Should not be used unless absolutely necessary
    pub fn inner(&self) -> &Arc<quinn::Connection> {
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
        Ok(Connection::new(connecting.await?))
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
        Ok(Connection::new(incoming.await?))
    })
}
