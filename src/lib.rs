use std::{net::SocketAddr, sync::Arc};

mod average;
pub mod error;
mod insecure;

use anyhow::anyhow;
pub use error::{Error, Result};
use insecure::SkipServerVerification;
use log::info;
use quinn::{
    ClientConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

pub use quinn::Connection;

const SERVER_NAME: &str = "netnet-server";
const PROTOCOL_NAME: &str = "netnet-protocol";

// TODO: reorder buffer? probably necessary to get smooth audio
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
    let config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));

    info!("Creating client endpoint");
    let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;

    endpoint.set_default_client_config(config);

    info!("Finished creating client endpoint");
    Ok(async move {
        info!("Connecting to server");
        let connecting = endpoint.connect(server_addr, SERVER_NAME)?;
        Ok(connecting.await?)
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
    let endpoint = quinn::Endpoint::server(server_config, format!("[::]:{port}").parse().unwrap())?;
    info!("Finished creating server endpoint");
    Ok(async move {
        info!("Accepting incoming connections");
        let incoming = endpoint
            .accept()
            .await
            .ok_or(anyhow!("Connection closed while waiting for client"))?;
        info!("Accepted connection");
        Ok(incoming.await?)
    })
}
