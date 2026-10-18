//! TLS 1.3 under the interface library, done here rather than by gRPC: the handshake is rustls's, built
//! with no code for any version below 1.3, and once it is done the connection is the transport a client
//! speaks HTTP/2 on, as on any connected socket. Never installed.

use std::sync::Arc;

use hyper_util::rt::TokioIo;
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::base::Error;

/// A TLS 1.3 connection to host:port: the Core's chain verified against `roots`, in PEM, and no others, for
/// `name` — the host where it is empty — with ALPN `h2` and no client certificate. A failure names what
/// failed, and nothing is attached.
pub(crate) async fn dial(address: &str, roots: &[u8], name: &str) -> Result<Channel, Error> {
    let host = address
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']))
        .ok_or_else(|| Error::Misuse(format!("{address} is not an address with a port")))?;
    let mut store = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(roots) {
        let cert = cert.map_err(|e| Error::Misuse(format!("the roots cannot be read: {e}")))?;
        store
            .add(cert)
            .map_err(|e| Error::Misuse(format!("a root cannot be used: {e}")))?;
    }
    if store.is_empty() {
        return Err(Error::Misuse(
            "TLS was asked with no roots, and the Core's chain is verified only against the roots its caller supplies"
                .into(),
        ));
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|e| Error::Misuse(e.to_string()))?
    .with_root_certificates(store)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let server_name = ServerName::try_from(if name.is_empty() { host } else { name }.to_string())
        .map_err(|e| {
        Error::Misuse(format!(
            "the name the certificate must carry is not one: {e}"
        ))
    })?;
    let connector = TlsConnector::from(Arc::new(config));
    let address = address.to_string();
    Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let (connector, server_name, address) =
                (connector.clone(), server_name.clone(), address.clone());
            async move {
                let tcp = TcpStream::connect(&address).await?;
                let tls = connector.connect(server_name, tcp).await?;
                Ok::<_, std::io::Error>(TokioIo::new(tls))
            }
        }))
        .await
        .map_err(|e| Error::Transport(e.to_string()))
}
