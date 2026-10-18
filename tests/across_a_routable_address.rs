//! The cases of across-a-routable-address.std.md: a Core on loopback over TLS, which records what each
//! connection negotiated, and the library's attachments to it.

mod common;

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;

use common::*;
use pb::interface_server::InterfaceServer;
use yoke_proto::interface::v1 as pb;
use yoke_sdk::interface::{Attachment, Options};

/// A root, and a chain for `localhost` issued by it.
struct Pki {
    root_pem: String,
    chain: Vec<CertificateDer<'static>>,
    key: Vec<u8>,
}

fn pki() -> Pki {
    let root_key = KeyPair::generate().unwrap();
    let mut root = CertificateParams::new(vec![]).unwrap();
    root.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    root.distinguished_name
        .push(rcgen::DnType::CommonName, "Yoke test root");
    let root_cert = root.self_signed(&root_key).unwrap();
    let issuer = Issuer::new(root, root_key);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let leaf_cert = leaf.signed_by(&leaf_key, &issuer).unwrap();
    Pki {
        root_pem: root_cert.pem(),
        chain: vec![CertificateDer::from(leaf_cert.der().to_vec())],
        key: leaf_key.serialize_der(),
    }
}

/// What the Core read of one connection.
#[derive(Debug, Clone)]
struct Negotiated {
    version: Option<rustls::ProtocolVersion>,
    alpn: Option<Vec<u8>>,
    client_certificate: bool,
}

struct Tls(tokio_rustls::server::TlsStream<tokio::net::TcpStream>);

impl Connected for Tls {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for Tls {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for Tls {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// A routable Core: where it listens, what its connections negotiated, and how many reached it.
struct Routable {
    address: String,
    negotiated: Arc<Mutex<Vec<Negotiated>>>,
    connections: Arc<AtomicUsize>,
    core: Core,
}

/// Serves a Core over TLS on loopback, limited to the protocol versions given.
fn routable(
    core: Core,
    pki: &Pki,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Routable {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(versions)
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        pki.chain.clone(),
        PrivatePkcs8KeyDer::from(pki.key.clone()).into(),
    )
    .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (negotiated, connections) = (
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
    );
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    {
        let (negotiated, connections) = (negotiated.clone(), connections.clone());
        tokio::spawn(async move {
            let listener = TcpListener::from_std(listener).unwrap();
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                connections.fetch_add(1, Ordering::SeqCst);
                let (acceptor, negotiated, tx) = (acceptor.clone(), negotiated.clone(), tx.clone());
                tokio::spawn(async move {
                    if let Ok(tls) = acceptor.accept(tcp).await {
                        let (_, session) = tls.get_ref();
                        negotiated.lock().unwrap().push(Negotiated {
                            version: session.protocol_version(),
                            alpn: session.alpn_protocol().map(<[u8]>::to_vec),
                            client_certificate: session.peer_certificates().is_some(),
                        });
                        let _ = tx.send(Ok::<_, std::io::Error>(Tls(tls))).await;
                    }
                });
            }
        });
    }
    let served = core.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InterfaceServer::new(served))
            .serve_with_incoming(ReceiverStream::new(rx))
            .await
            .ok();
    });
    Routable {
        address,
        negotiated,
        connections,
        core,
    }
}

fn greeting() -> Core {
    core(|mut a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            while a.requests.recv().await.is_some() {}
        });
    })
}

// std: yoke-sdk-rust:across-a-routable-address.01
#[tokio::test]
async fn the_library_dials_tls_13_alone_against_the_roots_its_caller_supplies() {
    let (a, b) = (pki(), pki());
    let roots = Options::new().tls(a.root_pem.as_bytes(), "localhost");

    let good = routable(greeting(), &a, &[&rustls::version::TLS13]);
    let mut attached = within(
        "the first attachment",
        Attachment::attach(&good.address, roots.clone()),
    )
    .await
    .unwrap();
    assert_eq!(attached.version, 1);
    {
        let read = good.negotiated.lock().unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].version, Some(rustls::ProtocolVersion::TLSv1_3));
        assert_eq!(read[0].alpn.as_deref(), Some(&b"h2"[..]));
        assert!(
            !read[0].client_certificate,
            "a client certificate was offered"
        );
    }
    attached.close().await;

    let old = routable(greeting(), &a, &[&rustls::version::TLS12]);
    assert!(
        Attachment::attach(&old.address, roots.clone())
            .await
            .is_err()
    );
    assert_eq!(
        old.core.attachments.load(Ordering::SeqCst),
        0,
        "something attached over TLS 1.2"
    );

    let stranger = routable(greeting(), &b, &[&rustls::version::TLS13]);
    assert!(
        Attachment::attach(&stranger.address, roots.clone())
            .await
            .is_err()
    );
    assert_eq!(
        stranger.core.attachments.load(Ordering::SeqCst),
        0,
        "something attached to a chain of another root"
    );

    let untouched = routable(greeting(), &a, &[&rustls::version::TLS13]);
    let refused = Attachment::attach(&untouched.address, Options::new().tls(b"", "localhost"))
        .await
        .err()
        .unwrap();
    assert!(refused.to_string().contains("roots"), "{refused}");
    assert_eq!(
        untouched.connections.load(Ordering::SeqCst),
        0,
        "a connection was dialled with no roots"
    );

    let local = Attachment::attach("/tmp/yoke-no-such.sock", roots)
        .await
        .err()
        .unwrap();
    assert!(local.to_string().contains("local socket"), "{local}");
}

// std: yoke-sdk-rust:across-a-routable-address.02
#[tokio::test]
async fn the_credential_is_the_attachments_first_frame_and_its_answer_is_surfaced() {
    use pb::authenticate::Credential;
    use pb::request::Operation as Op;
    let pki = pki();
    let c = core(|mut a: Attached| {
        tokio::spawn(async move {
            // The Core reads the first frame before it opens.
            let Some(Seen::Frame(first)) = a.requests.recv().await else {
                return;
            };
            let mut refused = true;
            if let Some(Op::Authenticate(auth)) =
                request_of(&first).and_then(|r| r.operation.as_ref())
            {
                let accepted = match &auth.credential {
                    Some(Credential::Password(p)) => p.account == "alice" && p.secret == "hunter2x",
                    Some(Credential::Token(t)) => t == "tok-alice",
                    None => false,
                };
                if accepted {
                    refused = false;
                    let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
                    let _ = a
                        .out
                        .send(Ok(answer(
                            &first.call,
                            pb::response::Answer::Authenticate(pb::Authenticated {
                                account: "alice".into(),
                                token: "tok-alice".into(),
                                expires: Some(prost_types::Timestamp {
                                    seconds: 4_000_000_000,
                                    nanos: 0,
                                }),
                            }),
                        )))
                        .await;
                }
            }
            if refused {
                let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
                let _ = a
                    .out
                    .send(Ok(refusal(
                        &first.call,
                        pb::Refusal {
                            code: "auth.invalid".into(),
                            message: "no".into(),
                            detail: None,
                        },
                    )))
                    .await;
            }
            while a.requests.recv().await.is_some() {}
        });
    });
    let server = routable(c, &pki, &[&rustls::version::TLS13]);
    let roots = |o: Options| o.tls(pki.root_pem.as_bytes(), "localhost");

    let mut by_password = within(
        "the password attachment",
        Attachment::attach(
            &server.address,
            roots(Options::new().password("alice", "hunter2x")),
        ),
    )
    .await
    .unwrap();
    let established = by_password.established.clone().unwrap();
    assert_eq!(
        (established.account.as_str(), established.token.as_str()),
        ("alice", "tok-alice")
    );
    assert!(established.expires.is_some());
    by_password.close().await;

    let mut by_token = within(
        "the token attachment",
        Attachment::attach(&server.address, roots(Options::new().token("tok-alice"))),
    )
    .await
    .unwrap();
    assert_eq!(by_token.established.clone().unwrap().account, "alice");
    by_token.close().await;

    let refused = Attachment::attach(
        &server.address,
        roots(Options::new().password("alice", "other")),
    )
    .await
    .err()
    .unwrap();
    match refused {
        yoke_sdk::base::Error::Refusal(r) => assert_eq!(r.code, "auth.invalid"),
        other => panic!("not a refusal: {other}"),
    }
    let frames = server.core.frames.lock().unwrap().clone();
    let firsts: Vec<_> = (1..=3usize)
        .map(|n| {
            frames
                .iter()
                .find(|(m, _)| *m == n)
                .map(|(_, f)| f.clone())
                .unwrap()
        })
        .collect();
    for f in &firsts {
        let r = request_of(f).unwrap();
        assert_eq!(r.version, 1);
        assert!(
            matches!(r.operation, Some(Op::Authenticate(_))),
            "the first frame was not authenticate"
        );
    }
    let credential = |f: &pb::ClientFrame| match request_of(f).and_then(|r| r.operation.clone()) {
        Some(Op::Authenticate(a)) => a.credential,
        _ => None,
    };
    assert!(
        matches!(credential(&firsts[0]), Some(Credential::Password(p)) if p.secret == "hunter2x")
    );
    assert!(matches!(credential(&firsts[1]), Some(Credential::Token(t)) if t == "tok-alice"));
    let _ = Duration::from_millis(0);
}
