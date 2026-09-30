//! The cases of the-plugin-library.std.md, one test each, against a plugin channel that records what
//! arrives and says what it is told to.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

use pb::register_server::{Register, RegisterServer};
use pb::session_server::{Session, SessionServer};
use yoke_base::Error;
use yoke_plugin::{
    Capability, Declaration, Event, Object, Outcome, SDK_LINE, Severity, Stream, start_with,
};
use yoke_proto::plugin::v1 as pb;

/// What the channel saw, and what it will answer.
#[derive(Default)]
struct Seen {
    registrations: Vec<pb::RegisterRequest>,
    bound_when_registered: Vec<bool>,
    sessions: usize,
    received: Vec<(Instant, pb::Envelope)>,
    to_unit: Option<mpsc::Sender<Result<pb::Envelope, Status>>>,
}

#[derive(Clone)]
struct Channel {
    seen: Arc<Mutex<Seen>>,
    answer: Arc<pb::RegisterResponse>,
    bind: PathBuf,
    // Sends nothing on a Session, not even the start of its answer, until two heartbeats arrived.
    quiet: bool,
}

#[tonic::async_trait]
impl Register for Channel {
    async fn register(
        &self,
        request: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterResponse>, Status> {
        let mut seen = self.seen.lock().unwrap();
        seen.registrations.push(request.into_inner());
        seen.bound_when_registered.push(self.bind.exists());
        Ok(Response::new((*self.answer).clone()))
    }
}

#[tonic::async_trait]
impl Session for Channel {
    type OpenStream = ReceiverStream<Result<pb::Envelope, Status>>;

    async fn open(
        &self,
        request: Request<Streaming<pb::Envelope>>,
    ) -> Result<Response<Self::OpenStream>, Status> {
        let (tx, rx) = mpsc::channel(16);
        {
            let mut seen = self.seen.lock().unwrap();
            seen.sessions += 1;
            seen.to_unit = Some(tx);
        }
        let mut inbound = request.into_inner();
        let seen = self.seen.clone();
        let quiet = self.quiet;
        let reading = tokio::spawn(async move {
            while let Some(Ok(e)) = inbound.next().await {
                let closing = matches!(&e.payload, Some(pb::envelope::Payload::Session(s))
                    if matches!(s.kind, Some(pb::session_message::Kind::Close(_))));
                let mut seen = seen.lock().unwrap();
                seen.received.push((Instant::now(), e));
                if closing {
                    // The Core ends the stream on a CLOSE.
                    seen.to_unit = None;
                }
            }
        });
        if quiet {
            let deadline = Instant::now() + Duration::from_millis(1500);
            while self
                .seen
                .lock()
                .unwrap()
                .received
                .iter()
                .filter(|(_, e)| is_health(e))
                .count()
                < 2
                && Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let _ = reading;
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// A plugin channel on a socket of its own, and the environment a unit started against it reads.
struct Bench {
    channel: Channel,
    dir: PathBuf,
    env: HashMap<String, String>,
}

impl Drop for Bench {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn accepted() -> pb::RegisterResponse {
    pb::RegisterResponse {
        outcome: pb::register_response::Outcome::Accepted as i32,
        session_id: "sid-1".into(),
        granted: Some(pb::Surface::default()),
        heartbeat: Some(pb::HeartbeatTerms {
            interval: Some(prost_types::Duration {
                seconds: 0,
                nanos: 100_000_000,
            }),
            tolerance: 3,
        }),
        ..Default::default()
    }
}

async fn bench(answer: pb::RegisterResponse) -> Bench {
    bench_of(answer, false).await
}

async fn bench_of(answer: pb::RegisterResponse, quiet: bool) -> Bench {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "ykr-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    std::fs::create_dir_all(dir.join("plugins")).unwrap();
    let socket = dir.join("plugin.sock");
    let bind = dir.join("plugins").join("acquire.sock");
    let channel = Channel {
        seen: Arc::default(),
        answer: Arc::new(answer),
        bind: bind.clone(),
        quiet,
    };
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let served = channel.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RegisterServer::new(served.clone()))
            .add_service(SessionServer::new(served))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .ok();
    });
    let env = HashMap::from([
        (
            "YOKE_PLUGIN".to_string(),
            "com.yoke.station.acquire".to_string(),
        ),
        ("YOKE_UNIT".to_string(), "acquire".to_string()),
        (
            "YOKE_SOCKET".to_string(),
            socket.to_string_lossy().into_owned(),
        ),
        ("YOKE_BIND".to_string(), bind.to_string_lossy().into_owned()),
        ("YOKE_TOKEN".to_string(), "t-1".to_string()),
    ]);
    Bench { channel, dir, env }
}

impl Bench {
    fn getenv(&self) -> impl Fn(&str) -> Option<String> + '_ {
        |k| self.env.get(k).cloned()
    }

    fn seen<T>(&self, read: impl FnOnce(&Seen) -> T) -> T {
        read(&self.channel.seen.lock().unwrap())
    }

    async fn send(&self, e: pb::Envelope) {
        let tx = self
            .seen(|s| s.to_unit.clone())
            .expect("no Session is open");
        tx.send(Ok(e)).await.unwrap();
    }

    /// Waits until the channel has received an envelope for which found holds, and returns it.
    async fn received(&self, found: impl Fn(&pb::Envelope) -> bool) -> pb::Envelope {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(e) = self.seen(|s| {
                s.received
                    .iter()
                    .map(|(_, e)| e)
                    .find(|e| found(e))
                    .cloned()
            }) {
                return e;
            }
            assert!(Instant::now() < deadline, "the channel never received it");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn station() -> Declaration {
    Declaration {
        id: "com.yoke.station.acquire".into(),
        needs: vec!["device:instrument".into()],
        streams: vec![
            Stream {
                id: "station.spectra".into(),
                tolerates_loss: false,
                tolerates_reorder: false,
            },
            Stream {
                id: "station.diagnostics".into(),
                tolerates_loss: true,
                tolerates_reorder: true,
            },
        ],
        commands: vec!["calibrate".into()],
        queries: vec!["head-status".into()],
        occurrences: vec!["calibration.drift".into()],
        capabilities: vec![
            Capability {
                name: "stream.spectra.publish".into(),
                governs: Object::Stream("station.spectra".into()),
            },
            Capability {
                name: "stream.diagnostics.publish".into(),
                governs: Object::Stream("station.diagnostics".into()),
            },
            Capability {
                name: "command.calibrate.accept".into(),
                governs: Object::Command("calibrate".into()),
            },
            Capability {
                name: "query.head-status.answer".into(),
                governs: Object::Query("head-status".into()),
            },
            Capability {
                name: "event.calibration-drift.report".into(),
                governs: Object::Occurrence("calibration.drift".into()),
            },
        ],
    }
}

fn is_health(e: &pb::Envelope) -> bool {
    matches!(e.payload, Some(pb::envelope::Payload::Health(_)))
}

// std: yoke-sdk-rust:the-plugin-library.01
#[test]
fn a_declaration_generates_the_manifest() {
    let want = "\
manifest: 1
id: com.yoke.station.acquire
protocol: 1
needs:
  - device:instrument
streams:
  - id: station.spectra
  - id: station.diagnostics
    tolerates_loss: true
    tolerates_reorder: true
commands:
  - id: calibrate
queries:
  - id: head-status
occurrences:
  - id: calibration.drift
capabilities:
  - name: stream.spectra.publish
    governs:
      stream: station.spectra
  - name: stream.diagnostics.publish
    governs:
      stream: station.diagnostics
  - name: command.calibrate.accept
    governs:
      command: calibrate
  - name: query.head-status.answer
    governs:
      query: head-status
  - name: event.calibration-drift.report
    governs:
      occurrence: calibration.drift
";
    assert_eq!(station().manifest(), want);
}

// std: yoke-sdk-rust:the-plugin-library.02
#[test]
fn nothing_the_model_does_not_have_can_be_declared() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"),
    )
    .unwrap();
    for name in ["Declaration", "Stream", "Capability", "Object"] {
        let start = source
            .find(&format!("pub struct {name} "))
            .or_else(|| source.find(&format!("pub enum {name} ")))
            .unwrap_or_else(|| panic!("no type {name}"));
        let body = &source[start..start + source[start..].find('}').unwrap()];
        for forbidden in ["endpoint", "autostart", "digest", "description"] {
            assert!(
                !body.to_lowercase().contains(forbidden),
                "{name} lets an author set {forbidden}"
            );
        }
    }
}

// std: yoke-sdk-rust:the-plugin-library.03
#[tokio::test]
async fn the_registration_claims_what_the_manifest_declares() {
    let b = bench(accepted()).await;
    let d = station();
    let _unit = start_with(&d, b.getenv()).await.unwrap();
    let request = b.seen(|s| s.registrations[0].clone());
    assert_eq!(
        (
            request.plugin.as_str(),
            request.unit.as_str(),
            request.token.as_str()
        ),
        ("com.yoke.station.acquire", "acquire", "t-1")
    );
    assert_eq!(request.protocol, 1);
    assert_eq!(request.language, "rust");
    assert_eq!(request.sdk_line, SDK_LINE);
    let declared = request.declared.unwrap();
    assert_eq!(
        declared.capabilities,
        d.capabilities
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        declared.streams,
        vec!["station.spectra", "station.diagnostics"]
    );
    assert_eq!(declared.commands, d.commands);
    assert_eq!(declared.queries, d.queries);
    assert_eq!(declared.occurrences, d.occurrences);
}

// std: yoke-sdk-rust:the-plugin-library.04
#[tokio::test]
async fn the_units_socket_is_bound_before_it_registers() {
    let b = bench(accepted()).await;
    let _unit = start_with(&station(), b.getenv()).await.unwrap();
    assert_eq!(b.seen(|s| s.bound_when_registered.clone()), vec![true]);
}

// std: yoke-sdk-rust:the-plugin-library.05
#[tokio::test]
async fn a_refusal_is_surfaced_and_never_retried() {
    let b = bench(pb::RegisterResponse {
        outcome: pb::register_response::Outcome::Refused as i32,
        stage: pb::Stage::Authentication as i32,
        code: "admission.auth.consumed".into(),
        message: "the token was already spent".into(),
        ..Default::default()
    })
    .await;
    match start_with(&station(), b.getenv()).await {
        Err(Error::Refusal(r)) => {
            assert_eq!(r.code, "admission.auth.consumed");
            assert_eq!(r.stage.as_deref(), Some("authentication"));
        }
        Err(other) => panic!("starting failed with {other}"),
        Ok(_) => panic!("a refused unit started"),
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(b.seen(|s| (s.registrations.len(), s.sessions)), (1, 0));
}

// std: yoke-sdk-rust:the-plugin-library.06
#[tokio::test]
async fn an_acceptance_with_restrictions_names_what_was_withheld() {
    let mut answer = accepted();
    answer.outcome = pb::register_response::Outcome::AcceptedWithRestrictions as i32;
    answer.granted = Some(pb::Surface {
        streams: vec!["station.spectra".into()],
        capabilities: vec!["stream.spectra.publish".into()],
        ..Default::default()
    });
    answer.withheld = Some(pb::Surface {
        streams: vec!["station.diagnostics".into()],
        capabilities: vec!["stream.diagnostics.publish".into()],
        occurrences: vec!["calibration.drift".into()],
        ..Default::default()
    });
    let b = bench(answer).await;
    let unit = start_with(&station(), b.getenv()).await.unwrap();
    let admission = unit.admission();
    assert!(admission.restricted);
    assert_eq!(admission.granted.streams, vec!["station.spectra"]);
    assert_eq!(admission.withheld.streams, vec!["station.diagnostics"]);
    assert_eq!(
        admission.withheld.capabilities,
        vec!["stream.diagnostics.publish"]
    );
    assert_eq!(admission.withheld.occurrences, vec!["calibration.drift"]);
}

// std: yoke-sdk-rust:the-plugin-library.07
#[tokio::test]
async fn the_session_opens_and_beats_on_the_cores_terms() {
    let b = bench(accepted()).await;
    let _unit = start_with(&station(), b.getenv()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(450)).await;
    let received = b.seen(|s| s.received.clone());
    let first = &received[0].1;
    assert!(
        matches!(&first.payload, Some(pb::envelope::Payload::Session(s)) if matches!(s.kind, Some(pb::session_message::Kind::Open(_)))),
        "the first envelope is {first:?}"
    );
    assert_eq!(first.session_id, "sid-1");
    let beats: Vec<Instant> = received
        .iter()
        .filter(|(_, e)| is_health(e))
        .map(|(at, _)| *at)
        .collect();
    assert!(beats.len() >= 3, "{} heartbeats in 450 ms", beats.len());
    for pair in beats.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(50),
            "two heartbeats {:?} apart",
            pair[1] - pair[0]
        );
    }
}

// std: yoke-sdk-rust:the-plugin-library.08
#[tokio::test]
async fn the_end_of_a_session_is_surfaced_and_nothing_reconnects() {
    let b = bench(accepted()).await;
    let unit = start_with(&station(), b.getenv()).await.unwrap();
    b.received(|e| matches!(e.payload, Some(pb::envelope::Payload::Session(_))))
        .await;
    b.send(pb::Envelope {
        message_id: "c-1".into(),
        session_id: "sid-1".into(),
        payload: Some(pb::envelope::Payload::Session(pb::SessionMessage {
            kind: Some(pb::session_message::Kind::Revoked(
                pb::session_message::Revoked {
                    cause: pb::session_message::revoked::Cause::PluginDisabled as i32,
                    line: "an operator disabled it".into(),
                },
            )),
        })),
        ..Default::default()
    })
    .await;
    match tokio::time::timeout(Duration::from_secs(2), unit.next())
        .await
        .unwrap()
    {
        Some(Event::Ended(end)) => {
            assert!(!end.closed);
            assert_eq!(end.cause.as_deref(), Some("plugin disabled"));
            assert_eq!(end.line, "an operator disabled it");
        }
        other => panic!("the unit surfaced {other:?}"),
    }
    assert!(unit.next().await.is_none(), "something followed the end");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(b.seen(|s| (s.registrations.len(), s.sessions)), (1, 1));
}

// std: yoke-sdk-rust:the-plugin-library.09
#[tokio::test]
async fn an_orderly_close_is_the_units() {
    let b = bench(accepted()).await;
    let unit = start_with(&station(), b.getenv()).await.unwrap();
    unit.close().await.unwrap();
    b.received(|e| matches!(&e.payload, Some(pb::envelope::Payload::Session(s)) if matches!(s.kind, Some(pb::session_message::Kind::Close(_))))).await;
    match tokio::time::timeout(Duration::from_secs(3), unit.next())
        .await
        .unwrap()
    {
        Some(Event::Ended(end)) => assert!(end.closed, "the end is {end:?}"),
        other => panic!("the unit surfaced {other:?}"),
    }
}

// std: yoke-sdk-rust:the-plugin-library.10
#[tokio::test]
async fn what_the_core_sends_is_surfaced_and_answered_correlated() {
    let b = bench(accepted()).await;
    let unit = start_with(&station(), b.getenv()).await.unwrap();
    b.received(|e| matches!(e.payload, Some(pb::envelope::Payload::Session(_))))
        .await;
    b.send(pb::Envelope {
        message_id: "c-1".into(),
        session_id: "sid-1".into(),
        payload: Some(pb::envelope::Payload::Control(pb::Control {
            kind: Some(pb::control::Kind::Command(pb::control::Command {
                r#type: "calibrate".into(),
                payload: vec![],
            })),
        })),
        ..Default::default()
    })
    .await;
    b.send(pb::Envelope {
        message_id: "c-2".into(),
        session_id: "sid-1".into(),
        payload: Some(pb::envelope::Payload::Query(pb::Query {
            kind: Some(pb::query::Kind::Question(pb::query::Question {
                r#type: "head-status".into(),
                payload: vec![],
            })),
        })),
        ..Default::default()
    })
    .await;
    let command = match tokio::time::timeout(Duration::from_secs(2), unit.next())
        .await
        .unwrap()
    {
        Some(Event::Command(c)) => c,
        other => panic!("first the unit surfaced {other:?}"),
    };
    assert_eq!(command.r#type, "calibrate");
    let question = match tokio::time::timeout(Duration::from_secs(2), unit.next())
        .await
        .unwrap()
    {
        Some(Event::Question(q)) => q,
        other => panic!("second the unit surfaced {other:?}"),
    };
    assert_eq!(question.r#type, "head-status");
    unit.ack(&command, Outcome::Done, "calibrated")
        .await
        .unwrap();
    unit.answer(&question, b"42".to_vec()).await.unwrap();
    let ack = b
        .received(|e| matches!(e.payload, Some(pb::envelope::Payload::Ack(_))))
        .await;
    assert_eq!(ack.correlation_id, "c-1");
    let answer = b
        .received(|e| matches!(e.payload, Some(pb::envelope::Payload::Query(_))))
        .await;
    assert_eq!(answer.correlation_id, "c-2");
}

// std: yoke-sdk-rust:the-plugin-library.11
#[tokio::test]
async fn an_occurrence_carries_the_authors_severity_or_is_refused() {
    let b = bench(accepted()).await;
    let unit = start_with(&station(), b.getenv()).await.unwrap();
    assert!(
        unit.report("calibration.drift", None, "drifting", vec![])
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        b.seen(|s| !s
            .received
            .iter()
            .any(|(_, e)| matches!(e.payload, Some(pb::envelope::Payload::Event(_))))),
        "an occurrence with no severity was sent"
    );
    unit.report(
        "calibration.drift",
        Some(Severity::of(40)),
        "drifting",
        vec![],
    )
    .await
    .unwrap();
    let event = b
        .received(|e| matches!(e.payload, Some(pb::envelope::Payload::Event(_))))
        .await;
    match event.payload {
        Some(pb::envelope::Payload::Event(ev)) => assert_eq!(
            (ev.occurrence.as_str(), ev.severity),
            ("calibration.drift", 40)
        ),
        _ => unreachable!(),
    }
}

// std: yoke-sdk-rust:the-plugin-library.12
#[tokio::test]
async fn nothing_is_emitted_on_a_stream_not_activated() {
    let b = bench(accepted()).await;
    let unit = start_with(&station(), b.getenv()).await.unwrap();
    match unit.emit("station.spectra", b"x".to_vec()).await {
        Err(Error::Refusal(r)) => assert_eq!(r.code, "stream.inactive"),
        other => panic!("emitting answered {other:?}"),
    }
    let mut entries: Vec<String> = std::fs::read_dir(b.dir.join("plugins"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    assert_eq!(
        entries,
        vec!["acquire.sock"],
        "a socket was made for the stream"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        b.seen(|s| !s
            .received
            .iter()
            .any(|(_, e)| matches!(e.payload, Some(pb::envelope::Payload::Data(_))))),
        "data reached the channel"
    );
}

// std: yoke-sdk-rust:the-plugin-library.13
#[tokio::test]
async fn the_unit_beats_whatever_the_core_has_sent() {
    let b = bench_of(accepted(), true).await;
    let _unit = tokio::time::timeout(
        Duration::from_millis(500),
        start_with(&station(), b.getenv()),
    )
    .await
    .expect("starting waited for the Core to send something")
    .unwrap();
    b.received(is_health).await;
}
