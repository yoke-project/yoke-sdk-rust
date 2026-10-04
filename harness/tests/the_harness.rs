//! The cases of the-harness.std.md, one test each. The harness runs as the process the suite launches,
//! against a plugin channel and the suite's control socket, both made here.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

use pb::register_server::{Register, RegisterServer};
use pb::session_server::{Session, SessionServer};
use yoke_proto::plugin::v1 as pb;

#[derive(Clone)]
struct Channel {
    answer: Arc<dyn Fn(&pb::RegisterRequest) -> pb::RegisterResponse + Send + Sync>,
    to_unit: Arc<Mutex<Option<mpsc::Sender<Result<pb::Envelope, Status>>>>>,
}

#[tonic::async_trait]
impl Register for Channel {
    async fn register(
        &self,
        request: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterResponse>, Status> {
        Ok(Response::new((self.answer)(request.get_ref())))
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
        *self.to_unit.lock().unwrap() = Some(tx.clone());
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            while let Some(Ok(e)) = inbound.next().await {
                if matches!(&e.payload, Some(pb::envelope::Payload::Session(s)) if matches!(s.kind, Some(pb::session_message::Kind::Close(_))))
                {
                    drop(tx);
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn restricted(req: &pb::RegisterRequest) -> pb::RegisterResponse {
    pb::RegisterResponse {
        outcome: pb::register_response::Outcome::AcceptedWithRestrictions as i32,
        session_id: "sid-1".into(),
        granted: Some(pb::Surface::default()),
        withheld: req.declared.clone(),
        heartbeat: Some(pb::HeartbeatTerms {
            interval: Some(prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
            tolerance: 3,
        }),
        ..Default::default()
    }
}

/// The suite's side of one harness: its control connection, and the channel it registers on.
struct Suite {
    lines: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    child: tokio::process::Child,
    channel: Channel,
    next: usize,
    dir: PathBuf,
}

impl Drop for Suite {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn started(
    answer: impl Fn(&pb::RegisterRequest) -> pb::RegisterResponse + Send + Sync + 'static,
) -> (Suite, Value) {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "ykh-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let channel = Channel {
        answer: Arc::new(answer),
        to_unit: Arc::default(),
    };
    let plugins = UnixListener::bind(dir.join("plugin.sock")).unwrap();
    let served = channel.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RegisterServer::new(served.clone()))
            .add_service(SessionServer::new(served))
            .serve_with_incoming(UnixListenerStream::new(plugins))
            .await
            .ok();
    });
    let control = UnixListener::bind(dir.join("control.sock")).unwrap();
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_yoke-rust-plugin-harness"))
        .env_clear()
        .env("CONFORMANCE_SOCKET", dir.join("control.sock"))
        .env("YOKE_PLUGIN", "com.yoke.conformance.rust")
        .env("YOKE_UNIT", "harness")
        .env("YOKE_SOCKET", dir.join("plugin.sock"))
        .env("YOKE_BIND", dir.join("bind.sock"))
        .env("YOKE_TOKEN", "t")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let (conn, _) = tokio::time::timeout(Duration::from_secs(5), control.accept())
        .await
        .expect("the harness never connected")
        .unwrap();
    let (read, write) = conn.into_split();
    let mut suite = Suite {
        lines: BufReader::new(read).lines(),
        write,
        child,
        channel,
        next: 0,
        dir,
    };
    let hello = suite.read().await;
    (suite, hello)
}

impl Suite {
    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .expect("the harness said nothing")
            .unwrap()
            .expect("the harness hung up");
        serde_json::from_str(&line).unwrap_or_else(|_| panic!("a line that is not JSON: {line}"))
    }

    async fn directive(&mut self, verb: &str, args: Value) -> (String, Value) {
        self.next += 1;
        let id = format!("d-{}", self.next);
        let mut line =
            serde_json::to_vec(&json!({"type": "directive", "id": id, "verb": verb, "args": args}))
                .unwrap();
        line.push(b'\n');
        self.write.write_all(&line).await.unwrap();
        loop {
            let m = self.read().await;
            if m["type"] == "result" {
                return (id, m);
            }
        }
    }

    async fn send(&self, e: pb::Envelope) {
        let tx = loop {
            if let Some(tx) = self.channel.to_unit.lock().unwrap().clone() {
                break tx;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        tx.send(Ok(e)).await.unwrap();
    }

    async fn exits(&mut self, within: Duration) -> i32 {
        tokio::time::timeout(within, self.child.wait())
            .await
            .expect("the harness did not leave")
            .unwrap()
            .code()
            .unwrap_or(-1)
    }
}

fn revoked(cause: pb::session_message::revoked::Cause) -> pb::Envelope {
    pb::Envelope {
        message_id: "c-9".into(),
        session_id: "sid-1".into(),
        payload: Some(pb::envelope::Payload::Session(pb::SessionMessage {
            kind: Some(pb::session_message::Kind::Revoked(
                pb::session_message::Revoked {
                    cause: cause as i32,
                    line: String::new(),
                },
            )),
        })),
        ..Default::default()
    }
}

// std: yoke-sdk-rust:the-harness.01
#[tokio::test]
async fn hello_first() {
    let (_suite, hello) = started(restricted).await;
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["contract"], "plugin");
    assert_eq!(hello["language"], "rust");
    assert_eq!(hello["sdk"], yoke_sdk::plugin::SDK_LINE);
    assert_eq!(hello["version"], 1);
    assert_eq!(hello["unit"], "harness");
}

// std: yoke-sdk-rust:the-harness.02
#[tokio::test]
async fn describe_answers_with_the_generated_manifest() {
    let (mut suite, _) = started(restricted).await;
    let (id, result) = suite.directive("describe", json!({})).await;
    assert_eq!(result["id"], id.as_str());
    assert_eq!(
        result["value"]["manifest"],
        yoke_plugin_harness::declaration().manifest().as_str()
    );
}

// std: yoke-sdk-rust:the-harness.03
#[tokio::test]
async fn start_reports_what_admission_answered() {
    let (mut suite, _) = started(restricted).await;
    let (_, result) = suite.directive("start", json!({})).await;
    assert_eq!(result["value"]["outcome"], "accepted with restrictions");
    assert!(
        !result["value"]["withheld"]["streams"]
            .as_array()
            .unwrap()
            .is_empty(),
        "start gave {result}"
    );

    let (mut suite, _) = started(|_: &pb::RegisterRequest| pb::RegisterResponse {
        outcome: pb::register_response::Outcome::Refused as i32,
        stage: pb::Stage::Authentication as i32,
        code: "admission.auth.consumed".into(),
        message: "the library's own words".into(),
        ..Default::default()
    })
    .await;
    let (_, result) = suite.directive("start", json!({})).await;
    assert_eq!(result["refusal"], "admission.auth.consumed");
    assert_eq!(result["value"]["stage"], "authentication");
    assert!(
        !result.to_string().contains("own words"),
        "the harness passed the library's words on: {result}"
    );
}

// std: yoke-sdk-rust:the-harness.04
#[tokio::test]
async fn a_verb_it_does_not_know_is_unrecognised() {
    let (mut suite, _) = started(restricted).await;
    let (id, result) = suite.directive("subscribe", json!({})).await;
    assert_eq!(result["id"], id.as_str());
    assert_eq!(result["unrecognised"], true);
}

// std: yoke-sdk-rust:the-harness.05
#[tokio::test]
async fn what_the_library_surfaces_is_an_observation() {
    let (mut suite, _) = started(restricted).await;
    suite.directive("start", json!({})).await;
    suite
        .send(pb::Envelope {
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
    suite
        .send(revoked(pb::session_message::revoked::Cause::PluginDisabled))
        .await;
    let (first, second) = (suite.read().await, suite.read().await);
    assert_eq!(
        (first["type"].as_str(), first["kind"].as_str()),
        (Some("observation"), Some("command")),
        "observed {first}"
    );
    assert_eq!(second["kind"], "session-ended", "then {second}");
    assert_eq!(second["fields"]["cause"], "plugin disabled");
    assert_eq!(second["fields"]["closed"], false);
    assert_eq!(suite.exits(Duration::from_secs(3)).await, 0);
}

// std: yoke-sdk-rust:the-harness.06
#[tokio::test]
async fn finish_ends_the_harness_which_holds_no_wire() {
    let (mut suite, _) = started(restricted).await;
    suite
        .write
        .write_all(b"{\"type\":\"finish\"}\n")
        .await
        .unwrap();
    assert_eq!(suite.exits(Duration::from_secs(3)).await, 0);
    let manifest =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
    let dependencies = manifest
        .split("[dependencies]")
        .nth(1)
        .unwrap()
        .split("\n[")
        .next()
        .unwrap();
    for wire in ["yoke-proto", "tonic", "prost"] {
        assert!(
            !dependencies.contains(wire),
            "the harness depends on {wire}"
        );
    }
}

// std: yoke-sdk-rust:the-harness.07
#[tokio::test]
async fn asked_to_stop_the_harness_reports_the_end_first() {
    let (mut suite, _) = started(restricted).await;
    suite.directive("start", json!({})).await;
    let pid = suite.child.id().unwrap();
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    suite
        .send(revoked(pb::session_message::revoked::Cause::LivenessLost))
        .await;
    let o = suite.read().await;
    assert_eq!(
        o["kind"], "session-ended",
        "after the signal the harness reported {o}"
    );
    assert_eq!(suite.exits(Duration::from_secs(5)).await, 0);
}

// std: yoke-sdk-rust:the-harness.08
#[test]
fn the_suite_is_the_published_pair_authenticated_and_never_built() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let script =
        std::fs::read_to_string(root.join("ci/conformance.sh")).expect("no ci/conformance.sh");
    assert!(
        script.contains("yoke-conformance-") && script.contains("releases/download"),
        "the script does not download the published archive"
    );
    assert!(
        script.contains("manifest.jsonl") && script.contains("sha256sum"),
        "the script does not authenticate it by the manifest"
    );
    // Commands are read as words, so that `cargo build` of the harness is not taken for `go build`.
    let words: Vec<&str> = script.split_whitespace().collect();
    for pair in words.windows(2) {
        assert!(
            !matches!(
                (pair[0], pair[1]),
                ("go", "install" | "build" | "run") | ("cargo", "install")
            ),
            "the script builds with {} {}",
            pair[0],
            pair[1]
        );
    }
    let workflow = std::fs::read_to_string(root.join(".github/workflows/verify.yml")).unwrap();
    let (test, conformance) = (
        workflow.find("run: just test"),
        workflow.find("run: ci/conformance.sh"),
    );
    assert!(
        test.is_some() && conformance.is_some() && conformance > test,
        "the workflow does not run the suite after just test"
    );
}

// std: yoke-sdk-rust:the-harness.09
#[tokio::test]
async fn a_question_is_observed_with_its_bytes() {
    let (mut suite, _) = started(restricted).await;
    suite.directive("start", json!({})).await;
    suite
        .send(pb::Envelope {
            message_id: "q-1".into(),
            session_id: "sid-1".into(),
            payload: Some(pb::envelope::Payload::Query(pb::Query {
                kind: Some(pb::query::Kind::Question(pb::query::Question {
                    r#type: "status".into(),
                    payload: b"how are you".to_vec(),
                })),
            })),
            ..Default::default()
        })
        .await;
    let observed = suite.read().await;
    assert_eq!(observed["kind"], "question", "observed {observed}");
    assert_eq!(observed["fields"]["id"], "q-1");
    assert_eq!(observed["fields"]["type"], "status");
    assert_eq!(observed["fields"]["payload"], "how are you");
}
