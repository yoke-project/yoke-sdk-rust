//! The cases of the-interface-harness.std.md, one test each, against a control socket the test listens on
//! and a Core that serves the channel `panel` under an instance's root.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

use pb::interface_server::{Interface, InterfaceServer};
use yoke_proto::interface::v1 as pb;

type Out = mpsc::Sender<Result<pb::CoreFrame, Status>>;
type Script = Arc<dyn Fn(Out, Streaming<pb::ClientFrame>) + Send + Sync>;

struct Core(Script);

#[tonic::async_trait]
impl Interface for Core {
    type AttachStream = ReceiverStream<Result<pb::CoreFrame, Status>>;

    async fn attach(
        &self,
        request: Request<Streaming<pb::ClientFrame>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let (out, rx) = mpsc::channel(32);
        (self.0)(out, request.into_inner());
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn opening(records: Vec<pb::Record>) -> pb::CoreFrame {
    pb::CoreFrame {
        call: String::new(),
        carries: Some(pb::core_frame::Carries::Opening(pb::Opening {
            picture: Some(pb::Snapshot { at: 4, records }),
            subscription: "standing".into(),
            version: 1,
        })),
    }
}

fn channel_record(name: &str) -> pb::Record {
    pb::Record {
        subject: Some(pb::record::Subject::Channel(pb::ChannelRecord {
            declared: Some(pb::channel_record::Declared {
                name: name.into(),
                projection: "local".into(),
                address_class: "local-socket".into(),
                clients: "single".into(),
            }),
            observed: Some(pb::channel_record::Observed {
                attached: true,
                ..Default::default()
            }),
        })),
    }
}

fn event(seq: u64) -> pb::CoreFrame {
    pb::CoreFrame {
        call: "standing".into(),
        carries: Some(pb::core_frame::Carries::Event(pb::Event {
            seq,
            r#type: "unit.state.changed".into(),
            subject: Some(pb::Subject {
                kind: "unit".into(),
                identity: "acquire".into(),
                incarnation: 0,
            }),
            severity: 30,
            ..Default::default()
        })),
    }
}

/// An instance's root with the channel `panel` served under it by the script, and a control socket.
struct Bench {
    instance: PathBuf,
    control: PathBuf,
    listener: UnixListener,
}

fn bench(script: Option<Script>) -> Bench {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let root = std::env::temp_dir().join(format!(
        "yih-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    std::fs::create_dir_all(root.join("interfaces")).unwrap();
    let control = root.join("control.sock");
    let listener = UnixListener::bind(&control).unwrap();
    if let Some(script) = script {
        let socket = UnixListener::bind(root.join("interfaces").join("panel.sock")).unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(InterfaceServer::new(Core(script)))
                .serve_with_incoming(UnixListenerStream::new(socket))
                .await
                .ok();
        });
    }
    Bench {
        instance: root,
        control,
        listener,
    }
}

/// The harness, launched with the control socket and the instance in its environment, and the lines it
/// sends.
struct Harness {
    lines: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    exit: tokio::task::JoinHandle<i32>,
}

async fn launch(b: &Bench) -> Harness {
    let (control, instance) = (b.control.clone(), b.instance.clone());
    let exit = tokio::spawn(async move {
        yoke_interface_harness::serve(move |k| match k {
            "CONFORMANCE_SOCKET" => Some(control.to_string_lossy().into_owned()),
            "CONFORMANCE_INSTANCE" => Some(instance.to_string_lossy().into_owned()),
            _ => None,
        })
        .await
    });
    let (conn, _) = b.listener.accept().await.unwrap();
    let (read, write) = conn.into_split();
    Harness {
        lines: BufReader::new(read).lines(),
        write,
        exit,
    }
}

impl Harness {
    async fn next(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .expect("the harness said nothing")
            .unwrap()
            .expect("the harness ended");
        serde_json::from_str(&line).unwrap()
    }

    async fn send(&mut self, v: Value) {
        let mut bytes = serde_json::to_vec(&v).unwrap();
        bytes.push(b'\n');
        self.write.write_all(&bytes).await.unwrap();
    }

    async fn direct(&mut self, id: u32, verb: &str, args: Value) -> Value {
        self.send(json!({"type": "directive", "id": id, "verb": verb, "args": args}))
            .await;
        loop {
            let line = self.next().await;
            if line["type"] == "result" {
                assert_eq!(line["id"], json!(id));
                return line;
            }
        }
    }
}

// std: yoke-sdk-rust:the-interface-harness.01
#[tokio::test]
async fn hello_first_for_the_interface_contract() {
    let b = bench(None);
    let mut h = launch(&b).await;
    let hello = h.next().await;
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["contract"], "interface");
    assert_eq!(hello["language"], "rust");
    assert_eq!(hello["sdk"], yoke_sdk::plugin::SDK_LINE);
    assert_eq!(hello["version"], 1);
    assert!(hello.get("unit").is_none(), "{hello}");
    h.send(json!({"type": "finish"})).await;
    assert_eq!(h.exit.await.unwrap(), 0);
}

// std: yoke-sdk-rust:the-interface-harness.02
#[tokio::test]
async fn attaching_reports_the_opening_and_the_standing_subscription_in_order() {
    let script: Script = Arc::new(|out, mut incoming| {
        tokio::spawn(async move {
            let _ = out.send(Ok(opening(vec![channel_record("panel")]))).await;
            let _ = out.send(Ok(event(5))).await;
            let _ = out.send(Ok(event(6))).await;
            while incoming.next().await.is_some() {}
        });
    });
    let b = bench(Some(script));
    let mut h = launch(&b).await;
    h.next().await;
    let result = h.direct(1, "attach", json!({"channel": "panel"})).await;
    let v = &result["value"];
    assert_eq!(v["version"], 1);
    assert_eq!(v["subscription"], "standing");
    assert_eq!(v["at"], 4);
    assert_eq!(v["records"][0]["channel"]["declared"]["name"], "panel");
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let line = h.next().await;
        assert_eq!(line["type"], "observation");
        assert_eq!(line["kind"], "event");
        seen.push((
            line["fields"]["seq"].clone(),
            line["fields"]["type"].clone(),
            line["fields"]["subject"].clone(),
        ));
    }
    assert_eq!(
        seen[0],
        (json!(5), json!("unit.state.changed"), json!("acquire"))
    );
    assert_eq!(seen[1].0, json!(6));
    h.send(json!({"type": "finish"})).await;
    h.exit.await.unwrap();
}

// std: yoke-sdk-rust:the-interface-harness.03
#[tokio::test]
async fn a_refusal_is_reported_as_its_code_and_what_it_names() {
    let script: Script = Arc::new(|out, mut incoming| {
        tokio::spawn(async move {
            let _ = out.send(Ok(opening(vec![]))).await;
            while let Some(Ok(f)) = incoming.next().await {
                let refusal = match &f.carries {
                    Some(pb::client_frame::Carries::Request(r))
                        if matches!(r.operation, Some(pb::request::Operation::Read(_))) =>
                    {
                        pb::Refusal {
                            code: "subject.unknown".into(),
                            message: "no such unit".into(),
                            detail: Some(pb::refusal::Detail::Subject(pb::Subject {
                                kind: "unit".into(),
                                identity: "nobody".into(),
                                incarnation: 0,
                            })),
                        }
                    }
                    _ => pb::Refusal {
                        code: "channel.suspended".into(),
                        message: "suspended".into(),
                        detail: Some(pb::refusal::Detail::Suspension(pb::Suspension {
                            grade: "read-only".into(),
                            by: "bench".into(),
                        })),
                    },
                };
                let _ = out
                    .send(Ok(pb::CoreFrame {
                        call: f.call,
                        carries: Some(pb::core_frame::Carries::Refusal(refusal)),
                    }))
                    .await;
            }
        });
    });
    let b = bench(Some(script));
    let mut h = launch(&b).await;
    h.next().await;
    h.direct(1, "attach", json!({"channel": "panel"})).await;
    let read = h
        .direct(2, "read", json!({"kind": "unit", "identity": "nobody"}))
        .await;
    assert_eq!(read["refusal"], "subject.unknown");
    assert_eq!(read["value"]["subject"]["kind"], "unit");
    assert_eq!(read["value"]["subject"]["identity"], "nobody");
    let command = h
        .direct(
            3,
            "command",
            json!({"unit": "acquire", "type": "calibrate", "payload": ""}),
        )
        .await;
    assert_eq!(command["refusal"], "channel.suspended");
    assert_eq!(
        command["value"]["suspension"],
        json!({"grade": "read-only", "by": "bench"})
    );
    for line in [&read, &command] {
        assert!(!line.to_string().contains("no such unit"), "{line}");
        assert!(line.get("message").is_none() && line["value"].get("message").is_none());
    }
    h.send(json!({"type": "finish"})).await;
    h.exit.await.unwrap();
}

// std: yoke-sdk-rust:the-interface-harness.04
#[tokio::test]
async fn a_verb_it_does_not_know_is_unrecognised_and_it_holds_no_wire() {
    let b = bench(None);
    let mut h = launch(&b).await;
    h.next().await;
    let result = h
        .direct(1, "emit", json!({"stream": "x", "payload": "y"}))
        .await;
    assert_eq!(result["unrecognised"], true);
    h.send(json!({"type": "finish"})).await;
    assert_eq!(h.exit.await.unwrap(), 0);
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for file in ["lib.rs", "plain.rs", "main.rs"] {
        let text = std::fs::read_to_string(src.join(file)).unwrap();
        for word in ["tonic", "hyper", "rustls", "TcpStream", "h2::"] {
            assert!(!text.contains(word), "{file} imports a transport: {word}");
        }
    }
}

// std: yoke-sdk-rust:the-interface-harness.05
#[test]
fn the_suite_is_run_against_it_as_against_the_plugin_harness() {
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("ci")
        .join("conformance.sh");
    let text = std::fs::read_to_string(&script).unwrap();
    assert!(
        text.contains("yoke-interface-harness"),
        "the script does not build the interface harness"
    );
    assert!(
        text.contains("run_suite \"$root/target/debug/yoke-rust-interface-harness\""),
        "the script does not run the suite against the interface harness"
    );
    assert!(
        text.contains("yoke-rust-plugin-harness"),
        "the plugin harness is not run beside it"
    );
}
