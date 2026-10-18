//! What the interface library's cases share: a Core that records what arrives and says what its script tells
//! it to, and the frames it says it with.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

use pb::interface_server::{Interface, InterfaceServer};
use yoke_proto::interface::v1 as pb;
use yoke_sdk::base::Error;

/// What a Core sees of one attachment's requests.
pub enum Seen {
    Frame(pb::ClientFrame),
    /// The client ended its requests: it closed.
    Closed,
}

/// One attachment as the script receives it: the client's frames, and the way to answer.
pub struct Attached {
    pub requests: mpsc::UnboundedReceiver<Seen>,
    pub out: mpsc::Sender<Result<pb::CoreFrame, Status>>,
}

pub type Script = Arc<dyn Fn(Attached) + Send + Sync>;

/// The Core: a script per attachment, and a record of what each attachment's requests came to.
#[derive(Clone)]
pub struct Core {
    pub script: Script,
    pub attachments: Arc<AtomicUsize>,
    pub frames: Arc<Mutex<Vec<(usize, pb::ClientFrame)>>>,
    pub ends: Arc<Mutex<Vec<&'static str>>>,
}

#[tonic::async_trait]
impl Interface for Core {
    type AttachStream = ReceiverStream<Result<pb::CoreFrame, Status>>;

    async fn attach(
        &self,
        request: Request<Streaming<pb::ClientFrame>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let n = self.attachments.fetch_add(1, Ordering::SeqCst) + 1;
        let (out, rx) = mpsc::channel(64);
        let (seen, requests) = mpsc::unbounded_channel();
        let mut incoming = request.into_inner();
        let (frames, ends) = (self.frames.clone(), self.ends.clone());
        tokio::spawn(async move {
            loop {
                match incoming.next().await {
                    Some(Ok(f)) => {
                        frames.lock().unwrap().push((n, f.clone()));
                        let _ = seen.send(Seen::Frame(f));
                    }
                    Some(Err(_)) => {
                        ends.lock().unwrap().push("went away");
                        return;
                    }
                    None => {
                        ends.lock().unwrap().push("closed");
                        let _ = seen.send(Seen::Closed);
                        return;
                    }
                }
            }
        });
        (self.script)(Attached { requests, out });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub fn core(script: impl Fn(Attached) + Send + Sync + 'static) -> Core {
    Core {
        script: Arc::new(script),
        attachments: Arc::default(),
        frames: Arc::default(),
        ends: Arc::default(),
    }
}

pub fn fresh_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "yki-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Serves a Core on a socket of its own, and returns the socket's address.
pub fn serve(core: &Core, dir: &Path) -> String {
    let socket = dir.join("panel.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let served = core.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InterfaceServer::new(served))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .ok();
    });
    socket.to_string_lossy().into_owned()
}

pub fn opening(subscription: &str, version: u32, records: Vec<pb::Record>) -> pb::CoreFrame {
    pb::CoreFrame {
        call: String::new(),
        carries: Some(pb::core_frame::Carries::Opening(pb::Opening {
            picture: Some(pb::Snapshot { at: 4, records }),
            subscription: subscription.into(),
            version,
        })),
    }
}

pub fn answer(call: &str, a: pb::response::Answer) -> pb::CoreFrame {
    pb::CoreFrame {
        call: call.into(),
        carries: Some(pb::core_frame::Carries::Answer(pb::Response {
            answer: Some(a),
        })),
    }
}

pub fn refusal(call: &str, r: pb::Refusal) -> pb::CoreFrame {
    pb::CoreFrame {
        call: call.into(),
        carries: Some(pb::core_frame::Carries::Refusal(r)),
    }
}

pub fn event(call: &str, seq: u64, kind: &str) -> pb::CoreFrame {
    pb::CoreFrame {
        call: call.into(),
        carries: Some(pb::core_frame::Carries::Event(pb::Event {
            seq,
            r#type: kind.into(),
            ..Default::default()
        })),
    }
}

pub fn completion(call: &str) -> pb::CoreFrame {
    pb::CoreFrame {
        call: call.into(),
        carries: Some(pb::core_frame::Carries::Completion(pb::Completion {
            by: pb::completion::By::Core as i32,
        })),
    }
}

pub fn unit_record(identity: &str) -> pb::Record {
    pb::Record {
        subject: Some(pb::record::Subject::Unit(pb::UnitRecord {
            declared: Some(pb::unit_record::Declared {
                identity: identity.into(),
                kind: "plugin".into(),
            }),
            ..Default::default()
        })),
    }
}

pub async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

/// The refusal an error is, and what it fails the case otherwise.
pub fn refusal_in(e: Error) -> yoke_sdk::base::Refusal {
    match e {
        Error::Refusal(r) => r,
        other => panic!("not a refusal: {other}"),
    }
}

/// The status a Core refuses an attachment with, carrying the refusal as the contract states.
pub fn refused_with(code: &str, message: &str) -> Status {
    #[derive(Clone, PartialEq, prost::Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<prost_types::Any>,
    }
    let refusal = pb::Refusal {
        code: code.into(),
        message: message.into(),
        detail: None,
    };
    let carried = RpcStatus {
        code: 9,
        message: message.into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/yoke.interface.v1.Refusal".into(),
            value: refusal.encode_to_vec(),
        }],
    };
    Status::with_details(
        tonic::Code::FailedPrecondition,
        message,
        carried.encode_to_vec().into(),
    )
}

/// A Core that refuses every attachment with one status.
pub struct RefusingCore {
    pub status: Status,
}

#[tonic::async_trait]
impl Interface for RefusingCore {
    type AttachStream = ReceiverStream<Result<pb::CoreFrame, Status>>;

    async fn attach(
        &self,
        _: Request<Streaming<pb::ClientFrame>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        Err(self.status.clone())
    }
}

pub fn request_of(f: &pb::ClientFrame) -> Option<&pb::Request> {
    match &f.carries {
        Some(pb::client_frame::Carries::Request(r)) => Some(r),
        _ => None,
    }
}

pub fn acknowledged(outcome: pb::acknowledged::Outcome) -> pb::response::Answer {
    pb::response::Answer::Command(pb::Acknowledged {
        outcome: outcome as i32,
        line: String::new(),
    })
}
