//! The library an interface client is written with: the interface contract, on its typed projection,
//! over the base.
//!
//! A client attaches to a channel and is given its opening: what the channel may address, the standing
//! subscription, and the contract's version. Every operation of the contract's union is one method, all
//! on the one connection, each answered on its own; a refusal is the base's error, carrying its code and
//! its detail. Nothing is retried and nothing reconnects, and nothing is sent the author did not ask for:
//! a confirmation is the author's statement of what it has processed, so the library never makes one.
//!
//! A channel at a routable address is reached over TLS 1.3 against the roots the caller supplies, and
//! establishes its caller before anything else: the credential is the attachment's first frame, and the
//! session token its answer carries is the caller's to present to a later attachment.
//!
//! This module names nothing of the administrative contract, and uses none of its definitions: a program
//! written with it cannot reach the operations an interface must never issue.

use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use tokio::io::unix::AsyncFd;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use yoke_proto::interface::v1 as pb;

use crate::base::{self, Error, Refusal, Subject, Suspension};

pub use pb::{Acknowledged, Authenticated, Event, Filter, Reclaimed, Record, Snapshot, Subscribed};

/// The version of the interface contract this library covers, stated on every request.
pub const CONTRACT: u32 = pb::Contract::Version as u32;

/// What a call answers once the attachment has ended.
pub const ENDED: &str = "the attachment ended";

/// Says whether an error is the end of the attachment.
pub fn is_ended(error: &Error) -> bool {
    matches!(error, Error::Transport(said) if said == ENDED)
}

fn ended() -> Error {
    Error::Transport(ENDED.into())
}

/// A channel's local socket under an instance's root; a name that could choose where the socket is is
/// refused.
fn channel_socket(root: &Path, channel: &str) -> Result<PathBuf, Error> {
    if channel.is_empty() || channel == "." || channel == ".." || channel.contains(['/', '\0']) {
        return Err(Error::Misuse(format!(
            "the channel name {channel:?} cannot name a channel"
        )));
    }
    Ok(root.join("interfaces").join(format!("{channel}.sock")))
}

/// A channel's address in the service form, under its runtime directory.
pub fn service(runtime_dir: &Path, channel: &str) -> Result<PathBuf, Error> {
    channel_socket(runtime_dir, channel)
}

/// A channel's address in an application instance, derived from the instance's name under the account's
/// runtime directory.
pub fn application(
    name: &str,
    channel: &str,
    getenv: impl Fn(&str) -> Option<String>,
) -> Result<PathBuf, Error> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
        return Err(Error::Misuse(format!(
            "the instance name {name:?} cannot name an instance"
        )));
    }
    let runtime = getenv("XDG_RUNTIME_DIR").unwrap_or_default();
    if runtime.is_empty() {
        return Err(Error::Environment(
            "XDG_RUNTIME_DIR is not set, and an application instance's addresses derive from it"
                .into(),
        ));
    }
    channel_socket(&Path::new(&runtime).join("yoke").join(name), channel)
}

/// The address of the channel a managed interface was launched for, which the Core hands it.
pub fn managed(getenv: impl Fn(&str) -> Option<String>) -> Result<PathBuf, Error> {
    match getenv("YOKE_SOCKET") {
        Some(socket) if !socket.is_empty() => Ok(PathBuf::from(socket)),
        _ => Err(Error::Environment(
            "YOKE_SOCKET is not set: a managed interface is handed its channel's address, and this process was not"
                .into(),
        )),
    }
}

/// The operation a request carries, as the contract names it.
pub fn name(request: &pb::Request) -> &'static str {
    use pb::request::Operation as Op;
    match request.operation {
        Some(Op::Authenticate(_)) => "authenticate",
        Some(Op::Read(_)) => "read",
        Some(Op::Subscribe(_)) => "subscribe",
        Some(Op::Confirm(_)) => "confirm",
        Some(Op::Command(_)) => "command",
        Some(Op::Query(_)) => "query",
        Some(Op::StreamStart(_)) => "stream.start",
        Some(Op::StreamStop(_)) => "stream.stop",
        Some(Op::StreamSubscribe(_)) => "stream.subscribe",
        Some(Op::StreamUnsubscribe(_)) => "stream.unsubscribe",
        Some(Op::Reclaim(_)) => "reclaim",
        None => "",
    }
}

/// A refusal as the base's error, with what it names.
fn refusal_of(r: &pb::Refusal) -> Error {
    let mut out = Refusal {
        code: r.code.clone(),
        message: r.message.clone(),
        ..Default::default()
    };
    match &r.detail {
        Some(pb::refusal::Detail::Subject(s)) => {
            out.subject = Some(Subject {
                kind: s.kind.clone(),
                identity: s.identity.clone(),
                incarnation: s.incarnation,
            })
        }
        Some(pb::refusal::Detail::Item(item)) => out.item = Some(item.clone()),
        Some(pb::refusal::Detail::Suspension(s)) => {
            out.suspension = Some(Suspension {
                grade: s.grade.clone(),
                by: s.by.clone(),
            })
        }
        None => {}
    }
    Error::Refusal(out)
}

/// The shape of what a gRPC status carries beside its code: `google.rpc.Status`, whose details are the
/// refusals the Core attaches.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// The refusal a transport's status carries, or the transport's own error where it carries none.
fn from_status(status: tonic::Status) -> Error {
    if let Ok(carried) = RpcStatus::decode(status.details()) {
        for any in carried.details {
            if any.type_url.ends_with("yoke.interface.v1.Refusal")
                && let Ok(r) = pb::Refusal::decode(any.value.as_slice())
            {
                return refusal_of(&r);
            }
        }
    }
    Error::from(status)
}

type Frames = mpsc::UnboundedReceiver<pb::CoreFrame>;

/// The queue of one call: the frames in the order they arrived, however many, so a call nobody is reading
/// never holds up the others.
struct Queue {
    tx: mpsc::UnboundedSender<pb::CoreFrame>,
    rx: Option<Frames>,
}

#[derive(Default)]
struct Calls {
    ended: bool,
    queues: HashMap<String, Queue>,
}

struct Inner {
    out: Mutex<Option<mpsc::Sender<pb::ClientFrame>>>,
    count: Mutex<u64>,
    calls: Mutex<Calls>,
    /// True once nothing more arrives from the Core.
    received: watch::Receiver<bool>,
}

impl Inner {
    /// The queue of a call, which the caller takes the reading side of unless it was taken already.
    fn register(&self, call: &str) -> Option<Frames> {
        let mut calls = self.calls.lock().unwrap();
        if calls.ended {
            return None;
        }
        let queue = calls.queues.entry(call.to_string()).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            Queue { tx, rx: Some(rx) }
        });
        queue.rx.take()
    }

    fn forget(&self, call: &str) {
        self.calls.lock().unwrap().queues.remove(call);
    }

    async fn send(&self, frame: pb::ClientFrame) -> Result<(), Error> {
        let tx = self.out.lock().unwrap().clone();
        match tx {
            Some(tx) => tx.send(frame).await.map_err(|_| ended()),
            None => Err(ended()),
        }
    }

    fn cancel(&self, call: &str) {
        let tx = self.out.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.try_send(pb::ClientFrame {
                call: call.to_string(),
                carries: Some(pb::client_frame::Carries::Cancel(pb::Cancel {})),
            });
        }
    }

    /// Sends a request under a new call identity, stating the contract's version, and returns the frames
    /// that answer it.
    async fn issue(&self, op: pb::request::Operation) -> Result<(String, Frames), Error> {
        let call = {
            let mut count = self.count.lock().unwrap();
            *count += 1;
            format!("c-{}", *count)
        };
        let Some(frames) = self.register(&call) else {
            return Err(ended());
        };
        let request = pb::Request {
            version: CONTRACT,
            operation: Some(op),
        };
        let frame = pb::ClientFrame {
            call: call.clone(),
            carries: Some(pb::client_frame::Carries::Request(request)),
        };
        if let Err(e) = self.send(frame).await {
            self.forget(&call);
            return Err(e);
        }
        Ok((call, frames))
    }
}

/// A call in flight: dropped before it was answered, it is cancelled on the wire and forgotten.
struct InFlight {
    inner: Arc<Inner>,
    call: String,
    answered: bool,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.inner.forget(&self.call);
        if !self.answered {
            self.inner.cancel(&self.call);
        }
    }
}

/// A call's first frame as its answer: a refusal is the base's error, and an attachment that ended
/// answers that it ended.
async fn first(frames: &mut Frames) -> Result<pb::Response, Error> {
    match frames.recv().await {
        None => Err(ended()),
        Some(f) => match f.carries {
            Some(pb::core_frame::Carries::Refusal(r)) => Err(refusal_of(&r)),
            Some(pb::core_frame::Carries::Answer(a)) => Ok(a),
            _ => Err(Error::Transport(
                "the Core answered a call with something that is not an answer".into(),
            )),
        },
    }
}

fn unexpected(operation: &str) -> Error {
    Error::Transport(format!(
        "the Core answered {operation} with the answer of another operation"
    ))
}

/// How an attachment reaches its channel, and what it presents there.
#[derive(Clone, Default)]
pub struct Options {
    tls: Option<(Vec<u8>, String)>,
    credential: Option<pb::Authenticate>,
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reaches the channel over TLS 1.3 and nothing below it, verifying the Core's chain against `roots`,
    /// in PEM, and no others; `name` is the name the certificate must carry, the address's host where it
    /// is empty. No client certificate is offered: the caller is established by a credential, above the
    /// transport.
    pub fn tls(mut self, roots: &[u8], name: &str) -> Self {
        self.tls = Some((roots.to_vec(), name.to_string()));
        self
    }

    /// Presents an account and its secret, for the host to judge, as the attachment's first frame.
    pub fn password(mut self, account: &str, secret: &str) -> Self {
        self.credential = Some(pb::Authenticate {
            credential: Some(pb::authenticate::Credential::Password(
                pb::authenticate::Password {
                    account: account.into(),
                    secret: secret.into(),
                },
            )),
        });
        self
    }

    /// Presents a session token an earlier authentication on the same channel issued, as the
    /// attachment's first frame.
    pub fn token(mut self, token: &str) -> Self {
        self.credential = Some(pb::Authenticate {
            credential: Some(pb::authenticate::Credential::Token(token.into())),
        });
        self
    }
}

/// Reaches a channel: a path is a local socket, anything else an address, over TLS where asked.
async fn dial(address: &str, options: &Options) -> Result<Channel, Error> {
    if address.starts_with('/') {
        if options.tls.is_some() {
            return Err(Error::Misuse(format!(
                "{address} is a local socket, and a local socket is not reached over TLS"
            )));
        }
        return base::dial(Path::new(address)).await;
    }
    match &options.tls {
        None => Endpoint::from_shared(format!("http://{address}"))
            .map_err(|e| Error::Misuse(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Error::Transport(e.to_string())),
        Some((roots, name)) => crate::tls::dial(address, roots, name).await,
    }
}

/// The call identity of the credential an attachment presents first.
const AUTHENTICATING: &str = "c-1";

/// How long a close waits for the Core to end an attachment told it is closing.
const CLOSING: Duration = Duration::from_secs(1);

/// A client attached to a channel. `picture` is what the channel was given when it attached, `standing`
/// the subscription it holds from then on, and `version` the contract's version the Core speaks.
/// `established` is the answer to the credential an attachment presented — the account, and the session
/// token with its expiry — and `None` where it presented none.
pub struct Attachment {
    pub picture: pb::Snapshot,
    pub standing: Option<Subscription>,
    pub version: u32,
    pub established: Option<pb::Authenticated>,
    inner: Arc<Inner>,
    task: tokio::task::JoinHandle<()>,
}

impl Attachment {
    /// Attaches to the channel at `address` and waits for its opening. A Core that speaks another version
    /// of the contract is refused before any operation, and the connection let go. A credential among
    /// `options` is sent before anything else, and attaching waits for its answer too. Nothing times it
    /// out: a caller that wants a deadline puts one around it.
    pub async fn attach(address: &str, options: Options) -> Result<Attachment, Error> {
        let channel = dial(address, &options).await?;
        let (tx, rx) = mpsc::channel::<pb::ClientFrame>(64);
        if let Some(credential) = options.credential.clone() {
            let request = pb::Request {
                version: CONTRACT,
                operation: Some(pb::request::Operation::Authenticate(credential)),
            };
            let _ = tx
                .send(pb::ClientFrame {
                    call: AUTHENTICATING.into(),
                    carries: Some(pb::client_frame::Carries::Request(request)),
                })
                .await;
        }
        let mut client = pb::interface_client::InterfaceClient::new(channel);
        let mut incoming = client
            .attach(ReceiverStream::new(rx))
            .await
            .map_err(from_status)?
            .into_inner();
        let opening = match incoming.message().await.map_err(from_status)? {
            Some(f) => match f.carries {
                Some(pb::core_frame::Carries::Opening(o)) => o,
                other => {
                    return Err(Error::Transport(format!(
                        "the attachment opened with {other:?}, and not an opening"
                    )));
                }
            },
            None => return Err(ended()),
        };
        if opening.version != CONTRACT {
            return Err(Error::refusal(
                "compat.unsupported",
                format!(
                    "this library speaks the interface contract's version {CONTRACT}, and the Core {}",
                    opening.version
                ),
            ));
        }
        let (done, received) = watch::channel(false);
        let inner = Arc::new(Inner {
            out: Mutex::new(Some(tx)),
            count: Mutex::new(u64::from(options.credential.is_some())),
            calls: Mutex::new(Calls::default()),
            received,
        });
        let standing = if opening.subscription.is_empty() {
            None
        } else {
            inner
                .register(&opening.subscription)
                .map(|frames| Subscription {
                    id: opening.subscription.clone(),
                    inner: inner.clone(),
                    frames,
                    pending: None,
                    standing: true,
                })
        };
        let mut credential = if options.credential.is_some() {
            inner.register(AUTHENTICATING)
        } else {
            None
        };
        let task = {
            let inner = inner.clone();
            tokio::spawn(async move {
                receive(inner, incoming).await;
                let _ = done.send(true);
            })
        };
        let mut attachment = Attachment {
            picture: opening.picture.unwrap_or_default(),
            standing,
            version: opening.version,
            established: None,
            inner,
            task,
        };
        if let Some(frames) = credential.as_mut() {
            let answered = first(frames).await;
            attachment.inner.forget(AUTHENTICATING);
            match answered {
                Ok(resp) => {
                    if let Some(pb::response::Answer::Authenticate(a)) = resp.answer {
                        attachment.established = Some(a);
                    }
                }
                Err(e) => {
                    attachment.close().await;
                    return Err(e);
                }
            }
        }
        Ok(attachment)
    }

    /// Ends the attachment in order: it tells the Core the client is done, so the Core reads a client that
    /// closed rather than one that went away, and lets the connection go once the Core has ended it.
    pub async fn close(&mut self) {
        self.inner.out.lock().unwrap().take();
        let mut received = self.inner.received.clone();
        let _ = tokio::time::timeout(CLOSING, received.wait_for(|ended| *ended)).await;
        self.task.abort();
    }

    async fn call(&self, op: pb::request::Operation) -> Result<pb::Response, Error> {
        let (call, mut frames) = self.inner.issue(op).await?;
        let mut flight = InFlight {
            inner: self.inner.clone(),
            call,
            answered: false,
        };
        let answered = first(&mut frames).await;
        flight.answered = true;
        answered
    }

    /// Presents an account and its secret, on a channel whose class takes a credential; on one whose
    /// class establishes the client, it answers who was established.
    pub async fn authenticate(
        &self,
        account: &str,
        secret: &str,
    ) -> Result<pb::Authenticated, Error> {
        let resp = self
            .call(pb::request::Operation::Authenticate(pb::Authenticate {
                credential: Some(pb::authenticate::Credential::Password(
                    pb::authenticate::Password {
                        account: account.into(),
                        secret: secret.into(),
                    },
                )),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Authenticate(a)) => Ok(a),
            _ => Err(unexpected("authenticate")),
        }
    }

    /// Presents a token an earlier authentication issued.
    pub async fn authenticate_token(&self, token: &str) -> Result<pb::Authenticated, Error> {
        let resp = self
            .call(pb::request::Operation::Authenticate(pb::Authenticate {
                credential: Some(pb::authenticate::Credential::Token(token.into())),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Authenticate(a)) => Ok(a),
            _ => Err(unexpected("authenticate")),
        }
    }

    /// The records of a kind the channel sees, or of the one subject an identity names.
    pub async fn read(&self, kind: &str, identity: &str) -> Result<Vec<pb::Record>, Error> {
        let resp = self
            .call(pb::request::Operation::Read(pb::Read {
                kind: kind.into(),
                identity: identity.into(),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Read(r)) => Ok(r.records),
            _ => Err(unexpected("read")),
        }
    }

    /// States that the author has processed a subscription up to a sequence. Only the author knows that,
    /// so the library never confirms on its own.
    pub async fn confirm(&self, subscription: &str, sequence: u64) -> Result<(), Error> {
        let resp = self
            .call(pb::request::Operation::Confirm(pb::Confirm {
                subscription: subscription.into(),
                sequence,
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Confirm(_)) => Ok(()),
            _ => Err(unexpected("confirm")),
        }
    }

    /// Sends a command to a unit, its payload opaque, and returns how the unit acknowledged it.
    pub async fn command(
        &self,
        unit: &str,
        r#type: &str,
        payload: &[u8],
    ) -> Result<pb::Acknowledged, Error> {
        let resp = self
            .call(pb::request::Operation::Command(pb::Command {
                unit: unit.into(),
                r#type: r#type.into(),
                payload: payload.to_vec(),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Command(a)) => Ok(a),
            _ => Err(unexpected("command")),
        }
    }

    /// Asks a unit a question, its payload opaque, and returns the answer's.
    pub async fn ask(&self, unit: &str, r#type: &str, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let resp = self
            .call(pb::request::Operation::Query(pb::Question {
                unit: unit.into(),
                r#type: r#type.into(),
                payload: payload.to_vec(),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Query(a)) => Ok(a.payload),
            _ => Err(unexpected("query")),
        }
    }

    /// Asks for a unit's stream to flow, for everyone.
    pub async fn start_stream(&self, unit: &str, stream: &str) -> Result<pb::Acknowledged, Error> {
        let resp = self
            .call(pb::request::Operation::StreamStart(pb::UnitStream {
                unit: unit.into(),
                stream: stream.into(),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::StreamStart(a)) => Ok(a),
            _ => Err(unexpected("stream.start")),
        }
    }

    /// Asks for a unit's stream to stop flowing.
    pub async fn stop_stream(&self, unit: &str, stream: &str) -> Result<pb::Acknowledged, Error> {
        let resp = self
            .call(pb::request::Operation::StreamStop(pb::UnitStream {
                unit: unit.into(),
                stream: stream.into(),
            }))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::StreamStop(a)) => Ok(a),
            _ => Err(unexpected("stream.stop")),
        }
    }

    /// Takes control back for a channel bound as a local socket.
    pub async fn reclaim(&self) -> Result<pb::Reclaimed, Error> {
        let resp = self
            .call(pb::request::Operation::Reclaim(pb::Reclaim {}))
            .await?;
        match resp.answer {
            Some(pb::response::Answer::Reclaim(r)) => Ok(r),
            _ => Err(unexpected("reclaim")),
        }
    }

    /// Opens a subscription narrowed by a filter, and returns once its snapshot or its refusal arrives.
    pub async fn subscribe(&self, filter: pb::Filter) -> Result<Subscription, Error> {
        let (call, mut frames) = self
            .inner
            .issue(pb::request::Operation::Subscribe(pb::Subscribe {
                filter: Some(filter),
            }))
            .await?;
        match first(&mut frames).await {
            Ok(resp) => match resp.answer {
                Some(pb::response::Answer::Subscribe(s)) => Ok(Subscription {
                    id: call,
                    inner: self.inner.clone(),
                    frames,
                    pending: Some(s),
                    standing: false,
                }),
                _ => {
                    self.inner.forget(&call);
                    Err(unexpected("subscribe"))
                }
            },
            Err(e) => {
                self.inner.forget(&call);
                Err(e)
            }
        }
    }

    /// Asks for a copy of a unit's stream, and opens it where the answer says it arrives: a per-subscriber
    /// socket the Core listens on, or the attachment's own connection.
    pub async fn subscribe_stream(&self, unit: &str, stream: &str) -> Result<Delivery, Error> {
        let resp = self
            .call(pb::request::Operation::StreamSubscribe(pb::UnitStream {
                unit: unit.into(),
                stream: stream.into(),
            }))
            .await?;
        let Some(pb::response::Answer::StreamSubscribe(d)) = resp.answer else {
            return Err(unexpected("stream.subscribe"));
        };
        let source = match &d.arrives {
            Some(pb::delivering::Arrives::Socket(path)) => {
                Source::Socket(SeqSocket::connect(path).map_err(|e| {
                    Error::Transport(format!(
                        "the delivery's socket {path} cannot be reached: {e}"
                    ))
                })?)
            }
            Some(pb::delivering::Arrives::Connection(_)) => {
                match self.inner.register(&d.delivery) {
                    Some(frames) => Source::Connection(frames),
                    None => return Err(ended()),
                }
            }
            other => {
                return Err(Error::Transport(format!(
                    "the delivery {} arrives where this projection cannot read it: {other:?}",
                    d.delivery
                )));
            }
        };
        Ok(Delivery {
            id: d.delivery,
            flowing: d.flowing,
            inner: self.inner.clone(),
            source,
        })
    }
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.inner.out.lock().unwrap().take();
        self.task.abort();
    }
}

/// Hands every frame to the call it belongs to, until the attachment ends. A delivery on the connection is
/// registered as its answer arrives, so none of its frames can arrive before it is.
async fn receive(inner: Arc<Inner>, mut incoming: tonic::Streaming<pb::CoreFrame>) {
    while let Ok(Some(f)) = incoming.message().await {
        if let Some(pb::core_frame::Carries::Answer(pb::Response {
            answer: Some(pb::response::Answer::StreamSubscribe(d)),
        })) = &f.carries
            && matches!(d.arrives, Some(pb::delivering::Arrives::Connection(true)))
        {
            // Registering makes the queue; the delivery takes its reading side when it is opened.
            let mut calls = inner.calls.lock().unwrap();
            calls.queues.entry(d.delivery.clone()).or_insert_with(|| {
                let (tx, rx) = mpsc::unbounded_channel();
                Queue { tx, rx: Some(rx) }
            });
        }
        let calls = inner.calls.lock().unwrap();
        if let Some(q) = calls.queues.get(&f.call) {
            let _ = q.tx.send(f);
        }
    }
    let mut calls = inner.calls.lock().unwrap();
    calls.ended = true;
    calls.queues.clear();
}

/// A subscription, delivered in order: its snapshot, then events and overflows, until its caller ends it,
/// the Core completes it or the attachment ends. Nothing reconnects it.
pub struct Subscription {
    /// What a confirmation names.
    pub id: String,
    inner: Arc<Inner>,
    frames: Frames,
    pending: Option<pb::Subscribed>,
    standing: bool,
}

impl Subscription {
    /// The next delivery; `None` once the subscription completed, an error that it ended once the
    /// attachment ended.
    pub async fn next(&mut self) -> Result<Option<pb::Subscribed>, Error> {
        if let Some(p) = self.pending.take() {
            return Ok(Some(p));
        }
        loop {
            let Some(f) = self.frames.recv().await else {
                return Err(ended());
            };
            match f.carries {
                Some(pb::core_frame::Carries::Refusal(r)) => return Err(refusal_of(&r)),
                Some(pb::core_frame::Carries::Completion(_)) => return Ok(None),
                Some(pb::core_frame::Carries::Event(e)) => {
                    return Ok(Some(pb::Subscribed {
                        carries: Some(pb::subscribed::Carries::Event(e)),
                    }));
                }
                Some(pb::core_frame::Carries::Answer(pb::Response {
                    answer: Some(pb::response::Answer::Subscribe(s)),
                })) => return Ok(Some(s)),
                _ => {}
            }
        }
    }

    /// Ends the subscription on the wire. The standing subscription is the attachment's, and ends with it.
    pub fn close(&mut self) {
        if self.standing {
            return;
        }
        self.inner.cancel(&self.id);
        self.inner.forget(&self.id);
        self.frames.close();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.standing {
            self.inner.cancel(&self.id);
            self.inner.forget(&self.id);
        }
    }
}

/// One message of a stream as a delivery carries it: its sequence, the producer's clock, and the payload,
/// opaque.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub sequence: u64,
    pub sent_at: u64,
    pub payload: Vec<u8>,
}

/// The size of a frame's header on a per-subscriber socket: the sequence and the clock.
const HEADER: usize = 16;

/// The largest frame a per-subscriber socket carries: a payload of 1 MiB and its header.
const PACKET: usize = HEADER + (1 << 20);

/// A packet socket the Core listens on, read without blocking a thread.
struct SeqSocket(AsyncFd<socket2::Socket>);

impl SeqSocket {
    fn connect(path: &str) -> std::io::Result<SeqSocket> {
        use socket2::{Domain, SockAddr, Socket, Type};
        let socket = Socket::new(Domain::UNIX, Type::SEQPACKET, None)?;
        socket.connect(&SockAddr::unix(path)?)?;
        socket.set_nonblocking(true)?;
        Ok(SeqSocket(AsyncFd::new(socket)?))
    }

    /// One packet; zero once the Core closed.
    async fn recv(&self, buf: &mut [MaybeUninit<u8>]) -> std::io::Result<usize> {
        loop {
            let mut ready = self.0.readable().await?;
            match ready.try_io(|fd| fd.get_ref().recv(buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

enum Source {
    Socket(SeqSocket),
    Connection(Frames),
}

/// A copy of a stream delivered to this client, read where the answer said it arrives. `flowing` is
/// whether the stream was flowing when the subscription was answered.
pub struct Delivery {
    pub id: String,
    pub flowing: bool,
    inner: Arc<Inner>,
    source: Source,
}

impl Delivery {
    /// The next frame, in order; `None` once the delivery ended, an error that it ended once the
    /// attachment did.
    pub async fn next(&mut self) -> Result<Option<Frame>, Error> {
        match &mut self.source {
            Source::Socket(socket) => {
                let mut buf = vec![MaybeUninit::<u8>::uninit(); PACKET];
                let n = match socket.recv(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => return Ok(None),
                };
                if n == 0 {
                    return Ok(None);
                }
                if n < HEADER {
                    return Err(Error::Transport(format!(
                        "a frame of {n} bytes is shorter than its header"
                    )));
                }
                let bytes: Vec<u8> = buf[..n]
                    .iter()
                    .map(|b| unsafe { b.assume_init() })
                    .collect();
                Ok(Some(Frame {
                    sequence: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    sent_at: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                    payload: bytes[HEADER..].to_vec(),
                }))
            }
            Source::Connection(frames) => loop {
                let Some(f) = frames.recv().await else {
                    return Err(ended());
                };
                match f.carries {
                    Some(pb::core_frame::Carries::Completion(_)) => return Ok(None),
                    Some(pb::core_frame::Carries::Refusal(r)) => return Err(refusal_of(&r)),
                    Some(pb::core_frame::Carries::Delivery(d)) => {
                        return Ok(Some(Frame {
                            sequence: d.sequence,
                            sent_at: d.sent_at,
                            payload: d.payload,
                        }));
                    }
                    _ => {}
                }
            },
        }
    }

    /// Ends the delivery: the Core is told, and what was opened for it is let go.
    pub async fn release(self) -> Result<(), Error> {
        let (call, mut frames) = self
            .inner
            .issue(pb::request::Operation::StreamUnsubscribe(
                pb::StreamRelease {
                    delivery: self.id.clone(),
                },
            ))
            .await?;
        let mut flight = InFlight {
            inner: self.inner.clone(),
            call,
            answered: false,
        };
        let answered = first(&mut frames).await;
        flight.answered = true;
        drop(flight);
        if matches!(self.source, Source::Connection(_)) {
            self.inner.forget(&self.id);
        }
        match answered?.answer {
            Some(pb::response::Answer::StreamUnsubscribe(_)) => Ok(()),
            _ => Err(unexpected("stream.unsubscribe")),
        }
    }
}
