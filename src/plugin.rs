//! The library a Plugin unit is written with.
//!
//! One declaration is both the Manifest the library generates and the surface its registration claims,
//! so the two cannot be written apart. Starting a unit performs the first acts in their order: read the
//! environment, bind the unit's own socket, register, open the Session — and then beats on the terms the
//! Core assigned, repeating the author's last health report. Everything the Session brings is surfaced,
//! its end included: a Session that ends ends the incarnation, and the library never reconnects, never
//! retries an admission, never polls, never creates a stream's transport and never chooses a severity or
//! a grade.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::base::{Envelopes, Error, PLUGIN_CONTRACT, Refusal, dial, environment, refusal_of};
use pb::envelope::Payload;
use yoke_proto::plugin::v1 as pb;

/// What this library says it is, at admission.
pub const SDK_LINE: &str = concat!("yoke-sdk-rust ", env!("CARGO_PKG_VERSION"));

/// What a Plugin says about itself: what is true of the binary wherever it runs.
#[derive(Debug, Clone, Default)]
pub struct Declaration {
    pub id: String,
    /// `<class>` or `<class>:<name>`.
    pub needs: Vec<String>,
    pub streams: Vec<Stream>,
    pub commands: Vec<String>,
    pub queries: Vec<String>,
    pub occurrences: Vec<String>,
    pub capabilities: Vec<Capability>,
}

/// A stream the Plugin may publish, and what its data tolerates.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    pub id: String,
    pub tolerates_loss: bool,
    pub tolerates_reorder: bool,
}

/// What an operator may grant, governing exactly one object.
#[derive(Debug, Clone)]
pub struct Capability {
    pub name: String,
    pub governs: Object,
}

/// The one thing a capability governs.
#[derive(Debug, Clone)]
pub enum Object {
    Stream(String),
    Command(String),
    Query(String),
    Occurrence(String),
    Surface(String),
}

impl Declaration {
    /// The document the declaration generates.
    pub fn manifest(&self) -> String {
        let mut m = String::new();
        let _ = writeln!(
            m,
            "manifest: 1\nid: {}\nprotocol: {}",
            scalar(&self.id),
            PLUGIN_CONTRACT
        );
        if !self.needs.is_empty() {
            m.push_str("needs:\n");
            for need in &self.needs {
                let _ = writeln!(m, "  - {}", scalar(need));
            }
        }
        if !self.streams.is_empty() {
            m.push_str("streams:\n");
            for s in &self.streams {
                let _ = writeln!(m, "  - id: {}", scalar(&s.id));
                if s.tolerates_loss {
                    m.push_str("    tolerates_loss: true\n");
                }
                if s.tolerates_reorder {
                    m.push_str("    tolerates_reorder: true\n");
                }
            }
        }
        for (key, list) in [
            ("commands", &self.commands),
            ("queries", &self.queries),
            ("occurrences", &self.occurrences),
        ] {
            if !list.is_empty() {
                let _ = writeln!(m, "{key}:");
                for id in list {
                    let _ = writeln!(m, "  - id: {}", scalar(id));
                }
            }
        }
        if !self.capabilities.is_empty() {
            m.push_str("capabilities:\n");
            for c in &self.capabilities {
                let (kind, id) = match &c.governs {
                    Object::Stream(id) => ("stream", id),
                    Object::Command(id) => ("command", id),
                    Object::Query(id) => ("query", id),
                    Object::Occurrence(id) => ("occurrence", id),
                    Object::Surface(id) => ("surface", id),
                };
                let _ = writeln!(
                    m,
                    "  - name: {}\n    governs:\n      {kind}: {}",
                    scalar(&c.name),
                    scalar(id)
                );
            }
        }
        m
    }

    /// What the registration claims: the declaration again, from the same value.
    fn surface(&self) -> pb::Surface {
        pb::Surface {
            capabilities: self.capabilities.iter().map(|c| c.name.clone()).collect(),
            streams: self.streams.iter().map(|s| s.id.clone()).collect(),
            commands: self.commands.clone(),
            queries: self.queries.clone(),
            occurrences: self.occurrences.clone(),
        }
    }
}

/// A value as YAML writes it: plain where it is an identifier, quoted otherwise.
fn scalar(value: &str) -> String {
    let plain = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._:/-".contains(c))
        && value
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
    if plain {
        value.to_string()
    } else {
        format!("{value:?}")
    }
}

/// Five lists: what was granted, or what was withheld.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scope {
    pub capabilities: Vec<String>,
    pub streams: Vec<String>,
    pub commands: Vec<String>,
    pub queries: Vec<String>,
    pub occurrences: Vec<String>,
}

impl From<Option<pb::Surface>> for Scope {
    fn from(s: Option<pb::Surface>) -> Self {
        let s = s.unwrap_or_default();
        Scope {
            capabilities: s.capabilities,
            streams: s.streams,
            commands: s.commands,
            queries: s.queries,
            occurrences: s.occurrences,
        }
    }
}

/// What the Core answered.
#[derive(Debug, Clone, Default)]
pub struct Admission {
    pub restricted: bool,
    pub granted: Scope,
    /// Item by item.
    pub withheld: Scope,
}

/// An instruction the Core sent, to be acknowledged.
#[derive(Debug, Clone)]
pub struct Command {
    pub id: String,
    pub r#type: String,
    pub payload: Vec<u8>,
}

/// A question the Core asked, to be answered.
#[derive(Debug, Clone)]
pub struct Question {
    pub id: String,
    pub r#type: String,
    pub payload: Vec<u8>,
}

/// A stream the unit may now emit on, and where.
#[derive(Debug, Clone)]
pub struct Activated {
    pub stream: String,
    pub transport: String,
    pub address: String,
}

/// The end of the Session: closed by this unit, or revoked by the Core. Nothing follows it.
#[derive(Debug, Clone)]
pub struct Ended {
    pub closed: bool,
    /// For a revocation: liveness lost, plugin disabled, scope exceeded, protocol failure.
    pub cause: Option<String>,
    pub line: String,
}

/// What the Session brings, in the order it arrived; the last is an `Ended`.
#[derive(Debug)]
pub enum Event {
    Command(Command),
    Question(Question),
    Activated(Activated),
    Stopped {
        stream: String,
    },
    /// An error the Core answered a message with.
    Refused {
        correlation: String,
        error: Error,
    },
    Ended(Ended),
}

/// What became of a command.
#[derive(Debug, Clone, Copy)]
pub enum Outcome {
    Accepted,
    Done,
    Failed,
}

/// How routine or alarming an occurrence is, from 0 to 99: the author's statement and nobody else's.
#[derive(Debug, Clone, Copy)]
pub struct Severity(u8);

impl Severity {
    /// A severity the author states.
    pub fn of(n: u8) -> Self {
        Severity(n)
    }
}

/// What the Session's two tasks and the author's calls share.
struct Shared {
    envelopes: Envelopes,
    state: Mutex<State>,
    // The author's last health report, which every beat repeats; none until one. Held while a report is
    // read and sent, so a beat never sends a report older than one the author has already sent.
    health: Mutex<Option<pb::Health>>,
}

struct State {
    out: Option<mpsc::UnboundedSender<pb::Envelope>>,
    events: Option<mpsc::UnboundedSender<Event>>,
    ended: bool,
    closing: bool,
    active: HashMap<String, Arc<Mutex<Emitting>>>,
}

/// One activated stream: the library's connection to its transport, and the next sequence.
struct Emitting {
    conn: Transport,
    next: u64,
}

enum Transport {
    /// One data envelope per packet.
    Ordered(socket2::Socket),
    /// One frame per datagram: a little-endian header of sequence and clock, then the payload.
    Framed(std::os::unix::net::UnixDatagram),
}

/// Reaches the transport an activation names, as the Core created it.
fn connect(a: &pb::control::Activate) -> Result<Emitting, String> {
    let unreachable = |e: std::io::Error| {
        format!(
            "the transport of {} at {} cannot be reached: {e}",
            a.stream, a.address
        )
    };
    let conn = match pb::control::activate::Transport::try_from(a.transport) {
        Ok(pb::control::activate::Transport::Ordered) => {
            use socket2::{Domain, SockAddr, Socket, Type};
            let socket = Socket::new(Domain::UNIX, Type::SEQPACKET, None).map_err(unreachable)?;
            socket
                .connect(&SockAddr::unix(&a.address).map_err(unreachable)?)
                .map_err(unreachable)?;
            Transport::Ordered(socket)
        }
        Ok(pb::control::activate::Transport::Framed) => {
            let socket = std::os::unix::net::UnixDatagram::unbound().map_err(unreachable)?;
            socket.connect(&a.address).map_err(unreachable)?;
            Transport::Framed(socket)
        }
        _ => {
            return Err(format!(
                "the transport {} of {} is not one this library reaches",
                a.transport, a.stream
            ));
        }
    };
    Ok(Emitting { conn, next: 0 })
}

fn acknowledgement(outcome: pb::ack::Outcome, line: String) -> Payload {
    Payload::Ack(pb::Ack {
        outcome: outcome as i32,
        line,
    })
}

impl Shared {
    fn send(&self, payload: Payload) -> Result<(), Error> {
        self.deliver(self.envelopes.seal(payload))
    }

    fn answer(&self, to: &str, payload: Payload) -> Result<(), Error> {
        self.deliver(self.envelopes.answer(to, payload))
    }

    fn deliver(&self, e: pb::Envelope) -> Result<(), Error> {
        let state = self.state.lock().unwrap();
        match (&state.out, state.ended) {
            (Some(out), false) => out
                .send(e)
                .map_err(|_| Error::refusal("session.revoked", "the Session has ended")),
            _ => Err(Error::refusal("session.revoked", "the Session has ended")),
        }
    }

    fn surface(&self, event: Event) {
        if let Some(events) = &self.state.lock().unwrap().events {
            let _ = events.send(event);
        }
    }

    /// Ends the Session once: the end is surfaced, nothing follows it, and nothing more is sent.
    fn finish(&self, end: Ended) {
        let mut state = self.state.lock().unwrap();
        if state.ended {
            return;
        }
        state.ended = true;
        state.out = None;
        state.active.clear();
        if let Some(events) = state.events.take() {
            let _ = events.send(Event::Ended(end));
        }
    }
}

/// A started unit and its Session.
pub struct Unit {
    admission: Admission,
    events: tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>,
    shared: Arc<Shared>,
    _socket: UnixListener,
}

/// Starts a unit from its environment.
pub async fn start(d: &Declaration) -> Result<Unit, Error> {
    start_with(d, |k| std::env::var(k).ok()).await
}

/// Starts a unit from the environment `getenv` reads: bind, register, open the Session. A refusal is
/// returned with its stage and code, and nothing is tried again.
pub async fn start_with(
    d: &Declaration,
    getenv: impl Fn(&str) -> Option<String>,
) -> Result<Unit, Error> {
    let env = environment(getenv)?;
    // Bind before registering: registered and unreachable is the one order that is wrong.
    let _ = std::fs::remove_file(&env.bind);
    let socket = UnixListener::bind(&env.bind).map_err(|e| {
        Error::Transport(format!(
            "the unit's socket {} cannot be bound: {e}",
            env.bind.display()
        ))
    })?;
    let channel = dial(&env.socket).await?;
    let response = pb::register_client::RegisterClient::new(channel.clone())
        .register(pb::RegisterRequest {
            plugin: env.plugin,
            unit: env.unit,
            token: env.token,
            protocol: PLUGIN_CONTRACT,
            language: "rust".into(),
            sdk_line: SDK_LINE.into(),
            declared: Some(d.surface()),
            ..Default::default()
        })
        .await?
        .into_inner();
    let outcome = pb::register_response::Outcome::try_from(response.outcome)
        .unwrap_or(pb::register_response::Outcome::Unspecified);
    if outcome == pb::register_response::Outcome::Refused {
        return Err(Error::Refusal(Refusal {
            code: response.code,
            message: response.message,
            stage: Some(stage_name(response.stage)),
        }));
    }

    let (out, outbound) = mpsc::unbounded_channel();
    let (events_tx, events) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        envelopes: Envelopes::new(&response.session_id),
        state: Mutex::new(State {
            out: Some(out),
            events: Some(events_tx),
            ended: false,
            closing: false,
            active: HashMap::new(),
        }),
        health: Mutex::new(None),
    });
    // The first envelope is the OPEN, carrying the identity admission issued.
    shared.send(Payload::Session(pb::SessionMessage {
        kind: Some(pb::session_message::Kind::Open(
            pb::session_message::Open {},
        )),
    }))?;
    // The stream is opened where nothing waits on it: a Core may send nothing, not even the start of its
    // answer, until it has something to say, and the heartbeat must not wait for that.
    tokio::spawn(receive(
        shared.clone(),
        pb::session_client::SessionClient::new(channel),
        outbound,
    ));
    let interval = response
        .heartbeat
        .and_then(|h| h.interval)
        .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32));
    if let Some(interval) = interval.filter(|d| !d.is_zero()) {
        tokio::spawn(beat(shared.clone(), interval));
    }
    Ok(Unit {
        admission: Admission {
            restricted: outcome == pb::register_response::Outcome::AcceptedWithRestrictions,
            granted: response.granted.into(),
            withheld: response.withheld.into(),
        },
        events: tokio::sync::Mutex::new(events),
        shared,
        _socket: socket,
    })
}

fn stage_name(stage: i32) -> String {
    pb::Stage::try_from(stage)
        .map(|s| words(s.as_str_name(), "STAGE_"))
        .unwrap_or_default()
}

/// An enumerator's name as a person reads it: `CAUSE_PLUGIN_DISABLED` is `plugin disabled`.
fn words(name: &str, prefix: &str) -> String {
    name.trim_start_matches(prefix)
        .to_lowercase()
        .replace('_', " ")
}

/// Repeats the author's last health report at the interval the Core assigned, until the Session ends.
/// Before the author's first report it sends nothing: a grade is the author's statement, and a unit that
/// never reports loses its liveness as a unit that sends nothing does.
async fn beat(shared: Arc<Shared>, interval: Duration) {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    loop {
        ticker.tick().await;
        let last = shared.health.lock().unwrap();
        let ended = match last.as_ref() {
            Some(h) => shared.send(Payload::Health(h.clone())).is_err(),
            None => shared.state.lock().unwrap().ended,
        };
        if ended {
            return;
        }
    }
}

/// Opens the Session's stream, and surfaces what it brings until it ends.
async fn receive(
    shared: Arc<Shared>,
    mut client: pb::session_client::SessionClient<tonic::transport::Channel>,
    outbound: mpsc::UnboundedReceiver<pb::Envelope>,
) {
    let mut inbound = match client.open(UnboundedReceiverStream::new(outbound)).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            shared.finish(Ended {
                closed: false,
                cause: Some("liveness lost".into()),
                line: format!("the Session's stream could not be opened: {status}"),
            });
            return;
        }
    };
    loop {
        let e = match inbound.message().await {
            Ok(Some(e)) => e,
            ended => {
                let closing = shared.state.lock().unwrap().closing;
                let end = if closing {
                    Ended {
                        closed: true,
                        cause: None,
                        line: String::new(),
                    }
                } else {
                    let why = match ended {
                        Err(status) => status.to_string(),
                        _ => "the stream ended".into(),
                    };
                    Ended {
                        closed: false,
                        cause: Some("liveness lost".into()),
                        line: format!("the Session's stream ended: {why}"),
                    }
                };
                shared.finish(end);
                return;
            }
        };
        match e.payload {
            Some(Payload::Session(pb::SessionMessage {
                kind: Some(pb::session_message::Kind::Revoked(r)),
            })) => {
                let cause = pb::session_message::revoked::Cause::try_from(r.cause)
                    .map(|c| words(c.as_str_name(), "CAUSE_"))
                    .unwrap_or_default();
                shared.finish(Ended {
                    closed: false,
                    cause: Some(cause),
                    line: r.line,
                });
                return;
            }
            Some(Payload::Control(pb::Control { kind: Some(kind) })) => match kind {
                pb::control::Kind::Command(c) => shared.surface(Event::Command(Command {
                    id: e.message_id,
                    r#type: c.r#type,
                    payload: c.payload,
                })),
                pb::control::Kind::Activate(a) => {
                    let transport = pb::control::activate::Transport::try_from(a.transport)
                        .map(|t| words(t.as_str_name(), "TRANSPORT_"))
                        .unwrap_or_default();
                    match connect(&a) {
                        Ok(flow) => {
                            shared
                                .state
                                .lock()
                                .unwrap()
                                .active
                                .insert(a.stream.clone(), Arc::new(Mutex::new(flow)));
                            let _ = shared.answer(
                                &e.message_id,
                                acknowledgement(pb::ack::Outcome::Done, String::new()),
                            );
                        }
                        Err(why) => {
                            let _ = shared.answer(
                                &e.message_id,
                                acknowledgement(pb::ack::Outcome::Failed, why),
                            );
                            continue;
                        }
                    }
                    shared.surface(Event::Activated(Activated {
                        stream: a.stream,
                        transport,
                        address: a.address,
                    }));
                }
                pb::control::Kind::Stop(s) => {
                    // Dropping the connection closes it.
                    shared.state.lock().unwrap().active.remove(&s.stream);
                    let _ = shared.answer(
                        &e.message_id,
                        acknowledgement(pb::ack::Outcome::Done, String::new()),
                    );
                    shared.surface(Event::Stopped { stream: s.stream });
                }
            },
            Some(Payload::Query(pb::Query {
                kind: Some(pb::query::Kind::Question(q)),
            })) => shared.surface(Event::Question(Question {
                id: e.message_id,
                r#type: q.r#type,
                payload: q.payload,
            })),
            Some(Payload::Error(err)) => shared.surface(Event::Refused {
                correlation: e.correlation_id,
                error: refusal_of(&err),
            }),
            _ => {}
        }
    }
}

impl Unit {
    /// What the Core answered the registration with.
    pub fn admission(&self) -> &Admission {
        &self.admission
    }

    /// The next thing the Session brings, in order; `None` once the end has been surfaced. The
    /// incarnation is then over, and the process should finish.
    pub async fn next(&self) -> Option<Event> {
        self.events.lock().await.recv().await
    }

    /// Ends the Session in order: a CLOSE, the unit's own departure.
    pub async fn close(&self) -> Result<(), Error> {
        {
            let mut state = self.shared.state.lock().unwrap();
            if state.ended {
                return Ok(());
            }
            state.closing = true;
        }
        let sent = self.shared.send(Payload::Session(pb::SessionMessage {
            kind: Some(pb::session_message::Kind::Close(
                pb::session_message::Close {},
            )),
        }));
        // Nothing more is sent: the Core ends the stream on a CLOSE.
        self.shared.state.lock().unwrap().out = None;
        let shared = self.shared.clone();
        tokio::spawn(async move {
            // If the Core does not end the stream, the departure still is one.
            tokio::time::sleep(Duration::from_secs(2)).await;
            shared.finish(Ended {
                closed: true,
                cause: None,
                line: String::new(),
            });
        });
        sent
    }

    /// Says what became of a command, correlated to it.
    pub async fn ack(&self, command: &Command, outcome: Outcome, line: &str) -> Result<(), Error> {
        let outcome = match outcome {
            Outcome::Accepted => pb::ack::Outcome::Accepted,
            Outcome::Done => pb::ack::Outcome::Done,
            Outcome::Failed => pb::ack::Outcome::Failed,
        };
        self.shared.answer(
            &command.id,
            Payload::Ack(pb::Ack {
                outcome: outcome as i32,
                line: line.into(),
            }),
        )
    }

    /// Answers a question, correlated to it.
    pub async fn answer(&self, question: &Question, payload: Vec<u8>) -> Result<(), Error> {
        self.shared.answer(
            &question.id,
            Payload::Query(pb::Query {
                kind: Some(pb::query::Kind::Answer(pb::query::Answer { payload })),
            }),
        )
    }

    /// Reports an occurrence of a declared class, with the author's severity. With none it is refused:
    /// the library never states one on the author's behalf.
    pub async fn report(
        &self,
        occurrence: &str,
        severity: Option<Severity>,
        line: &str,
        detail: Vec<u8>,
    ) -> Result<(), Error> {
        let Some(Severity(severity)) = severity else {
            return Err(Error::Misuse(
                "an occurrence is reported with the author's severity, and none was stated".into(),
            ));
        };
        if severity > 99 {
            return Err(Error::Misuse(format!(
                "a severity runs from 0 to 99, and {severity} is not one"
            )));
        }
        self.shared.send(Payload::Event(pb::Event {
            occurrence: occurrence.into(),
            severity: severity.into(),
            line: line.into(),
            detail,
        }))
    }

    /// Reports how well the unit is: a grade from 0 to 99, and a line. The library repeats the last
    /// report at every beat, and sends no beat before the first: a unit keeps its liveness only once its
    /// author has reported, so the first report must come within the tolerance the Core assigned.
    pub async fn health(&self, grade: u8, line: &str) -> Result<(), Error> {
        if grade > 99 {
            return Err(Error::Misuse(format!(
                "a grade runs from 0 to 99, and {grade} is not one"
            )));
        }
        let report = pb::Health {
            grade: grade.into(),
            line: line.into(),
        };
        let mut last = self.shared.health.lock().unwrap();
        *last = Some(report.clone());
        self.shared.send(Payload::Health(report))
    }

    /// Sends data on a stream, on the transport its activation named: one data envelope per packet on
    /// the ordered transport, one frame per datagram on the framed one, numbered from 1 within the
    /// activation. Only the Core creates a stream's transport, so a stream it has not activated has
    /// nowhere to be written, and the library refuses rather than make one.
    pub async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), Error> {
        let Some(flow) = self
            .shared
            .state
            .lock()
            .unwrap()
            .active
            .get(stream)
            .cloned()
        else {
            return Err(Error::refusal(
                "stream.inactive",
                format!("the stream {stream} has not been activated"),
            ));
        };
        let mut flow = flow.lock().unwrap();
        flow.next += 1;
        let sequence = flow.next;
        let written = match &flow.conn {
            Transport::Ordered(socket) => {
                let e = self
                    .shared
                    .envelopes
                    .seal(Payload::Data(pb::Data { sequence, payload }));
                socket.send(&prost::Message::encode_to_vec(&e))
            }
            Transport::Framed(socket) => {
                let clock = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                let mut frame = Vec::with_capacity(16 + payload.len());
                frame.extend_from_slice(&sequence.to_le_bytes());
                frame.extend_from_slice(&clock.to_le_bytes());
                frame.extend_from_slice(&payload);
                socket.send(&frame)
            }
        };
        written.map(|_| ()).map_err(|err| {
            Error::refusal(
                "stream.inactive",
                format!("the transport of {stream} took nothing: {err}"),
            )
        })
    }
}
