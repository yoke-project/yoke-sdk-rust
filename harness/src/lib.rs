//! The Rust plugin library's harness: the thinnest translation between the suite's directives and the
//! library. It turns a directive into a library call and what the library surfaces into an observation,
//! reports a refusal as its code and a verb it does not know as unrecognised, and judges nothing: what a
//! case requires lives in the suite. It speaks no wire of its own.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Notify;

use yoke_sdk::base::{Error, PLUGIN_CONTRACT};
use yoke_sdk::plugin::{
    Capability, Command, Declaration, Event, Object, Outcome, Question, SDK_LINE, Scope, Severity,
    Stream, Unit, start_with,
};

/// What the harness declares: one object of every kind, a stream on each transport, each governed by a
/// capability.
pub fn declaration() -> Declaration {
    Declaration {
        id: "com.yoke.conformance.rust".into(),
        streams: vec![
            Stream {
                id: "conformance.data".into(),
                ..Default::default()
            },
            Stream {
                id: "conformance.frames".into(),
                tolerates_loss: true,
                ..Default::default()
            },
        ],
        commands: vec!["calibrate".into()],
        queries: vec!["status".into()],
        occurrences: vec!["conformance.drift".into()],
        capabilities: vec![
            Capability {
                name: "stream.data.publish".into(),
                governs: Object::Stream("conformance.data".into()),
            },
            Capability {
                name: "stream.frames.publish".into(),
                governs: Object::Stream("conformance.frames".into()),
            },
            Capability {
                name: "command.calibrate.accept".into(),
                governs: Object::Command("calibrate".into()),
            },
            Capability {
                name: "query.status.answer".into(),
                governs: Object::Query("status".into()),
            },
            Capability {
                name: "event.drift.report".into(),
                governs: Object::Occurrence("conformance.drift".into()),
            },
        ],
        ..Default::default()
    }
}

/// The control socket's writing side, shared by the directives and the observations.
#[derive(Clone)]
struct Lines(Arc<tokio::sync::Mutex<OwnedWriteHalf>>);

impl Lines {
    async fn send(&self, line: Value) {
        let mut bytes = serde_json::to_vec(&line).unwrap_or_default();
        bytes.push(b'\n');
        let _ = self.0.lock().await.write_all(&bytes).await;
    }
}

/// What the harness holds: the unit once started, and what the library surfaced that a directive may
/// answer.
#[derive(Default)]
struct State {
    unit: Option<Arc<Unit>>,
    commands: HashMap<String, Command>,
    questions: HashMap<String, Question>,
}

/// Runs the harness until it is told to finish or its Session ends, and is its exit status.
pub async fn serve(getenv: impl Fn(&str) -> Option<String>) -> i32 {
    let Some(path) = getenv("CONFORMANCE_SOCKET") else {
        return 1;
    };
    let Ok(conn) = UnixStream::connect(path).await else {
        return 1;
    };
    let (read, write) = conn.into_split();
    let lines = Lines(Arc::new(tokio::sync::Mutex::new(write)));
    lines
        .send(
            json!({"type": "hello", "contract": "plugin", "language": "rust", "sdk": SDK_LINE,
            "version": PLUGIN_CONTRACT, "unit": getenv("YOKE_UNIT").unwrap_or_default()}),
        )
        .await;

    let state = Arc::new(Mutex::new(State::default()));
    let ended = Arc::new(Notify::new());
    let Ok(mut asked) = signal(SignalKind::terminate()) else {
        return 1;
    };
    let mut directives = BufReader::new(read).lines();
    loop {
        tokio::select! {
            _ = asked.recv() => {
                // Asked to stop, as the Core asks a process whose Session has ended: the end is reported
                // first, and the process leaves within the Core's window.
                let _ = tokio::time::timeout(Duration::from_secs(2), ended.notified()).await;
                return 0;
            }
            _ = ended.notified() => return 0,
            line = directives.next_line() => {
                let d: Value = match line {
                    Ok(Some(line)) => match serde_json::from_str(&line) { Ok(d) => d, Err(_) => continue },
                    _ => Value::Null,
                };
                if d.is_null() || d["type"] == "finish" {
                    let unit = state.lock().unwrap().unit.clone();
                    if let Some(unit) = unit {
                        let _ = unit.close().await;
                    }
                    return 0;
                }
                if d["type"] != "directive" {
                    continue;
                }
                let mut result = act(&d, &getenv, &state, &lines, &ended).await;
                result.insert("type".into(), json!("result"));
                result.insert("id".into(), d["id"].clone());
                lines.send(Value::Object(result)).await;
            }
        }
    }
}

/// A refusal as its code, and never as the library's words.
fn refusal(error: Error) -> Map<String, Value> {
    let mut line = Map::new();
    match error {
        Error::Refusal(r) => {
            line.insert("refusal".into(), json!(r.code));
            if let Some(stage) = r.stage {
                line.insert("value".into(), json!({"stage": stage}));
            }
        }
        _ => {
            line.insert("value".into(), json!({"failed": true}));
        }
    }
    line
}

fn value(v: Value) -> Map<String, Value> {
    let mut line = Map::new();
    line.insert("value".into(), v);
    line
}

fn scope(s: &Scope) -> Value {
    json!({"capabilities": s.capabilities, "streams": s.streams, "commands": s.commands, "queries": s.queries, "occurrences": s.occurrences})
}

async fn act(
    d: &Value,
    getenv: &impl Fn(&str) -> Option<String>,
    state: &Arc<Mutex<State>>,
    lines: &Lines,
    ended: &Arc<Notify>,
) -> Map<String, Value> {
    let arg = |name: &str| d["args"][name].as_str().unwrap_or_default().to_string();
    let verb = d["verb"].as_str().unwrap_or_default();
    match verb {
        "describe" => return value(json!({"manifest": declaration().manifest()})),
        "start" => {
            let unit = match start_with(&declaration(), getenv).await {
                Ok(unit) => Arc::new(unit),
                Err(e) => return refusal(e),
            };
            let a = unit.admission().clone();
            let first = {
                let mut s = state.lock().unwrap();
                let first = s.unit.is_none();
                if first {
                    s.unit = Some(unit.clone());
                }
                first
            };
            if first {
                tokio::spawn(observe(unit, state.clone(), lines.clone(), ended.clone()));
            }
            let outcome = if a.restricted {
                "accepted with restrictions"
            } else {
                "accepted"
            };
            return value(
                json!({"outcome": outcome, "granted": scope(&a.granted), "withheld": scope(&a.withheld)}),
            );
        }
        _ => {}
    }
    let known = ["close", "emit", "report-health", "report", "ack", "answer"];
    let Some(unit) = state.lock().unwrap().unit.clone() else {
        if known.contains(&verb) {
            return value(json!({"failed": true, "started": false}));
        }
        let mut line = Map::new();
        line.insert("unrecognised".into(), json!(true));
        return line;
    };
    let done = match verb {
        "close" => unit.close().await,
        "emit" => unit.emit(&arg("stream"), arg("payload").into_bytes()).await,
        "report-health" => {
            unit.health(
                d["args"]["grade"].as_u64().unwrap_or(0).min(255) as u8,
                &arg("line"),
            )
            .await
        }
        "report" => {
            let severity = d["args"]["severity"]
                .as_u64()
                .map(|n| Severity::of(n.min(255) as u8));
            unit.report(&arg("occurrence"), severity, &arg("line"), vec![])
                .await
        }
        "ack" => {
            let command = state.lock().unwrap().commands.get(&arg("command")).cloned();
            match command {
                Some(c) => unit.ack(&c, Outcome::Done, &arg("line")).await,
                None => Err(Error::Misuse("no such command was surfaced".into())),
            }
        }
        "answer" => {
            let question = state
                .lock()
                .unwrap()
                .questions
                .get(&arg("question"))
                .cloned();
            match question {
                Some(q) => unit.answer(&q, arg("payload").into_bytes()).await,
                None => Err(Error::Misuse("no such question was surfaced".into())),
            }
        }
        _ => {
            let mut line = Map::new();
            line.insert("unrecognised".into(), json!(true));
            return line;
        }
    };
    match done {
        Ok(()) => value(json!({})),
        Err(e) => refusal(e),
    }
}

/// Reports everything the library surfaces, in the order it surfaced it; the end ends the harness.
async fn observe(unit: Arc<Unit>, state: Arc<Mutex<State>>, lines: Lines, ended: Arc<Notify>) {
    while let Some(event) = unit.next().await {
        let (kind, fields) = match event {
            Event::Command(c) => {
                let fields = json!({"id": c.id, "type": c.r#type});
                state.lock().unwrap().commands.insert(c.id.clone(), c);
                ("command", fields)
            }
            Event::Question(q) => {
                let fields = json!({"id": q.id, "type": q.r#type, "payload": String::from_utf8_lossy(&q.payload)});
                state.lock().unwrap().questions.insert(q.id.clone(), q);
                ("question", fields)
            }
            Event::Activated(a) => (
                "activated",
                json!({"stream": a.stream, "transport": a.transport}),
            ),
            Event::Stopped { stream } => ("stopped", json!({"stream": stream})),
            Event::Refused { correlation, error } => {
                let code = match error {
                    Error::Refusal(r) => r.code,
                    _ => String::new(),
                };
                ("refused", json!({"correlation": correlation, "code": code}))
            }
            Event::Ended(end) => {
                lines
                    .send(json!({"type": "observation", "kind": "session-ended",
                        "fields": {"closed": end.closed, "cause": end.cause.unwrap_or_default()}}))
                    .await;
                ended.notify_one();
                return;
            }
        };
        lines
            .send(json!({"type": "observation", "kind": kind, "fields": fields}))
            .await;
    }
}
