//! The Rust interface library's harness: the thinnest translation between the suite's directives and the
//! library. Launched by the suite against an instance, it attaches to the channel a directive names at the
//! address the library computes, turns each further directive into one call of the library, reports what
//! the library answered in the suite's vocabulary — a refusal as its code and what it names, never the
//! library's words — and what the standing subscription or a subscription delivers as observations, in
//! order. It judges nothing, confirms only when told to, and speaks no wire of its own.

mod plain;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;

use yoke_proto::interface::v1 as pb;
use yoke_sdk::base::Error;
use yoke_sdk::interface::{self, Attachment, Options, Subscription};
use yoke_sdk::plugin::SDK_LINE;

/// The control socket's writing side, shared by the results and the observations.
#[derive(Clone)]
struct Lines(Arc<tokio::sync::Mutex<OwnedWriteHalf>>);

impl Lines {
    async fn send(&self, line: Value) {
        let mut bytes = serde_json::to_vec(&line).unwrap_or_default();
        bytes.push(b'\n');
        let _ = self.0.lock().await.write_all(&bytes).await;
    }

    async fn observe(&self, kind: &str, fields: Value) {
        self.send(json!({"type": "observation", "kind": kind, "fields": fields}))
            .await;
    }
}

/// An event as the suite reads it.
fn event(e: &pb::Event) -> Value {
    let subject = e.subject.clone().unwrap_or_default();
    json!({"seq": e.seq, "type": e.r#type, "subject_kind": subject.kind,
        "subject": subject.identity, "severity": e.severity})
}

/// Reports what a subscription delivers, in order, until it ends.
async fn subscribed(mut sub: Subscription, lines: Lines) {
    loop {
        match sub.next().await {
            Ok(Some(d)) => match d.carries {
                Some(pb::subscribed::Carries::Snapshot(s)) => {
                    lines
                        .observe(
                            "snapshot",
                            json!({"at": s.at, "records": plain::records(&s.records)}),
                        )
                        .await
                }
                Some(pb::subscribed::Carries::Overflow(s)) => {
                    lines
                        .observe(
                            "overflow",
                            json!({"at": s.at, "records": plain::records(&s.records)}),
                        )
                        .await
                }
                Some(pb::subscribed::Carries::Event(e)) => lines.observe("event", event(&e)).await,
                None => {}
            },
            _ => {
                lines.observe("ended", json!({})).await;
                return;
            }
        }
    }
}

fn text(args: &Value, key: &str) -> String {
    args[key].as_str().unwrap_or_default().to_string()
}

/// A number the suite sent as a number, or as the string a 64-bit value is written as.
fn number(args: &Value, key: &str) -> u64 {
    match &args[key] {
        Value::Number(n) => n.as_u64().unwrap_or(0),
        Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn value(v: Value) -> Map<String, Value> {
    let mut line = Map::new();
    line.insert("value".into(), v);
    line
}

/// A refusal as its code and what it names; anything else the library failed with is a failure the harness
/// does not interpret.
fn refused(error: Error) -> Map<String, Value> {
    let Error::Refusal(r) = error else {
        return value(json!({"failed": true}));
    };
    let mut named = Map::new();
    if let Some(s) = &r.subject {
        named.insert(
            "subject".into(),
            json!({"kind": s.kind, "identity": s.identity, "incarnation": s.incarnation}),
        );
    }
    if let Some(item) = r.item.as_ref().filter(|i| !i.is_empty()) {
        named.insert("item".into(), json!(item));
    }
    if let Some(s) = &r.suspension {
        named.insert("suspension".into(), json!({"grade": s.grade, "by": s.by}));
    }
    let mut line = value(Value::Object(named));
    line.insert("refusal".into(), json!(r.code));
    line
}

/// What the harness holds between directives: its attachments, the first being the one directed.
#[derive(Default)]
struct State {
    attached: Vec<Attachment>,
    standing: Option<String>,
}

/// Runs the harness until it is told to finish, and is its exit status.
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
            json!({"type": "hello", "contract": "interface", "language": "rust", "sdk": SDK_LINE,
            "version": interface::CONTRACT}),
        )
        .await;
    let mut state = State::default();
    let mut directives = BufReader::new(read).lines();
    loop {
        let d: Value = match directives.next_line().await {
            Ok(Some(line)) => match serde_json::from_str(&line) {
                Ok(d) => d,
                Err(_) => continue,
            },
            _ => Value::Null,
        };
        if d.is_null() || d["type"] == "finish" {
            for a in state.attached.iter_mut() {
                a.close().await;
            }
            return 0;
        }
        if d["type"] != "directive" {
            continue;
        }
        let verb = d["verb"].as_str().unwrap_or_default().to_string();
        let mut result = act(&verb, &d["args"], &getenv, &mut state, &lines).await;
        result.insert("type".into(), json!("result"));
        result.insert("id".into(), d["id"].clone());
        lines.send(Value::Object(result)).await;
    }
}

/// Attaches to the channel named, at the address the library computes under the instance.
async fn attach(
    channel: &str,
    getenv: &impl Fn(&str) -> Option<String>,
    state: &mut State,
    lines: &Lines,
) -> Map<String, Value> {
    let instance = getenv("CONFORMANCE_INSTANCE").unwrap_or_default();
    let Ok(address) = interface::service(Path::new(&instance), channel) else {
        return value(json!({"failed": true}));
    };
    let attached = tokio::time::timeout(
        Duration::from_secs(30),
        Attachment::attach(&address.to_string_lossy(), Options::new()),
    )
    .await;
    let mut a = match attached {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => return refused(e),
        Err(_) => return value(json!({"failed": true})),
    };
    let first = state.attached.is_empty();
    let standing = a.standing.take();
    let id = standing.as_ref().map(|s| s.id.clone()).unwrap_or_default();
    // A second attachment the channel admitted is kept beside the first, which stays the one directed.
    if first {
        state.standing = standing.as_ref().map(|s| s.id.clone());
        if let Some(sub) = standing {
            tokio::spawn(subscribed(sub, lines.clone()));
        }
    }
    let line = value(
        json!({"version": a.version, "subscription": id, "at": a.picture.at,
        "records": plain::records(&a.picture.records)}),
    );
    state.attached.push(a);
    line
}

/// Performs one directive, and reports what the library answered.
async fn act(
    verb: &str,
    args: &Value,
    getenv: &impl Fn(&str) -> Option<String>,
    state: &mut State,
    lines: &Lines,
) -> Map<String, Value> {
    if verb == "attach" {
        return attach(&text(args, "channel"), getenv, state, lines).await;
    }
    let known = [
        "authenticate",
        "read",
        "subscribe",
        "confirm",
        "command",
        "query",
        "stream-start",
        "stream-stop",
        "stream-subscribe",
        "reclaim",
    ];
    if !known.contains(&verb) {
        let mut line = Map::new();
        line.insert("unrecognised".into(), json!(true));
        return line;
    }
    let Some(a) = state.attached.first() else {
        return value(json!({"failed": true, "attached": false}));
    };
    let said = tokio::time::timeout(Duration::from_secs(30), async {
        match verb {
            "authenticate" => {
                let token = text(args, "token");
                let got = if token.is_empty() {
                    a.authenticate(&text(args, "account"), &text(args, "secret"))
                        .await
                } else {
                    a.authenticate_token(&token).await
                };
                got.map(|g| value(plain::authenticated(&g)))
            }
            "read" => a
                .read(&text(args, "kind"), &text(args, "identity"))
                .await
                .map(|rs| value(json!({"records": plain::records(&rs)}))),
            "subscribe" => {
                let filter = pb::Filter {
                    subject_kind: text(args, "subject_kind"),
                    subject_identity: text(args, "subject_identity"),
                    r#type: text(args, "type"),
                    ..Default::default()
                };
                a.subscribe(filter).await.map(|sub| {
                    tokio::spawn(subscribed(sub, lines.clone()));
                    value(json!({}))
                })
            }
            "confirm" => {
                let mut subscription = text(args, "subscription");
                if subscription.is_empty() {
                    subscription = state.standing.clone().unwrap_or_default();
                }
                a.confirm(&subscription, number(args, "sequence"))
                    .await
                    .map(|_| value(json!({})))
            }
            "command" => a
                .command(
                    &text(args, "unit"),
                    &text(args, "type"),
                    text(args, "payload").as_bytes(),
                )
                .await
                .map(|k| value(plain::acknowledged(&k))),
            "query" => a
                .ask(
                    &text(args, "unit"),
                    &text(args, "type"),
                    text(args, "payload").as_bytes(),
                )
                .await
                .map(|p| value(json!({"answer": String::from_utf8_lossy(&p)}))),
            "stream-start" => a
                .start_stream(&text(args, "unit"), &text(args, "stream"))
                .await
                .map(|k| value(plain::acknowledged(&k))),
            "stream-stop" => a
                .stop_stream(&text(args, "unit"), &text(args, "stream"))
                .await
                .map(|k| value(plain::acknowledged(&k))),
            "reclaim" => a.reclaim().await.map(|r| value(plain::reclaimed(&r))),
            "stream-subscribe" => a
                .subscribe_stream(&text(args, "unit"), &text(args, "stream"))
                .await
                .map(|mut d| {
                    let (delivery, flowing) = (d.id.clone(), d.flowing);
                    let (id, lines) = (delivery.clone(), lines.clone());
                    tokio::spawn(async move {
                        loop {
                            match d.next().await {
                                Ok(Some(f)) => {
                                    lines
                                        .observe(
                                            "frame",
                                            json!({"delivery": id, "sequence": f.sequence,
                                                "sent_at": f.sent_at,
                                                "payload": String::from_utf8_lossy(&f.payload)}),
                                        )
                                        .await
                                }
                                _ => {
                                    lines
                                        .observe("delivery-ended", json!({"delivery": id}))
                                        .await;
                                    return;
                                }
                            }
                        }
                    });
                    value(json!({"delivery": delivery, "flowing": flowing}))
                }),
            _ => unreachable!(),
        }
    })
    .await;
    match said {
        Ok(Ok(line)) => line,
        Ok(Err(e)) => refused(e),
        Err(_) => value(json!({"failed": true})),
    }
}
