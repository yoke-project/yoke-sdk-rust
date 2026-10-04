//! What the three libraries of this project share and nothing else: connecting to a socket, the
//! envelope and its correlation, a refusal as a Rust error carrying its code, the addresses a party
//! computes from its environment, and the contract version each library states.
//!
//! A concept that exists on one contract only does not live here: a candidate is in the base only if
//! all three libraries would otherwise implement it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Uri};

pub use yoke_proto;
use yoke_proto::plugin::v1 as pb;

/// The version of the plugin contract the definitions carry, which the plugin library declares.
pub const PLUGIN_CONTRACT: u32 = pb::Contract::Version as u32;

/// What a party is handed, and all it may assume.
#[derive(Debug, Clone)]
pub struct Env {
    /// The plugin this unit is a copy of.
    pub plugin: String,
    /// This unit's identity.
    pub unit: String,
    /// The instance's plugin channel.
    pub socket: PathBuf,
    /// The path this unit is expected to bind.
    pub bind: PathBuf,
    /// The bootstrap token.
    pub token: String,
}

/// Reads the reserved variables. A missing one is an error naming it; no path is assumed.
pub fn environment(getenv: impl Fn(&str) -> Option<String>) -> Result<Env, Error> {
    let read = |name: &str| getenv(name).unwrap_or_default();
    let env = Env {
        plugin: read("YOKE_PLUGIN"),
        unit: read("YOKE_UNIT"),
        socket: PathBuf::from(read("YOKE_SOCKET")),
        bind: PathBuf::from(read("YOKE_BIND")),
        token: read("YOKE_TOKEN"),
    };
    let missing: Vec<&str> = ["YOKE_UNIT", "YOKE_SOCKET", "YOKE_BIND"]
        .into_iter()
        .filter(|name| read(name).is_empty())
        .collect();
    if !missing.is_empty() {
        return Err(Error::Environment(format!(
            "the environment does not carry {}",
            missing.join(", ")
        )));
    }
    Ok(env)
}

/// Connects to a Unix socket.
pub async fn dial(path: &Path) -> Result<Channel, Error> {
    let path = path.to_path_buf();
    // The URI is required and never used: the connector below is where the socket is reached.
    Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|e| Error::Transport(e.to_string()))
}

/// A refusal as it travels: a code from the one namespace, a message for a person, and the stage where
/// there is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: String,
    pub message: String,
    pub stage: Option<String>,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.stage {
            Some(stage) => write!(f, "{} at {}: {}", self.code, stage, self.message),
            None => write!(f, "{}: {}", self.code, self.message),
        }
    }
}

/// What can go wrong: a refusal from the other party, an environment that does not carry what a party
/// needs, a transport that failed, or something the library refused to do on the author's behalf.
#[derive(Debug)]
pub enum Error {
    Environment(String),
    Refusal(Refusal),
    Transport(String),
    Misuse(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Environment(said) | Error::Transport(said) | Error::Misuse(said) => {
                f.write_str(said)
            }
            Error::Refusal(r) => r.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// A refusal with the code and the message given.
    pub fn refusal(code: &str, message: impl Into<String>) -> Self {
        Error::Refusal(Refusal {
            code: code.into(),
            message: message.into(),
            stage: None,
        })
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::Transport(s.to_string())
    }
}

/// The refusal an error envelope carries.
pub fn refusal_of(e: &pb::Error) -> Error {
    Error::Refusal(Refusal {
        code: e.code.clone(),
        message: e.message.clone(),
        stage: None,
    })
}

/// Fills the header of every envelope one party sends in one Session: a message identity no other of
/// its envelopes has, the Session's identity, and the sender's clock.
pub struct Envelopes {
    session: String,
    next: AtomicU64,
}

impl Envelopes {
    /// Numbers the envelopes of one Session.
    pub fn new(session: &str) -> Self {
        Envelopes {
            session: session.to_string(),
            next: AtomicU64::new(0),
        }
    }

    /// An envelope carrying the payload, with its header filled.
    pub fn seal(&self, payload: pb::envelope::Payload) -> pb::Envelope {
        let n = self.next.fetch_add(1, Ordering::SeqCst) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        pb::Envelope {
            message_id: format!("u-{n}"),
            session_id: self.session.clone(),
            sent_at_unix_nano: now,
            correlation_id: String::new(),
            payload: Some(payload),
        }
    }

    /// An envelope answering the message identified by `to`.
    pub fn answer(&self, to: &str, payload: pb::envelope::Payload) -> pb::Envelope {
        let mut e = self.seal(payload);
        e.correlation_id = to.to_string();
        e
    }
}
