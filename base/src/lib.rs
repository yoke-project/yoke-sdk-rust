//! What the three libraries of this project share and nothing else: connecting to a socket, the
//! envelope and its correlation, a refusal as a Rust error carrying its code, the addresses a party
//! computes from its environment, and the contract version each library states.
//!
//! A concept that exists on one contract only does not live here: a candidate is in the base only if
//! all three libraries would otherwise implement it.

use std::path::PathBuf;

pub use yoke_proto;
use yoke_proto::plugin::v1 as pb;

/// The version of the plugin contract the definitions carry, which the plugin library declares.
pub const PLUGIN_CONTRACT: u32 = 0;

/// What a party is handed, and all it may assume.
#[derive(Debug, Clone)]
pub struct Env {
    pub plugin: String,
    pub unit: String,
    pub socket: PathBuf,
    pub bind: PathBuf,
    pub token: String,
}

/// Reads the reserved variables.
pub fn environment(getenv: impl Fn(&str) -> Option<String>) -> Result<Env, Error> {
    let _ = getenv;
    Err(Error::Environment(String::new()))
}

/// A refusal as it travels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: String,
    pub message: String,
    pub stage: Option<String>,
}

/// What can go wrong.
#[derive(Debug)]
pub enum Error {
    Environment(String),
    Refusal(Refusal),
    Transport(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

impl std::error::Error for Error {}

/// The refusal an error envelope carries.
pub fn refusal_of(e: &pb::Error) -> Error {
    let _ = e;
    Error::Transport(String::new())
}

/// Fills the header of every envelope one party sends in one Session.
pub struct Envelopes {
    session: String,
}

impl Envelopes {
    pub fn new(session: &str) -> Self {
        Envelopes {
            session: session.to_string(),
        }
    }

    pub fn seal(&self, payload: pb::envelope::Payload) -> pb::Envelope {
        let _ = &self.session;
        pb::Envelope {
            payload: Some(payload),
            ..Default::default()
        }
    }

    pub fn answer(&self, to: &str, payload: pb::envelope::Payload) -> pb::Envelope {
        let _ = to;
        self.seal(payload)
    }
}
