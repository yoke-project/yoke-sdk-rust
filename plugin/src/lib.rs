//! The library a Plugin unit is written with.

use yoke_base::Error;

/// What this library says it is, at admission.
pub const SDK_LINE: &str = "yoke-sdk-rust 0.0.0";

/// What a Plugin says about itself.
#[derive(Debug, Clone, Default)]
pub struct Declaration {
    pub id: String,
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
        String::new()
    }
}

/// Four lists: what was granted, or what was withheld.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scope {
    pub capabilities: Vec<String>,
    pub streams: Vec<String>,
    pub commands: Vec<String>,
    pub queries: Vec<String>,
}

/// What the Core answered.
#[derive(Debug, Clone, Default)]
pub struct Admission {
    pub restricted: bool,
    pub granted: Scope,
    pub withheld: Scope,
}

/// An instruction the Core sent.
#[derive(Debug, Clone)]
pub struct Command {
    pub id: String,
    pub r#type: String,
    pub payload: Vec<u8>,
}

/// A question the Core asked.
#[derive(Debug, Clone)]
pub struct Question {
    pub id: String,
    pub r#type: String,
    pub payload: Vec<u8>,
}

/// The end of the Session.
#[derive(Debug, Clone)]
pub struct Ended {
    pub closed: bool,
    pub cause: Option<String>,
    pub line: String,
}

/// What the Session brings.
#[derive(Debug)]
pub enum Event {
    Command(Command),
    Question(Question),
    Ended(Ended),
}

/// What became of a command.
#[derive(Debug, Clone, Copy)]
pub enum Outcome {
    Accepted,
    Done,
    Failed,
}

/// How serious an occurrence is.
#[derive(Debug, Clone, Copy)]
pub struct Severity(u8);

impl Severity {
    pub fn of(n: u8) -> Self {
        Severity(n)
    }
}

/// A started unit and its Session.
pub struct Unit {
    admission: Admission,
}

impl Unit {
    pub fn admission(&self) -> &Admission {
        &self.admission
    }
    pub async fn next(&mut self) -> Option<Event> {
        None
    }
    pub async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
    pub async fn ack(&self, _c: &Command, _o: Outcome, _line: &str) -> Result<(), Error> {
        Ok(())
    }
    pub async fn answer(&self, _q: &Question, _payload: Vec<u8>) -> Result<(), Error> {
        Ok(())
    }
    pub async fn report(
        &self,
        _occurrence: &str,
        _s: Option<Severity>,
        _line: &str,
        _detail: Vec<u8>,
    ) -> Result<(), Error> {
        Ok(())
    }
    pub async fn emit(&self, _stream: &str, _payload: Vec<u8>) -> Result<(), Error> {
        Ok(())
    }
}

/// Starts a unit from its environment.
pub async fn start(d: &Declaration) -> Result<Unit, Error> {
    start_with(d, |k| std::env::var(k).ok()).await
}

/// Starts a unit from the environment getenv reads.
pub async fn start_with(
    _d: &Declaration,
    _getenv: impl Fn(&str) -> Option<String>,
) -> Result<Unit, Error> {
    Err(Error::Transport("not yet".into()))
}
