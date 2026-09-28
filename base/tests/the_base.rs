//! The cases of the-base.std.md, one test each.

use std::collections::{HashMap, HashSet};

use yoke_base::{Envelopes, Error, environment, refusal_of};
use yoke_proto::plugin::v1 as pb;

fn read(relative: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(relative))
        .unwrap()
}

// std: yoke-sdk-rust:the-base.01
#[test]
fn the_base_holds_nothing_of_one_contract() {
    let source = read("src/lib.rs");
    for forbidden in [
        "RegisterRequest",
        "RegisterResponse",
        "register_client",
        "SessionMessage",
        "session_client",
        "Surface",
        "HeartbeatTerms",
        "Stage",
    ] {
        assert!(
            !source.contains(forbidden),
            "the base refers to {forbidden}"
        );
    }
    assert!(
        !source.to_lowercase().contains("subscri"),
        "the base refers to a subscription"
    );
    let manifest = read("Cargo.toml");
    for line in manifest.lines() {
        let name = line
            .split('=')
            .next()
            .unwrap_or("")
            .trim()
            .split('.')
            .next()
            .unwrap_or("");
        assert!(
            !name.starts_with("yoke-") || name == "yoke-proto",
            "the base depends on {name}, a library of this project"
        );
    }
}

// std: yoke-sdk-rust:the-base.02
#[test]
fn a_refusal_is_an_error_carrying_its_code_and_envelopes_are_correlated() {
    let error = refusal_of(&pb::Error {
        code: "session.correlation.unknown".into(),
        message: "names nothing".into(),
        detail: None,
    });
    let as_error: &dyn std::error::Error = &error;
    assert!(as_error.to_string().contains("session.correlation.unknown"));
    match &error {
        Error::Refusal(r) => {
            assert_eq!(r.code, "session.correlation.unknown");
            assert_eq!(r.message, "names nothing");
        }
        other => panic!("the error is {other:?}, not a refusal"),
    }

    let envelopes = Envelopes::new("sid-1");
    let heartbeat = || {
        pb::envelope::Payload::Health(pb::Health {
            grade: 99,
            line: String::new(),
        })
    };
    let first = envelopes.seal(heartbeat());
    let second = envelopes.seal(heartbeat());
    let answer = envelopes.answer(&first.message_id, heartbeat());
    let mut seen = HashSet::new();
    for e in [&first, &second, &answer] {
        assert_eq!(e.session_id, "sid-1");
        assert!(
            seen.insert(e.message_id.clone()),
            "{} was used twice",
            e.message_id
        );
        assert!(e.sent_at_unix_nano > 0);
    }
    assert_eq!(answer.correlation_id, first.message_id);
    assert_ne!(answer.correlation_id, answer.message_id);
    assert!(first.correlation_id.is_empty());
}

// std: yoke-sdk-rust:the-base.03
#[test]
fn the_addresses_come_from_the_environment() {
    let mut vars: HashMap<&str, &str> = HashMap::from([
        ("YOKE_PLUGIN", "com.yoke.station.acquire"),
        ("YOKE_UNIT", "acquire"),
        ("YOKE_SOCKET", "/run/yoke/plugin.sock"),
        ("YOKE_BIND", "/run/yoke/plugins/acquire.sock"),
        ("YOKE_TOKEN", "t-1"),
    ]);
    let env = environment(|k| vars.get(k).map(|v| v.to_string())).unwrap();
    assert_eq!(env.plugin, "com.yoke.station.acquire");
    assert_eq!(env.unit, "acquire");
    assert_eq!(env.socket.to_str(), Some("/run/yoke/plugin.sock"));
    assert_eq!(env.bind.to_str(), Some("/run/yoke/plugins/acquire.sock"));
    assert_eq!(env.token, "t-1");

    vars.remove("YOKE_SOCKET");
    match environment(|k| vars.get(k).map(|v| v.to_string())) {
        Err(e) => assert!(
            e.to_string().contains("YOKE_SOCKET"),
            "the error does not name it: {e}"
        ),
        Ok(env) => panic!("an environment without YOKE_SOCKET gave {:?}", env.socket),
    }
}
