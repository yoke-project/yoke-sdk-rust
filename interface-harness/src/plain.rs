//! A message of the contract as the suite reads it: its fields by their names in the contract, as JSON the
//! way the contract's own JSON mapping writes them — a field at its default is left out, a 64-bit number is
//! a string, an enumeration is its name and a time is RFC 3339.

use serde_json::{Map, Value, json};
use yoke_proto::interface::v1 as pb;

fn put(into: &mut Map<String, Value>, key: &str, value: Value) {
    let default = match &value {
        Value::Null => true,
        Value::Bool(b) => !b,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    };
    if !default {
        into.insert(key.into(), value);
    }
}

fn number(n: u64) -> Value {
    if n == 0 {
        Value::Null
    } else {
        json!(n.to_string())
    }
}

fn grade(n: u32) -> Value {
    if n == 0 { Value::Null } else { json!(n) }
}

/// A time as RFC 3339, in UTC.
fn time(t: &prost_types::Timestamp) -> Value {
    let days = t.seconds.div_euclid(86_400);
    let secs = t.seconds.rem_euclid(86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let (h, m, s) = (secs / 3600, secs % 3600 / 60, secs % 60);
    let fraction = if t.nanos == 0 {
        String::new()
    } else {
        format!(".{:09}", t.nanos).trim_end_matches('0').to_string()
    };
    json!(format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}{fraction}Z"
    ))
}

fn strings(list: &[String]) -> Value {
    json!(list)
}

fn condition(c: &pb::Condition) -> Value {
    let mut o = Map::new();
    put(&mut o, "grade", grade(c.grade));
    put(&mut o, "line", json!(c.line));
    put(&mut o, "since", c.since.as_ref().map_or(Value::Null, time));
    Value::Object(o)
}

/// One record.
pub fn record(r: &pb::Record) -> Value {
    let mut o = Map::new();
    match &r.subject {
        Some(pb::record::Subject::Instance(i)) => {
            let mut v = Map::new();
            put(&mut v, "ready", json!(i.ready));
            put(&mut v, "stopping", json!(i.stopping));
            put(&mut v, "since", i.since.as_ref().map_or(Value::Null, time));
            o.insert("instance".into(), Value::Object(v));
        }
        Some(pb::record::Subject::Unit(u)) => {
            let mut v = Map::new();
            if let Some(d) = &u.declared {
                let mut d_ = Map::new();
                put(&mut d_, "identity", json!(d.identity));
                put(&mut d_, "kind", json!(d.kind));
                v.insert("declared".into(), Value::Object(d_));
            }
            if let Some(ob) = &u.observed {
                let mut o_ = Map::new();
                put(&mut o_, "state", json!(ob.state));
                put(&mut o_, "incarnation", number(ob.incarnation));
                put(
                    &mut o_,
                    "since",
                    ob.since.as_ref().map_or(Value::Null, time),
                );
                put(
                    &mut o_,
                    "condition",
                    ob.condition.as_ref().map_or(Value::Null, condition),
                );
                put(&mut o_, "streams", strings(&ob.streams));
                v.insert("observed".into(), Value::Object(o_));
            }
            if let Some(a) = &u.addressed {
                let mut a_ = Map::new();
                put(&mut a_, "streams", strings(&a.streams));
                put(&mut a_, "commands", strings(&a.commands));
                put(&mut a_, "queries", strings(&a.queries));
                v.insert("addressed".into(), Value::Object(a_));
            }
            o.insert("unit".into(), Value::Object(v));
        }
        Some(pb::record::Subject::Channel(c)) => {
            o.insert("channel".into(), channel(c));
        }
        None => {}
    }
    Value::Object(o)
}

/// One channel's record.
pub fn channel(c: &pb::ChannelRecord) -> Value {
    let mut v = Map::new();
    if let Some(d) = &c.declared {
        let mut d_ = Map::new();
        put(&mut d_, "name", json!(d.name));
        put(&mut d_, "projection", json!(d.projection));
        put(&mut d_, "address_class", json!(d.address_class));
        put(&mut d_, "clients", json!(d.clients));
        v.insert("declared".into(), Value::Object(d_));
    }
    if let Some(ob) = &c.observed {
        let mut o_ = Map::new();
        put(&mut o_, "attached", json!(ob.attached));
        put(&mut o_, "client", json!(ob.client));
        put(&mut o_, "suspended", json!(ob.suspended));
        put(&mut o_, "grade", json!(ob.grade));
        put(&mut o_, "by", json!(ob.by));
        put(&mut o_, "reason", json!(ob.reason));
        v.insert("observed".into(), Value::Object(o_));
    }
    Value::Object(v)
}

pub fn records(rs: &[pb::Record]) -> Value {
    Value::Array(rs.iter().map(record).collect())
}

pub fn authenticated(a: &pb::Authenticated) -> Value {
    let mut o = Map::new();
    put(&mut o, "account", json!(a.account));
    put(&mut o, "token", json!(a.token));
    put(
        &mut o,
        "expires",
        a.expires.as_ref().map_or(Value::Null, time),
    );
    Value::Object(o)
}

pub fn acknowledged(a: &pb::Acknowledged) -> Value {
    let mut o = Map::new();
    let outcome = pb::acknowledged::Outcome::try_from(a.outcome)
        .map(|n| n.as_str_name())
        .unwrap_or("OUTCOME_UNSPECIFIED");
    put(
        &mut o,
        "outcome",
        json!(if a.outcome == 0 { "" } else { outcome }),
    );
    put(&mut o, "line", json!(a.line));
    Value::Object(o)
}

pub fn reclaimed(r: &pb::Reclaimed) -> Value {
    let mut o = Map::new();
    put(
        &mut o,
        "channel",
        r.channel.as_ref().map_or(Value::Null, channel),
    );
    put(&mut o, "changed", json!(r.changed));
    Value::Object(o)
}
