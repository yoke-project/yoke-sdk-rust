//! The cases of the-interface-library.std.md, one test each, against a Core that records what arrives and
//! says what its script tells it to.

mod common;

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_stream::wrappers::UnixListenerStream;

use common::*;
use pb::interface_server::InterfaceServer;
use yoke_proto::interface::v1 as pb;
use yoke_sdk::interface::{self, Attachment, Options};

// std: yoke-sdk-rust:the-interface-library.01
#[test]
fn a_channels_address_is_computed_or_handed_to_a_managed_interface() {
    assert_eq!(
        interface::service(Path::new("/run/yoke"), "panel").unwrap(),
        Path::new("/run/yoke/interfaces/panel.sock")
    );
    let env = |k: &str| (k == "XDG_RUNTIME_DIR").then(|| "/run/user/1000".to_string());
    assert_eq!(
        interface::application("bench-a", "panel", env).unwrap(),
        Path::new("/run/user/1000/yoke/bench-a/interfaces/panel.sock")
    );
    let handed =
        |k: &str| (k == "YOKE_SOCKET").then(|| "/run/yoke/interfaces/panel.sock".to_string());
    assert_eq!(
        interface::managed(handed).unwrap(),
        Path::new("/run/yoke/interfaces/panel.sock")
    );
    let none = interface::managed(|_| None).unwrap_err();
    assert!(none.to_string().contains("YOKE_SOCKET"), "{none}");
    assert!(interface::service(Path::new("/run/yoke"), "a/b").is_err());
    assert!(interface::service(Path::new("/run/yoke"), "..").is_err());
    assert!(interface::application("a/b", "panel", env).is_err());
}

// std: yoke-sdk-rust:the-interface-library.02
#[tokio::test]
async fn attaching_surfaces_the_opening_and_a_version_it_does_not_speak_is_refused() {
    let dir = fresh_dir();
    let good = core(|a: Attached| {
        tokio::spawn(async move {
            let _ = a
                .out
                .send(Ok(opening("standing", 1, vec![unit_record("acquire")])))
                .await;
            let _requests = a.requests;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&good, &dir), Options::new()),
    )
    .await
    .unwrap();
    assert_eq!(attached.version, 1);
    assert_eq!(attached.standing.as_ref().unwrap().id, "standing");
    assert_eq!(attached.picture.records.len(), 1);
    assert_eq!(attached.picture.records[0], unit_record("acquire"));
    attached.close().await;

    let dir = fresh_dir();
    let other = core(|a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 2, vec![]))).await;
            let _requests = a.requests;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
    });
    let address = serve(&other, &dir);
    let refused = Attachment::attach(&address, Options::new())
        .await
        .err()
        .unwrap();
    let r = refusal_in(refused);
    assert_eq!(r.code, "compat.unsupported");
    assert!(
        r.message.contains('1') && r.message.contains('2'),
        "{}",
        r.message
    );
    within("the connection to be let go", async {
        while other.ends.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let dir = fresh_dir();
    // A Core that refuses the attachment itself: the status carries the refusal.
    let refusing = RefusingCore {
        status: refused_with("channel.in_use", "the channel has its client"),
    };
    let socket = dir.join("panel.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InterfaceServer::new(refusing))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .ok();
    });
    let e = Attachment::attach(&socket.to_string_lossy(), Options::new())
        .await
        .err()
        .unwrap();
    assert_eq!(refusal_in(e).code, "channel.in_use");
}

// std: yoke-sdk-rust:the-interface-library.03
#[tokio::test]
async fn one_method_per_operation_each_stating_the_version_on_one_connection() {
    use pb::request::Operation as Op;
    use pb::response::Answer;
    let dir = fresh_dir();
    let c = core(|mut a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            let mut slow: Option<String> = None;
            while let Some(Seen::Frame(f)) = a.requests.recv().await {
                let Some(r) = request_of(&f) else { continue };
                let reply = match r.operation.as_ref().unwrap() {
                    Op::Authenticate(_) => Some(Answer::Authenticate(pb::Authenticated {
                        account: "alice".into(),
                        token: "t-1".into(),
                        expires: None,
                    })),
                    Op::Read(_) => Some(Answer::Read(pb::Records::default())),
                    Op::Subscribe(_) => Some(Answer::Subscribe(pb::Subscribed {
                        carries: Some(pb::subscribed::Carries::Snapshot(pb::Snapshot::default())),
                    })),
                    Op::Confirm(_) => Some(Answer::Confirm(pb::Confirmed {})),
                    Op::Command(c) => {
                        let done = acknowledged(pb::acknowledged::Outcome::Done);
                        let _ = a.out.send(Ok(answer(&f.call, done))).await;
                        if c.unit == "fast"
                            && let Some(call) = slow.take()
                        {
                            // The question's answer comes after the command's has been taken.
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            let _ = a
                                .out
                                .send(Ok(answer(
                                    &call,
                                    Answer::Query(pb::Answered {
                                        payload: b"slow".to_vec(),
                                    }),
                                )))
                                .await;
                        }
                        None
                    }
                    Op::Query(q) if q.unit == "slow" => {
                        slow = Some(f.call.clone());
                        None
                    }
                    Op::Query(_) => Some(Answer::Query(pb::Answered {
                        payload: b"answer".to_vec(),
                    })),
                    Op::StreamStart(_) => Some(Answer::StreamStart(pb::Acknowledged {
                        outcome: pb::acknowledged::Outcome::Accepted as i32,
                        line: String::new(),
                    })),
                    Op::StreamStop(_) => Some(Answer::StreamStop(pb::Acknowledged {
                        outcome: pb::acknowledged::Outcome::Accepted as i32,
                        line: String::new(),
                    })),
                    Op::StreamSubscribe(_) => Some(Answer::StreamSubscribe(pb::Delivering {
                        delivery: "00000001".into(),
                        arrives: Some(pb::delivering::Arrives::Connection(true)),
                        flowing: true,
                    })),
                    Op::StreamUnsubscribe(_) => Some(Answer::StreamUnsubscribe(pb::Released {})),
                    Op::Reclaim(_) => Some(Answer::Reclaim(pb::Reclaimed {
                        channel: None,
                        changed: true,
                    })),
                };
                if let Some(reply) = reply {
                    let _ = a.out.send(Ok(answer(&f.call, reply))).await;
                }
            }
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&c, &dir), Options::new()),
    )
    .await
    .unwrap();
    let unit = "acquire";
    assert_eq!(
        attached
            .authenticate("alice", "hunter2x")
            .await
            .unwrap()
            .account,
        "alice"
    );
    attached.read("unit", "").await.unwrap();
    let mut subscription = attached.subscribe(pb::Filter::default()).await.unwrap();
    assert!(matches!(
        subscription.next().await.unwrap().unwrap().carries,
        Some(pb::subscribed::Carries::Snapshot(_))
    ));
    attached.confirm("standing", 1).await.unwrap();
    attached.command(unit, "calibrate", b"x").await.unwrap();
    attached.ask(unit, "status", b"").await.unwrap();
    attached.start_stream(unit, "spectra").await.unwrap();
    attached.stop_stream(unit, "spectra").await.unwrap();
    let delivery = attached.subscribe_stream(unit, "spectra").await.unwrap();
    assert!(delivery.flowing);
    delivery.release().await.unwrap();
    assert!(attached.reclaim().await.unwrap().changed);

    let order = Arc::new(Mutex::new(Vec::new()));
    let (slow, fast) = tokio::join!(
        async {
            let a = attached.ask("slow", "status", b"").await.unwrap();
            order.lock().unwrap().push("question");
            a
        },
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let a = attached.command("fast", "calibrate", b"").await.unwrap();
            order.lock().unwrap().push("command");
            a
        }
    );
    assert_eq!(slow, b"slow");
    assert_eq!(fast.outcome, pb::acknowledged::Outcome::Done as i32);
    assert_eq!(*order.lock().unwrap(), ["command", "question"]);
    attached.close().await;

    let frames = c.frames.lock().unwrap();
    assert_eq!(c.attachments.load(Ordering::SeqCst), 1);
    let mut names: Vec<&str> = frames
        .iter()
        .filter_map(|(_, f)| request_of(f).map(interface::name))
        .collect();
    for (_, f) in frames.iter() {
        if let Some(r) = request_of(f) {
            assert_eq!(r.version, 1, "{} states no version", interface::name(r));
        }
    }
    names.sort();
    names.dedup();
    assert_eq!(
        names,
        [
            "authenticate",
            "command",
            "confirm",
            "query",
            "read",
            "reclaim",
            "stream.start",
            "stream.stop",
            "stream.subscribe",
            "stream.unsubscribe",
            "subscribe"
        ]
    );
}

// std: yoke-sdk-rust:the-interface-library.04
#[tokio::test]
async fn a_refusal_is_the_bases_error_with_its_code_and_its_detail() {
    use pb::request::Operation as Op;
    let dir = fresh_dir();
    let c = core(|mut a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            while let Some(Seen::Frame(f)) = a.requests.recv().await {
                let refused = match request_of(&f).and_then(|r| r.operation.as_ref()) {
                    Some(Op::Read(_)) => pb::Refusal {
                        code: "subject.unknown".into(),
                        message: "no such unit".into(),
                        detail: Some(pb::refusal::Detail::Subject(pb::Subject {
                            kind: "unit".into(),
                            identity: "nobody".into(),
                            incarnation: 0,
                        })),
                    },
                    _ => pb::Refusal {
                        code: "channel.suspended".into(),
                        message: "suspended".into(),
                        detail: Some(pb::refusal::Detail::Suspension(pb::Suspension {
                            grade: "read-only".into(),
                            by: "service".into(),
                        })),
                    },
                };
                let _ = a.out.send(Ok(refusal(&f.call, refused))).await;
            }
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&c, &dir), Options::new()),
    )
    .await
    .unwrap();
    let read = refusal_in(attached.read("unit", "nobody").await.unwrap_err());
    assert_eq!(read.code, "subject.unknown");
    let subject = read.subject.unwrap();
    assert_eq!(
        (subject.kind.as_str(), subject.identity.as_str()),
        ("unit", "nobody")
    );
    let command = refusal_in(
        attached
            .command("acquire", "calibrate", b"")
            .await
            .unwrap_err(),
    );
    assert_eq!(command.code, "channel.suspended");
    let suspension = command.suspension.unwrap();
    assert_eq!(
        (suspension.grade.as_str(), suspension.by.as_str()),
        ("read-only", "service")
    );
    attached.close().await;
}

// std: yoke-sdk-rust:the-interface-library.05
#[tokio::test]
async fn a_subscription_is_delivered_in_order_until_ended_and_nothing_reconnects() {
    use pb::request::Operation as Op;
    let dir = fresh_dir();
    let c = core(|mut a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            for seq in 1..=3 {
                let _ = a
                    .out
                    .send(Ok(event("standing", seq, "unit.state.changed")))
                    .await;
            }
            let overflow = pb::Subscribed {
                carries: Some(pb::subscribed::Carries::Overflow(pb::Snapshot {
                    at: 9,
                    records: vec![unit_record("acquire")],
                })),
            };
            let _ = a
                .out
                .send(Ok(answer(
                    "standing",
                    pb::response::Answer::Subscribe(overflow),
                )))
                .await;
            while let Some(Seen::Frame(f)) = a.requests.recv().await {
                match &f.carries {
                    Some(pb::client_frame::Carries::Request(r))
                        if matches!(r.operation, Some(Op::Subscribe(_))) =>
                    {
                        let snapshot = pb::Subscribed {
                            carries: Some(pb::subscribed::Carries::Snapshot(
                                pb::Snapshot::default(),
                            )),
                        };
                        let _ = a
                            .out
                            .send(Ok(answer(
                                &f.call,
                                pb::response::Answer::Subscribe(snapshot),
                            )))
                            .await;
                        for seq in 10..=11 {
                            let _ = a
                                .out
                                .send(Ok(event(&f.call, seq, "unit.state.changed")))
                                .await;
                        }
                    }
                    Some(pb::client_frame::Carries::Cancel(_)) => {
                        // The Core ends the attachment once the subscription is cancelled.
                        return;
                    }
                    _ => {}
                }
            }
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&c, &dir), Options::new()),
    )
    .await
    .unwrap();
    let mut standing = attached.standing.take().unwrap();
    for seq in 1..=3 {
        match within("a standing event", standing.next())
            .await
            .unwrap()
            .unwrap()
            .carries
        {
            Some(pb::subscribed::Carries::Event(e)) => assert_eq!(e.seq, seq),
            other => panic!("the standing subscription delivered {other:?}"),
        }
    }
    match within("the overflow", standing.next())
        .await
        .unwrap()
        .unwrap()
        .carries
    {
        Some(pb::subscribed::Carries::Overflow(s)) => assert_eq!(s.at, 9),
        other => panic!("the overflow was {other:?}"),
    }
    let mut sub = attached.subscribe(pb::Filter::default()).await.unwrap();
    assert!(matches!(
        sub.next().await.unwrap().unwrap().carries,
        Some(pb::subscribed::Carries::Snapshot(_))
    ));
    for seq in 10..=11 {
        match within("an event", sub.next())
            .await
            .unwrap()
            .unwrap()
            .carries
        {
            Some(pb::subscribed::Carries::Event(e)) => assert_eq!(e.seq, seq),
            other => panic!("the subscription delivered {other:?}"),
        }
    }
    let id = sub.id.clone();
    sub.close();
    within("the cancel", async {
        while !c.frames.lock().unwrap().iter().any(|(_, f)| {
            f.call == id && matches!(f.carries, Some(pb::client_frame::Carries::Cancel(_)))
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let after = within("the end", async {
        loop {
            match attached.read("unit", "").await {
                Err(e) => break e,
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await;
    assert!(interface::is_ended(&after), "{after}");
    assert_eq!(
        c.attachments.load(Ordering::SeqCst),
        1,
        "something reconnected"
    );
}

// std: yoke-sdk-rust:the-interface-library.06
#[tokio::test]
async fn confirmation_is_sent_only_when_the_author_confirms() {
    use pb::request::Operation as Op;
    let dir = fresh_dir();
    let c = core(|mut a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            while let Some(Seen::Frame(f)) = a.requests.recv().await {
                if let Some(Op::Confirm(_)) = request_of(&f).and_then(|r| r.operation.as_ref()) {
                    let _ = a
                        .out
                        .send(Ok(answer(
                            &f.call,
                            pb::response::Answer::Confirm(pb::Confirmed {}),
                        )))
                        .await;
                }
            }
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&c, &dir), Options::new()),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        c.frames.lock().unwrap().is_empty(),
        "the library sent something unasked"
    );
    attached.confirm("standing", 7).await.unwrap();
    let frames = c.frames.lock().unwrap().clone();
    assert_eq!(frames.len(), 1);
    match request_of(&frames[0].1).and_then(|r| r.operation.clone()) {
        Some(Op::Confirm(c)) => assert_eq!((c.subscription.as_str(), c.sequence), ("standing", 7)),
        other => panic!("the one request was {other:?}"),
    }
    attached.close().await;
}

// std: yoke-sdk-rust:the-interface-library.07
#[tokio::test]
async fn a_streams_delivery_is_read_where_its_answer_says_it_arrives() {
    use pb::request::Operation as Op;
    use socket2::{Domain, SockAddr, Socket, Type};
    let dir = fresh_dir();
    let socket_path = dir.join("delivery.sock");
    let listener = Socket::new(Domain::UNIX, Type::SEQPACKET, None).unwrap();
    listener
        .bind(&SockAddr::unix(&socket_path).unwrap())
        .unwrap();
    listener.listen(1).unwrap();
    std::thread::spawn(move || {
        let Ok((conn, _)) = listener.accept() else {
            return;
        };
        for n in 1u64..=2 {
            let mut packet = Vec::new();
            packet.extend_from_slice(&n.to_le_bytes());
            packet.extend_from_slice(&(1000 + n).to_le_bytes());
            packet.extend_from_slice(format!("frame {n}").as_bytes());
            let _ = conn.send(&packet);
        }
    });
    let socket_arrival = socket_path.to_string_lossy().into_owned();
    let c = core(move |mut a: Attached| {
        let socket_arrival = socket_arrival.clone();
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            let mut asked = 0;
            while let Some(Seen::Frame(f)) = a.requests.recv().await {
                match request_of(&f).and_then(|r| r.operation.as_ref()) {
                    Some(Op::StreamSubscribe(_)) => {
                        asked += 1;
                        let (id, arrives) = if asked == 1 {
                            (
                                "00000001",
                                pb::delivering::Arrives::Socket(socket_arrival.clone()),
                            )
                        } else {
                            ("00000002", pb::delivering::Arrives::Connection(true))
                        };
                        let _ = a
                            .out
                            .send(Ok(answer(
                                &f.call,
                                pb::response::Answer::StreamSubscribe(pb::Delivering {
                                    delivery: id.into(),
                                    arrives: Some(arrives),
                                    flowing: asked == 1,
                                }),
                            )))
                            .await;
                        if asked == 2 {
                            for n in 1u64..=2 {
                                let _ = a
                                    .out
                                    .send(Ok(pb::CoreFrame {
                                        call: id.into(),
                                        carries: Some(pb::core_frame::Carries::Delivery(
                                            pb::StreamDelivery {
                                                delivery: id.into(),
                                                sequence: n,
                                                sent_at: 2000 + n,
                                                payload: format!("frame {n}").into_bytes(),
                                            },
                                        )),
                                    }))
                                    .await;
                            }
                            let _ = a.out.send(Ok(completion(id))).await;
                        }
                    }
                    Some(Op::StreamUnsubscribe(_)) => {
                        let _ = a
                            .out
                            .send(Ok(answer(
                                &f.call,
                                pb::response::Answer::StreamUnsubscribe(pb::Released {}),
                            )))
                            .await;
                    }
                    _ => {}
                }
            }
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&c, &dir), Options::new()),
    )
    .await
    .unwrap();
    let mut by_socket = attached
        .subscribe_stream("acquire", "spectra")
        .await
        .unwrap();
    assert!(by_socket.flowing);
    for n in 1u64..=2 {
        let f = within("a frame on the socket", by_socket.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (f.sequence, f.sent_at, f.payload),
            (n, 1000 + n, format!("frame {n}").into_bytes())
        );
    }
    assert!(
        within("the socket's end", by_socket.next())
            .await
            .unwrap()
            .is_none()
    );
    let mut by_connection = attached
        .subscribe_stream("acquire", "spectra")
        .await
        .unwrap();
    assert!(!by_connection.flowing);
    for n in 1u64..=2 {
        let f = within("a frame on the connection", by_connection.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (f.sequence, f.sent_at, f.payload),
            (n, 2000 + n, format!("frame {n}").into_bytes())
        );
    }
    assert!(
        within("the delivery's end", by_connection.next())
            .await
            .unwrap()
            .is_none()
    );
    by_connection.release().await.unwrap();
    let released = c
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter_map(
            |(_, f)| match request_of(f).and_then(|r| r.operation.clone()) {
                Some(Op::StreamUnsubscribe(r)) => Some(r.delivery),
                _ => None,
            },
        )
        .collect::<Vec<_>>();
    assert_eq!(released, ["00000002"]);
    attached.close().await;
}

// std: yoke-sdk-rust:the-interface-library.08
#[tokio::test]
async fn closing_an_attachment_ends_it_in_order() {
    use pb::request::Operation as Op;
    let dir = fresh_dir();
    let c = core(|mut a: Attached| {
        tokio::spawn(async move {
            let _ = a.out.send(Ok(opening("standing", 1, vec![]))).await;
            while let Some(seen) = a.requests.recv().await {
                match seen {
                    Seen::Frame(f) => {
                        if let Some(Op::Read(_)) = request_of(&f).and_then(|r| r.operation.as_ref())
                        {
                            let _ = a
                                .out
                                .send(Ok(answer(
                                    &f.call,
                                    pb::response::Answer::Read(pb::Records::default()),
                                )))
                                .await;
                        }
                    }
                    // The Core ends the stream once the client's requests have ended.
                    Seen::Closed => return,
                }
            }
        });
    });
    let mut attached = within(
        "the attachment",
        Attachment::attach(&serve(&c, &dir), Options::new()),
    )
    .await
    .unwrap();
    attached.read("unit", "").await.unwrap();
    within("the close", attached.close()).await;
    assert_eq!(*c.ends.lock().unwrap(), ["closed"]);
}

// std: yoke-sdk-rust:the-interface-library.09
#[test]
fn a_panel_built_on_the_library_names_nothing_of_the_administrative_contract() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for module in ["interface.rs", "tls.rs"] {
        let text = std::fs::read_to_string(src.join(module)).unwrap();
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            assert!(
                !code.contains("administrative"),
                "{module}:{} reaches the administrative contract: {line}",
                n + 1
            );
        }
    }
}
