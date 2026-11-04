//! @YOKE_PROGRAM@: a Yoke Plugin. Its Manifest is kept beside the source, as @YOKE_ID@.yaml, and is what
//! `@YOKE_PROGRAM@ --manifest` prints; the continuous integration of this project compares the two.
use tokio::signal::unix::{SignalKind, signal};
use yoke_sdk::plugin::{Declaration, Event, start};

/// What this Plugin says about itself: its identity, and whatever it publishes and answers.
fn declaration() -> Declaration {
    Declaration {
        id: "@YOKE_ID@".into(),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--manifest") {
        print!("{}", declaration().manifest());
        return;
    }
    let unit = match start(&declaration()).await {
        Ok(unit) => unit,
        Err(err) => {
            eprintln!("@YOKE_PROGRAM@: {err}");
            std::process::exit(1);
        }
    };
    let _ = unit.health(0, "ready").await;
    let mut term = signal(SignalKind::terminate()).expect("@YOKE_PROGRAM@ listens for SIGTERM");
    loop {
        tokio::select! {
            event = unit.next() => match event {
                None | Some(Event::Ended(_)) => return,
                Some(_) => {}
            },
            _ = term.recv() => {
                let _ = unit.close().await;
                while let Some(event) = unit.next().await {
                    if matches!(event, Event::Ended(_)) {
                        break;
                    }
                }
                return;
            }
        }
    }
}
