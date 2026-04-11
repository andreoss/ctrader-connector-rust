//! Output of the live tests.
//!
//! Whatever a live test prints passes through [`veiled`], which replaces every identifier
//! made known with [`veil`]: the lines printed with `say!` and the messages of panics, failed
//! assertions included. The live tests print with nothing else, and each of them starts with
//! [`enter`], which puts the redacting panic hook in place before anything can fail.

#![allow(dead_code)]

use std::fmt;
use std::process::Command;
use std::sync::{Mutex, Once, PoisonError};

use ctrader_connector::common::errors::{ConnectorError, ErrorCode, ServerError};
use ctrader_connector::common::ids::AccountId;
use tokio::sync::MutexGuard;

static LIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static KNOWN: Mutex<Vec<String>> = Mutex::new(Vec::new());

static HOOK: Once = Once::new();

/// Text printed in place of an identifier.
pub const VEIL: &str = "<account>";

/// Keeps `identifier` out of everything printed from now on.
pub fn veil(identifier: impl fmt::Display) {
    install();
    let identifier = identifier.to_string();
    let mut known = KNOWN.lock().unwrap_or_else(PoisonError::into_inner);
    if !identifier.is_empty() && !known.contains(&identifier) {
        known.push(identifier);
        known.sort_by_key(|known| std::cmp::Reverse(known.len()));
    }
}

/// Returns `text` with every known identifier replaced by [`VEIL`].
pub fn veiled(text: &str) -> String {
    KNOWN
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .fold(text.to_owned(), |text, known| text.replace(known, VEIL))
}

/// Prints one line of a live test.
pub fn say(text: &str) {
    println!("{}", veiled(text));
}

/// Prints one formatted line of a live test through [`say`].
macro_rules! say {
    ($($argument:tt)*) => {
        $crate::live_support::say(&format!($($argument)*))
    };
}

/// Replaces the panic hook with one that prints the message through [`veiled`].
pub fn install() {
    HOOK.call_once(|| {
        std::panic::set_hook(Box::new(|panic| {
            let payload = panic.payload();
            let text = payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "a value that is no text".to_owned());
            let place = panic.location().map_or_else(String::new, |place| {
                format!(" at {}:{}", place.file(), place.line())
            });
            eprintln!("{}", veiled(&format!("panicked{place}: {text}")));
        }));
    });
}

/// Starts a live test: installs the redacting panic hook and waits for the turn of the test.
pub async fn enter() -> MutexGuard<'static, ()> {
    install();
    LIVE.lock().await
}

const PROBE: &str = "LIVE_OUTPUT_PROBE";
const REGISTERED: i64 = 918_273_645;
const UNREGISTERED: i64 = 546_372_819;
const PRINT_PATHS: [(&str, bool); 7] = [
    ("print", true),
    ("panic", false),
    ("assert", false),
    ("expect", false),
    ("sweep", true),
    ("report", false),
    ("task", false),
];

fn refusal(account: i64) -> ConnectorError {
    let account = AccountId::new(account).expect("positive id");
    ServerError::new(ErrorCode::TradingNotAllowed)
        .with_description(format!("account {REGISTERED} may not trade"))
        .with_account_id(account)
        .into()
}

fn failed<E>(error: E) -> Result<(), E> {
    Err(error)
}

#[test]
fn output_probe() {
    let Ok(path) = std::env::var(PROBE) else {
        return;
    };
    install();
    veil(REGISTERED);
    let known = refusal(REGISTERED);
    let unknown = refusal(UNREGISTERED);
    match path.as_str() {
        "print" => say!("probe {known:?} {known} {unknown:?} {unknown:#?} account {REGISTERED}"),
        "panic" => panic!("probe {known:?} {known} {unknown:?} account {REGISTERED}"),
        "assert" => assert_eq!(Err(known), Ok(()), "probe {unknown:?} account {REGISTERED}"),
        "expect" => failed(known).expect("probe"),
        "sweep" => say!(
            "live sweep: cancel order 1 answered {:?} then {:?}",
            Err::<(), _>(known),
            Err::<(), _>(unknown)
        ),
        "report" => failed(format!("probe {known:?} {known} {unknown:?}")).expect("sweep"),
        "task" => std::thread::spawn(move || panic!("probe {known:?} {known} {unknown:?}"))
            .join()
            .expect("probe thread"),
        other => panic!("no print path named {other}"),
    }
}

#[test]
fn no_print_path_shows_an_account_identifier() {
    if std::env::var(PROBE).is_ok() {
        return;
    }
    install();
    let binary = std::env::current_exe().expect("path of the test binary");
    for (path, passes) in PRINT_PATHS {
        let output = Command::new(&binary)
            .args(["live_support::output_probe", "--exact", "--nocapture"])
            .env(PROBE, path)
            .output()
            .expect("run of the probe");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.success(), passes, "{path}: {text}");
        assert!(
            text.contains("probe") || text.contains("live sweep"),
            "{path}: {text}"
        );
        assert!(text.contains("TradingNotAllowed"), "{path}: {text}");
        assert!(text.contains(VEIL), "{path}: {text}");
        for identifier in [REGISTERED, UNREGISTERED] {
            assert!(
                !text.contains(&identifier.to_string()),
                "{path} shows an identifier: {text}"
            );
        }
    }
}

#[test]
fn live_tests_print_through_the_veil_only() {
    let sources = [
        ("open_api_live.rs", include_str!("../open_api_live.rs")),
        ("fix_live.rs", include_str!("../fix_live.rs")),
        (
            "fix_live_trading.rs",
            include_str!("../fix_live_trading.rs"),
        ),
    ];
    let bare = [
        ["print", "ln!("].concat(),
        ["eprint", "ln!("].concat(),
        ["print", "!("].concat(),
        ["eprint", "!("].concat(),
        ["dbg", "!("].concat(),
        ["stdout", "()"].concat(),
        ["stderr", "()"].concat(),
        ["set_", "hook"].concat(),
    ];
    let lock = ["Mutex::", "const_new"].concat();
    for (name, source) in sources {
        for pattern in &bare {
            assert!(!source.contains(pattern.as_str()), "{name} uses {pattern}");
        }
        assert!(
            source.contains("live_support::enter().await"),
            "{name} holds no live test"
        );
        assert!(
            !source.contains(lock.as_str()),
            "{name} takes a turn without the redacting panic hook"
        );
    }
}
