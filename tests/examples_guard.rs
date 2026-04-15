//! Tests of the guard that keeps the examples on a demo account, and of its use.
//!
//! The guard module of the examples is compiled into this test, so its own tests run here.
//! The scan below reads every other source of the examples and fails when one of them builds
//! a configuration or opens a connection without the guard.

use std::fs;
use std::path::{Path, PathBuf};

#[path = "../examples/support/guard.rs"]
#[allow(dead_code, unused_imports)]
mod guard;

const GUARD: &str = "support/guard.rs";

const CALLS: [&str; 6] = [
    "connect",
    "connect_with",
    "from_env",
    "from_lookup",
    "builder",
    "start",
];

const PATHS: [&str; 5] = [
    "ConfigurationOpenApi::",
    "ConfigurationFix::",
    "ConfigurationOpenApiBuilder",
    "ConfigurationFixBuilder",
    "Checked(",
];

fn is_word(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn calls(source: &str, name: &str) -> bool {
    source.match_indices(name).any(|(at, _)| {
        let before = source[..at].chars().next_back();
        let after = source[at + name.len()..].trim_start();
        !before.is_some_and(is_word) && (after.starts_with('(') || after.starts_with("::<"))
    })
}

fn bypasses(source: &str) -> Vec<&'static str> {
    let called = CALLS.into_iter().filter(|name| calls(source, name));
    let named = PATHS.into_iter().filter(|path| source.contains(path));
    called.chain(named).collect()
}

fn sources(directory: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            sources(&path, found)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
    Ok(())
}

fn examples() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples")
}

#[test]
fn the_scan_finds_every_way_around_the_guard() {
    for (source, found) in [
        ("OpenApiClient::connect(configuration).await", "connect"),
        ("FixClient::connect (configuration, kind)", "connect"),
        (
            "FixClient::connect_with(configuration, kind, Plain)",
            "connect_with",
        ),
        ("ConfigurationFix::from_env()?", "from_env"),
        ("Configuration::from_lookup(lookup)", "from_lookup"),
        ("Configuration::builder()", "builder"),
        ("builder::<T>()", "builder"),
        ("session::start(connector, configuration, kind)", "start"),
        ("ConfigurationOpenApi::demo()", "ConfigurationOpenApi::"),
        ("ConfigurationOpenApi::live()", "ConfigurationOpenApi::"),
        ("ConfigurationFix::builder", "ConfigurationFix::"),
        ("let b: ConfigurationFixBuilder;", "ConfigurationFixBuilder"),
        (
            "let b: ConfigurationOpenApiBuilder;",
            "ConfigurationOpenApiBuilder",
        ),
        ("guard::Checked(configuration)", "Checked("),
    ] {
        assert!(bypasses(source).contains(&found), "{source}");
    }
}

#[test]
fn the_scan_accepts_what_goes_through_the_guard() {
    for source in [
        "quote.force_reconnect().await?",
        "let quote = session(SessionKind::Quote, Access::ReadOnly).await?;",
        "//! cargo run --example open_api_connect --features open-api",
        "fn held(checked: &Checked<ConfigurationOpenApi>) {}",
        "println!(\"connected to the {environment} environment\");",
        "let restart = 1;",
    ] {
        assert_eq!(bypasses(source), Vec::<&str>::new(), "{source}");
    }
}

#[test]
fn no_example_builds_a_configuration_or_connects_without_the_guard() {
    let root = examples();
    let mut found = Vec::new();
    assert!(sources(&root, &mut found).is_ok());
    let guard = root.join(GUARD);
    assert!(found.contains(&guard));
    let others: Vec<_> = found.iter().filter(|path| **path != guard).collect();
    assert!(others.len() >= 22, "{}", others.len());
    for path in others {
        let source = fs::read_to_string(path).unwrap_or_default();
        assert!(!source.is_empty(), "{}", path.display());
        assert_eq!(bypasses(&source), Vec::<&str>::new(), "{}", path.display());
    }
}

#[test]
fn every_example_takes_its_client_from_the_support_of_its_protocol() {
    let root = examples();
    for (directory, needle) in [
        ("open_api", "support::open_api::"),
        ("fix", "support::fix::"),
    ] {
        let mut found = Vec::new();
        assert!(sources(&root.join(directory), &mut found).is_ok());
        assert!(!found.is_empty());
        for path in found {
            let source = fs::read_to_string(&path).unwrap_or_default();
            assert!(source.contains(needle), "{}", path.display());
        }
    }
}

#[test]
fn the_support_reaches_the_server_through_the_guard_only() {
    let root = examples().join("support");
    for (file, needles) in [
        (
            "open_api.rs",
            ["guard::open_api(", "guard::open_api_client("],
        ),
        ("fix.rs", ["guard::fix(", "guard::fix_client("]),
    ] {
        let source = fs::read_to_string(root.join(file)).unwrap_or_default();
        for needle in needles {
            assert!(source.contains(needle), "{file}: {needle}");
        }
    }
}
