//! Tests of the guard that keeps the examples on a demo account, and of its use.
//!
//! The guard module of the examples is compiled into this test, so its own tests run here.
//! The scan below reads every other source of the examples: it walks every `.rs` file under
//! `examples/`. The manifest turns the discovery of examples off, and a file outside
//! `examples/support` that is not the file of an `[[example]]` with an access level pinned
//! in [`LEVELS`] fails the test. The scan works on the words of the code, without the
//! comments, and holds an example to an allowlist:
//!
//! - of this crate an example names only the items of [`ITEMS`], and only in a `use` that
//!   neither renames nor globs; outside a `use` the name of the crate does not occur;
//! - of the guard an example names only `support::guard::Access`, in a `use`;
//! - the words of [`REFUSED`] occur nowhere: what builds a configuration, opens a connection
//!   or a transport, names a host, exchanges a token pair or changes the environment;
//! - the only module is `support`, taken from `../support/mod.rs`;
//! - an example pinned read-only names none of the words of [`REFUSED_IN_READ_ONLY`]: what
//!   places, changes, cancels or closes an order or a position.
//!
//! The support modules next to the guard are held to the shorter list [`REFUSED_IN_SUPPORT`].

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use guard::Access;

#[path = "../examples/support/guard.rs"]
#[allow(dead_code, unused_imports)]
mod guard;

const GUARD: &str = "support/guard.rs";
const SUPPORT: &str = "../support/mod.rs";
const CRATE: &str = "ctrader_connector";
const ACCESS: &str = "support::guard::Access";

/// Items of the crate an example may import.
const ITEMS: [&str; 35] = [
    "common::config::ENV_ACCESS_TOKEN",
    "common::config::ENV_REFRESH_TOKEN",
    "common::config::Environment",
    "common::decimals::Price",
    "common::decimals::Volume",
    "common::errors::ConnectorError",
    "common::ids::AccountId",
    "common::time::TimeRange",
    "common::time::Timestamp",
    "fix::amend::Replacement",
    "fix::client::FixClient",
    "fix::messages::Side",
    "fix::quotes::BookEntry",
    "fix::securities::Security",
    "fix::session::Event",
    "fix::session::SessionKind",
    "fix::trading::NewOrder",
    "open_api::client::OpenApiClient",
    "open_api::depth::DepthQuote",
    "open_api::executions::TradeEvent",
    "open_api::quotes::QuoteSide",
    "open_api::symbols::Symbol",
    "open_api::symbols::SymbolsParams",
    "open_api::ticks::TickDataParams",
    "open_api::trading::Order",
    "open_api::trading::OrderAmendment",
    "open_api::trading::Position",
    "open_api::trading::PositionAmendment",
    "open_api::trading::ExecutionType",
    "open_api::trading::Label",
    "open_api::trading::NewOrder",
    "open_api::trading::TradeSide",
    "open_api::trendbars::Period",
    "open_api::trendbars::Trendbar",
    "open_api::trendbars::TrendbarsParams",
];

/// Words that occur in no example outside a `use`, and in none of its imports either.
const REFUSED: [&str; 28] = [
    CRATE,
    "guard",
    "connect",
    "connect_with",
    "connect_timeout",
    "from_env",
    "from_lookup",
    "builder",
    "start",
    "ConfigurationOpenApi",
    "ConfigurationFix",
    "ConfigurationOpenApiBuilder",
    "ConfigurationFixBuilder",
    "Checked",
    "Connection",
    "ConnectionOptions",
    "Connect",
    "Tls",
    "Plain",
    "DEMO_HOST",
    "LIVE_HOST",
    "refresh_token",
    "refresh_token_over_http",
    "refresh_token_over_http_with",
    "refresh_over_http",
    "set_var",
    "remove_var",
    "include",
];

/// Words that occur in no support module but the guard.
const REFUSED_IN_SUPPORT: [&str; 14] = [
    "connect",
    "connect_with",
    "from_env",
    "from_lookup",
    "builder",
    "start",
    "Connection",
    "ConnectionOptions",
    "Connect",
    "Tls",
    "Plain",
    "LIVE_HOST",
    "set_var",
    "remove_var",
];

/// Words that occur nowhere in an example pinned [`Access::ReadOnly`]: the methods of the
/// clients that place, change, cancel or close an order or a position, and the values those
/// methods take. The library has no call that cancels all orders at once, and what rotates
/// the token pair is in [`REFUSED`] for every example.
const REFUSED_IN_READ_ONLY: [&str; 12] = [
    "place",
    "replace",
    "cancel",
    "place_order",
    "amend_order",
    "amend_position",
    "cancel_order",
    "close_position",
    "NewOrder",
    "Replacement",
    "OrderAmendment",
    "PositionAmendment",
];

/// Access every example asks the guard for; an example that places an order asks for
/// [`Access::Trading`].
const LEVELS: [(&str, Access); 19] = [
    ("fix_depth", Access::ReadOnly),
    ("fix_orders", Access::Trading),
    ("fix_reconnect", Access::ReadOnly),
    ("fix_reports", Access::Trading),
    ("fix_securities", Access::ReadOnly),
    ("fix_spot", Access::ReadOnly),
    ("open_api_connect", Access::ReadOnly),
    ("open_api_depth", Access::ReadOnly),
    ("open_api_executions", Access::Trading),
    ("open_api_history", Access::ReadOnly),
    ("open_api_margin", Access::ReadOnly),
    ("open_api_quotes", Access::ReadOnly),
    ("open_api_reconcile", Access::ReadOnly),
    ("open_api_symbols", Access::ReadOnly),
    ("open_api_ticks", Access::ReadOnly),
    ("open_api_token_refresh", Access::ReadOnly),
    ("open_api_trader", Access::ReadOnly),
    ("open_api_trading", Access::Trading),
    ("open_api_trendbars", Access::ReadOnly),
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    Mark(char),
    Text(String),
}

fn word(text: &str) -> Token {
    Token::Word(text.to_owned())
}

fn is_word(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn run(chars: &[char], from: usize, wanted: impl Fn(char) -> bool) -> usize {
    let length = chars
        .get(from..)
        .unwrap_or(&[])
        .iter()
        .take_while(|character| wanted(**character))
        .count();
    from + length
}

fn quoted(chars: &[char], mut at: usize, hashes: Option<usize>) -> (String, usize) {
    let mut text = String::new();
    while let Some(character) = chars.get(at).copied() {
        at += 1;
        match hashes {
            None if character == '\\' => {
                text.push(character);
                text.extend(chars.get(at));
                at += 1;
            }
            None if character == '"' => break,
            Some(hashes) if character == '"' && run(chars, at, |c| c == '#') - at >= hashes => {
                at += hashes;
                break;
            }
            _ => text.push(character),
        }
    }
    (text, at)
}

fn comment(chars: &[char], mut at: usize) -> usize {
    let mut depth = 0_usize;
    loop {
        match (chars.get(at), chars.get(at + 1)) {
            (Some('/'), Some('*')) => {
                depth += 1;
                at += 2;
            }
            (Some('*'), Some('/')) => {
                depth = depth.saturating_sub(1);
                at += 2;
                if depth == 0 {
                    return at;
                }
            }
            (Some(_), _) => at += 1,
            (None, _) => return at,
        }
    }
}

fn character_or_lifetime(chars: &[char], at: usize) -> usize {
    if chars.get(at + 1) == Some(&'\\') {
        return run(chars, at + 3, |c| c != '\'') + 1;
    }
    if chars.get(at + 2) == Some(&'\'') {
        return at + 3;
    }
    at + 1
}

/// Splits code into its words, its string literals and its other characters; comments,
/// numbers and character literals are left out and a raw identifier counts as its word.
fn tokens(source: &str) -> Vec<Token> {
    let chars: Vec<char> = source.chars().collect();
    let mut found = Vec::new();
    let mut at = 0;
    while let Some(character) = chars.get(at).copied() {
        let next = chars.get(at + 1).copied();
        if character == '/' && next == Some('/') {
            at = run(&chars, at, |c| c != '\n');
        } else if character == '/' && next == Some('*') {
            at = comment(&chars, at);
        } else if character == '"' {
            let (text, end) = quoted(&chars, at + 1, None);
            found.push(Token::Text(text));
            at = end;
        } else if character == '\'' {
            at = character_or_lifetime(&chars, at);
        } else if character.is_ascii_digit() {
            at = run(&chars, at, is_word);
        } else if is_word(character) {
            let end = run(&chars, at, is_word);
            let name: String = chars.get(at..end).unwrap_or(&[]).iter().collect();
            let hashes = run(&chars, end, |c| c == '#');
            let after = chars.get(hashes).copied();
            let raw = matches!(name.as_str(), "r" | "br" | "cr");
            if raw && after == Some('"') {
                let (text, end) = quoted(&chars, hashes + 1, Some(hashes - end));
                found.push(Token::Text(text));
                at = end;
            } else if name == "r" && hashes == end + 1 && after.is_some_and(is_word) {
                at = hashes;
            } else if matches!(name.as_str(), "b" | "c") && matches!(after, Some('"' | '\'')) {
                at = end;
            } else {
                found.push(Token::Word(name));
                at = end;
            }
        } else {
            if !character.is_whitespace() {
                found.push(Token::Mark(character));
            }
            at += 1;
        }
    }
    found
}

fn tree(
    tokens: &[Token],
    at: &mut usize,
    prefix: &[String],
    leaves: &mut Vec<String>,
    faults: &mut Vec<String>,
) {
    let mut path = prefix.to_vec();
    loop {
        match tokens.get(*at) {
            None | Some(Token::Mark(',' | '}')) => break,
            Some(Token::Mark(':')) => *at += 1,
            Some(Token::Mark('{')) => {
                *at += 1;
                loop {
                    match tokens.get(*at) {
                        None => faults.push("use that does not close".to_owned()),
                        Some(Token::Mark('}')) => *at += 1,
                        Some(Token::Mark(',')) => {
                            *at += 1;
                            continue;
                        }
                        Some(_) => {
                            tree(tokens, at, &path, leaves, faults);
                            continue;
                        }
                    }
                    return;
                }
            }
            Some(Token::Word(name)) if name == "as" => {
                faults.push(format!("use that renames {}", path.join("::")));
                *at += 2;
            }
            Some(Token::Word(name)) => {
                path.push(name.clone());
                *at += 1;
            }
            Some(Token::Mark('*')) => {
                faults.push(format!("use of everything in {}", path.join("::")));
                path.push("*".to_owned());
                *at += 1;
            }
            Some(_) => {
                faults.push("use that is no path".to_owned());
                *at += 1;
            }
        }
    }
    leaves.push(path.join("::"));
}

/// Returns the paths the `use` declarations import, what is wrong with the declarations
/// themselves, and the tokens outside them.
fn imports(tokens: &[Token]) -> (Vec<String>, Vec<String>, Vec<Token>) {
    let (mut leaves, mut faults, mut rest) = (Vec::new(), Vec::new(), Vec::new());
    let mut at = 0;
    while let Some(token) = tokens.get(at) {
        at += 1;
        if *token != word("use") || tokens.get(at) == Some(&Token::Mark('<')) {
            rest.push(token.clone());
            continue;
        }
        let length = tokens
            .get(at..)
            .unwrap_or(&[])
            .iter()
            .take_while(|token| **token != Token::Mark(';'))
            .count();
        let statement = tokens.get(at..at + length).unwrap_or(&[]);
        let mut inside = 0;
        while inside < statement.len() {
            tree(statement, &mut inside, &[], &mut leaves, &mut faults);
            inside += 1;
        }
        at += length + 1;
    }
    (leaves, faults, rest)
}

fn refused_import(leaf: &str) -> Option<String> {
    let segments: Vec<&str> = leaf.split("::").collect();
    let named = |name: &str| segments.contains(&name);
    let allowed = match segments.split_first() {
        Some((&CRATE, item)) => ITEMS.contains(&item.join("::").as_str()),
        _ if named("guard") => leaf == ACCESS,
        _ => !REFUSED.into_iter().any(named),
    };
    (!allowed).then(|| format!("use of {leaf}"))
}

fn refused_words(rest: &[Token], refused: &[&str]) -> Vec<String> {
    rest.iter()
        .filter_map(|token| match token {
            Token::Word(name) if refused.contains(&name.as_str()) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn refused_modules(rest: &[Token]) -> Vec<String> {
    let mut faults = Vec::new();
    for (at, token) in rest.iter().enumerate() {
        let after = (rest.get(at + 1), rest.get(at + 2));
        if *token == word("mod") && after != (Some(&word("support")), Some(&Token::Mark(';'))) {
            faults.push("module other than support".to_owned());
        }
        if let (Token::Word(name), (Some(Token::Mark('=')), Some(Token::Text(file)))) =
            (token, after)
        {
            if name == "path" && file != SUPPORT {
                faults.push(format!("module taken from {file}"));
            }
        }
    }
    faults
}

/// Tells what in the source of an example goes around the guard; nothing when it is empty.
fn violations(source: &str) -> Vec<String> {
    let (leaves, mut faults, rest) = imports(&tokens(source));
    faults.extend(leaves.iter().filter_map(|leaf| refused_import(leaf)));
    faults.extend(refused_words(&rest, &REFUSED));
    faults.extend(refused_modules(&rest));
    faults
}

/// Tells what in the source of an example pinned read-only changes an order or a position.
fn read_only_violations(source: &str) -> Vec<String> {
    refused_words(&tokens(source), &REFUSED_IN_READ_ONLY)
}

/// Tells what in the source of a support module goes around the guard.
fn support_violations(source: &str) -> Vec<String> {
    let all = tokens(source);
    let mut faults = refused_words(&all, &REFUSED_IN_SUPPORT);
    for pair in all.windows(2) {
        let built = match pair {
            [Token::Word(name), Token::Mark('(')] => name == "Checked",
            [Token::Word(name), Token::Mark(':')] => {
                name == "ConfigurationOpenApi" || name == "ConfigurationFix"
            }
            _ => false,
        };
        if let (true, Some(Token::Word(name))) = (built, pair.first()) {
            faults.push(name.clone());
        }
    }
    faults
}

/// Returns the access the source asks the guard for: the highest one it names.
fn access(source: &str) -> Option<Access> {
    let all = tokens(source);
    let colon = Token::Mark(':');
    let named = |level: &str| {
        all.windows(4)
            .any(|four| four == [word("Access"), colon.clone(), colon.clone(), word(level)])
    };
    if named("Trading") {
        Some(Access::Trading)
    } else if named("ReadOnly") {
        Some(Access::ReadOnly)
    } else {
        None
    }
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

fn manifest() -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    fs::read_to_string(manifest).unwrap_or_default()
}

/// Returns the path of every `[[example]]` of the manifest under the name of its target.
fn targets_of(manifest: &str) -> BTreeMap<String, String> {
    let value = |table: &str, key: &str| {
        let line = |line: &str| {
            let quoted = line.strip_prefix(key)?.trim_start().strip_prefix('=')?;
            let text = quoted.trim().strip_prefix('"')?.strip_suffix('"')?;
            Some(text.to_owned())
        };
        table.lines().find_map(line).unwrap_or_default()
    };
    manifest
        .split("[[example]]")
        .skip(1)
        .map(|rest| rest.split("\n[").next().unwrap_or_default())
        .map(|table| (value(table, "name"), value(table, "path")))
        .collect()
}

/// Returns the source of every example under `root` under the name of its target, or what
/// is wrong with the tree: every `.rs` file outside `support`, at any depth, has to be the
/// file of one of `targets`, and every target has to have its file.
fn programs_under(
    root: &Path,
    targets: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, Vec<String>> {
    let mut found = Vec::new();
    assert!(sources(root, &mut found).is_ok());
    let (mut programs, mut faults) = (BTreeMap::new(), Vec::new());
    for path in found {
        let parts: Vec<_> = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .iter()
            .map(|part| part.to_string_lossy())
            .collect();
        if parts.len() > 1 && parts.first().is_some_and(|part| part == "support") {
            continue;
        }
        let file = format!("examples/{}", parts.join("/"));
        let target = targets
            .iter()
            .find(|(name, path)| **path == file && !programs.contains_key(*name));
        let source = fs::read_to_string(&path).unwrap_or_default();
        match target {
            Some((name, _)) if !source.is_empty() => {
                programs.insert(name.clone(), source);
            }
            Some(_) => faults.push(format!("empty file {file}")),
            None => faults.push(format!("unlisted file {file}")),
        }
    }
    let missing = targets
        .iter()
        .filter(|(name, _)| !programs.contains_key(*name))
        .map(|(name, path)| format!("no file {path} of {name}"));
    faults.extend(missing);
    faults.sort();
    if faults.is_empty() {
        Ok(programs)
    } else {
        Err(faults)
    }
}

/// Returns the source of every example under the name its target has.
fn programs() -> BTreeMap<String, String> {
    let programs = programs_under(&examples(), &targets_of(&manifest()));
    assert_eq!(programs.as_ref().err(), None);
    programs.unwrap_or_default()
}

/// Returns the access pinned for the example `name`.
fn pinned(name: &str) -> Option<Access> {
    LEVELS
        .into_iter()
        .find_map(|(pinned, level)| (pinned == name).then_some(level))
}

const HEAD: &str = "#[path = \"../support/mod.rs\"]\nmod support;\nuse support::Failure;\n";

/// An example that renames the configuration and binds the function that connects.
const RENAMED: &str = "
use ctrader_connector::common::config::ConfigurationOpenApi as C;
#[tokio::main]
async fn main() -> Result<(), Failure> {
    let go = ctrader_connector::open_api::client::OpenApiClient::connect;
    let client = go(C::live().client_id(\"id\").client_secret(\"s\").access_token(\"t\").build()?);
    client.await?.shutdown().await;
    Ok(())
}
";

/// An example that opens the connection itself, from its parts.
const BUILT_BY_HAND: &str = "
use ctrader_connector::common::config::LIVE_HOST;
use ctrader_connector::common::transport::Tls;
use ctrader_connector::open_api::connection::{Connection, ConnectionOptions};
#[tokio::main]
async fn main() -> Result<(), Failure> {
    let options = ConnectionOptions::new();
    let connection = Connection::open(Tls::new()?, LIVE_HOST, 5035, options).await?;
    connection.close().await;
    Ok(())
}
";

fn refuses(source: &str, fault: &str) -> bool {
    violations(source).iter().any(|found| found == fault)
}

#[test]
fn the_scan_reads_code_and_neither_comments_nor_the_inside_of_literals() {
    let source = "// connect\n/* builder /* start */ Tls */ let a = 'x'; let b: &'static str = \
                  \"connect\\\" Tls\"; r#\"start \" \"# b\"Tls\" r#connect 1u8 b'c' c\"Tls\"";
    let expected = [
        word("let"),
        word("a"),
        Token::Mark('='),
        Token::Mark(';'),
        word("let"),
        word("b"),
        Token::Mark(':'),
        Token::Mark('&'),
        word("static"),
        word("str"),
        Token::Mark('='),
        Token::Text("connect\\\" Tls".to_owned()),
        Token::Mark(';'),
        Token::Text("start \" ".to_owned()),
        Token::Text("Tls".to_owned()),
        word("connect"),
        Token::Text("Tls".to_owned()),
    ];
    assert_eq!(tokens(source), expected);
    for open in [
        "/* connect",
        "\"connect",
        "r#\"connect",
        "'\\",
        "use {a",
        "use",
    ] {
        assert!(violations(open).iter().all(|fault| fault != "connect"));
    }
}

#[test]
fn the_scan_expands_the_imports_of_a_use() {
    let source = "use a::{b::{self, C}, d as e, f::*}; use ::g; fn h() -> impl I + use<> {}";
    let (leaves, faults, rest) = imports(&tokens(source));
    assert_eq!(leaves, ["a::b::self", "a::b::C", "a::d", "a::f::*", "g"]);
    assert_eq!(
        faults,
        ["use that renames a::d", "use of everything in a::f"]
    );
    assert_eq!(rest.first(), Some(&word("fn")));
    assert!(rest.contains(&word("use")));
}

#[test]
fn the_scan_refuses_both_known_ways_around_it() {
    let renamed = violations(&format!("{HEAD}{RENAMED}"));
    for fault in [
        "use that renames ctrader_connector::common::config::ConfigurationOpenApi",
        "use of ctrader_connector::common::config::ConfigurationOpenApi",
        CRATE,
        "connect",
    ] {
        assert!(renamed.iter().any(|found| found == fault), "{fault}");
    }
    let built = violations(&format!("{HEAD}{BUILT_BY_HAND}"));
    for fault in [
        "use of ctrader_connector::common::config::LIVE_HOST",
        "use of ctrader_connector::common::transport::Tls",
        "use of ctrader_connector::open_api::connection::Connection",
        "use of ctrader_connector::open_api::connection::ConnectionOptions",
        "Connection",
        "ConnectionOptions",
        "Tls",
        "LIVE_HOST",
    ] {
        assert!(built.iter().any(|found| found == fault), "{fault}");
    }
    assert_eq!(violations(HEAD), Vec::<String>::new());
}

#[test]
fn the_scan_finds_every_way_around_the_guard() {
    for (source, fault) in [
        ("OpenApiClient::connect(configuration).await", "connect"),
        ("FixClient::connect (configuration, kind)", "connect"),
        ("let go = FixClient::connect;", "connect"),
        ("OpenApiClient::r#connect(configuration)", "connect"),
        ("FixClient::connect_with(c, kind, Plain)", "connect_with"),
        ("FixClient::connect_with(c, kind, Plain)", "Plain"),
        ("client.connect_timeout(limit)", "connect_timeout"),
        ("ConfigurationFix::from_env()?", "from_env"),
        ("Configuration::from_lookup(lookup)", "from_lookup"),
        ("Configuration::builder()", "builder"),
        ("builder::<T>()", "builder"),
        ("session::start(connector, configuration, kind)", "start"),
        ("ConfigurationOpenApi::demo()", "ConfigurationOpenApi"),
        ("fn held(c: &ConfigurationFix) {}", "ConfigurationFix"),
        ("let b: ConfigurationFixBuilder;", "ConfigurationFixBuilder"),
        (
            "let b: ConfigurationOpenApiBuilder;",
            "ConfigurationOpenApiBuilder",
        ),
        ("support::guard::Checked(configuration)", "Checked"),
        ("support::guard::open_api_from(lookup, access)", "guard"),
        ("fn connector<C: Connect>(c: C) {}", "Connect"),
        ("let host = DEMO_HOST;", "DEMO_HOST"),
        ("client.refresh_token().await?", "refresh_token"),
        (
            "client.refresh_token_over_http().await?",
            "refresh_token_over_http",
        ),
        (
            "client.refresh_token_over_http_with(&c).await?",
            "refresh_token_over_http_with",
        ),
        (
            "refresh_over_http(&c, &configuration, &t)",
            "refresh_over_http",
        ),
        ("std::env::set_var(ENV, \"yes\")", "set_var"),
        ("std::env::remove_var(ENV)", "remove_var"),
        ("include!(\"../support/guard.rs\");", "include"),
        (
            "type Client = ctrader_connector::fix::client::FixClient;",
            CRATE,
        ),
        ("extern crate ctrader_connector as library;", CRATE),
        ("m!(ctrader_connector::common::config::LIVE_HOST)", CRATE),
        ("use ctrader_connector;", "use of ctrader_connector"),
        (
            "use ctrader_connector::fix::client::*;",
            "use of everything in ctrader_connector::fix::client",
        ),
        (
            "use ctrader_connector::fix::client::FixClient as Client;",
            "use that renames ctrader_connector::fix::client::FixClient",
        ),
        (
            "use ::ctrader_connector::common::transport::{Connect, Tls};",
            "use of ctrader_connector::common::transport::Tls",
        ),
        (
            "use ctrader_connector::{fix::client::FixClient, common::config::ConfigurationFix};",
            "use of ctrader_connector::common::config::ConfigurationFix",
        ),
        (
            "use ctrader_connector::open_api::client;",
            "use of ctrader_connector::open_api::client",
        ),
        ("use support::guard;", "use of support::guard"),
        (
            "use support::guard::{self, Access};",
            "use of support::guard::self",
        ),
        (
            "use support::guard::open_api_from;",
            "use of support::guard::open_api_from",
        ),
        ("use support::guard::*;", "use of support::guard::*"),
        (
            "use support::guard::Access as Level;",
            "use that renames support::guard::Access",
        ),
        ("use std::env::set_var;", "use of std::env::set_var"),
        ("mod other;", "module other than support"),
        ("mod support { }", "module other than support"),
        (
            "#[path = \"../support/guard.rs\"] mod support;",
            "module taken from ../support/guard.rs",
        ),
        (
            "#[cfg_attr(all(), path = \"../../src/lib.rs\")] mod support;",
            "module taken from ../../src/lib.rs",
        ),
    ] {
        assert!(refuses(source, fault), "{source}: {:?}", violations(source));
    }
}

#[test]
fn the_scan_accepts_what_goes_through_the_guard() {
    for source in [
        HEAD,
        "quote.force_reconnect().await?",
        "let quote = session(SessionKind::Quote, Access::ReadOnly).await?;",
        "//! cargo run --example open_api_connect --features open-api",
        "println!(\"connected to the {environment} environment\");",
        "let restart = 1;",
        "let path = std::env::var(ENV_FILE);",
        "use support::guard::Access;",
        "use support::open_api::{account, configuration, open, refreshable, renewed, rotate};",
        "use ctrader_connector::common::config::{ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN};",
        "use ctrader_connector::fix::session::{Event, SessionKind};",
        "use std::time::Duration;",
        "async fn list(client: &OpenApiClient, account: AccountId) -> Result<(), Failure> {}",
    ] {
        assert_eq!(violations(source), Vec::<String>::new(), "{source}");
    }
}

#[test]
fn no_example_reaches_past_the_allowlist() {
    let programs = programs();
    assert_eq!(programs.len(), 19);
    for (name, source) in &programs {
        assert_eq!(violations(source), Vec::<String>::new(), "{name}");
    }
    let imported: Vec<String> = programs
        .values()
        .flat_map(|source| imports(&tokens(source)).0)
        .filter_map(|leaf| leaf.strip_prefix("ctrader_connector::").map(str::to_owned))
        .collect();
    for item in ITEMS {
        assert!(imported.iter().any(|leaf| leaf == item), "{item}");
    }
}

#[test]
fn the_scan_refuses_a_file_that_is_no_pinned_example() {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join("examples_guard_tree");
    let _ = fs::remove_dir_all(&root);
    let source = "OpenApiClient::connect(ConfigurationOpenApi::from_env()?)";
    let listed = ["fix/spot.rs", "support/mod.rs", "support/deep/more.rs"];
    let unlisted = [
        "quick.rs",
        "misc/main.rs",
        "fix/deep/other.rs",
        "support.rs",
    ];
    for file in listed.into_iter().chain(unlisted) {
        let path = root.join(file);
        assert!(fs::create_dir_all(path.parent().unwrap_or(&root)).is_ok());
        assert!(fs::write(path, source).is_ok());
    }
    assert!(fs::write(root.join("notes.txt"), source).is_ok());
    let spot = ("fix_spot".to_owned(), "examples/fix/spot.rs".to_owned());
    let gone = ("fix_gone".to_owned(), "examples/fix/gone.rs".to_owned());
    let targets = BTreeMap::from([spot.clone(), gone]);
    let faults = [
        "no file examples/fix/gone.rs of fix_gone",
        "unlisted file examples/fix/deep/other.rs",
        "unlisted file examples/misc/main.rs",
        "unlisted file examples/quick.rs",
        "unlisted file examples/support.rs",
    ];
    assert_eq!(
        programs_under(&root, &targets),
        Err(faults.map(str::to_owned).to_vec())
    );
    for file in unlisted {
        assert!(fs::remove_file(root.join(file)).is_ok());
    }
    let programs = programs_under(&root, &BTreeMap::from([spot]));
    assert_eq!(
        programs,
        Ok(BTreeMap::from([("fix_spot".to_owned(), source.to_owned())]))
    );
    assert!(fs::remove_dir_all(&root).is_ok());
    let manifest = "[package]\nname = \"a\"\n\n[[example]]\nname = \"b\"\npath = \"examples/b.rs\"\n\
                    required-features = [\"c\"]\n\n[[example]]\nname = \"d\"\n\n[[test]]\n\
                    path = \"tests/e.rs\"\n";
    let expected = [("b", "examples/b.rs"), ("d", "")];
    let expected = expected.map(|(name, path)| (name.to_owned(), path.to_owned()));
    assert_eq!(targets_of(manifest), BTreeMap::from(expected));
}

#[test]
fn no_example_pinned_read_only_changes_an_order_or_a_position() {
    let programs = programs();
    let mut read_only = 0;
    for (name, source) in &programs {
        if pinned(name) == Some(Access::ReadOnly) {
            read_only += 1;
            assert_eq!(read_only_violations(source), Vec::<String>::new(), "{name}");
        }
    }
    assert_eq!(read_only, 15);
    let quotes = programs.get("open_api_quotes").cloned().unwrap_or_default();
    assert_eq!(pinned("open_api_quotes"), Some(Access::ReadOnly));
    assert!(!quotes.is_empty());
    let gained = format!(
        "{quotes}\nasync fn buy(client: &OpenApiClient, order: &Order) {{\n    let _ = \
         client.place_order(order).await;\n}}\n"
    );
    assert_eq!(read_only_violations(&gained), ["place_order"]);
    for (source, fault) in [
        ("trade.place(order).await?", "place"),
        ("trade.replace(&id, change).await?", "replace"),
        ("trade.cancel(&id).await?", "cancel"),
        ("client.place_order(&order).await?", "place_order"),
        ("client.amend_order(&change).await?", "amend_order"),
        ("client.amend_position(&change).await?", "amend_position"),
        ("client.cancel_order(account, id).await?", "cancel_order"),
        (
            "client.close_position(a, &p, v, &s).await?",
            "close_position",
        ),
        ("use ctrader_connector::fix::trading::NewOrder;", "NewOrder"),
        ("let change = Replacement::of(&report);", "Replacement"),
        ("OrderAmendment::of(account, &order)", "OrderAmendment"),
        (
            "PositionAmendment::of(account, &position)",
            "PositionAmendment",
        ),
    ] {
        assert_eq!(read_only_violations(source), [fault], "{source}");
    }
    let reads = "// place_order\nlet orders = client.reconcile(account).await?.orders;";
    assert_eq!(read_only_violations(reads), Vec::<String>::new());
}

#[test]
fn every_example_asks_for_the_access_pinned_here() {
    let asked: Vec<(String, Option<Access>)> = programs()
        .iter()
        .map(|(name, source)| (name.clone(), access(source)))
        .collect();
    let pinned: Vec<(String, Option<Access>)> = LEVELS
        .into_iter()
        .map(|(name, level)| (name.to_owned(), Some(level)))
        .collect();
    assert_eq!(asked, pinned);
    let manifest = manifest();
    assert!(manifest.lines().any(|line| line == "autoexamples = false"));
    assert_eq!(manifest.matches("[[example]]").count(), LEVELS.len());
    let targets = targets_of(&manifest);
    let names: Vec<&str> = targets.keys().map(String::as_str).collect();
    assert_eq!(names, LEVELS.map(|(name, _)| name));
    assert_eq!(
        access("session(Access::ReadOnly); sweep(Access::Trading)"),
        Some(Access::Trading)
    );
    assert_eq!(
        access("session(Access :: ReadOnly)"),
        Some(Access::ReadOnly)
    );
    assert_eq!(access("// Access::Trading\nsession(level)"), None);
}

#[test]
fn no_support_module_builds_a_configuration_or_connects_without_the_guard() {
    let root = examples();
    let mut found = Vec::new();
    assert!(sources(&root.join("support"), &mut found).is_ok());
    let guard = root.join(GUARD);
    assert!(found.contains(&guard));
    let others: Vec<_> = found.iter().filter(|path| **path != guard).collect();
    assert_eq!(others.len(), 3);
    for path in others {
        let source = fs::read_to_string(path).unwrap_or_default();
        assert!(!source.is_empty(), "{}", path.display());
        let faults = support_violations(&source);
        assert_eq!(faults, Vec::<String>::new(), "{}", path.display());
    }
    for (source, fault) in [
        ("OpenApiClient::connect(configuration).await", "connect"),
        ("ConfigurationFix::from_env()?", "from_env"),
        ("ConfigurationFix::from_env()?", "ConfigurationFix"),
        ("ConfigurationOpenApi::live()", "ConfigurationOpenApi"),
        ("Ok(Checked(configuration))", "Checked"),
        (
            "Connection::open(Tls::new()?, LIVE_HOST, port, options)",
            "Tls",
        ),
    ] {
        let faults = support_violations(source);
        assert!(faults.iter().any(|found| found == fault), "{source}");
    }
    let held = "fn held(checked: &Checked<ConfigurationOpenApi>) -> bool {}";
    assert_eq!(support_violations(held), Vec::<String>::new());
}

#[test]
fn every_example_takes_its_client_from_the_support_of_its_protocol() {
    for (name, source) in &programs() {
        let needle = if name.starts_with("open_api_") {
            "support::open_api::"
        } else {
            assert!(name.starts_with("fix_"), "{name}");
            "support::fix::"
        };
        assert!(source.contains(needle), "{name}");
    }
}

#[test]
fn the_support_reaches_the_server_through_the_guard_only() {
    let root = examples().join("support");
    for (file, needles) in [
        (
            "open_api.rs",
            &[
                "guard::open_api(",
                "guard::open_api_client(",
                "guard::open_api_rotation(checked)?;\n    Ok(client.refresh_token().await?)",
            ][..],
        ),
        ("fix.rs", &["guard::fix(", "guard::fix_client("][..]),
    ] {
        let source = fs::read_to_string(root.join(file)).unwrap_or_default();
        for needle in needles {
            assert!(source.contains(needle), "{file}: {needle}");
        }
        assert!(source.matches(".refresh_token()").count() <= 2, "{file}");
    }
}
