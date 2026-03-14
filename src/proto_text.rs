//! Test support: reads enums and messages from the text of the vendored proto files.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

const PROTO_FILES: [&str; 4] = [
    "OpenApiCommonModelMessages.proto",
    "OpenApiCommonMessages.proto",
    "OpenApiModelMessages.proto",
    "OpenApiMessages.proto",
];

fn lines() -> Vec<String> {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    PROTO_FILES
        .iter()
        .flat_map(|file| {
            fs::read_to_string(directory.join(file))
                .expect("vendored proto file is readable")
                .lines()
                .map(|line| line.trim().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn block_name(line: &str, keyword: &str) -> Option<String> {
    line.strip_prefix(keyword)?
        .strip_suffix('{')
        .map(|name| name.trim().to_lowercase())
}

/// Returns the values of every enum, keyed by the lower-cased enum name.
pub(crate) fn enums() -> BTreeMap<String, Vec<(String, i32)>> {
    let mut enums: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
    let mut current = None;
    for line in lines() {
        if let Some(name) = block_name(&line, "enum ") {
            current = Some(name);
        } else if line == "}" {
            current = None;
        } else if let (Some(name), Some((value, number))) = (&current, line.split_once('=')) {
            let number = number
                .trim()
                .trim_end_matches(';')
                .parse()
                .expect("enum value has a number");
            enums
                .entry(name.clone())
                .or_default()
                .push((value.trim().to_owned(), number));
        }
    }
    enums
}

/// Returns the default payload type of every message, keyed by the lower-cased message name.
#[cfg(feature = "open-api")]
pub(crate) fn messages() -> BTreeMap<String, Option<String>> {
    let mut messages = BTreeMap::new();
    let mut current = None;
    for line in lines() {
        if let Some(name) = block_name(&line, "message ") {
            messages.insert(name.clone(), None);
            current = Some(name);
        } else if line == "}" {
            current = None;
        } else if let (Some(name), true) = (&current, line.contains(" payloadType = ")) {
            let default = line
                .split_once("[default = ")
                .and_then(|(_, rest)| rest.split_once(']'))
                .map(|(value, _)| value.to_owned());
            messages.insert(name.clone(), default);
        }
    }
    messages
}
