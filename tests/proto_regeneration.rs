//! Regenerates the Protobuf messages from the vendored proto files and compares the result with
//! the checked-in code.

use std::error::Error;
use std::fs;
use std::path::Path;

const PROTO_FILES: [&str; 4] = [
    "OpenApiCommonModelMessages.proto",
    "OpenApiCommonMessages.proto",
    "OpenApiModelMessages.proto",
    "OpenApiMessages.proto",
];

#[test]
fn checked_in_messages_are_reproduced_byte_for_byte() -> Result<(), Box<dyn Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join("proto_regeneration");
    fs::create_dir_all(&out)?;
    let descriptors = protox::compile(PROTO_FILES, [root.join("proto")])?;
    prost_build::Config::new()
        .out_dir(&out)
        .compile_fds(descriptors)?;
    let generated = fs::read(out.join("_.rs"))?;
    let checked_in = fs::read(root.join("src/open_api/messages.rs"))?;
    assert!(
        generated == checked_in,
        "src/open_api/messages.rs differs from the code generated into {}",
        out.display()
    );
    Ok(())
}
