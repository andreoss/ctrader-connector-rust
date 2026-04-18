//! Exchanges the refresh token for a new token pair, only when asked to.
//!
//! A refresh rotates the pair: the old access and refresh tokens stop working at once, in
//! every program that still holds them. Without the option the example therefore only checks
//! that the current access token authorises the account, reports whether a refresh token is
//! configured and changes nothing. With `--rotate`, or with
//! `BROKER_EXAMPLE_ROTATE=1`, it refreshes the pair, writes the new one into the
//! `export BROKER_ACCESS_TOKEN=` and `export BROKER_REFRESH_TOKEN=` lines of the file named by
//! `BROKER_ENV_FILE` before anything else, and authorises the account with the new token.
//!
//! The configuration comes through the guard of the examples, which refuses anything but
//! the demo environment on its default host before the connection is opened unless
//! `BROKER_EXAMPLE_ALLOW_LIVE` is `read-only` or `yes`; either value permits the rotation
//! too.
//!
//! ```sh
//! cargo run --example open_api_token_refresh --features open-api
//! cargo run --example open_api_token_refresh --features open-api -- --rotate
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::path::PathBuf;

use ctrader_connector::common::config::{ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN};

use support::guard::Access;
use support::open_api::{account, configuration, open, renewed};
use support::{Failure, flag};

const ENV_FILE: &str = "BROKER_ENV_FILE";
const ENV_ROTATE: &str = "BROKER_EXAMPLE_ROTATE";

fn line_prefix(key: &str) -> String {
    format!("export {key}=")
}

fn stored(text: &str, key: &str, value: &str) -> String {
    let prefix = line_prefix(key);
    let mut stored = String::with_capacity(text.len() + value.len());
    for line in text.split_inclusive('\n') {
        match line.strip_prefix(&prefix) {
            Some(old) => {
                let quote = if old.trim_start().starts_with('"') {
                    "\""
                } else {
                    ""
                };
                let end = if line.ends_with('\n') { "\n" } else { "" };
                for part in [prefix.as_str(), quote, value, quote, end] {
                    stored.push_str(part);
                }
            }
            None => stored.push_str(line),
        }
    }
    stored
}

fn writable_token_file() -> Result<(PathBuf, String), Failure> {
    let path = std::env::var(ENV_FILE)
        .map(PathBuf::from)
        .map_err(|_| Failure::new(format!("{ENV_FILE} must name the file that holds the pair")))?;
    if !path.is_absolute() {
        return Err(Failure::new(format!("{ENV_FILE} must be an absolute path")));
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|error| Failure::new(format!("the token file is unreadable: {error}")))?;
    for key in [ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN] {
        let prefix = line_prefix(key);
        if !text.lines().any(|line| line.starts_with(&prefix)) {
            return Err(Failure::new(format!(
                "the token file has no `{prefix}` line"
            )));
        }
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .map_err(|error| Failure::new(format!("the token file is not writable: {error}")))?;
    Ok((path, text))
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let checked = configuration(Access::ReadOnly)?;
    let environment = checked.get().environment();
    let configured = checked.get().refresh_token().is_some();
    let client = open(&checked).await?;
    client.authorise_application().await?;
    let account = account(&client, environment).await?;
    client.authorise_account(account).await?;
    println!("the current access token authorises the {environment} account");
    println!(
        "a refresh token is {}",
        if configured {
            "configured"
        } else {
            "not configured"
        }
    );
    if !flag("--rotate", ENV_ROTATE) {
        client.shutdown().await;
        println!("the pair was left as it is; pass --rotate to exchange it for a new one");
        return Ok(());
    }
    let (path, text) = writable_token_file()?;
    let pair = client.refresh_token().await?;
    let text = stored(&text, ENV_ACCESS_TOKEN, pair.access.expose());
    let text = stored(&text, ENV_REFRESH_TOKEN, pair.refresh.expose());
    std::fs::write(&path, text)
        .map_err(|error| Failure::new(format!("the new pair could not be stored: {error}")))?;
    println!(
        "the pair was rotated and stored; the new {} token expires in {} s",
        pair.token_type,
        pair.expires_in.as_secs()
    );
    client.shutdown().await;
    let rotated = renewed(&checked, pair.access.expose(), pair.refresh.expose())?;
    let fresh = open(&rotated).await?;
    fresh.authorise_application().await?;
    let authorised = fresh.authorise_account(account).await;
    fresh.shutdown().await;
    authorised?;
    println!("the new access token authorises the {environment} account over a new connection");
    Ok(())
}
