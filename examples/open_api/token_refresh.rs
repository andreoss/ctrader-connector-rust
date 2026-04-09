//! Exchanges the refresh token for a new token pair, only when asked to.
//!
//! A refresh rotates the pair: the old access and refresh tokens stop working at once, in
//! every program that still holds them. Without the option the example therefore only checks
//! that a refresh is possible and changes nothing. With `--rotate`, or with
//! `BROKER_EXAMPLE_ROTATE=1`, it refreshes the pair, writes the new one into the
//! `export BROKER_ACCESS_TOKEN=` and `export BROKER_REFRESH_TOKEN=` lines of the file named by
//! `BROKER_ENV_FILE` before anything else, and authorises the account with the new token.
//!
//! ```sh
//! cargo run --example open_api_token_refresh --features open-api
//! cargo run --example open_api_token_refresh --features open-api -- --rotate
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::path::PathBuf;

use ctrader_connector::common::config::{
    ConfigurationOpenApi, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN,
};
use ctrader_connector::open_api::client::OpenApiClient;

use support::open_api::{configuration, demo_account};
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
    let configuration = configuration()?;
    let configured = configuration.refresh_token().is_some();
    let client = OpenApiClient::connect(configuration.clone()).await?;
    client.authorise_application().await?;
    let account = demo_account(&client).await?;
    client.authorise_account(account).await?;
    println!("the current access token authorises the demo account");
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
    let rotated = ConfigurationOpenApi::demo()
        .host(configuration.host())
        .port(configuration.port())
        .client_id(configuration.client_id().as_str())
        .client_secret(configuration.client_secret().expose())
        .access_token(pair.access.expose())
        .refresh_token(pair.refresh.expose())
        .token_url(configuration.token_url())
        .build()?;
    let fresh = OpenApiClient::connect(rotated).await?;
    fresh.authorise_application().await?;
    let authorised = fresh.authorise_account(account).await;
    fresh.shutdown().await;
    authorised?;
    println!("the new access token authorises the demo account over a new connection");
    Ok(())
}
