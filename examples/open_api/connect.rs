//! Connects to the Open API and authorises the application and a demo account.
//!
//! The steps are spelled out here; the other examples take them from the shared `session`
//! helper. The configuration comes through the guard of the examples, which refuses any
//! environment but demo before the connection is opened unless
//! `BROKER_EXAMPLE_ALLOW_LIVE` is `read-only` or `yes`. Needs `BROKER_CLIENT_ID`,
//! `BROKER_CLIENT_SECRET` and `BROKER_ACCESS_TOKEN`.
//!
//! ```sh
//! cargo run --example open_api_connect --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::config::Environment;

use support::guard::Access;
use support::open_api::{configuration, open};
use support::{Failure, veil};

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let checked = configuration(Access::ReadOnly)?;
    let environment = checked.get().environment();
    let client = open(&checked).await?;
    println!("connected to the {environment} environment");
    let version = client.version().await?;
    println!("the server speaks version {version} of the protocol");
    client.authorise_application().await?;
    println!("application authorised");
    let accounts = client.accounts().await?;
    for account in &accounts {
        veil(account.id);
    }
    let live = environment == Environment::Live;
    let own: Vec<_> = accounts
        .iter()
        .filter(|account| account.is_live == live)
        .collect();
    println!(
        "the access token grants access to {} accounts, {} of them {environment}",
        accounts.len(),
        own.len()
    );
    let Some(account) = own.first() else {
        client.shutdown().await;
        return Err(Failure::new(format!(
            "the access token grants access to no {environment} account"
        )));
    };
    client.authorise_account(account.id).await?;
    println!("{environment} account authorised");
    client.shutdown().await;
    println!("connection shut down");
    Ok(())
}
