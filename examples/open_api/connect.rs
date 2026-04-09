//! Connects to the Open API and authorises the application and a demo account.
//!
//! The steps are spelled out here; the other examples take them from the shared `session`
//! helper. Needs `BROKER_CLIENT_ID`, `BROKER_CLIENT_SECRET` and `BROKER_ACCESS_TOKEN`.
//!
//! ```sh
//! cargo run --example open_api_connect --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::config::ConfigurationOpenApi;
use ctrader_connector::open_api::client::OpenApiClient;

use support::{Failure, veil};

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let configuration = ConfigurationOpenApi::from_env()?;
    let environment = configuration.environment();
    let client = OpenApiClient::connect(configuration).await?;
    println!("connected to the {environment} environment");
    let version = client.version().await?;
    println!("the server speaks version {version} of the protocol");
    client.authorise_application().await?;
    println!("application authorised");
    let accounts = client.accounts().await?;
    for account in &accounts {
        veil(account.id);
    }
    let demo: Vec<_> = accounts.iter().filter(|account| !account.is_live).collect();
    println!(
        "the access token grants access to {} accounts, {} of them demo",
        accounts.len(),
        demo.len()
    );
    let Some(account) = demo.first() else {
        client.shutdown().await;
        return Err(Failure::new(
            "the access token grants access to no demo account",
        ));
    };
    client.authorise_account(account.id).await?;
    println!("demo account authorised");
    client.shutdown().await;
    println!("connection shut down");
    Ok(())
}
