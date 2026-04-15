//! Loses the connection of a quote session on purpose and watches the session recover.
//!
//! The client logs on again by itself and subscribes to the prices once more. The stream of
//! prices reports the loss once, since prices sent in between are missed, and then goes on.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example fix_reconnect --features fix
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::decimals::Price;
use ctrader_connector::common::errors::ConnectorError;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::session::{Event, SessionKind};

use support::fix::{leave, session, symbol};
use support::guard::Access;
use support::{Failure, QUOTES, STEP, within};

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "none".to_owned(), |price| price.to_string())
}

async fn recovery(quote: &FixClient) -> Result<(), Failure> {
    let mut events = quote.events();
    quote.force_reconnect().await?;
    loop {
        let event = within(STEP, "the next event of the session", events.recv())
            .await?
            .map_err(|_| Failure::new("the events of the session were missed"))?;
        match event {
            Event::Disconnected(cause) => println!("disconnected: {cause}"),
            Event::Reconnected { attempt } => {
                println!("logged on again at attempt {attempt}");
                return Ok(());
            }
            Event::ReconnectFailed { .. } => println!("an attempt to reconnect failed"),
            Event::Ended => return Err(Failure::new("the session ended")),
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let quote = session(SessionKind::Quote, Access::ReadOnly).await?;
    let security = symbol(&quote).await?;
    let mut spots = within(STEP, "the first prices", quote.subscribe_spot(security.id)).await??;
    let before = spots.last();
    println!(
        "{} before: bid {} offer {}",
        security.name,
        shown(before.bid),
        shown(before.offer)
    );
    recovery(&quote).await?;
    let after = within(QUOTES, "prices after the reconnection", async {
        loop {
            match spots.next().await {
                Ok(next) => return Ok(next),
                Err(ConnectorError::Transport(error)) => {
                    println!("the stream reported the loss: {error}");
                }
                Err(error) => return Err(Failure::from(error)),
            }
        }
    })
    .await??;
    println!(
        "{} after: bid {} offer {}",
        security.name,
        shown(after.bid),
        shown(after.offer)
    );
    spots.unsubscribe().await?;
    leave(&quote).await?;
    println!("unsubscribed and logged out");
    Ok(())
}
