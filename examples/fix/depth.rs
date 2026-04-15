//! Keeps the order book of a symbol over the quote session.
//!
//! The server answers the subscription with an empty snapshot and fills the book by
//! changes. The example waits for five changes and both sides of the book, then prints the
//! best levels.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example fix_depth --features fix
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::fix::quotes::BookEntry;
use ctrader_connector::fix::session::SessionKind;

use support::fix::{leave, session, symbol};
use support::guard::Access;
use support::{Failure, QUOTES, STEP, within};

const CHANGES: usize = 5;
const LEVELS: usize = 3;

fn print_side(name: &str, entries: &[&BookEntry]) {
    for entry in entries.iter().take(LEVELS) {
        println!(
            "{name}: {} at {}",
            entry
                .size
                .map_or_else(|| "no size".to_owned(), |size| size.to_string()),
            entry.price
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let quote = session(SessionKind::Quote, Access::ReadOnly).await?;
    let security = symbol(&quote).await?;
    let mut depth = within(STEP, "the first book", quote.subscribe_depth(security.id)).await??;
    println!(
        "{}: the first book holds {} entries",
        security.name,
        depth.book().len()
    );
    let filled = within(QUOTES, "five changes of the book", async {
        let mut changes = 0_usize;
        while changes < CHANGES
            || depth.book().bids().is_empty()
            || depth.book().offers().is_empty()
        {
            depth.next().await?;
            changes += 1;
        }
        Ok::<_, Failure>(changes)
    })
    .await;
    let book = depth.book().clone();
    depth.unsubscribe().await?;
    leave(&quote).await?;
    let changes = filled??;
    println!(
        "after {changes} changes: {} bids and {} offers",
        book.bids().len(),
        book.offers().len()
    );
    print_side("bid", &book.bids());
    print_side("offer", &book.offers());
    Ok(())
}
