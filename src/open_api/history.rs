//! Deals, orders and changes of the balance of an account inside a range of time.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::common::time::{TimeRange, Timestamp};
//! use ctrader_connector::open_api::client::OpenApiClient;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let day = TimeRange::ending_at(Timestamp::now(), Duration::from_secs(86_400));
//! for deal in client.deal_history(account, day).await? {
//!     println!("{} {:?} {}", deal.id, deal.side, deal.filled_volume);
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, CashFlowId};
use crate::common::time::{TimeRange, Timestamp};
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{
    ProtoOaCashFlowHistoryListReq, ProtoOaChangeBalanceType, ProtoOaDealListReq,
    ProtoOaDepositWithdraw, ProtoOaOrderListReq,
};
use crate::open_api::trading::{Deal, Order, money};

const MAX_RANGE_MILLIS: i64 = 604_800_000;

const HISTORY_TIMESTAMP: &str = "end of the range of history";
const CASH_FLOW_KIND: &str = "kind of cash flow";

/// Longest range of time the server accepts in one request for deals, orders or cash flow.
pub const MAX_HISTORY_RANGE: Duration = Duration::from_millis(MAX_RANGE_MILLIS.unsigned_abs());

/// Parts of a range of time still to be asked for, each short enough for one request.
///
/// The server includes both ends of a request and refuses one whose ends are the same
/// millisecond, so a part that is a single millisecond is asked for together with the
/// millisecond before it, or after it at the Unix epoch.
#[derive(Debug)]
pub(crate) struct Windows {
    pending: Vec<(i64, i64)>,
}

impl Windows {
    /// Cuts the range into parts of at most [`MAX_HISTORY_RANGE`], latest first.
    pub(crate) fn new(range: TimeRange) -> Result<Self, ValueError> {
        let (from, to) = (range.from().as_millis(), range.to().as_millis());
        let mut pending = Vec::new();
        let mut start = from;
        loop {
            let end = start.saturating_add(MAX_RANGE_MILLIS).min(to);
            pending.push(widened(start, end)?);
            if end >= to {
                break;
            }
            start = end.saturating_add(1);
        }
        Ok(Self { pending })
    }

    /// Returns the next part to ask for, as the two timestamps of the request.
    pub(crate) fn next(&mut self) -> Option<(i64, i64)> {
        self.pending.pop()
    }

    /// Replaces a part the server did not answer whole by its two halves, which share their
    /// middle millisecond.
    ///
    /// Fails with [`ConnectorError::Incomplete`] when the part is too short to be halved.
    pub(crate) fn split(&mut self, (from, to): (i64, i64)) -> Result<(), ConnectorError> {
        let middle = to
            .checked_sub(from)
            .map(|length| length / 2)
            .and_then(|half| from.checked_add(half))
            .filter(|middle| from < *middle && *middle < to);
        let Some(middle) = middle else {
            return Err(ConnectorError::Incomplete {
                from: Timestamp::from_millis(from)?,
                to: Timestamp::from_millis(to)?,
            });
        };
        self.pending.push((from, middle));
        self.pending.push((middle, to));
        Ok(())
    }
}

/// Tells whether a moment of last update, when the server states one, lies inside the range.
fn inside(range: TimeRange, updated: Option<i64>) -> bool {
    updated.is_none_or(|at| range.from().as_millis() <= at && at <= range.to().as_millis())
}

fn widened(start: i64, end: i64) -> Result<(i64, i64), ValueError> {
    if start < end {
        return Ok((start, end));
    }
    match start.checked_sub(1).filter(|before| *before >= 0) {
        Some(before) => Ok((before, end)),
        None => end
            .checked_add(1)
            .map(|after| (start, after))
            .ok_or(ValueError::OutOfRange {
                kind: HISTORY_TIMESTAMP,
            }),
    }
}

macro_rules! cash_flow_kinds {
    ($($(#[$doc:meta])* $name:ident => $wire:ident, $deposit:literal;)*) => {
        /// Reason the balance of an account changed outside of trading.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum CashFlowKind {
            $($(#[$doc])* $name,)*
        }

        impl CashFlowKind {
            fn from_wire(raw: i32) -> Result<Self, ValueError> {
                match ProtoOaChangeBalanceType::try_from(raw) {
                    $(Ok(ProtoOaChangeBalanceType::$wire) => Ok(Self::$name),)*
                    Err(_) => Err(ValueError::OutOfRange { kind: CASH_FLOW_KIND }),
                }
            }

            /// Tells whether the change adds money to the account; otherwise it takes money
            /// from it.
            #[must_use]
            pub const fn is_deposit(self) -> bool {
                match self {
                    $(Self::$name => $deposit,)*
                }
            }
        }
    };
}

cash_flow_kinds! {
    /// Money paid in.
    Deposit => BalanceDeposit, true;
    /// Money paid out.
    Withdraw => BalanceWithdraw, false;
    /// Commission of a strategy received from an account of the same broker.
    DepositStrategyCommissionInner => BalanceDepositStrategyCommissionInner, true;
    /// Commission of a strategy paid to an account of the same broker.
    WithdrawStrategyCommissionInner => BalanceWithdrawStrategyCommissionInner, false;
    /// Commissions received as an introducing broker.
    DepositIbCommissions => BalanceDepositIbCommissions, true;
    /// Share of commissions passed on by an introducing broker.
    WithdrawIbSharedPercentage => BalanceWithdrawIbSharedPercentage, false;
    /// Share of commissions received from a sub introducing broker.
    DepositIbSharedPercentageFromSubIb => BalanceDepositIbSharedPercentageFromSubIb, true;
    /// Share of commissions received from the broker.
    DepositIbSharedPercentageFromBroker => BalanceDepositIbSharedPercentageFromBroker, true;
    /// Rebate received.
    DepositRebate => BalanceDepositRebate, true;
    /// Rebate taken back.
    WithdrawRebate => BalanceWithdrawRebate, false;
    /// Commission of a strategy received from an account of another broker.
    DepositStrategyCommissionOuter => BalanceDepositStrategyCommissionOuter, true;
    /// Commission of a strategy paid to an account of another broker.
    WithdrawStrategyCommissionOuter => BalanceWithdrawStrategyCommissionOuter, false;
    /// Compensation of a bonus taken from the account.
    WithdrawBonusCompensation => BalanceWithdrawBonusCompensation, false;
    /// Share of commissions passed on to the broker.
    WithdrawIbSharedPercentageToBroker => BalanceWithdrawIbSharedPercentageToBroker, false;
    /// Dividends received.
    DepositDividends => BalanceDepositDividends, true;
    /// Dividends charged.
    WithdrawDividends => BalanceWithdrawDividends, false;
    /// Charge for a guaranteed stop loss.
    WithdrawGslCharge => BalanceWithdrawGslCharge, false;
    /// Charge for holding a position over the rollover.
    WithdrawRollover => BalanceWithdrawRollover, false;
    /// Bonus that cannot be paid out, added.
    DepositNonwithdrawableBonus => BalanceDepositNonwithdrawableBonus, true;
    /// Bonus that cannot be paid out, removed.
    WithdrawNonwithdrawableBonus => BalanceWithdrawNonwithdrawableBonus, false;
    /// Swap received.
    DepositSwap => BalanceDepositSwap, true;
    /// Swap charged.
    WithdrawSwap => BalanceWithdrawSwap, false;
    /// Management fee received.
    DepositManagementFee => BalanceDepositManagementFee, true;
    /// Management fee paid.
    WithdrawManagementFee => BalanceWithdrawManagementFee, false;
    /// Performance fee received.
    DepositPerformanceFee => BalanceDepositPerformanceFee, true;
    /// Money moved out for a subaccount.
    WithdrawForSubaccount => BalanceWithdrawForSubaccount, false;
    /// Money moved in to a subaccount.
    DepositToSubaccount => BalanceDepositToSubaccount, true;
    /// Money moved out of a subaccount.
    WithdrawFromSubaccount => BalanceWithdrawFromSubaccount, false;
    /// Money moved in from a subaccount.
    DepositFromSubaccount => BalanceDepositFromSubaccount, true;
    /// Fee for copying a strategy.
    WithdrawCopyFee => BalanceWithdrawCopyFee, false;
    /// Fee for an account that does not trade.
    WithdrawInactivityFee => BalanceWithdrawInactivityFee, false;
    /// Transfer received from another account.
    DepositTransfer => BalanceDepositTransfer, true;
    /// Transfer sent to another account.
    WithdrawTransfer => BalanceWithdrawTransfer, false;
    /// Bonus turned into money of the account.
    DepositConvertedBonus => BalanceDepositConvertedBonus, true;
    /// Money added to bring a negative balance back to zero.
    DepositNegativeBalanceProtection => BalanceDepositNegativeBalanceProtection, true;
}

/// One change of the balance of an account outside of trading, such as a deposit or a
/// withdrawal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct CashFlow {
    /// Identifier of the change in the history of the balance.
    pub id: CashFlowId,
    /// Reason of the change.
    pub kind: CashFlowKind,
    /// Size of the change in the deposit currency; [`CashFlowKind::is_deposit`] tells its
    /// direction.
    pub amount: Decimal,
    /// Balance after the change, in the deposit currency.
    pub balance: Decimal,
    /// Equity after the change, in the deposit currency.
    pub equity: Option<Decimal>,
    /// Moment of the change.
    pub at: Timestamp,
    /// Note the broker attached.
    pub note: Option<String>,
    /// Version of the balance after the change.
    pub balance_version: Option<i64>,
}

impl TryFrom<ProtoOaDepositWithdraw> for CashFlow {
    type Error = ValueError;

    fn try_from(change: ProtoOaDepositWithdraw) -> Result<Self, Self::Error> {
        let digits = change.money_digits;
        Ok(Self {
            id: CashFlowId::new(change.balance_history_id)?,
            kind: CashFlowKind::from_wire(change.operation_type)?,
            amount: money(change.delta, digits)?,
            balance: money(change.balance, digits)?,
            equity: change.equity.map(|raw| money(raw, digits)).transpose()?,
            at: Timestamp::from_millis(change.change_balance_timestamp)?,
            note: change.external_note,
            balance_version: change.balance_version,
        })
    }
}

impl OpenApiClient {
    /// Fetches the deals of an authorised account inside a range of time, oldest first.
    ///
    /// The server selects a deal by the moment it was last updated, which for a deal is the
    /// moment it was created, and includes both ends of the range. The server refuses a request
    /// whose ends are the same, so a single millisecond is asked for together with the one
    /// before it, and what belongs to that other millisecond is left out.
    ///
    /// A range longer than [`MAX_HISTORY_RANGE`] is fetched with several requests, each of
    /// which counts against the limit for historical data. The server sends a limited number
    /// of deals per request; a part of the range it reports to hold more is halved and asked
    /// for again, until every part is answered whole. Every deal of the range is thus
    /// returned, and returned once.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a request,
    /// [`ConnectorError::Value`] when a deal carries a value out of range,
    /// [`ConnectorError::Incomplete`] when the server reports more deals than it sends for a
    /// part too short to be halved, in which case no part of the range is returned, and the
    /// errors of [`Connection::request`](crate::open_api::connection::Connection::request)
    /// otherwise.
    pub async fn deal_history(
        &self,
        account: AccountId,
        range: TimeRange,
    ) -> Result<Vec<Deal>, ConnectorError> {
        let mut windows = Windows::new(range)?;
        let mut deals = BTreeMap::new();
        while let Some((from, to)) = windows.next() {
            let request = ProtoOaDealListReq {
                payload_type: None,
                ctid_trader_account_id: account.get(),
                from_timestamp: from,
                to_timestamp: to,
                max_rows: None,
            };
            let response = self.connection().request(&request).await?;
            if response.has_more {
                windows.split((from, to))?;
                continue;
            }
            for deal in response.deal {
                let updated = deal.utc_last_update_timestamp;
                let deal = Deal::try_from(deal)?;
                if inside(range, updated) {
                    deals.insert(deal.id, deal);
                }
            }
        }
        let mut deals: Vec<Deal> = deals.into_values().collect();
        deals.sort_by_key(|deal| (deal.executed, deal.id));
        Ok(deals)
    }

    /// Fetches the orders of an authorised account that were last updated inside a range of
    /// time, in the order of their last update, oldest first.
    ///
    /// The orders are the ones in force and the ones that are filled, cancelled, expired or
    /// refused. Both ends of the range are included, and the range is cut into requests and
    /// answered whole as described for [`OpenApiClient::deal_history`].
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::deal_history`].
    pub async fn order_history(
        &self,
        account: AccountId,
        range: TimeRange,
    ) -> Result<Vec<Order>, ConnectorError> {
        let mut windows = Windows::new(range)?;
        let mut orders = BTreeMap::new();
        while let Some((from, to)) = windows.next() {
            let request = ProtoOaOrderListReq {
                payload_type: None,
                ctid_trader_account_id: account.get(),
                from_timestamp: from,
                to_timestamp: to,
            };
            let response = self.connection().request(&request).await?;
            if response.has_more {
                windows.split((from, to))?;
                continue;
            }
            for order in response.order {
                let updated = order.utc_last_update_timestamp;
                let order = Order::try_from(order)?;
                if inside(range, updated) {
                    orders.insert(order.id, order);
                }
            }
        }
        let mut orders: Vec<Order> = orders.into_values().collect();
        orders.sort_by_key(|order| (order.updated, order.id));
        Ok(orders)
    }

    /// Fetches the deposits, withdrawals and other changes of the balance of an authorised
    /// account outside of trading that were made inside a range of time, oldest first.
    ///
    /// A range longer than [`MAX_HISTORY_RANGE`] is fetched with several requests, each of
    /// which counts against the limit for historical data. A single millisecond is asked for
    /// together with the one before it, and what belongs to that other millisecond is left
    /// out.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a request,
    /// [`ConnectorError::Value`] when a change carries a value out of range, in which case no
    /// part of the range is returned, and the errors of
    /// [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn cash_flow_history(
        &self,
        account: AccountId,
        range: TimeRange,
    ) -> Result<Vec<CashFlow>, ConnectorError> {
        let mut windows = Windows::new(range)?;
        let mut changes = BTreeMap::new();
        while let Some((from, to)) = windows.next() {
            let request = ProtoOaCashFlowHistoryListReq {
                payload_type: None,
                ctid_trader_account_id: account.get(),
                from_timestamp: from,
                to_timestamp: to,
            };
            let response = self.connection().request(&request).await?;
            for change in response.deposit_withdraw {
                let change = CashFlow::try_from(change)?;
                if inside(range, Some(change.at.as_millis())) {
                    changes.insert(change.id, change);
                }
            }
        }
        let mut changes: Vec<CashFlow> = changes.into_values().collect();
        changes.sort_by_key(|change| (change.at, change.id));
        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CashFlow, CashFlowKind, MAX_HISTORY_RANGE, MAX_RANGE_MILLIS, Windows, widened};
    use crate::common::errors::{ConnectorError, ValueError};
    use crate::common::time::{TimeRange, Timestamp};
    use crate::open_api::messages::{
        ProtoOaChangeBalanceType, ProtoOaDepositWithdraw, ProtoOaExecutionEvent,
    };
    use crate::open_api::trading::{Execution, ExecutionType};

    fn range(from: i64, to: i64) -> TimeRange {
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        TimeRange::new(at(from), at(to)).expect("range")
    }

    fn all(mut windows: Windows) -> Vec<(i64, i64)> {
        std::iter::from_fn(|| windows.next()).collect()
    }

    #[test]
    fn range_is_cut_into_parts_the_server_accepts_latest_first() {
        assert_eq!(MAX_HISTORY_RANGE, Duration::from_secs(7 * 86_400));
        let week = MAX_RANGE_MILLIS;
        assert_eq!(
            all(Windows::new(range(10, 20)).expect("windows")),
            [(10, 20)]
        );
        assert_eq!(
            all(Windows::new(range(10, 10 + week)).expect("windows")),
            [(10, 10 + week)]
        );
        assert_eq!(
            all(Windows::new(range(10, 12 + 2 * week)).expect("windows")),
            [
                (11 + 2 * week, 12 + 2 * week),
                (11 + week, 11 + 2 * week),
                (10, 10 + week)
            ]
        );
    }

    #[test]
    fn part_of_one_millisecond_is_widened_to_two() {
        let week = MAX_RANGE_MILLIS;
        assert_eq!(
            all(Windows::new(range(10, 10)).expect("windows")),
            [(9, 10)]
        );
        assert_eq!(all(Windows::new(range(0, 0)).expect("windows")), [(0, 1)]);
        assert_eq!(
            all(Windows::new(range(10, 11 + week)).expect("windows")),
            [(10 + week, 11 + week), (10, 10 + week)]
        );
        assert_eq!(
            widened(-5, -5),
            Ok((-5, -4)),
            "a moment before the epoch is widened forwards"
        );
        assert_eq!(
            widened(i64::MAX, i64::MAX).map_err(|error| error.to_string()),
            Ok((i64::MAX - 1, i64::MAX))
        );
        assert_eq!(
            widened(-1, i64::MAX).map_err(|error| error.to_string()),
            Ok((-1, i64::MAX))
        );
        assert!(matches!(
            widened(i64::MIN, i64::MIN),
            Ok((i64::MIN, end)) if end == i64::MIN + 1
        ));
    }

    #[test]
    fn part_is_halved_until_it_is_too_short() {
        let mut windows = Windows::new(range(10, 20)).expect("windows");
        let whole = windows.next().expect("window");
        windows.split(whole).expect("halves");
        assert_eq!(windows.next(), Some((15, 20)));
        assert_eq!(windows.next(), Some((10, 15)));
        windows.split((10, 12)).expect("halves");
        assert_eq!(all(windows), [(11, 12), (10, 11)]);
        let mut windows = Windows::new(range(10, 11)).expect("windows");
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        assert_eq!(
            windows.split((10, 11)),
            Err(ConnectorError::Incomplete {
                from: at(10),
                to: at(11)
            })
        );
        assert!(windows.split((i64::MIN, i64::MAX)).is_err());
    }

    #[test]
    fn every_kind_of_cash_flow_of_the_wire_is_known_with_its_direction() {
        let mut known = 0;
        for raw in -1..=64 {
            match ProtoOaChangeBalanceType::try_from(raw) {
                Ok(wire) => {
                    let kind = CashFlowKind::from_wire(raw).expect("kind");
                    assert_eq!(
                        kind.is_deposit(),
                        wire.as_str_name().starts_with("BALANCE_DEPOSIT"),
                        "{}",
                        wire.as_str_name()
                    );
                    known += 1;
                }
                Err(_) => assert_eq!(
                    CashFlowKind::from_wire(raw),
                    Err(ValueError::OutOfRange {
                        kind: "kind of cash flow"
                    })
                ),
            }
        }
        assert_eq!(known, 35);
        assert_eq!(CashFlowKind::from_wire(1), Ok(CashFlowKind::Withdraw));
    }

    #[test]
    fn execution_event_carries_the_cash_flow_it_reports() {
        let change = ProtoOaDepositWithdraw {
            operation_type: 0,
            balance_history_id: 77,
            balance: 150_000,
            delta: 100_000,
            change_balance_timestamp: 1_000,
            external_note: Some("first deposit".to_owned()),
            balance_version: Some(3),
            equity: Some(150_100),
            money_digits: Some(2),
        };
        let event = ProtoOaExecutionEvent {
            ctid_trader_account_id: 101,
            execution_type: 10,
            deposit_withdraw: Some(change.clone()),
            ..ProtoOaExecutionEvent::default()
        };
        let execution = Execution::try_from(event).expect("execution");
        assert_eq!(execution.kind, ExecutionType::DepositWithdraw);
        let flow = execution.cash_flow.expect("cash flow");
        assert_eq!(flow.id.get(), 77);
        assert_eq!(flow.kind, CashFlowKind::Deposit);
        assert_eq!(flow.amount.to_string(), "1000.00");
        assert_eq!(flow.balance.to_string(), "1500.00");
        assert_eq!(
            flow.equity.map(|equity| equity.to_string()).as_deref(),
            Some("1501.00")
        );
        assert_eq!(flow.at.as_millis(), 1_000);
        assert_eq!(flow.note.as_deref(), Some("first deposit"));
        assert_eq!(flow.balance_version, Some(3));
        assert!(
            CashFlow::try_from(ProtoOaDepositWithdraw {
                balance_history_id: 0,
                ..change
            })
            .is_err()
        );
    }
}
