//! Margin of positions and margin calls of an account.
//!
//! The server reports by itself when the margin a position uses changes, when a margin call
//! of an account is changed and when one is triggered; nothing is subscribed to.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::margin::MarginEvent;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! for call in client.margin_calls(account).await? {
//!     println!("{:?} at {}", call.level, call.threshold);
//! }
//! let mut margin = client.margin_events(account);
//! loop {
//!     if let MarginEvent::Changed(change) = margin.next().await? {
//!         println!("{} uses {}", change.position, change.used_margin);
//!     }
//! }
//! # }
//! ```

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, PositionId};
use crate::common::time::Timestamp;
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{
    ProtoOaMarginCall, ProtoOaMarginCallListReq, ProtoOaMarginCallTriggerEvent,
    ProtoOaMarginCallUpdateEvent, ProtoOaMarginCallUpdateReq, ProtoOaMarginChangedEvent,
    ProtoOaNotificationType,
};
use crate::open_api::stream::Feed;
use crate::open_api::trading::DEFAULT_MONEY_DIGITS;

const MARGIN_CALL_LEVEL: &str = "level of margin call";
const THRESHOLD: &str = "margin level threshold";
const USED_MARGIN: &str = "used margin";
const POSITION: &str = "position id";

/// Which of the margin calls of an account one is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum MarginCallLevel {
    /// First margin call.
    First,
    /// Second margin call.
    Second,
    /// Third margin call.
    Third,
}

impl MarginCallLevel {
    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaNotificationType::try_from(raw) {
            Ok(ProtoOaNotificationType::MarginLevelThreshold1) => Ok(Self::First),
            Ok(ProtoOaNotificationType::MarginLevelThreshold2) => Ok(Self::Second),
            Ok(ProtoOaNotificationType::MarginLevelThreshold3) => Ok(Self::Third),
            Err(_) => Err(ValueError::OutOfRange {
                kind: MARGIN_CALL_LEVEL,
            }),
        }
    }

    const fn to_wire(self) -> ProtoOaNotificationType {
        match self {
            Self::First => ProtoOaNotificationType::MarginLevelThreshold1,
            Self::Second => ProtoOaNotificationType::MarginLevelThreshold2,
            Self::Third => ProtoOaNotificationType::MarginLevelThreshold3,
        }
    }
}

/// Margin call of an account: the margin level at which the account is notified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct MarginCall {
    /// Which margin call of the account this is.
    pub level: MarginCallLevel,
    /// Margin level at or below which the margin call is triggered, as a ratio of equity to
    /// used margin: 1.5 stands for 150 %.
    pub threshold: Decimal,
    /// Moment the margin call was last changed.
    pub updated: Option<Timestamp>,
}

impl MarginCall {
    /// Describes a margin call with the given threshold, a ratio of equity to used margin.
    #[must_use]
    pub const fn new(level: MarginCallLevel, threshold: Decimal) -> Self {
        Self {
            level,
            threshold,
            updated: None,
        }
    }

    fn to_wire(self) -> Result<ProtoOaMarginCall, ValueError> {
        let unrepresentable = ValueError::Unrepresentable {
            kind: THRESHOLD,
            value: self.threshold,
        };
        if self.threshold.is_sign_negative() {
            return Err(unrepresentable);
        }
        Ok(ProtoOaMarginCall {
            margin_call_type: self.level.to_wire().into(),
            margin_level_threshold: self.threshold.to_f64().ok_or(unrepresentable)?,
            utc_last_update_timestamp: None,
        })
    }
}

impl TryFrom<ProtoOaMarginCall> for MarginCall {
    type Error = ValueError;

    fn try_from(call: ProtoOaMarginCall) -> Result<Self, Self::Error> {
        let threshold = Decimal::try_from(call.margin_level_threshold)
            .map_err(|_| ValueError::OutOfRange { kind: THRESHOLD })?;
        Ok(Self {
            level: MarginCallLevel::from_wire(call.margin_call_type)?,
            threshold: threshold.normalize(),
            updated: call
                .utc_last_update_timestamp
                .map(Timestamp::from_millis)
                .transpose()?,
        })
    }
}

/// Change of the margin a position uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct MarginChange {
    /// Account the position belongs to.
    pub account: AccountId,
    /// Position whose margin changed.
    pub position: PositionId,
    /// Margin the position uses after the change, in the deposit currency.
    pub used_margin: Decimal,
}

impl TryFrom<ProtoOaMarginChangedEvent> for MarginChange {
    type Error = ValueError;

    fn try_from(event: ProtoOaMarginChangedEvent) -> Result<Self, Self::Error> {
        let position = i64::try_from(event.position_id)
            .map_err(|_| ValueError::OutOfRange { kind: POSITION })?;
        let used = i64::try_from(event.used_margin)
            .ok()
            .and_then(|raw| {
                Decimal::try_new(raw, event.money_digits.unwrap_or(DEFAULT_MONEY_DIGITS)).ok()
            })
            .ok_or(ValueError::OutOfRange { kind: USED_MARGIN })?;
        Ok(Self {
            account: AccountId::new(event.ctid_trader_account_id)?,
            position: PositionId::new(position)?,
            used_margin: used,
        })
    }
}

/// One event about the margin of an account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MarginEvent {
    /// The margin a position uses changed.
    Changed(MarginChange),
    /// A margin call of the account was changed.
    CallUpdated(MarginCall),
    /// The margin level of the account reached the threshold of a margin call.
    CallTriggered(MarginCall),
}

/// Margin events of one account, in the order the server sent them.
///
/// The stream starts with the first event that arrives after it was created and goes on
/// across reconnections; events the server sent while the connection was down are not sent
/// again.
///
/// A stream that is read too slowly loses events: the connection holds a limited number of
/// them, set with
/// [`ConnectionOptions::event_capacity`](crate::open_api::connection::ConnectionOptions::event_capacity).
/// The loss is reported once by [`MarginStream::next`] with [`ConnectorError::Lagged`] and the
/// stream then goes on with the oldest event still held.
#[derive(Debug)]
pub struct MarginStream {
    feed: Feed,
    account: AccountId,
}

impl MarginStream {
    /// Returns the account the events belong to.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Waits for the next margin event of the account.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down,
    /// [`ConnectorError::SessionEnded`] when the server ended the authorisation of the account,
    /// [`ConnectorError::Lagged`] when events were dropped because the stream was read too
    /// slowly, and [`ConnectorError::Value`] when the server sends a value out of range; the
    /// stream can be read on after the last two.
    pub async fn next(&mut self) -> Result<MarginEvent, ConnectorError> {
        let account = self.account.get();
        loop {
            let event = self.feed.next().await?;
            if let Some(change) = event.decode::<ProtoOaMarginChangedEvent>() {
                if change.ctid_trader_account_id == account {
                    return Ok(MarginEvent::Changed(MarginChange::try_from(change)?));
                }
            } else if let Some(update) = event.decode::<ProtoOaMarginCallUpdateEvent>() {
                if update.ctid_trader_account_id == account {
                    let call = MarginCall::try_from(update.margin_call)?;
                    return Ok(MarginEvent::CallUpdated(call));
                }
            } else if let Some(trigger) = event.decode::<ProtoOaMarginCallTriggerEvent>() {
                if trigger.ctid_trader_account_id == account {
                    let call = MarginCall::try_from(trigger.margin_call)?;
                    return Ok(MarginEvent::CallTriggered(call));
                }
            }
        }
    }
}

impl OpenApiClient {
    /// Starts to read the margin events of an authorised account.
    ///
    /// Nothing is sent: the server delivers these events for every account authorised on the
    /// connection. Create the stream before the request whose events are to be read.
    #[must_use]
    pub fn margin_events(&self, account: AccountId) -> MarginStream {
        MarginStream {
            feed: Feed::new(self.connection(), account),
            account,
        }
    }

    /// Lists the margin calls of an authorised account.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the request,
    /// [`ConnectorError::Value`] when a margin call carries a value out of range, and the
    /// errors of [`Connection::request`](crate::open_api::connection::Connection::request)
    /// otherwise.
    pub async fn margin_calls(
        &self,
        account: AccountId,
    ) -> Result<Vec<MarginCall>, ConnectorError> {
        let request = ProtoOaMarginCallListReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
        };
        let response = self.connection().request(&request).await?;
        let calls: Result<Vec<MarginCall>, ValueError> = response
            .margin_call
            .into_iter()
            .map(MarginCall::try_from)
            .collect();
        Ok(calls?)
    }

    /// Sets the threshold of a margin call of an authorised account.
    ///
    /// The request states the whole margin call and has the same outcome however often it
    /// arrives, so it is sent again after a reconnection like a request that reads.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Value`] when the threshold is negative, in which case nothing
    /// is sent, [`ConnectorError::Server`] when the server refuses the request, and the errors
    /// of [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn update_margin_call(
        &self,
        account: AccountId,
        call: MarginCall,
    ) -> Result<(), ConnectorError> {
        let request = ProtoOaMarginCallUpdateReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            margin_call: call.to_wire()?,
        };
        self.connection().request(&request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{MarginCall, MarginCallLevel, MarginChange};
    use crate::common::errors::ValueError;
    use crate::open_api::messages::{ProtoOaMarginCall, ProtoOaMarginChangedEvent};

    #[test]
    fn margin_call_keeps_its_level_and_threshold_across_the_wire() {
        for (level, raw) in [
            (MarginCallLevel::First, 61),
            (MarginCallLevel::Second, 62),
            (MarginCallLevel::Third, 63),
        ] {
            let wire = MarginCall::new(level, Decimal::new(15, 1))
                .to_wire()
                .expect("wire");
            assert_eq!(wire.margin_call_type, raw);
            assert_eq!(wire.utc_last_update_timestamp, None);
            let back = MarginCall::try_from(ProtoOaMarginCall {
                utc_last_update_timestamp: Some(5),
                ..wire
            })
            .expect("margin call");
            assert_eq!((back.level, back.threshold), (level, Decimal::new(15, 1)));
            assert_eq!(
                back.updated,
                crate::common::time::Timestamp::from_millis(5).ok()
            );
        }
        assert!(MarginCallLevel::First < MarginCallLevel::Third);
    }

    #[test]
    fn margin_call_out_of_range_is_refused() {
        let call = |kind, threshold| {
            MarginCall::try_from(ProtoOaMarginCall {
                margin_call_type: kind,
                margin_level_threshold: threshold,
                utc_last_update_timestamp: None,
            })
        };
        assert!(matches!(call(60, 1.0), Err(ValueError::OutOfRange { .. })));
        assert!(matches!(
            call(61, f64::NAN),
            Err(ValueError::OutOfRange { .. })
        ));
        assert!(matches!(call(61, 1e40), Err(ValueError::OutOfRange { .. })));
        assert!(matches!(
            MarginCall::new(MarginCallLevel::First, Decimal::new(-1, 1)).to_wire(),
            Err(ValueError::Unrepresentable { .. })
        ));
        assert_eq!(
            MarginCall::new(MarginCallLevel::Second, Decimal::ZERO)
                .to_wire()
                .map(|wire| wire.margin_level_threshold.to_bits()),
            Ok(0.0_f64.to_bits())
        );
    }

    #[test]
    fn margin_change_scales_the_margin_and_refuses_what_does_not_fit() {
        let event = |position, used, digits| ProtoOaMarginChangedEvent {
            payload_type: None,
            ctid_trader_account_id: 101,
            position_id: position,
            used_margin: used,
            money_digits: digits,
        };
        let change = MarginChange::try_from(event(7, 12_345, None)).expect("change");
        assert_eq!(change.account.get(), 101);
        assert_eq!(change.position.get(), 7);
        assert_eq!(change.used_margin.to_string(), "123.45");
        let fine = MarginChange::try_from(event(7, 12_345, Some(4))).expect("change");
        assert_eq!(fine.used_margin.to_string(), "1.2345");
        for refused in [
            event(u64::MAX, 1, None),
            event(0, 1, None),
            event(7, u64::MAX, None),
            event(7, 1, Some(99)),
        ] {
            assert!(MarginChange::try_from(refused).is_err());
        }
    }
}
