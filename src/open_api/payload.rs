//! Typed pairing of messages with their payload types and of requests with their responses.

use prost::Message;

use crate::common::errors::{ConnectorError, SessionEndCause, SessionEnded, TransportError};
use crate::common::ids::AccountId;
use crate::open_api::messages::{
    ProtoErrorRes, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
    ProtoOaAccountDisconnectEvent, ProtoOaAccountsTokenInvalidatedEvent, ProtoOaAmendOrderReq,
    ProtoOaAmendPositionSltpReq, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes,
    ProtoOaAssetListReq, ProtoOaAssetListRes, ProtoOaCancelOrderReq, ProtoOaCashFlowHistoryListReq,
    ProtoOaCashFlowHistoryListRes, ProtoOaClientDisconnectEvent, ProtoOaClosePositionReq,
    ProtoOaDealListReq, ProtoOaDealListRes, ProtoOaDepthEvent, ProtoOaErrorRes,
    ProtoOaExecutionEvent, ProtoOaGetAccountListByAccessTokenReq,
    ProtoOaGetAccountListByAccessTokenRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes,
    ProtoOaGetTrendbarsReq, ProtoOaGetTrendbarsRes, ProtoOaMarginCallListReq,
    ProtoOaMarginCallListRes, ProtoOaMarginCallTriggerEvent, ProtoOaMarginCallUpdateEvent,
    ProtoOaMarginCallUpdateReq, ProtoOaMarginCallUpdateRes, ProtoOaMarginChangedEvent,
    ProtoOaNewOrderReq, ProtoOaOrderErrorEvent, ProtoOaOrderListReq, ProtoOaOrderListRes,
    ProtoOaPayloadType, ProtoOaReconcileReq, ProtoOaReconcileRes, ProtoOaRefreshTokenReq,
    ProtoOaRefreshTokenRes, ProtoOaSpotEvent, ProtoOaSubscribeDepthQuotesReq,
    ProtoOaSubscribeDepthQuotesRes, ProtoOaSubscribeLiveTrendbarReq,
    ProtoOaSubscribeLiveTrendbarRes, ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes,
    ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq, ProtoOaSymbolsListRes,
    ProtoOaTraderReq, ProtoOaTraderRes, ProtoOaUnsubscribeDepthQuotesReq,
    ProtoOaUnsubscribeDepthQuotesRes, ProtoOaUnsubscribeLiveTrendbarReq,
    ProtoOaUnsubscribeLiveTrendbarRes, ProtoOaUnsubscribeSpotsReq, ProtoOaUnsubscribeSpotsRes,
    ProtoOaVersionReq, ProtoOaVersionRes, ProtoPayloadType,
};

/// Message that travels inside a [`ProtoMessage`] under a fixed payload type.
pub trait Payload: Message + Default {
    /// Number that identifies the message on the wire.
    const PAYLOAD_TYPE: u32;

    /// Wraps the message in the envelope the transport frames.
    fn to_message(&self, client_msg_id: Option<String>) -> ProtoMessage {
        ProtoMessage {
            payload_type: Self::PAYLOAD_TYPE,
            payload: Some(self.encode_to_vec()),
            client_msg_id,
        }
    }

    /// Unwraps the message from its envelope.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::UnexpectedResponse`] when the envelope carries another payload
    /// type and [`TransportError::Decode`] when the payload is malformed.
    fn from_message(message: &ProtoMessage) -> Result<Self, ConnectorError> {
        if message.payload_type != Self::PAYLOAD_TYPE {
            return Err(ConnectorError::UnexpectedResponse {
                expected: Self::PAYLOAD_TYPE,
                received: message.payload_type,
            });
        }
        Self::decode(message.payload.as_deref().unwrap_or_default())
            .map_err(|error| TransportError::Decode(error.to_string()).into())
    }
}

/// Message the server answers with exactly one response.
pub trait Request: Payload {
    /// Message the server answers with.
    type Response: Payload;

    /// Tells whether the request counts against the stricter limit for historical data.
    const HISTORICAL: bool = false;
}

macro_rules! payloads {
    ($($message:ident => $kind:expr,)*) => {
        $(impl Payload for $message {
            const PAYLOAD_TYPE: u32 = $kind as u32;
        })*

        #[cfg(test)]
        fn declared() -> Vec<(&'static str, u32, i32)> {
            vec![$((
                stringify!($message),
                <$message>::PAYLOAD_TYPE,
                <$message>::default().payload_type() as i32,
            ),)*]
        }
    };
}

macro_rules! requests {
    ($($request:ident => $response:ident, $historical:literal;)*) => {
        $(impl Request for $request {
            type Response = $response;

            const HISTORICAL: bool = $historical;
        })*

        /// Tells whether requests of the payload type count against the limit for historical data.
        #[must_use]
        pub fn is_historical(payload_type: u32) -> bool {
            [$((<$request as Payload>::PAYLOAD_TYPE, $historical)),*]
                .iter()
                .any(|(known, historical)| *historical && *known == payload_type)
        }
    };
}

#[derive(Clone, PartialEq, Eq, Message)]
struct AccountScope {
    #[prost(int64, optional, tag = "2")]
    account: Option<i64>,
}

/// Returns the account the message of the payload type acts on, read from its encoded form.
///
/// Every message of the Open API that belongs to an account carries the account in its second
/// field. The requests that belong to the application as a whole name no account.
pub(crate) fn account_of(payload_type: u32, payload: &[u8]) -> Option<AccountId> {
    let unscoped = [
        ProtoHeartbeatEvent::PAYLOAD_TYPE,
        ProtoOaApplicationAuthReq::PAYLOAD_TYPE,
        ProtoOaVersionReq::PAYLOAD_TYPE,
        ProtoOaGetAccountListByAccessTokenReq::PAYLOAD_TYPE,
        ProtoOaRefreshTokenReq::PAYLOAD_TYPE,
    ];
    if unscoped.contains(&payload_type) {
        return None;
    }
    let scope = AccountScope::decode(payload).ok()?;
    AccountId::new(scope.account?).ok()
}

/// Returns the sessions whose authorisation the pushed message ends, one for every account
/// it names, or the session of the application.
pub(crate) fn sessions_ended_by(message: &ProtoMessage) -> Vec<SessionEnded> {
    if let Ok(event) = ProtoOaAccountsTokenInvalidatedEvent::from_message(message) {
        let cause = SessionEndCause::TokenInvalidated {
            reason: event.reason,
        };
        event
            .ctid_trader_account_ids
            .into_iter()
            .filter_map(|id| AccountId::new(id).ok())
            .map(|account| SessionEnded::new(Some(account), cause.clone()))
            .collect()
    } else if let Ok(event) = ProtoOaAccountDisconnectEvent::from_message(message) {
        AccountId::new(event.ctid_trader_account_id)
            .ok()
            .map(|account| SessionEnded::new(Some(account), SessionEndCause::AccountDisconnected))
            .into_iter()
            .collect()
    } else if let Ok(event) = ProtoOaClientDisconnectEvent::from_message(message) {
        vec![SessionEnded::new(
            None,
            SessionEndCause::ClientDisconnected {
                reason: event.reason,
            },
        )]
    } else {
        Vec::new()
    }
}

payloads! {
    ProtoErrorRes => ProtoPayloadType::ErrorRes,
    ProtoHeartbeatEvent => ProtoPayloadType::HeartbeatEvent,
    ProtoOaApplicationAuthReq => ProtoOaPayloadType::ProtoOaApplicationAuthReq,
    ProtoOaApplicationAuthRes => ProtoOaPayloadType::ProtoOaApplicationAuthRes,
    ProtoOaAccountAuthReq => ProtoOaPayloadType::ProtoOaAccountAuthReq,
    ProtoOaAccountAuthRes => ProtoOaPayloadType::ProtoOaAccountAuthRes,
    ProtoOaVersionReq => ProtoOaPayloadType::ProtoOaVersionReq,
    ProtoOaVersionRes => ProtoOaPayloadType::ProtoOaVersionRes,
    ProtoOaSubscribeSpotsReq => ProtoOaPayloadType::ProtoOaSubscribeSpotsReq,
    ProtoOaSubscribeSpotsRes => ProtoOaPayloadType::ProtoOaSubscribeSpotsRes,
    ProtoOaUnsubscribeSpotsReq => ProtoOaPayloadType::ProtoOaUnsubscribeSpotsReq,
    ProtoOaUnsubscribeSpotsRes => ProtoOaPayloadType::ProtoOaUnsubscribeSpotsRes,
    ProtoOaSpotEvent => ProtoOaPayloadType::ProtoOaSpotEvent,
    ProtoOaDealListReq => ProtoOaPayloadType::ProtoOaDealListReq,
    ProtoOaDealListRes => ProtoOaPayloadType::ProtoOaDealListRes,
    ProtoOaGetTrendbarsReq => ProtoOaPayloadType::ProtoOaGetTrendbarsReq,
    ProtoOaGetTrendbarsRes => ProtoOaPayloadType::ProtoOaGetTrendbarsRes,
    ProtoOaErrorRes => ProtoOaPayloadType::ProtoOaErrorRes,
    ProtoOaCashFlowHistoryListReq => ProtoOaPayloadType::ProtoOaCashFlowHistoryListReq,
    ProtoOaCashFlowHistoryListRes => ProtoOaPayloadType::ProtoOaCashFlowHistoryListRes,
    ProtoOaGetTickDataReq => ProtoOaPayloadType::ProtoOaGetTickdataReq,
    ProtoOaGetTickDataRes => ProtoOaPayloadType::ProtoOaGetTickdataRes,
    ProtoOaAccountsTokenInvalidatedEvent => ProtoOaPayloadType::ProtoOaAccountsTokenInvalidatedEvent,
    ProtoOaClientDisconnectEvent => ProtoOaPayloadType::ProtoOaClientDisconnectEvent,
    ProtoOaGetAccountListByAccessTokenReq => ProtoOaPayloadType::ProtoOaGetAccountsByAccessTokenReq,
    ProtoOaGetAccountListByAccessTokenRes => ProtoOaPayloadType::ProtoOaGetAccountsByAccessTokenRes,
    ProtoOaAccountDisconnectEvent => ProtoOaPayloadType::ProtoOaAccountDisconnectEvent,
    ProtoOaRefreshTokenReq => ProtoOaPayloadType::ProtoOaRefreshTokenReq,
    ProtoOaRefreshTokenRes => ProtoOaPayloadType::ProtoOaRefreshTokenRes,
    ProtoOaSubscribeLiveTrendbarReq => ProtoOaPayloadType::ProtoOaSubscribeLiveTrendbarReq,
    ProtoOaSubscribeLiveTrendbarRes => ProtoOaPayloadType::ProtoOaSubscribeLiveTrendbarRes,
    ProtoOaUnsubscribeLiveTrendbarReq => ProtoOaPayloadType::ProtoOaUnsubscribeLiveTrendbarReq,
    ProtoOaUnsubscribeLiveTrendbarRes => ProtoOaPayloadType::ProtoOaUnsubscribeLiveTrendbarRes,
    ProtoOaDepthEvent => ProtoOaPayloadType::ProtoOaDepthEvent,
    ProtoOaSubscribeDepthQuotesReq => ProtoOaPayloadType::ProtoOaSubscribeDepthQuotesReq,
    ProtoOaSubscribeDepthQuotesRes => ProtoOaPayloadType::ProtoOaSubscribeDepthQuotesRes,
    ProtoOaUnsubscribeDepthQuotesReq => ProtoOaPayloadType::ProtoOaUnsubscribeDepthQuotesReq,
    ProtoOaUnsubscribeDepthQuotesRes => ProtoOaPayloadType::ProtoOaUnsubscribeDepthQuotesRes,
    ProtoOaAssetListReq => ProtoOaPayloadType::ProtoOaAssetListReq,
    ProtoOaAssetListRes => ProtoOaPayloadType::ProtoOaAssetListRes,
    ProtoOaSymbolsListReq => ProtoOaPayloadType::ProtoOaSymbolsListReq,
    ProtoOaSymbolsListRes => ProtoOaPayloadType::ProtoOaSymbolsListRes,
    ProtoOaSymbolByIdReq => ProtoOaPayloadType::ProtoOaSymbolByIdReq,
    ProtoOaSymbolByIdRes => ProtoOaPayloadType::ProtoOaSymbolByIdRes,
    ProtoOaNewOrderReq => ProtoOaPayloadType::ProtoOaNewOrderReq,
    ProtoOaCancelOrderReq => ProtoOaPayloadType::ProtoOaCancelOrderReq,
    ProtoOaAmendOrderReq => ProtoOaPayloadType::ProtoOaAmendOrderReq,
    ProtoOaAmendPositionSltpReq => ProtoOaPayloadType::ProtoOaAmendPositionSltpReq,
    ProtoOaClosePositionReq => ProtoOaPayloadType::ProtoOaClosePositionReq,
    ProtoOaReconcileReq => ProtoOaPayloadType::ProtoOaReconcileReq,
    ProtoOaReconcileRes => ProtoOaPayloadType::ProtoOaReconcileRes,
    ProtoOaExecutionEvent => ProtoOaPayloadType::ProtoOaExecutionEvent,
    ProtoOaOrderErrorEvent => ProtoOaPayloadType::ProtoOaOrderErrorEvent,
    ProtoOaTraderReq => ProtoOaPayloadType::ProtoOaTraderReq,
    ProtoOaTraderRes => ProtoOaPayloadType::ProtoOaTraderRes,
    ProtoOaOrderListReq => ProtoOaPayloadType::ProtoOaOrderListReq,
    ProtoOaOrderListRes => ProtoOaPayloadType::ProtoOaOrderListRes,
    ProtoOaMarginChangedEvent => ProtoOaPayloadType::ProtoOaMarginChangedEvent,
    ProtoOaMarginCallListReq => ProtoOaPayloadType::ProtoOaMarginCallListReq,
    ProtoOaMarginCallListRes => ProtoOaPayloadType::ProtoOaMarginCallListRes,
    ProtoOaMarginCallUpdateReq => ProtoOaPayloadType::ProtoOaMarginCallUpdateReq,
    ProtoOaMarginCallUpdateRes => ProtoOaPayloadType::ProtoOaMarginCallUpdateRes,
    ProtoOaMarginCallUpdateEvent => ProtoOaPayloadType::ProtoOaMarginCallUpdateEvent,
    ProtoOaMarginCallTriggerEvent => ProtoOaPayloadType::ProtoOaMarginCallTriggerEvent,
}

requests! {
    ProtoOaApplicationAuthReq => ProtoOaApplicationAuthRes, false;
    ProtoOaAccountAuthReq => ProtoOaAccountAuthRes, false;
    ProtoOaVersionReq => ProtoOaVersionRes, false;
    ProtoOaSubscribeSpotsReq => ProtoOaSubscribeSpotsRes, false;
    ProtoOaUnsubscribeSpotsReq => ProtoOaUnsubscribeSpotsRes, false;
    ProtoOaGetAccountListByAccessTokenReq => ProtoOaGetAccountListByAccessTokenRes, false;
    ProtoOaRefreshTokenReq => ProtoOaRefreshTokenRes, false;
    ProtoOaSubscribeLiveTrendbarReq => ProtoOaSubscribeLiveTrendbarRes, false;
    ProtoOaUnsubscribeLiveTrendbarReq => ProtoOaUnsubscribeLiveTrendbarRes, false;
    ProtoOaSubscribeDepthQuotesReq => ProtoOaSubscribeDepthQuotesRes, false;
    ProtoOaUnsubscribeDepthQuotesReq => ProtoOaUnsubscribeDepthQuotesRes, false;
    ProtoOaAssetListReq => ProtoOaAssetListRes, false;
    ProtoOaSymbolsListReq => ProtoOaSymbolsListRes, false;
    ProtoOaSymbolByIdReq => ProtoOaSymbolByIdRes, false;
    ProtoOaReconcileReq => ProtoOaReconcileRes, false;
    ProtoOaTraderReq => ProtoOaTraderRes, false;
    ProtoOaMarginCallListReq => ProtoOaMarginCallListRes, false;
    ProtoOaMarginCallUpdateReq => ProtoOaMarginCallUpdateRes, false;
    ProtoOaDealListReq => ProtoOaDealListRes, true;
    ProtoOaOrderListReq => ProtoOaOrderListRes, true;
    ProtoOaGetTrendbarsReq => ProtoOaGetTrendbarsRes, true;
    ProtoOaCashFlowHistoryListReq => ProtoOaCashFlowHistoryListRes, true;
    ProtoOaGetTickDataReq => ProtoOaGetTickDataRes, true;
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::{Payload, Request, account_of, declared, is_historical, sessions_ended_by};
    use crate::common::errors::{ConnectorError, SessionEndCause, SessionEnded, TransportError};
    use crate::common::ids::AccountId;
    use crate::open_api::messages::{
        ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountDisconnectEvent,
        ProtoOaAccountsTokenInvalidatedEvent, ProtoOaClientDisconnectEvent,
        ProtoOaGetAccountListByAccessTokenReq, ProtoOaGetTrendbarsReq, ProtoOaTraderReq,
        ProtoOaVersionReq, ProtoOaVersionRes,
    };

    #[test]
    fn declared_payload_types_match_the_defaults_in_the_proto_files() {
        let declared = declared();
        assert_eq!(declared.len(), 64);
        for (name, constant, default) in declared {
            assert_eq!(i64::from(constant), i64::from(default), "{name}");
        }
        assert_eq!(ProtoHeartbeatEvent::PAYLOAD_TYPE, 51);
        assert_eq!(ProtoOaVersionReq::PAYLOAD_TYPE, 2104);
    }

    #[test]
    fn only_requests_for_historical_data_are_marked_historical() {
        fn historical<R: Request>(_: &R) -> bool {
            R::HISTORICAL
        }
        assert!(historical(&ProtoOaGetTrendbarsReq::default()));
        assert!(!historical(&ProtoOaVersionReq::default()));
        assert!(is_historical(ProtoOaGetTrendbarsReq::PAYLOAD_TYPE));
        assert!(!is_historical(ProtoOaVersionReq::PAYLOAD_TYPE));
        assert!(!is_historical(0));
    }

    #[test]
    fn account_is_read_from_scoped_messages_only() {
        let account = AccountId::new(7).expect("account id");
        let scoped = ProtoOaTraderReq {
            payload_type: None,
            ctid_trader_account_id: 7,
        };
        assert_eq!(
            account_of(ProtoOaTraderReq::PAYLOAD_TYPE, &scoped.encode_to_vec()),
            Some(account)
        );
        let authorisation = ProtoOaAccountAuthReq {
            payload_type: None,
            ctid_trader_account_id: 7,
            access_token: "token".to_owned(),
        };
        assert_eq!(
            account_of(
                ProtoOaAccountAuthReq::PAYLOAD_TYPE,
                &authorisation.encode_to_vec()
            ),
            Some(account)
        );
        let list = ProtoOaGetAccountListByAccessTokenReq {
            payload_type: None,
            access_token: "token".to_owned(),
        };
        assert_eq!(
            account_of(
                ProtoOaGetAccountListByAccessTokenReq::PAYLOAD_TYPE,
                &list.encode_to_vec()
            ),
            None
        );
        assert_eq!(account_of(ProtoOaVersionReq::PAYLOAD_TYPE, &[]), None);
        assert_eq!(account_of(ProtoOaTraderReq::PAYLOAD_TYPE, &[0xff]), None);
        let nobody = ProtoOaTraderReq {
            payload_type: None,
            ctid_trader_account_id: 0,
        };
        assert_eq!(
            account_of(ProtoOaTraderReq::PAYLOAD_TYPE, &nobody.encode_to_vec()),
            None
        );
    }

    #[test]
    fn only_the_three_events_end_sessions() {
        let account = |id| AccountId::new(id).expect("account id");
        let invalidated = ProtoOaAccountsTokenInvalidatedEvent {
            payload_type: None,
            ctid_trader_account_ids: vec![7, 0, 8],
            reason: Some("refreshed".to_owned()),
        };
        let cause = SessionEndCause::TokenInvalidated {
            reason: Some("refreshed".to_owned()),
        };
        assert_eq!(
            sessions_ended_by(&invalidated.to_message(None)),
            vec![
                SessionEnded::new(Some(account(7)), cause.clone()),
                SessionEnded::new(Some(account(8)), cause),
            ]
        );
        let dropped = ProtoOaAccountDisconnectEvent {
            payload_type: None,
            ctid_trader_account_id: 7,
        };
        assert_eq!(
            sessions_ended_by(&dropped.to_message(None)),
            vec![SessionEnded::new(
                Some(account(7)),
                SessionEndCause::AccountDisconnected
            )]
        );
        let nobody = ProtoOaAccountDisconnectEvent {
            payload_type: None,
            ctid_trader_account_id: -1,
        };
        assert_eq!(sessions_ended_by(&nobody.to_message(None)), Vec::new());
        let client = ProtoOaClientDisconnectEvent {
            payload_type: None,
            reason: None,
        };
        assert_eq!(
            sessions_ended_by(&client.to_message(None)),
            vec![SessionEnded::new(
                None,
                SessionEndCause::ClientDisconnected { reason: None }
            )]
        );
        assert_eq!(
            sessions_ended_by(&ProtoOaVersionReq::default().to_message(None)),
            Vec::new()
        );
    }

    #[test]
    fn message_round_trips_through_its_envelope() {
        let response = ProtoOaVersionRes {
            payload_type: None,
            version: "97".to_owned(),
        };
        let envelope = response.to_message(Some("id".to_owned()));
        assert_eq!(envelope.payload_type, 2105);
        assert_eq!(envelope.client_msg_id.as_deref(), Some("id"));
        assert_eq!(ProtoOaVersionRes::from_message(&envelope), Ok(response));
    }

    #[test]
    fn envelope_of_another_type_is_rejected() {
        let envelope = ProtoOaVersionReq::default().to_message(None);
        assert_eq!(
            ProtoOaVersionRes::from_message(&envelope),
            Err(ConnectorError::UnexpectedResponse {
                expected: 2105,
                received: 2104,
            })
        );
    }

    #[test]
    fn malformed_payload_is_a_decode_error_and_an_absent_one_is_empty() {
        let envelope = ProtoMessage {
            payload_type: 2105,
            payload: Some(vec![0xff]),
            client_msg_id: None,
        };
        assert!(matches!(
            ProtoOaVersionRes::from_message(&envelope),
            Err(ConnectorError::Transport(TransportError::Decode(_)))
        ));
        let empty = ProtoMessage {
            payload_type: 2105,
            payload: None,
            client_msg_id: None,
        };
        assert_eq!(
            ProtoOaVersionRes::from_message(&empty),
            Ok(ProtoOaVersionRes::default())
        );
    }
}
