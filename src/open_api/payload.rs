//! Typed pairing of messages with their payload types and of requests with their responses.

use prost::Message;

use crate::common::errors::{ConnectorError, TransportError};
use crate::open_api::messages::{
    ProtoErrorRes, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
    ProtoOaAccountDisconnectEvent, ProtoOaAccountsTokenInvalidatedEvent, ProtoOaApplicationAuthReq,
    ProtoOaApplicationAuthRes, ProtoOaAssetListReq, ProtoOaAssetListRes,
    ProtoOaCashFlowHistoryListReq, ProtoOaCashFlowHistoryListRes, ProtoOaClientDisconnectEvent,
    ProtoOaDealListReq, ProtoOaDealListRes, ProtoOaErrorRes, ProtoOaGetAccountListByAccessTokenReq,
    ProtoOaGetAccountListByAccessTokenRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes,
    ProtoOaGetTrendbarsReq, ProtoOaGetTrendbarsRes, ProtoOaPayloadType, ProtoOaRefreshTokenReq,
    ProtoOaRefreshTokenRes, ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes,
    ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq, ProtoOaSymbolsListRes,
    ProtoOaUnsubscribeSpotsReq, ProtoOaUnsubscribeSpotsRes, ProtoOaVersionReq, ProtoOaVersionRes,
    ProtoPayloadType,
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
    ProtoOaAssetListReq => ProtoOaPayloadType::ProtoOaAssetListReq,
    ProtoOaAssetListRes => ProtoOaPayloadType::ProtoOaAssetListRes,
    ProtoOaSymbolsListReq => ProtoOaPayloadType::ProtoOaSymbolsListReq,
    ProtoOaSymbolsListRes => ProtoOaPayloadType::ProtoOaSymbolsListRes,
    ProtoOaSymbolByIdReq => ProtoOaPayloadType::ProtoOaSymbolByIdReq,
    ProtoOaSymbolByIdRes => ProtoOaPayloadType::ProtoOaSymbolByIdRes,
}

requests! {
    ProtoOaApplicationAuthReq => ProtoOaApplicationAuthRes, false;
    ProtoOaAccountAuthReq => ProtoOaAccountAuthRes, false;
    ProtoOaVersionReq => ProtoOaVersionRes, false;
    ProtoOaSubscribeSpotsReq => ProtoOaSubscribeSpotsRes, false;
    ProtoOaUnsubscribeSpotsReq => ProtoOaUnsubscribeSpotsRes, false;
    ProtoOaGetAccountListByAccessTokenReq => ProtoOaGetAccountListByAccessTokenRes, false;
    ProtoOaRefreshTokenReq => ProtoOaRefreshTokenRes, false;
    ProtoOaAssetListReq => ProtoOaAssetListRes, false;
    ProtoOaSymbolsListReq => ProtoOaSymbolsListRes, false;
    ProtoOaSymbolByIdReq => ProtoOaSymbolByIdRes, false;
    ProtoOaDealListReq => ProtoOaDealListRes, true;
    ProtoOaGetTrendbarsReq => ProtoOaGetTrendbarsRes, true;
    ProtoOaCashFlowHistoryListReq => ProtoOaCashFlowHistoryListRes, true;
    ProtoOaGetTickDataReq => ProtoOaGetTickDataRes, true;
}

#[cfg(test)]
mod tests {
    use super::{Payload, Request, declared, is_historical};
    use crate::common::errors::{ConnectorError, TransportError};
    use crate::open_api::messages::{
        ProtoHeartbeatEvent, ProtoMessage, ProtoOaGetTrendbarsReq, ProtoOaVersionReq,
        ProtoOaVersionRes,
    };

    #[test]
    fn declared_payload_types_match_the_defaults_in_the_proto_files() {
        let declared = declared();
        assert_eq!(declared.len(), 35);
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
