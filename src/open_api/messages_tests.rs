//! Cross-checks of the generated messages against the vendored proto files.

use prost::Message;

use super::messages::*;
use crate::proto_text::{enums as proto_enums, messages as proto_messages};

macro_rules! check_enums {
    ($($name:ident),* $(,)?) => {{
        let mut expected = proto_enums();
        $(
            let key = stringify!($name).to_lowercase();
            let values = expected.remove(&key).expect(stringify!($name));
            assert!(!values.is_empty(), "{key} has no values");
            for (text, number) in values {
                let value = $name::from_str_name(&text).expect(&text);
                assert_eq!(value as i32, number, "{text}");
                assert_eq!(value.as_str_name(), text);
                assert_eq!($name::try_from(number), Ok(value));
            }
            assert_eq!($name::from_str_name("NOT_A_VALUE"), None);
            assert!($name::try_from(i32::MIN).is_err());
        )*
        assert_eq!(expected.into_keys().collect::<Vec<_>>(), Vec::<String>::new());
    }};
}

type Check = (&'static str, fn() -> Option<&'static str>);

macro_rules! round_trip {
    ($name:ident) => {{
        let message = $name::default();
        assert_eq!(
            $name::decode(message.encode_to_vec().as_slice()),
            Ok(message.clone())
        );
        message
    }};
}

macro_rules! with_payload_type {
    ($name:ident) => {
        Some(round_trip!($name).payload_type().as_str_name())
    };
}

macro_rules! without_payload_type {
    ($name:ident) => {{
        round_trip!($name);
        None
    }};
}

macro_rules! checks {
    ($table:ident, $check:ident, $($name:ident),* $(,)?) => {
        const $table: &[Check] = &[$((stringify!($name), || $check!($name))),*];
    };
}

checks!(
    WITH_PAYLOAD_TYPE,
    with_payload_type,
    ProtoErrorRes,
    ProtoHeartbeatEvent,
    ProtoOaApplicationAuthReq,
    ProtoOaApplicationAuthRes,
    ProtoOaAccountAuthReq,
    ProtoOaAccountAuthRes,
    ProtoOaErrorRes,
    ProtoOaClientDisconnectEvent,
    ProtoOaAccountsTokenInvalidatedEvent,
    ProtoOaVersionReq,
    ProtoOaVersionRes,
    ProtoOaNewOrderReq,
    ProtoOaExecutionEvent,
    ProtoOaCancelOrderReq,
    ProtoOaAmendOrderReq,
    ProtoOaAmendPositionSltpReq,
    ProtoOaClosePositionReq,
    ProtoOaTrailingSlChangedEvent,
    ProtoOaAssetListReq,
    ProtoOaAssetListRes,
    ProtoOaSymbolsListReq,
    ProtoOaSymbolsListRes,
    ProtoOaSymbolByIdReq,
    ProtoOaSymbolByIdRes,
    ProtoOaSymbolsForConversionReq,
    ProtoOaSymbolsForConversionRes,
    ProtoOaSymbolChangedEvent,
    ProtoOaAssetClassListReq,
    ProtoOaAssetClassListRes,
    ProtoOaTraderReq,
    ProtoOaTraderRes,
    ProtoOaTraderUpdatedEvent,
    ProtoOaReconcileReq,
    ProtoOaReconcileRes,
    ProtoOaOrderErrorEvent,
    ProtoOaDealListReq,
    ProtoOaDealListRes,
    ProtoOaOrderListReq,
    ProtoOaOrderListRes,
    ProtoOaExpectedMarginReq,
    ProtoOaExpectedMarginRes,
    ProtoOaMarginChangedEvent,
    ProtoOaCashFlowHistoryListReq,
    ProtoOaCashFlowHistoryListRes,
    ProtoOaGetAccountListByAccessTokenReq,
    ProtoOaGetAccountListByAccessTokenRes,
    ProtoOaRefreshTokenReq,
    ProtoOaRefreshTokenRes,
    ProtoOaSubscribeSpotsReq,
    ProtoOaSubscribeSpotsRes,
    ProtoOaUnsubscribeSpotsReq,
    ProtoOaUnsubscribeSpotsRes,
    ProtoOaSpotEvent,
    ProtoOaSubscribeLiveTrendbarReq,
    ProtoOaSubscribeLiveTrendbarRes,
    ProtoOaUnsubscribeLiveTrendbarReq,
    ProtoOaUnsubscribeLiveTrendbarRes,
    ProtoOaGetTrendbarsReq,
    ProtoOaGetTrendbarsRes,
    ProtoOaGetTickDataReq,
    ProtoOaGetTickDataRes,
    ProtoOaGetCtidProfileByTokenReq,
    ProtoOaGetCtidProfileByTokenRes,
    ProtoOaDepthEvent,
    ProtoOaSubscribeDepthQuotesReq,
    ProtoOaSubscribeDepthQuotesRes,
    ProtoOaUnsubscribeDepthQuotesReq,
    ProtoOaUnsubscribeDepthQuotesRes,
    ProtoOaSymbolCategoryListReq,
    ProtoOaSymbolCategoryListRes,
    ProtoOaAccountLogoutReq,
    ProtoOaAccountLogoutRes,
    ProtoOaAccountDisconnectEvent,
    ProtoOaMarginCallListReq,
    ProtoOaMarginCallListRes,
    ProtoOaMarginCallUpdateReq,
    ProtoOaMarginCallUpdateRes,
    ProtoOaMarginCallUpdateEvent,
    ProtoOaMarginCallTriggerEvent,
    ProtoOaGetDynamicLeverageByIdReq,
    ProtoOaGetDynamicLeverageByIdRes,
    ProtoOaDealListByPositionIdReq,
    ProtoOaDealListByPositionIdRes,
    ProtoOaOrderDetailsReq,
    ProtoOaOrderDetailsRes,
    ProtoOaOrderListByPositionIdReq,
    ProtoOaOrderListByPositionIdRes,
    ProtoOaDealOffsetListReq,
    ProtoOaDealOffsetListRes,
    ProtoOaGetPositionUnrealizedPnLReq,
    ProtoOaGetPositionUnrealizedPnLRes,
);

checks!(
    WITHOUT_PAYLOAD_TYPE,
    without_payload_type,
    ProtoMessage,
    ProtoOaArchivedSymbol,
    ProtoOaAsset,
    ProtoOaAssetClass,
    ProtoOaBonusDepositWithdraw,
    ProtoOaClosePositionDetail,
    ProtoOaCtidProfile,
    ProtoOaCtidTraderAccount,
    ProtoOaDeal,
    ProtoOaDealOffset,
    ProtoOaDepositWithdraw,
    ProtoOaDepthQuote,
    ProtoOaDynamicLeverage,
    ProtoOaDynamicLeverageTier,
    ProtoOaExpectedMargin,
    ProtoOaHoliday,
    ProtoOaInterval,
    ProtoOaLightSymbol,
    ProtoOaMarginCall,
    ProtoOaOrder,
    ProtoOaPosition,
    ProtoOaPositionUnrealizedPnL,
    ProtoOaSymbol,
    ProtoOaSymbolCategory,
    ProtoOaTickData,
    ProtoOaTradeData,
    ProtoOaTrader,
    ProtoOaTrendbar,
);

#[test]
fn every_proto_enum_value_is_generated_with_its_name_and_number() {
    check_enums!(
        ProtoPayloadType,
        ProtoErrorCode,
        ProtoOaPayloadType,
        ProtoOaDayOfWeek,
        ProtoOaCommissionType,
        ProtoOaSymbolDistanceType,
        ProtoOaMinCommissionType,
        ProtoOaTradingMode,
        ProtoOaSwapCalculationType,
        ProtoOaAccessRights,
        ProtoOaTotalMarginCalculationType,
        ProtoOaAccountType,
        ProtoOaPositionStatus,
        ProtoOaTradeSide,
        ProtoOaOrderType,
        ProtoOaTimeInForce,
        ProtoOaOrderStatus,
        ProtoOaOrderTriggerMethod,
        ProtoOaExecutionType,
        ProtoOaChangeBonusType,
        ProtoOaChangeBalanceType,
        ProtoOaDealStatus,
        ProtoOaTrendbarPeriod,
        ProtoOaQuoteType,
        ProtoOaClientPermissionScope,
        ProtoOaNotificationType,
        ProtoOaErrorCode,
        ProtoOaLimitedRiskMarginCalculationStrategy,
    );
}

#[test]
fn every_proto_message_is_generated_with_its_default_payload_type() {
    let mut expected = proto_messages();
    for (name, check) in WITH_PAYLOAD_TYPE.iter().chain(WITHOUT_PAYLOAD_TYPE) {
        let default = expected.remove(&name.to_lowercase()).expect(name);
        assert_eq!(check(), default.as_deref(), "{name}");
    }
    assert_eq!(
        expected.into_keys().collect::<Vec<_>>(),
        Vec::<String>::new()
    );
    assert_eq!(WITH_PAYLOAD_TYPE.len(), 91);
    assert_eq!(WITHOUT_PAYLOAD_TYPE.len(), 28);
}

#[test]
fn error_response_survives_the_wire() {
    let sent = ProtoOaErrorRes {
        payload_type: None,
        ctid_trader_account_id: Some(42),
        error_code: "MARKET_CLOSED".to_owned(),
        description: Some("closed".to_owned()),
        maintenance_end_timestamp: None,
    };
    let frame = ProtoMessage {
        payload_type: sent.payload_type() as u32,
        payload: Some(sent.encode_to_vec()),
        client_msg_id: Some("1".to_owned()),
    };
    let received = ProtoMessage::decode(frame.encode_to_vec().as_slice()).expect("frame decodes");
    assert_eq!(received.payload_type, 2142);
    assert_eq!(received.client_msg_id(), "1");
    let error = ProtoOaErrorRes::decode(received.payload()).expect("payload decodes");
    assert_eq!(error, sent);
    assert_eq!(error.description(), "closed");
    assert_eq!(error.ctid_trader_account_id(), 42);
    assert_eq!(error.maintenance_end_timestamp(), 0);
}
