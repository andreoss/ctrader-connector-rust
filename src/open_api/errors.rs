//! Conversions from the error messages of the Open API to the shared error types.

use crate::common::errors::{ErrorCode, ServerError};
use crate::open_api::messages::{ProtoErrorCode, ProtoErrorRes, ProtoOaErrorCode, ProtoOaErrorRes};

impl From<ProtoErrorCode> for ErrorCode {
    fn from(code: ProtoErrorCode) -> Self {
        Self::from_wire(code.as_str_name())
    }
}

impl From<ProtoOaErrorCode> for ErrorCode {
    fn from(code: ProtoOaErrorCode) -> Self {
        Self::from_wire(code.as_str_name())
    }
}

impl From<ProtoErrorRes> for ServerError {
    fn from(response: ProtoErrorRes) -> Self {
        let mut error = Self::new(ErrorCode::from_wire(&response.error_code));
        error.description = response.description;
        error.maintenance_end_timestamp = response
            .maintenance_end_timestamp
            .and_then(|timestamp| i64::try_from(timestamp).ok());
        error
    }
}

impl From<ProtoOaErrorRes> for ServerError {
    fn from(response: ProtoOaErrorRes) -> Self {
        let mut error = Self::new(ErrorCode::from_wire(&response.error_code));
        error.description = response.description;
        error.maintenance_end_timestamp = response.maintenance_end_timestamp;
        error
    }
}

#[cfg(test)]
mod tests {
    use crate::common::errors::{ErrorCode, ServerError};
    use crate::open_api::messages::{
        ProtoErrorCode, ProtoErrorRes, ProtoOaErrorCode, ProtoOaErrorRes,
    };

    #[test]
    fn every_common_code_maps_to_a_variant() {
        let codes: Vec<_> = (0..=1000)
            .filter_map(|number| ProtoErrorCode::try_from(number).ok())
            .collect();
        assert_eq!(codes.len(), 10);
        for code in codes {
            let mapped = ErrorCode::from(code);
            assert!(mapped.is_known(), "{code:?}");
            assert_eq!(mapped.as_str(), code.as_str_name());
        }
        assert_eq!(
            ErrorCode::from(ProtoErrorCode::MarketClosed),
            ErrorCode::MarketClosed
        );
    }

    #[test]
    fn every_open_api_code_maps_to_a_variant() {
        let codes: Vec<_> = (0..=1000)
            .filter_map(|number| ProtoOaErrorCode::try_from(number).ok())
            .collect();
        assert_eq!(codes.len(), 43);
        for code in codes {
            let mapped = ErrorCode::from(code);
            assert!(mapped.is_known(), "{code:?}");
            assert_eq!(mapped.as_str(), code.as_str_name());
        }
        assert_eq!(
            ErrorCode::from(ProtoOaErrorCode::ChAccessTokenInvalid),
            ErrorCode::ChAccessTokenInvalid
        );
    }

    #[test]
    fn common_error_response_keeps_code_and_detail() {
        let error = ServerError::from(ProtoErrorRes {
            payload_type: None,
            error_code: "FRAME_TOO_LONG".to_owned(),
            description: Some("frame of 70000 bytes".to_owned()),
            maintenance_end_timestamp: Some(1_700_000_000_000),
        });
        assert_eq!(error.code, ErrorCode::FrameTooLong);
        assert_eq!(error.description.as_deref(), Some("frame of 70000 bytes"));
        assert_eq!(error.maintenance_end_timestamp, Some(1_700_000_000_000));
        let overflowing = ServerError::from(ProtoErrorRes {
            payload_type: None,
            error_code: "UNKNOWN_ERROR".to_owned(),
            description: None,
            maintenance_end_timestamp: Some(u64::MAX),
        });
        assert_eq!(overflowing.maintenance_end_timestamp, None);
    }

    #[test]
    fn open_api_error_response_keeps_code_and_detail() {
        let error = ServerError::from(ProtoOaErrorRes {
            payload_type: None,
            ctid_trader_account_id: Some(7),
            error_code: "SERVER_IS_UNDER_MAINTENANCE".to_owned(),
            description: Some("back soon".to_owned()),
            maintenance_end_timestamp: Some(1_700_000_000_000),
        });
        assert_eq!(error.code, ErrorCode::ServerIsUnderMaintenance);
        assert_eq!(error.description.as_deref(), Some("back soon"));
        assert_eq!(error.maintenance_end_timestamp, Some(1_700_000_000_000));
    }

    #[test]
    fn unknown_code_in_a_response_is_kept_as_text() {
        let error = ServerError::from(ProtoOaErrorRes {
            payload_type: None,
            ctid_trader_account_id: None,
            error_code: "BRAND_NEW_CODE".to_owned(),
            description: None,
            maintenance_end_timestamp: None,
        });
        assert_eq!(error.code, ErrorCode::Unknown("BRAND_NEW_CODE".to_owned()));
        assert_eq!(error.to_string(), "server error BRAND_NEW_CODE");
    }
}
