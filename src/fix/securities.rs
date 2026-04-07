//! Symbols of the account, as the quote session lists them.

use crate::common::errors::{ConnectorError, FixError};
use crate::common::ids::SymbolId;
use crate::fix::client::FixClient;
use crate::fix::messages::{Message, SecurityList, SecurityListRequest};

const REQUEST_PREFIX: &str = "sl";
const ALL_SYMBOLS: u32 = 0;
const VALID_REQUEST: u32 = 0;
const TAG_NAME: u32 = 1007;
const TAG_DIGITS: u32 = 1008;

/// One symbol of the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Security {
    /// Identifier of the symbol, the same the other API of the server uses.
    pub id: SymbolId,
    /// Name of the symbol.
    pub name: String,
    /// Number of decimal places of a price of the symbol.
    pub digits: u32,
}

fn securities(list: SecurityList) -> Result<Vec<Security>, ConnectorError> {
    if list.result != VALID_REQUEST {
        return Err(FixError::BusinessRejected {
            ref_id: Some(list.security_req_id),
            reason: Some(list.result),
            text: None,
        }
        .into());
    }
    let missing = |tag| FixError::MissingField {
        msg_type: SecurityList::MSG_TYPE.to_owned(),
        tag,
    };
    list.symbols
        .into_iter()
        .map(|entry| {
            Ok(Security {
                id: entry.symbol,
                name: entry.name.ok_or_else(|| missing(TAG_NAME))?,
                digits: entry.digits.ok_or_else(|| missing(TAG_DIGITS))?,
            })
        })
        .collect()
}

impl FixClient {
    /// Lists the symbols of the account.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] when the server refuses the request or lists a symbol
    /// without its name or its digits, in which case no symbol is returned, and otherwise the
    /// errors of [`FixClient::request`].
    pub async fn securities(&self) -> Result<Vec<Security>, ConnectorError> {
        let id = self.next_id(REQUEST_PREFIX);
        let request = SecurityListRequest {
            security_req_id: id.clone(),
            request_type: ALL_SYMBOLS,
            symbol: None,
        };
        self.request(request.into(), &id, |message| match message {
            Message::SecurityList(list) if list.security_req_id == id => {
                Some(securities(list.clone()))
            }
            _ => None,
        })
        .await
    }
}
