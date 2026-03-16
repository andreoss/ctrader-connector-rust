//! Typed identifiers.
//!
//! Each kind of identifier is its own type, so an order id cannot be passed where a position id
//! is expected. The server numbers entities from one; zero and negative values are rejected.

use std::fmt;
use std::str::FromStr;

use crate::common::errors::ValueError;

macro_rules! id {
    ($(#[$doc:meta])* $name:ident, $kind:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(i64);

        impl $name {
            /// Wraps a value received from or sent to the server.
            ///
            /// # Errors
            ///
            /// Returns [`ValueError::InvalidId`] when the value is zero or negative.
            pub fn new(value: i64) -> Result<Self, ValueError> {
                if value > 0 {
                    Ok(Self(value))
                } else {
                    Err(ValueError::InvalidId {
                        kind: $kind,
                        value: value.to_string(),
                    })
                }
            }

            /// Returns the value as the wire carries it.
            #[must_use]
            pub const fn get(self) -> i64 {
                self.0
            }
        }

        impl TryFrom<i64> for $name {
            type Error = ValueError;

            fn try_from(value: i64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for i64 {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl FromStr for $name {
            type Err = ValueError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                let invalid = || ValueError::InvalidId {
                    kind: $kind,
                    value: text.to_owned(),
                };
                Self::new(text.parse().map_err(|_| invalid())?).map_err(|_| invalid())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id!(
    /// Identifier of a trading account.
    AccountId,
    "account id"
);
id!(
    /// Identifier of a tradable symbol.
    SymbolId,
    "symbol id"
);
id!(
    /// Identifier of an order.
    OrderId,
    "order id"
);
id!(
    /// Identifier of a position.
    PositionId,
    "position id"
);
id!(
    /// Identifier of a deal.
    DealId,
    "deal id"
);

id!(
    /// Identifier of an asset, such as a currency.
    AssetId,
    "asset id"
);
id!(
    /// Identifier of a category of symbols.
    SymbolCategoryId,
    "symbol category id"
);

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{AccountId, AssetId, DealId, OrderId, PositionId, SymbolCategoryId, SymbolId};
    use crate::common::errors::ValueError;

    macro_rules! id_tests {
        ($($module:ident: $name:ident, $kind:literal;)*) => {$(
            mod $module {
                use super::*;

                fn invalid(value: &str) -> ValueError {
                    ValueError::InvalidId {
                        kind: $kind,
                        value: value.to_owned(),
                    }
                }

                #[test]
                fn positive_values_round_trip() -> Result<(), ValueError> {
                    for value in [1, 42, i64::MAX] {
                        let id = $name::new(value)?;
                        assert_eq!(id.get(), value);
                        assert_eq!(i64::from(id), value);
                        assert_eq!($name::try_from(value), Ok(id));
                        assert_eq!(id.to_string(), value.to_string());
                        assert_eq!(id.to_string().parse(), Ok(id));
                    }
                    Ok(())
                }

                #[test]
                fn zero_and_negative_values_are_rejected() {
                    for value in [0, -1, i64::MIN] {
                        let text = value.to_string();
                        assert_eq!($name::new(value), Err(invalid(&text)));
                        assert_eq!($name::try_from(value), Err(invalid(&text)));
                        assert_eq!(text.parse::<$name>(), Err(invalid(&text)));
                    }
                }

                #[test]
                fn text_that_is_not_a_number_is_rejected() {
                    for text in ["", " 7", "7.0", "seven", "9223372036854775808"] {
                        assert_eq!(text.parse::<$name>(), Err(invalid(text)));
                    }
                }

                #[test]
                fn rejection_names_the_kind() {
                    assert_eq!(
                        invalid("0").to_string(),
                        concat!($kind, " must be a positive integer, got 0")
                    );
                }

                #[test]
                fn ids_order_and_hash_by_value() -> Result<(), ValueError> {
                    let low = $name::new(1)?;
                    let high = $name::new(2)?;
                    assert!(low < high);
                    let set: HashSet<_> = [low, high, low].into_iter().collect();
                    assert_eq!(set.len(), 2);
                    Ok(())
                }
            }
        )*};
    }

    id_tests! {
        account: AccountId, "account id";
        symbol: SymbolId, "symbol id";
        order: OrderId, "order id";
        position: PositionId, "position id";
        deal: DealId, "deal id";
        asset: AssetId, "asset id";
        symbol_category: SymbolCategoryId, "symbol category id";
    }
}
