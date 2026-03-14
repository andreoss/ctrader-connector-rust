//! Exact prices and volumes.
//!
//! The wire carries prices as integers in 1/100000 of a unit and volumes as integers in 0.01 of
//! a unit. Both are held here as decimals, never as floats, and convert to and from the wire
//! integers without loss.
//!
//! ```
//! use ctrader_connector::common::decimals::{Price, Volume};
//!
//! let price = Price::from_wire(123_456, 5)?;
//! assert_eq!(price.to_string(), "1.23456");
//! assert_eq!(price.to_wire()?, 123_456);
//! assert_eq!(Volume::from_wire(150)?.to_string(), "1.50");
//! # Ok::<(), ctrader_connector::common::errors::ValueError>(())
//! ```

use std::fmt;
use std::str::FromStr;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use crate::common::errors::ValueError;

/// Number of decimal places a wire price carries.
pub const PRICE_SCALE: u32 = 5;
/// Number of decimal places a wire volume carries.
pub const VOLUME_SCALE: u32 = 2;

const PRICE: &str = "price";
const VOLUME: &str = "volume";

fn parsed(text: &str, kind: &'static str) -> Result<Decimal, ValueError> {
    Decimal::from_str_exact(text).map_err(|_| ValueError::InvalidDecimal {
        kind,
        text: text.to_owned(),
    })
}

fn non_negative(value: Decimal, kind: &'static str) -> Result<Decimal, ValueError> {
    if value.is_zero() {
        Ok(value.abs())
    } else if value.is_sign_negative() {
        Err(ValueError::Negative { kind, value })
    } else {
        Ok(value)
    }
}

fn from_units(units: i128, scale: u32, kind: &'static str) -> Result<Decimal, ValueError> {
    Decimal::try_from_i128_with_scale(units, scale).map_err(|_| ValueError::OutOfRange { kind })
}

fn to_units(value: Decimal, scale: u32, kind: &'static str) -> Result<Decimal, ValueError> {
    let step = 10_i128
        .checked_pow(scale)
        .ok_or(ValueError::OutOfRange { kind })?;
    let units = from_units(step, 0, kind)?.checked_mul(value);
    match units {
        Some(units) if units.fract().is_zero() => Ok(units),
        _ => Err(ValueError::Unrepresentable { kind, value }),
    }
}

/// Price of a symbol; never negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Price(Decimal);

impl Price {
    /// Wraps a decimal price.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::Negative`] when the value is below zero.
    pub fn new(value: Decimal) -> Result<Self, ValueError> {
        non_negative(value, PRICE).map(Self)
    }

    /// Converts a wire integer, in 1/100000 of a unit, rounding it to the digits of the symbol.
    ///
    /// A value halfway between two steps is rounded away from zero.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::TooManyDigits`] when `digits` exceeds [`PRICE_SCALE`].
    pub fn from_wire(raw: u64, digits: u32) -> Result<Self, ValueError> {
        if digits > PRICE_SCALE {
            return Err(ValueError::TooManyDigits { digits });
        }
        let value = from_units(i128::from(raw), PRICE_SCALE, PRICE)?;
        Ok(Self(value.round_dp_with_strategy(
            digits,
            RoundingStrategy::MidpointAwayFromZero,
        )))
    }

    /// Converts the price to a wire integer, in 1/100000 of a unit.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::Unrepresentable`] when the price is finer than 1/100000 of a unit
    /// or too large for the wire.
    pub fn to_wire(self) -> Result<u64, ValueError> {
        to_units(self.0, PRICE_SCALE, PRICE)?
            .to_u64()
            .ok_or(ValueError::Unrepresentable {
                kind: PRICE,
                value: self.0,
            })
    }

    /// Returns the decimal value.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

impl FromStr for Price {
    type Err = ValueError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(parsed(text, PRICE)?)
    }
}

impl TryFrom<Decimal> for Price {
    type Error = ValueError;

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Price> for Decimal {
    fn from(price: Price) -> Self {
        price.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Volume of an order, position or deal, in units of the symbol; never negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Volume(Decimal);

impl Volume {
    /// Wraps a decimal volume.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::Negative`] when the value is below zero.
    pub fn new(value: Decimal) -> Result<Self, ValueError> {
        non_negative(value, VOLUME).map(Self)
    }

    /// Converts a wire integer, in 0.01 of a unit.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::Negative`] when the integer is below zero.
    pub fn from_wire(raw: i64) -> Result<Self, ValueError> {
        Self::new(from_units(i128::from(raw), VOLUME_SCALE, VOLUME)?)
    }

    /// Converts the volume to a wire integer, in 0.01 of a unit.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::Unrepresentable`] when the volume is finer than 0.01 of a unit or
    /// too large for the wire.
    pub fn to_wire(self) -> Result<i64, ValueError> {
        to_units(self.0, VOLUME_SCALE, VOLUME)?
            .to_i64()
            .ok_or(ValueError::Unrepresentable {
                kind: VOLUME,
                value: self.0,
            })
    }

    /// Returns the decimal value.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

impl FromStr for Volume {
    type Err = ValueError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(parsed(text, VOLUME)?)
    }
}

impl TryFrom<Decimal> for Volume {
    type Error = ValueError;

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Volume> for Decimal {
    fn from(volume: Volume) -> Self {
        volume.0
    }
}

impl fmt::Display for Volume {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{Price, Volume, from_units, to_units};
    use crate::common::errors::ValueError;

    fn decimal(text: &str) -> Decimal {
        Decimal::from_str_exact(text).expect("test literal is a decimal")
    }

    #[test]
    fn wire_price_becomes_a_decimal_at_full_scale() -> Result<(), ValueError> {
        assert_eq!(Price::from_wire(123_456, 5)?.value(), decimal("1.23456"));
        assert_eq!(Price::from_wire(123_456, 5)?.to_string(), "1.23456");
        assert_eq!(Price::from_wire(0, 5)?.value(), Decimal::ZERO);
        assert_eq!(Price::from_wire(1, 5)?.to_string(), "0.00001");
        Ok(())
    }

    #[test]
    fn wire_price_is_rounded_to_symbol_digits() -> Result<(), ValueError> {
        assert_eq!(Price::from_wire(123_456, 3)?.to_string(), "1.235");
        assert_eq!(Price::from_wire(123_444, 3)?.to_string(), "1.234");
        assert_eq!(Price::from_wire(123_450, 3)?.to_string(), "1.235");
        assert_eq!(Price::from_wire(123_350, 3)?.to_string(), "1.234");
        assert_eq!(
            Price::from_wire(15_012_300_000, 2)?.to_string(),
            "150123.00"
        );
        assert_eq!(Price::from_wire(150_000, 0)?.to_string(), "2");
        Ok(())
    }

    #[test]
    fn wire_price_round_trips_for_every_digit_count() -> Result<(), ValueError> {
        for digits in 0..=5 {
            let step = 10_u64.pow(5 - digits);
            for raw in [0, step, 7 * step, 123_456 * step, (u64::MAX / step) * step] {
                assert_eq!(
                    Price::from_wire(raw, digits)?.to_wire()?,
                    raw,
                    "{raw} at {digits}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn symbol_digits_beyond_the_wire_scale_are_rejected() {
        assert_eq!(
            Price::from_wire(1, 6),
            Err(ValueError::TooManyDigits { digits: 6 })
        );
        assert_eq!(
            ValueError::TooManyDigits { digits: 6 }.to_string(),
            "symbol digits must be at most 5, got 6"
        );
    }

    #[test]
    fn prices_parse_from_text() -> Result<(), ValueError> {
        let price: Price = "1.10250".parse()?;
        assert_eq!(price.value(), decimal("1.1025"));
        assert_eq!(price.to_wire()?, 110_250);
        assert_eq!("0".parse::<Price>()?.to_wire()?, 0);
        assert_eq!("-0".parse::<Price>()?.to_wire()?, 0);
        assert_eq!(Price::try_from(decimal("2.5"))?, "2.5".parse()?);
        assert_eq!(Decimal::from(price), decimal("1.1025"));
        Ok(())
    }

    #[test]
    fn bad_price_text_is_rejected() {
        for text in ["", "abc", "1,5", "1.2.3", "1e3", " 1"] {
            assert_eq!(
                text.parse::<Price>(),
                Err(ValueError::InvalidDecimal {
                    kind: "price",
                    text: text.to_owned()
                })
            );
        }
        assert_eq!(
            ValueError::InvalidDecimal {
                kind: "price",
                text: "abc".to_owned()
            }
            .to_string(),
            "price is not a decimal number: abc"
        );
    }

    #[test]
    fn negative_prices_are_rejected() {
        let negative = ValueError::Negative {
            kind: "price",
            value: decimal("-0.00001"),
        };
        assert_eq!(Price::new(decimal("-0.00001")), Err(negative.clone()));
        assert_eq!("-0.00001".parse::<Price>(), Err(negative.clone()));
        assert_eq!(Price::try_from(decimal("-0.00001")), Err(negative.clone()));
        assert_eq!(
            negative.to_string(),
            "price must not be negative, got -0.00001"
        );
    }

    #[test]
    fn prices_the_wire_cannot_carry_are_rejected() -> Result<(), ValueError> {
        for text in [
            "1.000001",
            "184467440737095.51616",
            "79228162514264337593543950335",
        ] {
            let value = decimal(text);
            assert_eq!(
                Price::new(value)?.to_wire(),
                Err(ValueError::Unrepresentable {
                    kind: "price",
                    value
                })
            );
        }
        assert_eq!(
            "184467440737095.51615".parse::<Price>()?.to_wire()?,
            u64::MAX
        );
        assert_eq!(
            ValueError::Unrepresentable {
                kind: "price",
                value: decimal("1.000001")
            }
            .to_string(),
            "price cannot be carried on the wire: 1.000001"
        );
        Ok(())
    }

    #[test]
    fn wire_volume_becomes_a_decimal() -> Result<(), ValueError> {
        assert_eq!(Volume::from_wire(150)?.value(), decimal("1.5"));
        assert_eq!(Volume::from_wire(150)?.to_string(), "1.50");
        assert_eq!(Volume::from_wire(100_000)?.to_string(), "1000.00");
        assert_eq!(Volume::from_wire(0)?.value(), Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn wire_volume_round_trips() -> Result<(), ValueError> {
        for raw in [0, 1, 99, 100, 150, 10_000_000, i64::MAX] {
            assert_eq!(Volume::from_wire(raw)?.to_wire()?, raw);
        }
        Ok(())
    }

    #[test]
    fn volumes_parse_from_text() -> Result<(), ValueError> {
        let volume: Volume = "0.01".parse()?;
        assert_eq!(volume.to_wire()?, 1);
        assert_eq!("1000".parse::<Volume>()?.to_wire()?, 100_000);
        assert_eq!(Volume::try_from(decimal("0.01"))?, volume);
        assert_eq!(Decimal::from(volume), decimal("0.01"));
        Ok(())
    }

    #[test]
    fn negative_volumes_are_rejected() {
        let negative = |text| ValueError::Negative {
            kind: "volume",
            value: decimal(text),
        };
        assert_eq!(Volume::from_wire(-1), Err(negative("-0.01")));
        assert_eq!(Volume::from_wire(i64::MIN).ok(), None);
        assert_eq!(Volume::new(decimal("-2")), Err(negative("-2")));
        assert_eq!("-2".parse::<Volume>(), Err(negative("-2")));
        assert_eq!(Volume::try_from(decimal("-2")), Err(negative("-2")));
    }

    #[test]
    fn bad_volume_text_is_rejected() {
        for text in ["", "lots", "1.0.0"] {
            assert_eq!(
                text.parse::<Volume>(),
                Err(ValueError::InvalidDecimal {
                    kind: "volume",
                    text: text.to_owned()
                })
            );
        }
    }

    #[test]
    fn volumes_the_wire_cannot_carry_are_rejected() -> Result<(), ValueError> {
        for text in [
            "0.001",
            "92233720368547758.08",
            "79228162514264337593543950335",
        ] {
            let value = decimal(text);
            assert_eq!(
                Volume::new(value)?.to_wire(),
                Err(ValueError::Unrepresentable {
                    kind: "volume",
                    value
                })
            );
        }
        assert_eq!(
            "92233720368547758.07".parse::<Volume>()?.to_wire()?,
            i64::MAX
        );
        Ok(())
    }

    #[test]
    fn wire_integers_beyond_the_decimal_range_are_rejected() {
        let out_of_range = ValueError::OutOfRange { kind: "price" };
        assert_eq!(from_units(i128::MAX, 5, "price"), Err(out_of_range.clone()));
        assert_eq!(from_units(1, 29, "price"), Err(out_of_range.clone()));
        assert_eq!(
            to_units(Decimal::ONE, 38, "price"),
            Err(out_of_range.clone())
        );
        assert_eq!(out_of_range.to_string(), "price is out of range");
    }

    #[test]
    fn values_order_numerically() -> Result<(), ValueError> {
        assert!(Price::from_wire(100_000, 5)? < Price::from_wire(100_001, 5)?);
        assert_eq!(Price::from_wire(100_000, 5)?, Price::from_wire(100_000, 1)?);
        assert!(Volume::from_wire(1)? < Volume::from_wire(2)?);
        Ok(())
    }
}
