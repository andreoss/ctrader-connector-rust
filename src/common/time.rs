//! Points in time as the wire carries them: milliseconds since the Unix epoch, in UTC.
//!
//! ```
//! use std::time::Duration;
//!
//! use ctrader_connector::common::time::Timestamp;
//!
//! let start = Timestamp::from_millis(1_700_000_000_000)?;
//! let end = start.checked_add(Duration::from_secs(60)).unwrap_or(start);
//! assert_eq!(end.as_millis(), 1_700_000_060_000);
//! assert!(start < end);
//! # Ok::<(), ctrader_connector::common::errors::ValueError>(())
//! ```

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::errors::ValueError;

const TIMESTAMP: &str = "timestamp";
const MILLIS_PER_MINUTE: i64 = 60_000;

/// Moment in UTC with millisecond resolution; never before the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The Unix epoch.
    pub const EPOCH: Self = Self(0);

    /// Wraps a number of milliseconds since the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::OutOfRange`] when the number is negative.
    pub const fn from_millis(millis: i64) -> Result<Self, ValueError> {
        if millis < 0 {
            Err(ValueError::OutOfRange { kind: TIMESTAMP })
        } else {
            Ok(Self(millis))
        }
    }

    /// Converts a number of minutes since the Unix epoch, as trendbars carry it.
    #[must_use]
    pub fn from_minutes(minutes: u32) -> Self {
        Self(i64::from(minutes).saturating_mul(MILLIS_PER_MINUTE))
    }

    /// Returns the current time of the system clock.
    ///
    /// A clock set before the Unix epoch yields the epoch.
    #[must_use]
    pub fn now() -> Self {
        Self::try_from(SystemTime::now()).unwrap_or(Self::EPOCH)
    }

    /// Returns the number of milliseconds since the Unix epoch.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// Returns the moment `duration` later, or `None` when it cannot be represented.
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let millis = i64::try_from(duration.as_millis()).ok()?;
        self.0.checked_add(millis).map(Self)
    }

    /// Returns the moment `duration` earlier, or `None` when it lies before the Unix epoch.
    #[must_use]
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        let millis = i64::try_from(duration.as_millis()).ok()?;
        Self::from_millis(self.0.checked_sub(millis)?).ok()
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = ValueError;

    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let out_of_range = ValueError::OutOfRange { kind: TIMESTAMP };
        let elapsed = time
            .duration_since(UNIX_EPOCH)
            .map_err(|_| out_of_range.clone())?;
        i64::try_from(elapsed.as_millis())
            .map(Self)
            .map_err(|_| out_of_range)
    }
}

impl From<Timestamp> for SystemTime {
    fn from(timestamp: Timestamp) -> Self {
        UNIX_EPOCH + Duration::from_millis(timestamp.0.unsigned_abs())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::Timestamp;
    use crate::common::errors::ValueError;

    const OUT_OF_RANGE: ValueError = ValueError::OutOfRange { kind: "timestamp" };

    #[test]
    fn milliseconds_round_trip_and_negative_ones_are_rejected() {
        let timestamp = Timestamp::from_millis(1_700_000_000_123).expect("timestamp");
        assert_eq!(timestamp.as_millis(), 1_700_000_000_123);
        assert_eq!(timestamp.to_string(), "1700000000123");
        assert_eq!(Timestamp::from_millis(0), Ok(Timestamp::EPOCH));
        assert_eq!(Timestamp::from_millis(-1), Err(OUT_OF_RANGE));
    }

    #[test]
    fn minutes_convert_to_milliseconds() {
        assert_eq!(Timestamp::from_minutes(0), Timestamp::EPOCH);
        assert_eq!(Timestamp::from_minutes(2).as_millis(), 120_000);
        assert_eq!(
            Timestamp::from_minutes(u32::MAX).as_millis(),
            i64::from(u32::MAX) * 60_000
        );
    }

    #[test]
    fn arithmetic_stays_within_the_representable_range() {
        let start = Timestamp::from_millis(1_000).expect("timestamp");
        let later = start.checked_add(Duration::from_millis(500));
        assert_eq!(later.map(Timestamp::as_millis), Some(1_500));
        let earlier = start.checked_sub(Duration::from_millis(1_000));
        assert_eq!(earlier, Some(Timestamp::EPOCH));
        assert_eq!(start.checked_sub(Duration::from_millis(1_001)), None);
        assert_eq!(start.checked_add(Duration::MAX), None);
        assert_eq!(start.checked_sub(Duration::MAX), None);
        let last = Timestamp::from_millis(i64::MAX).expect("timestamp");
        assert_eq!(last.checked_add(Duration::from_millis(1)), None);
        assert!(start < last);
    }

    #[test]
    fn system_time_converts_both_ways() {
        let time = UNIX_EPOCH + Duration::from_millis(42);
        let timestamp = Timestamp::try_from(time).expect("timestamp");
        assert_eq!(timestamp.as_millis(), 42);
        assert_eq!(SystemTime::from(timestamp), time);
        assert_eq!(
            Timestamp::try_from(UNIX_EPOCH - Duration::from_secs(1)),
            Err(OUT_OF_RANGE)
        );
        assert!(Timestamp::now() > timestamp);
    }
}
