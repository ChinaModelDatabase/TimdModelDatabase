/// TTL manager - handles data retention and expiration.

use crate::TsUnit;

/// TTL configuration
#[derive(Debug, Clone)]
pub struct TtlConfig {
    pub retention_days: u32,
    pub check_interval_secs: u64,
    /// Unit the timestamps passed to [`TtlManager`] are expressed in.
    ///
    /// Defaults to milliseconds, which is what this type assumed before units
    /// existed. A table storing seconds or nanoseconds has to say so, or every
    /// cutoff it computes is off by a factor of a thousand or a million.
    pub ts_unit: TsUnit,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            retention_days: 365,
            check_interval_secs: 3600, // check every hour
            ts_unit: TsUnit::Millis,
        }
    }
}

impl TtlConfig {
    /// Same config, with timestamps in `ts_unit`.
    pub fn with_unit(mut self, ts_unit: TsUnit) -> Self {
        self.ts_unit = ts_unit;
        self
    }
}

/// TTL manager
pub struct TtlManager {
    config: TtlConfig,
}

impl TtlManager {
    pub fn new(config: TtlConfig) -> Self {
        Self { config }
    }

    /// Unit the timestamps given to this manager are in.
    pub fn ts_unit(&self) -> TsUnit {
        self.config.ts_unit
    }

    /// How many ticks of this manager's unit make up the whole retention window.
    fn retention(&self) -> i64 {
        self.config.ts_unit.per_day().saturating_mul(self.config.retention_days as i64)
    }

    /// Check if a timestamp has expired
    pub fn is_expired(&self, ts: i64, now: i64) -> bool {
        // A zero retention window would otherwise expire everything at once,
        // including rows written at exactly `now`.
        if self.config.retention_days == 0 {
            return false;
        }
        now.saturating_sub(ts) > self.retention()
    }

    /// Get the retention cutoff timestamp
    pub fn cutoff_ts(&self, now: i64) -> i64 {
        now.saturating_sub(self.retention())
    }

    /// Get retention days
    pub fn retention_days(&self) -> u32 {
        self.config.retention_days
    }

    /// Set retention days
    pub fn set_retention_days(&mut self, days: u32) {
        self.config.retention_days = days;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_expiry() {
        let ttl = TtlManager::new(TtlConfig {
            retention_days: 30,
            ..Default::default()
        });

        let now = 1_700_000_000_000i64; // ~2023-11-14
        let ts_35_days_ago = now - 35 * 24 * 60 * 60 * 1000;
        let ts_25_days_ago = now - 25 * 24 * 60 * 60 * 1000;

        assert!(ttl.is_expired(ts_35_days_ago, now));
        assert!(!ttl.is_expired(ts_25_days_ago, now));
    }

    #[test]
    fn test_cutoff() {
        let ttl = TtlManager::new(TtlConfig {
            retention_days: 90,
            ..Default::default()
        });

        let now = 1_700_000_000_000i64;
        let cutoff = ttl.cutoff_ts(now);
        assert_eq!(cutoff, now - 90 * 24 * 60 * 60 * 1000);
    }

    /// Retention is stated in days but the timestamps are in whatever unit the
    /// table uses, so the window has to be scaled to match.
    ///
    /// Hardcoding milliseconds here meant a seconds table expired a thousand
    /// times too late and a nanosecond table a million times too late: a row
    /// that should have been purged sat on disk forever.
    #[test]
    fn test_retention_respects_the_timestamp_unit() {
        const DAY_MS: i64 = 24 * 60 * 60 * 1000;

        // The same instant, expressed in each unit.
        let cases: &[(TsUnit, i64, i64)] = &[
            (TsUnit::Seconds, 1_700_000_000, DAY_MS / 1_000),
            (TsUnit::Millis, 1_700_000_000_000, DAY_MS),
            (TsUnit::Micros, 1_700_000_000_000_000, DAY_MS * 1_000),
            (TsUnit::Nanos, 1_700_000_000_000_000_000, DAY_MS * 1_000_000),
        ];

        for (unit, now, day) in cases {
            let (now, day) = (*now, *day);
            let ttl = TtlManager::new(TtlConfig {
                retention_days: 30,
                ..Default::default()
            }
            .with_unit(*unit));
            assert_eq!(ttl.ts_unit(), *unit);

            let expired = now - 35 * day;
            let kept = now - 25 * day;

            assert!(
                ttl.is_expired(expired, now),
                "{:?} should have expired a 35-day-old row",
                unit
            );
            assert!(
                !ttl.is_expired(kept, now),
                "{:?} should still hold a 25-day-old row",
                unit
            );
            assert_eq!(ttl.cutoff_ts(now), now - 30 * day, "{:?} cutoff", unit);
        }
    }

    /// A boundary row exactly at the cutoff is still inside the window.
    #[test]
    fn test_expiry_boundary_is_not_expired() {
        let ttl = TtlManager::new(TtlConfig {
            retention_days: 7,
            ..Default::default()
        });
        let now = 1_700_000_000_000i64;
        let day = 24 * 60 * 60 * 1000;

        assert!(!ttl.is_expired(now - 7 * day, now), "exactly at the cutoff");
        assert!(ttl.is_expired(now - 7 * day - 1, now), "one tick past it");
    }

    /// Zero retention means "keep everything".
    ///
    /// Every other component treats a zero window as retention disabled, so this
    /// one expiring the entire table would be a nasty surprise.
    #[test]
    fn test_zero_retention_expires_nothing() {
        let ttl = TtlManager::new(TtlConfig {
            retention_days: 0,
            ..Default::default()
        });
        let now = 1_700_000_000_000i64;

        assert!(!ttl.is_expired(now, now));
        assert!(!ttl.is_expired(0, now), "even an ancient row survives");
    }

    /// Timestamps at the extremes of the range must saturate, not wrap around.
    ///
    /// A plain `now - ts` overflows here and in release mode wraps silently, so
    /// the oldest representable row would come back looking like the newest.
    #[test]
    fn test_extreme_timestamps_do_not_overflow() {
        let ttl = TtlManager::new(TtlConfig {
            retention_days: 365,
            ..Default::default()
        });
        let day = 24 * 60 * 60 * 1000;

        // A row at the very bottom of the range is a long way older than a year.
        assert!(ttl.is_expired(i64::MIN, i64::MAX));
        // And one exactly at the top is not.
        assert!(!ttl.is_expired(i64::MAX, i64::MAX));

        // The cutoff saturates at the bottom rather than wrapping to a large
        // positive number, which would purge everything instead of nothing.
        assert_eq!(ttl.cutoff_ts(i64::MIN), i64::MIN);
        assert_eq!(ttl.cutoff_ts(i64::MAX), i64::MAX - 365 * day);
    }
}
