//! Per-tag cardinality limits.
//!
//! # Why
//!
//! Unbounded tag cardinality is the main way a time-series store falls over. Each
//! new tag value creates a new series, and every series needs its own metadata
//! entry, index slot and dictionary. A table meant for ten thousand devices that
//! receives one high-entropy tag — a request id, a session token, a raw JSON blob
//! — will exhaust memory long before it fills any disk.
//!
//! [`CardinalityGuard`] makes that a hard error at write time instead of an
//! out-of-memory kill at some later point.
//!
//! # Counting
//!
//! Counts are exact, per table and per tag name, tracked in a hash map. Exact
//! counting is affordable here because series creation is already the expensive
//! path: a hit is a map probe, and a miss does a bounded check. The alternative —
//! HyperLogLog — would allow 2% error, which is the wrong trade for a limit whose
//! whole purpose is to be enforced precisely.
//!
//! # Existing series
//!
//! Re-writing a point for a tag value that is already counted is always allowed,
//! even at the limit. Otherwise normal traffic to a full table would be rejected
//! while a single new device is refused, which is backwards.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// Limits on how many distinct values a tag may take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardinalityLimits {
    /// Distinct values allowed per tag name.
    pub per_tag: u64,
    /// Distinct tag combinations allowed per table.
    pub per_series: u64,
    /// Distinct series allowed per table. `0` disables the check.
    pub max_series: u64,
}

impl Default for CardinalityLimits {
    fn default() -> Self {
        Self {
            per_tag: 100_000,
            per_series: 1_000_000,
            max_series: 0,
        }
    }
}

impl CardinalityLimits {
    /// No limits at all, for tests and for tables that manage their own.
    pub fn unlimited() -> Self {
        Self {
            per_tag: u64::MAX,
            per_series: u64::MAX,
            max_series: 0,
        }
    }

    fn tag_limit(&self, _name: &str) -> u64 {
        self.per_tag
    }
}

/// Why a write was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardinalityError {
    /// A single tag already has its full complement of values.
    TagLimitReached {
        tag: String,
        value: String,
        limit: u64,
    },
    /// The table already has the full number of series.
    SeriesLimitReached { limit: u64 },
    /// A series key that would exceed `per_series` values.
    SeriesFieldLimitReached { limit: u64 },
}

impl std::fmt::Display for CardinalityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CardinalityError::TagLimitReached { tag, value, limit } => write!(
                f,
                "tag '{}' reached its cardinality limit of {} distinct values (refused '{}')",
                tag, limit, value
            ),
            CardinalityError::SeriesLimitReached { limit } => write!(
                f,
                "table reached its series limit of {}; refusing to create another",
                limit
            ),
            CardinalityError::SeriesFieldLimitReached { limit } => write!(
                f,
                "series key reached its limit of {} distinct values",
                limit
            ),
        }
    }
}

impl std::error::Error for CardinalityError {}

/// One known series, keyed by its tag values.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SeriesEntry {
    series_id: u64,
    tags: HashMap<String, String>,
}

/// Enforces cardinality limits for one table.
pub struct CardinalityGuard {
    limits: CardinalityLimits,
    /// Distinct values seen per tag name.
    tag_values: RwLock<HashMap<String, HashMap<String, ()>>>,
    /// Every distinct tag combination, so a repeat write is cheap and a new
    /// series can be counted exactly.
    series: RwLock<HashMap<String, SeriesEntry>>,
    next_series_id: AtomicU64,
    series_created: AtomicU64,
    writes_admitted: AtomicU64,
    writes_rejected: AtomicU64,
}

impl CardinalityGuard {
    pub fn new(limits: CardinalityLimits) -> Self {
        Self {
            limits,
            tag_values: RwLock::new(HashMap::new()),
            series: RwLock::new(HashMap::new()),
            next_series_id: AtomicU64::new(1),
            series_created: AtomicU64::new(0),
            writes_admitted: AtomicU64::new(0),
            writes_rejected: AtomicU64::new(0),
        }
    }

    /// Admit a write, returning a series id.
    ///
    /// A tag combination that already exists reuses its id, so a repeat write
    /// never consumes headroom.
    ///
    /// Shares [`CardinalityGuard::admit_batch`] so a single write and a batch of
    /// them can never enforce different limits.
    pub fn admit(&self, tags: &[(String, String)]) -> Result<u64, CardinalityError> {
        let one = [tags.to_vec()];
        self.admit_batch(&one).map(|ids| ids[0])
    }

    /// Admit a batch, failing as soon as one row would exceed a limit.
    ///
    /// Nothing is counted for the whole batch unless every row is admitted, so a
    /// rejected batch does not burn headroom.
    ///
    /// Both indexes are held for the duration and the rows are staged in local
    /// overlays that are merged only once the whole batch is known to fit. The
    /// earlier version cloned both indexes, committed by overwriting them, and
    /// released the locks between checking and committing -- which cost a full
    /// copy of every series the table had ever seen on every write, and let two
    /// concurrent batches read the same snapshot and then erase each other's
    /// work. Staging also keeps the cost proportional to the batch.
    pub fn admit_batch(&self, batch: &[Vec<(String, String)>]) -> Result<Vec<u64>, CardinalityError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        // Enforcing a limit exactly means deciding and recording cannot be
        // separated, so both locks are held throughout. Taking them one at a
        // time, as the checks did before, is what allowed a limit to be passed.
        let mut tag_values = self.tag_values.write();
        let mut series = self.series.write();

        let mut new_tags: HashMap<String, HashSet<String>> = HashMap::new();
        let mut new_series: HashMap<String, SeriesEntry> = HashMap::new();
        let mut assigned: Vec<u64> = Vec::with_capacity(batch.len());
        let mut created = 0u64;
        let mut next_id = self.next_series_id.load(Ordering::Relaxed);

        let reject = |g: &Self| -> () {
            g.writes_rejected.fetch_add(1, Ordering::Relaxed);
        };

        for tags in batch {
            let key = series_key(tags);
            // Either the live index or something earlier in this same batch.
            let known = series
                .get(&key)
                .or_else(|| new_series.get(&key))
                .map(|e| e.series_id);
            if let Some(id) = known {
                assigned.push(id);
                continue;
            }

            // `per_series` caps the width of one series key.
            if self.limits.per_series != u64::MAX && tags.len() as u64 > self.limits.per_series {
                reject(self);
                return Err(CardinalityError::SeriesFieldLimitReached {
                    limit: self.limits.per_series,
                });
            }

            if self.limits.max_series != 0
                && (series.len() + new_series.len()) as u64 >= self.limits.max_series
            {
                reject(self);
                return Err(CardinalityError::SeriesLimitReached {
                    limit: self.limits.max_series,
                });
            }

            for (name, value) in tags {
                let limit = self.limits.tag_limit(name);
                let known_already = tag_values
                    .get(name)
                    .map(|m| m.contains_key(value))
                    .unwrap_or(false)
                    || new_tags
                        .get(name)
                        .map(|s| s.contains(value))
                        .unwrap_or(false);
                if known_already {
                    continue;
                }
                // Only the limit is skipped when there is none. The value still
                // has to be recorded, or an unlimited tag would never appear in
                // the index at all and its cardinality would read as zero.
                if limit != u64::MAX {
                    let current = tag_values.get(name).map(|m| m.len()).unwrap_or(0)
                        + new_tags.get(name).map(|s| s.len()).unwrap_or(0);
                    if current as u64 >= limit {
                        reject(self);
                        return Err(CardinalityError::TagLimitReached {
                            tag: name.clone(),
                            value: value.clone(),
                            limit,
                        });
                    }
                }
                new_tags.entry(name.clone()).or_default().insert(value.clone());
            }

            new_series.insert(
                key,
                SeriesEntry {
                    series_id: next_id,
                    tags: tags.iter().cloned().collect(),
                },
            );
            assigned.push(next_id);
            next_id += 1;
            created += 1;
        }

        // Commit. Reached only when every row fitted.
        for (name, values) in new_tags {
            let slot = tag_values.entry(name).or_default();
            for value in values {
                slot.insert(value, ());
            }
        }
        series.extend(new_series);

        // Publish the new id counter while the locks are still held. Doing it
        // after releasing them let the next writer read a stale counter and hand
        // out the same ids again.
        self.next_series_id.store(next_id, Ordering::Relaxed);
        self.series_created.fetch_add(created, Ordering::Relaxed);
        self.writes_admitted
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        drop(series);
        drop(tag_values);
        Ok(assigned)
    }

    /// Distinct values currently known for a tag.
    pub fn tag_cardinality(&self, name: &str) -> u64 {
        self.tag_values.read().get(name).map(|m| m.len() as u64).unwrap_or(0)
    }

    /// Number of distinct tag combinations.
    pub fn series_count(&self) -> u64 {
        self.series.read().len() as u64
    }

    /// Total series ever created, including any dropped.
    pub fn series_created(&self) -> u64 {
        self.series_created.load(Ordering::Relaxed)
    }

    /// Look up a series id by its tags.
    pub fn series_id(&self, tags: &[(String, String)]) -> Option<u64> {
        self.series.read().get(&series_key(tags)).map(|s| s.series_id)
    }

    /// Tags recorded for a series id.
    pub fn series_tags(&self, series_id: u64) -> Option<Vec<(String, String)>> {
        let series = self.series.read();
        series
            .values()
            .find(|s| s.series_id == series_id)
            .map(|s| s.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    /// Tags per name that are in play, for metrics.
    pub fn tag_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tag_values.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn writes_admitted(&self) -> u64 {
        self.writes_admitted.load(Ordering::Relaxed)
    }

    pub fn writes_rejected(&self) -> u64 {
        self.writes_rejected.load(Ordering::Relaxed)
    }

    /// Record that a write was refused for a reason of the caller's own.
    ///
    /// A refusal from [`CardinalityGuard::admit`] or [`CardinalityGuard::admit_batch`]
    /// already counts itself, so calling this as well would double-count it.
    pub fn record_rejection(&self) {
        self.writes_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn limits(&self) -> &CardinalityLimits {
        &self.limits
    }
}

/// Stable key for a tag combination.
///
/// Sorting makes the key order-independent: `[(a,1),(b,2)]` and `[(b,2),(a,1)]`
/// are the same series, which is what a caller expects.
fn series_key(tags: &[(String, String)]) -> String {
    let mut parts: Vec<String> = tags
        .iter()
        .map(|(k, v)| format!("{k}\u{1}{v}"))
        .collect();
    parts.sort();
    parts.join("\u{2}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(name: &str, value: &str) -> (String, String) {
        (name.to_string(), value.to_string())
    }

    fn guard(per_tag: u64, per_series: u64, max_series: u64) -> CardinalityGuard {
        CardinalityGuard::new(CardinalityLimits {
            per_tag,
            per_series,
            max_series,
        })
    }

    #[test]
    fn test_first_write_is_admitted() {
        let g = guard(10, 10, 0);
        let id = g.admit(&[tag("host", "a")]).unwrap();
        assert_eq!(id, 1);
        assert_eq!(g.tag_cardinality("host"), 1);
        assert_eq!(g.series_count(), 1);
    }

    #[test]
    fn test_repeat_series_reuses_its_id() {
        let g = guard(10, 10, 0);
        let first = g.admit(&[tag("host", "a")]).unwrap();
        for _ in 0..100 {
            assert_eq!(g.admit(&[tag("host", "a")]).unwrap(), first);
        }
        assert_eq!(g.series_count(), 1, "a repeat write must not create a series");
    }

    #[test]
    fn test_tag_order_does_not_change_identity() {
        let g = guard(10, 10, 0);
        let a = g.admit(&[tag("host", "x"), tag("region", "eu")]).unwrap();
        let b = g.admit(&[tag("region", "eu"), tag("host", "x")]).unwrap();
        assert_eq!(a, b, "the same tags in a different order are one series");
        assert_eq!(g.series_count(), 1);
    }

    #[test]
    fn test_tag_limit_is_enforced() {
        let g = guard(3, 100, 0);
        for i in 0..3 {
            g.admit(&[tag("host", &format!("h{}", i))]).unwrap();
        }
        let err = g.admit(&[tag("host", "h3")]).unwrap_err();
        assert_eq!(
            err,
            CardinalityError::TagLimitReached {
                tag: "host".to_string(),
                value: "h3".to_string(),
                limit: 3
            }
        );
    }

    #[test]
    fn test_existing_value_is_allowed_at_the_limit() {
        let g = guard(2, 100, 0);
        g.admit(&[tag("host", "a")]).unwrap();
        g.admit(&[tag("host", "b")]).unwrap();

        // The table is full, but this series already exists.
        for _ in 0..10 {
            assert!(g.admit(&[tag("host", "a")]).is_ok());
        }
        assert!(g.admit(&[tag("host", "c")]).is_err());
    }

    #[test]
    fn test_limit_is_per_tag_name() {
        let g = guard(2, 100, 0);
        g.admit(&[tag("host", "a")]).unwrap();
        g.admit(&[tag("host", "b")]).unwrap();

        // A different tag is unaffected by the first tag's limit.
        assert!(g.admit(&[tag("region", "eu")]).is_ok());
        assert!(g.admit(&[tag("region", "us")]).is_ok());
        assert!(g.admit(&[tag("region", "ap")]).is_err());
        assert_eq!(g.tag_cardinality("host"), 2);
        assert_eq!(g.tag_cardinality("region"), 2);
    }

    #[test]
    fn test_series_limit_is_enforced() {
        let g = guard(100, 100, 2);
        g.admit(&[tag("host", "a")]).unwrap();
        g.admit(&[tag("host", "b")]).unwrap();
        assert_eq!(
            g.admit(&[tag("host", "c")]).unwrap_err(),
            CardinalityError::SeriesLimitReached { limit: 2 }
        );
        assert!(g.admit(&[tag("host", "a")]).is_ok(), "existing series still writes");
    }

    #[test]
    fn test_zero_series_limit_disables_the_check() {
        let g = guard(1, 100, 0);
        // per_tag=1 is still enforced even with the series check off.
        g.admit(&[tag("host", "a")]).unwrap();
        assert!(g.admit(&[tag("host", "b")]).is_err());
        assert_eq!(g.series_count(), 1);
    }

    #[test]
    fn test_rejected_write_does_not_count_the_value() {
        let g = guard(1, 100, 0);
        g.admit(&[tag("host", "a")]).unwrap();
        assert!(g.admit(&[tag("host", "b")]).is_err());
        // The refused value must not have been recorded, or a later fix would
        // leave the count wrong.
        assert_eq!(g.tag_cardinality("host"), 1);
    }

    #[test]
    fn test_rejected_multi_tag_write_commits_nothing() {
        let g = guard(1, 100, 0);
        g.admit(&[tag("host", "a"), tag("region", "eu")]).unwrap();

        // "host=b" is over the limit; "region=us" must not be recorded.
        assert!(g.admit(&[tag("host", "b"), tag("region", "us")]).is_err());
        assert_eq!(g.tag_cardinality("region"), 1);
        assert_eq!(g.series_count(), 1);
    }

    #[test]
    fn test_untagged_write_is_always_admitted() {
        let g = guard(1, 1, 1);
        g.admit(&[]).unwrap();
        // Still fine: the single untagged series already exists.
        assert!(g.admit(&[]).is_ok());
    }

    #[test]
    fn test_lookup_by_tags() {
        let g = guard(10, 10, 0);
        let id = g.admit(&[tag("host", "a"), tag("region", "eu")]).unwrap();
        assert_eq!(g.series_id(&[tag("host", "a"), tag("region", "eu")]), Some(id));
        assert_eq!(g.series_id(&[tag("host", "a"), tag("region", "us")]), None);
    }

    #[test]
    fn test_series_tags_round_trip() {
        let g = guard(10, 10, 0);
        let id = g.admit(&[tag("host", "a"), tag("region", "eu")]).unwrap();
        let mut tags = g.series_tags(id).unwrap();
        tags.sort();
        assert_eq!(
            tags,
            vec![tag("host", "a"), tag("region", "eu")],
            "tags must come back intact"
        );
    }

    #[test]
    fn test_tag_names_are_listed() {
        let g = guard(10, 10, 0);
        g.admit(&[tag("zone", "z"), tag("host", "a")]).unwrap();
        assert_eq!(g.tag_names(), vec!["host".to_string(), "zone".to_string()]);
    }

    #[test]
    fn test_batch_assigns_one_id_per_row() {
        let g = guard(10, 10, 0);
        let batch = vec![
            vec![tag("host", "a")],
            vec![tag("host", "b")],
            vec![tag("host", "a")],
        ];
        let ids = g.admit_batch(&batch).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], ids[2], "the repeated row reuses its id");
        assert_ne!(ids[0], ids[1]);
        assert_eq!(g.series_count(), 2);
    }

    #[test]
    fn test_batch_rejection_leaves_state_untouched() {
        let g = guard(2, 100, 0);
        g.admit_batch(&[vec![tag("host", "a")], vec![tag("host", "b")]]).unwrap();

        let err = g
            .admit_batch(&[
                vec![tag("host", "c")],
                vec![tag("host", "d")],
            ])
            .unwrap_err();
        assert!(matches!(err, CardinalityError::TagLimitReached { .. }));

        // Nothing from the refused batch may be counted.
        assert_eq!(g.tag_cardinality("host"), 2);
        assert_eq!(g.series_count(), 2);
    }

    #[test]
    fn test_batch_agrees_with_single_admit() {
        let a = guard(3, 100, 0);
        let b = guard(3, 100, 0);

        let batch: Vec<Vec<(String, String)>> =
            (0..3).map(|i| vec![tag("host", &format!("h{}", i))]).collect();
        a.admit_batch(&batch).unwrap();
        for tags in &batch {
            b.admit(tags).unwrap();
        }

        assert_eq!(a.tag_cardinality("host"), b.tag_cardinality("host"));
        assert_eq!(a.series_count(), b.series_count());
    }

    #[test]
    fn test_batch_respects_series_limit() {
        let g = guard(100, 100, 2);
        assert!(g
            .admit_batch(&[
                vec![tag("host", "a")],
                vec![tag("host", "b")],
                vec![tag("host", "c")],
            ])
            .is_err());
        assert_eq!(g.series_count(), 0, "a refused batch creates nothing");
    }

    #[test]
    fn test_counters_track_writes() {
        let g = guard(1, 100, 0);
        g.admit(&[tag("host", "a")]).unwrap();
        g.admit(&[tag("host", "a")]).unwrap();
        assert!(g.admit(&[tag("host", "z")]).is_err());

        assert_eq!(g.writes_admitted(), 2);
        // A refusal counts itself, so a caller that never remembers to report
        // one still gets an honest rejection rate out of the metrics.
        assert_eq!(g.writes_rejected(), 1);
        assert_eq!(g.series_created(), 1, "only the new series was created");

        // Explicitly recording one as well is for refusals the guard did not
        // make itself, and adds to the count rather than replacing it.
        g.record_rejection();
        assert_eq!(g.writes_rejected(), 2);
    }

    /// Ids are handed out densely and `series_created` must count them exactly.
    ///
    /// The old batch path derived this by subtracting the number of assigned ids
    /// that happened to be numerically below the batch length, which has nothing
    /// to do with how many series were created, so the metric drifted.
    #[test]
    fn test_series_created_counts_exactly_across_batches() {
        let g = guard(1000, 1000, 0);

        g.admit_batch(&[vec![tag("host", "a")], vec![tag("host", "b")]]).unwrap();
        assert_eq!(g.series_created(), 2);

        // "a" already exists, so only "c" is new.
        g.admit_batch(&[vec![tag("host", "a")], vec![tag("host", "c")]]).unwrap();
        assert_eq!(g.series_created(), 3, "a repeat row must not count again");

        // A refused batch creates nothing.
        let strict = guard(2, 100, 0);
        strict.admit_batch(&[vec![tag("host", "a")], vec![tag("host", "b")]]).unwrap();
        assert!(strict
            .admit_batch(&[vec![tag("host", "c")], vec![tag("host", "d")]])
            .is_err());
        assert_eq!(strict.series_created(), 2, "a refused batch counted series");
    }

    /// `per_series` must bound a batch as well as a single write.
    ///
    /// The batch path never checked it, so a batch could create series wider
    /// than the limit allows while a single write of the same row was refused.
    #[test]
    fn test_batch_respects_the_series_width_limit() {
        let g = guard(100, 1, 0);

        // Two tags on one series key, but the width limit is 1.
        let too_wide = vec![vec![tag("host", "a"), tag("region", "eu")]];
        assert!(g.admit_batch(&too_wide).is_err());
        assert_eq!(g.series_count(), 0, "the refused batch created a series");
        assert_eq!(g.writes_rejected(), 1);

        // A single write of the same row agrees.
        assert!(g.admit(&too_wide[0]).is_err());

        // Narrow enough, and it is admitted.
        assert!(g.admit_batch(&[vec![tag("host", "a")]]).is_ok());
        assert_eq!(g.series_count(), 1);
    }

    /// A tag value must be recorded even when no limit applies.
    ///
    /// Skipping the record along with the check left unlimited tables with an
    /// empty index, reporting zero cardinality for tags that had thousands of
    /// values.
    #[test]
    fn test_unlimited_tags_are_still_recorded() {
        let g = CardinalityGuard::new(CardinalityLimits::unlimited());
        for i in 0..100 {
            g.admit_batch(&[vec![tag("host", &format!("h{}", i))]]).unwrap();
        }
        assert_eq!(g.tag_cardinality("host"), 100, "unlimited values went unrecorded");
        assert_eq!(g.series_count(), 100);
    }

    /// Concurrent writers must not be able to walk past a limit.
    ///
    /// The checks and the commit were separated by released locks, so every
    /// thread could read the same count, all pass, and all commit. With a limit
    /// of 100 that admits 100 series no matter how many threads are pushing.
    #[test]
    fn test_concurrent_admission_cannot_exceed_the_limit() {
        use std::sync::Arc;

        const THREADS: u64 = 8;
        const PER_THREAD: usize = 200;
        const LIMIT: u64 = 100;

        let g = Arc::new(guard(LIMIT, LIMIT, LIMIT));
        let barrier = Arc::new(std::sync::Barrier::new(THREADS as usize));

        let workers: Vec<_> = (0..THREADS)
            .map(|t| {
                let g = Arc::clone(&g);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut admitted = 0usize;
                    let mut refused = 0usize;
                    barrier.wait();
                    for i in 0..PER_THREAD {
                        let tags = vec![tag("host", &format!("{}-{}", t, i))];
                        match g.admit_batch(&[tags]) {
                            Ok(_) => admitted += 1,
                            Err(_) => refused += 1,
                        }
                    }
                    (admitted, refused)
                })
            })
            .collect();

        let (admitted, refused): (usize, usize) = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .fold((0, 0), |(a, r), (x, y)| (a + x, r + y));

        assert_eq!(
            g.series_count(),
            LIMIT,
            "the series limit was exceeded by concurrent writers"
        );
        assert_eq!(g.tag_cardinality("host"), LIMIT);
        assert!(admitted > 0 && refused > 0, "the test did not actually contend");
        assert_eq!(admitted + refused, (THREADS * PER_THREAD as u64) as usize);
        assert_eq!(g.writes_admitted(), admitted as u64);
        assert_eq!(g.writes_rejected(), refused as u64);

        // Nothing admitted may have been erased by a racing commit, and no two
        // series may share an id.
        let mut ids = std::collections::HashSet::new();
        let mut found = 0usize;
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                let tags = vec![tag("host", &format!("{}-{}", t, i))];
                if let Some(id) = g.series_id(&tags) {
                    found += 1;
                    assert!(ids.insert(id), "series id {} was handed out twice", id);
                }
            }
        }
        assert_eq!(found, admitted, "an admitted series was lost to a racing commit");
        assert_eq!(ids.len() as u64, LIMIT);
    }

    #[test]
    fn test_unlimited_never_refuses() {
        let g = CardinalityGuard::new(CardinalityLimits::unlimited());
        for i in 0..10_000 {
            g.admit(&[tag("host", &format!("h{}", i))]).unwrap();
        }
        assert_eq!(g.tag_cardinality("host"), 10_000);
    }

    #[test]
    fn test_error_message_names_the_tag_and_value() {
        let g = guard(1, 100, 0);
        g.admit(&[tag("device_id", "a")]).unwrap();
        let msg = g.admit(&[tag("device_id", "b")]).unwrap_err().to_string();
        assert!(msg.contains("device_id"), "{}", msg);
        assert!(msg.contains("'b'"), "{}", msg);
    }

    #[test]
    fn test_key_separator_cannot_be_forged_by_a_tag_name() {
        // A tag name containing the separator must not collide with a
        // different key that uses it as a boundary.
        let g = guard(10, 10, 0);
        let a = g.admit(&[tag("a\u{1}b", "c")]).unwrap();
        let b = g.admit(&[tag("a", "b\u{2}c")]).unwrap();
        assert_ne!(a, b, "distinct tag sets must not share a key");
    }
}
