use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};

use crate::error::Error;

const DAY_FMT: &str = "%Y-%m-%d";
const HOUR_FMT: &str = "%Y-%m-%dT%H";

/// how many partitions a build with nothing named will launch, unless
/// [`Partitions::build_limit`] says otherwise.
pub(crate) const DEFAULT_BUILD_LIMIT: usize = 31;

/// the most keys a set ever reports. a daily set running since 2020 is a few
/// thousand; an hourly one is not, and staleness walks every key it is handed.
/// past this the *newest* keys are the ones kept, since those are the ones
/// anything is about to build.
const MAX_KEYS: usize = 10_000;

/// the key set a [partitioned asset](crate::Asset::partitioned) is
/// materialized over: one materialization, one fingerprint and one history per
/// key.
///
/// ```no_run
/// # use hestan::{Asset, Partitions};
/// # use serde_json::json;
/// Asset::new("daily_orders", |ctx: hestan::OpCtx| async move {
///     let day = ctx.partition().expect("partitioned");
///     Ok(json!({ "day": day }))
/// })
/// .partitioned(Partitions::daily("2026-01-01"));
/// ```
///
/// `daily` keys are `YYYY-MM-DD` and `hourly` keys are `YYYY-MM-DDTHH`, both
/// in utc, running from `start` to now — the set grows with the clock.
/// [`keys`](Self::keys) is a fixed set of whatever strings you like.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Partitions {
    kind: Kind,
    build_limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    Daily { start: String },
    Hourly { start: String },
    Static { keys: Vec<String> },
}

impl Kind {
    fn label(&self) -> &'static str {
        match self {
            Kind::Daily { .. } => "daily",
            Kind::Hourly { .. } => "hourly",
            Kind::Static { .. } => "static",
        }
    }
}

impl Partitions {
    /// one key per utc day from `start` (`YYYY-MM-DD`) to today, inclusive.
    pub fn daily(start: impl Into<String>) -> Partitions {
        Partitions::of(Kind::Daily {
            start: start.into(),
        })
    }

    /// one key per utc hour from `start` to the current hour, inclusive.
    /// `start` is `YYYY-MM-DDTHH`, or `YYYY-MM-DD` for midnight.
    pub fn hourly(start: impl Into<String>) -> Partitions {
        Partitions::of(Kind::Hourly {
            start: start.into(),
        })
    }

    /// a fixed set of keys, in the order given. repeats are dropped.
    pub fn keys<I>(keys: I) -> Partitions
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let mut unique: Vec<String> = Vec::new();
        for key in keys {
            let key = key.into();
            if !unique.contains(&key) {
                unique.push(key);
            }
        }
        Partitions::of(Kind::Static { keys: unique })
    }

    fn of(kind: Kind) -> Partitions {
        Partitions {
            kind,
            build_limit: DEFAULT_BUILD_LIMIT,
        }
    }

    /// how many partitions a build that names none of them will launch
    /// (default 31), newest first. it exists so an unbounded daily range
    /// cannot start a thousand instances by accident; naming keys explicitly,
    /// or a [backfill](crate::Hestan), goes past it deliberately.
    pub fn build_limit(mut self, n: usize) -> Partitions {
        self.build_limit = n.max(1);
        self
    }

    pub(crate) fn limit(&self) -> usize {
        self.build_limit
    }

    /// daily / hourly / static, for saying in an error why two sets cannot be
    /// mapped onto each other.
    pub(crate) fn kind_label(&self) -> &'static str {
        self.kind.label()
    }

    pub(crate) fn same_kind(&self, other: &Partitions) -> bool {
        self.kind.label() == other.kind.label()
    }

    /// what a registry check rejects before anything runs.
    pub(crate) fn validate(&self, asset: &str) -> Result<(), Error> {
        let bad = |why: String| Err(Error::Graph(format!("asset {asset}: {why}")));
        match &self.kind {
            Kind::Daily { start } => {
                if NaiveDate::parse_from_str(start, DAY_FMT).is_err() {
                    return bad(format!(
                        "daily partitions start at {start:?}, not a YYYY-MM-DD date"
                    ));
                }
            }
            Kind::Hourly { start } => {
                if parse_hour(start).is_none() {
                    return bad(format!(
                        "hourly partitions start at {start:?}, not a YYYY-MM-DDTHH hour"
                    ));
                }
            }
            Kind::Static { keys } => {
                if keys.is_empty() {
                    return bad("partitioned over no keys at all".into());
                }
            }
        }
        // an instance is named `{asset}[{key}]`, so a key carrying a bracket
        // would name something it is not
        if let Some(key) = self.keys_now().into_iter().find(|k| bracketed(k)) {
            return bad(format!("partition key {key:?} contains a bracket"));
        }
        Ok(())
    }

    /// every key of the set right now, oldest first.
    pub(crate) fn keys_now(&self) -> Vec<String> {
        self.keys_until(Utc::now())
    }

    /// every key up to `now`, oldest first — the whole set for a static one.
    pub(crate) fn keys_until(&self, now: DateTime<Utc>) -> Vec<String> {
        let keys = match &self.kind {
            Kind::Static { keys } => return keys.clone(),
            Kind::Daily { start } => {
                let Ok(start) = NaiveDate::parse_from_str(start, DAY_FMT) else {
                    return Vec::new();
                };
                let mut days = Vec::new();
                let mut day = start;
                let today = now.date_naive();
                while day <= today {
                    days.push(day.format(DAY_FMT).to_string());
                    let Some(next) = day.succ_opt() else { break };
                    day = next;
                }
                days
            }
            Kind::Hourly { start } => {
                let Some(start) = parse_hour(start) else {
                    return Vec::new();
                };
                let mut hours = Vec::new();
                let mut hour = start;
                let last = now
                    .naive_utc()
                    .with_minute(0)
                    .and_then(|t| t.with_second(0))
                    .and_then(|t| t.with_nanosecond(0))
                    .unwrap_or(now.naive_utc());
                while hour <= last {
                    hours.push(hour.format(HOUR_FMT).to_string());
                    hour += Duration::hours(1);
                }
                hours
            }
        };
        // the newest are the ones a build is about to want
        match keys.len() > MAX_KEYS {
            true => keys[keys.len() - MAX_KEYS..].to_vec(),
            false => keys,
        }
    }

    pub(crate) fn contains(&self, key: &str) -> bool {
        self.keys_now().iter().any(|k| k == key)
    }
}

fn bracketed(key: &str) -> bool {
    key.contains('[') || key.contains(']')
}

// chrono will not parse a datetime that names no minute, so the hour key is
// completed rather than parsed as written
fn parse_hour(start: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(&format!("{start}:00:00"), "%Y-%m-%dT%H:%M:%S")
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(start, DAY_FMT)
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn daily_keys_run_from_the_start_to_today_inclusive() {
        let p = Partitions::daily("2026-01-30");
        // both ends are in the set, and a month boundary is just the next day
        assert_eq!(
            p.keys_until(at("2026-02-02T09:15:00Z")),
            ["2026-01-30", "2026-01-31", "2026-02-01", "2026-02-02"]
        );
        // the last key is today whatever the time of day
        assert_eq!(
            p.keys_until(at("2026-01-30T00:00:00Z")),
            ["2026-01-30"],
            "the start day itself is a partition"
        );
        // a start in the future has no keys at all rather than a backwards range
        assert!(p.keys_until(at("2026-01-29T23:59:59Z")).is_empty());
        // and a leap day is a day like any other
        let leap = Partitions::daily("2028-02-28");
        assert_eq!(
            leap.keys_until(at("2028-03-01T00:00:00Z")),
            ["2028-02-28", "2028-02-29", "2028-03-01"]
        );
    }

    #[test]
    fn hourly_keys_run_to_the_current_hour() {
        let p = Partitions::hourly("2026-01-01T22");
        assert_eq!(
            p.keys_until(at("2026-01-02T01:59:00Z")),
            [
                "2026-01-01T22",
                "2026-01-01T23",
                "2026-01-02T00",
                "2026-01-02T01"
            ]
        );
        // the current hour counts the moment it starts
        assert_eq!(p.keys_until(at("2026-01-01T22:00:00Z")), ["2026-01-01T22"]);
        // a plain date starts at midnight
        assert_eq!(
            Partitions::hourly("2026-01-01").keys_until(at("2026-01-01T01:00:00Z")),
            ["2026-01-01T00", "2026-01-01T01"]
        );
    }

    #[test]
    fn static_keys_keep_their_order_and_drop_repeats() {
        let p = Partitions::keys(["emea", "amer", "emea"]);
        assert_eq!(p.keys_now(), ["emea", "amer"]);
        assert!(p.contains("amer") && !p.contains("apac"));
    }

    #[test]
    fn a_range_longer_than_the_cap_keeps_its_newest_keys() {
        let p = Partitions::hourly("2020-01-01T00");
        let keys = p.keys_until(at("2026-01-01T00:00:00Z"));
        assert_eq!(keys.len(), MAX_KEYS);
        assert_eq!(keys[keys.len() - 1], "2026-01-01T00");
    }

    #[test]
    fn validation_names_what_is_wrong_with_a_set() {
        let err = Partitions::daily("last tuesday").validate("a").unwrap_err();
        assert!(err.to_string().contains("not a YYYY-MM-DD date"), "{err}");
        let err = Partitions::hourly("2026-01-01T99")
            .validate("a")
            .unwrap_err();
        assert!(
            err.to_string().contains("not a YYYY-MM-DDTHH hour"),
            "{err}"
        );
        let err = Partitions::keys(Vec::<String>::new())
            .validate("a")
            .unwrap_err();
        assert!(err.to_string().contains("no keys at all"), "{err}");
        // instances are named `{asset}[{key}]`, so a key cannot carry brackets
        let err = Partitions::keys(["eu[1]"]).validate("a").unwrap_err();
        assert!(err.to_string().contains("contains a bracket"), "{err}");
        Partitions::daily("2026-01-01").validate("a").unwrap();
    }

    #[test]
    fn the_build_limit_defaults_and_floors_at_one() {
        assert_eq!(Partitions::daily("2026-01-01").limit(), 31);
        assert_eq!(Partitions::daily("2026-01-01").build_limit(7).limit(), 7);
        assert_eq!(Partitions::daily("2026-01-01").build_limit(0).limit(), 1);
    }

    #[test]
    fn kinds_only_map_onto_their_own() {
        let daily = Partitions::daily("2026-01-01");
        assert!(daily.same_kind(&Partitions::daily("2020-01-01")));
        assert!(!daily.same_kind(&Partitions::hourly("2026-01-01T00")));
        assert!(!daily.same_kind(&Partitions::keys(["a"])));
        let _ = Utc.timestamp_opt(0, 0);
    }
}
