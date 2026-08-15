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
/// in utc, running from `start` to now: the set grows with the clock.
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

    /// the instant a key names, for the two kinds whose keys name one.
    fn instant(&self, key: &str) -> Option<NaiveDateTime> {
        match self {
            Kind::Daily { .. } => NaiveDate::parse_from_str(key, DAY_FMT)
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0)),
            Kind::Hourly { .. } => parse_hour(key),
            Kind::Static { .. } => None,
        }
    }

    fn format(&self, at: NaiveDateTime) -> Option<String> {
        match self {
            Kind::Daily { .. } => Some(at.format(DAY_FMT).to_string()),
            Kind::Hourly { .. } => Some(at.format(HOUR_FMT).to_string()),
            Kind::Static { .. } => None,
        }
    }

    /// how much time one key spans, which is also the distance to the next.
    fn step(&self) -> Option<Duration> {
        match self {
            Kind::Daily { .. } => Some(Duration::days(1)),
            Kind::Hourly { .. } => Some(Duration::hours(1)),
            Kind::Static { .. } => None,
        }
    }
}

/// the `dep` keys inside the span of one `own` key, whether or not the dep
/// holds them. a key that does not land on the dep's own grain is not one of
/// its keys at all, so the round trip through its format is the test.
fn covered(own: &Kind, key: &str, dep: &Kind) -> Vec<String> {
    let (Some(start), Some(span), Some(step)) = (own.instant(key), own.step(), dep.step()) else {
        return Vec::new();
    };
    let Some(end) = start.checked_add_signed(span) else {
        return Vec::new();
    };
    let mut keys = Vec::new();
    let mut at = start;
    while at < end {
        if let Some(k) = dep.format(at).filter(|k| dep.instant(k) == Some(at)) {
            keys.push(k);
        }
        let Some(next) = at.checked_add_signed(step) else {
            break;
        };
        at = next;
    }
    keys
}

/// the key `n` steps from `key` along `own`'s order; `None` off either end of
/// what a date can be, or on a set with no order to step along.
fn shifted(own: &Kind, key: &str, n: i64) -> Option<String> {
    let by = own.step()?.checked_mul(i32::try_from(n).ok()?)?;
    own.format(own.instant(key)?.checked_add_signed(by)?)
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

    /// whether the keys run in an order an [offset](PartitionMapping::offset)
    /// could step along. a generated set runs with the clock; a static one is
    /// in the order it was written, which is not the same claim.
    pub(crate) fn ordered(&self) -> bool {
        self.kind.step().is_some()
    }

    /// how much time one key spans, for deciding whether one set's keys can
    /// cover another's; `None` for a set whose keys span no time at all.
    pub(crate) fn grain(&self) -> Option<Duration> {
        self.kind.step()
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

    /// every key up to `now`, oldest first; the whole set for a static one.
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

    /// the keys from `from` to `to` inclusive, which is what a
    /// [backfill](crate::Hestan) resolves its range to. a generated set is in
    /// time order, so the range is every key between the two; a static set is
    /// in the order it was declared, so the range is the slice between them.
    pub(crate) fn range(&self, from: &str, to: &str) -> Result<Vec<String>, Error> {
        let keys = self.keys_now();
        let at = |key: &str| {
            keys.iter().position(|k| k == key).ok_or_else(|| {
                Error::Graph(format!(
                    "{key:?} is not a key of this asset's {} partitions",
                    self.kind_label()
                ))
            })
        };
        let (first, last) = (at(from)?, at(to)?);
        if first > last {
            return Err(Error::Graph(format!(
                "range runs backwards: {from:?} comes after {to:?}"
            )));
        }
        Ok(keys[first..=last].to_vec())
    }
}

/// which of a dep's [partition keys](Partitions) one partition reads.
///
/// this is a property of the *edge* rather than of either asset, so it is
/// declared where the dependency is, with
/// [`Asset::reads`](crate::Asset::reads):
///
/// ```no_run
/// # use hestan::{Asset, OpCtx, PartitionMapping, Partitions};
/// # use serde_json::json;
/// # let hourly = Asset::new("hourly_traffic", |_: OpCtx| async { Ok(json!(null)) })
/// #     .partitioned(Partitions::hourly("2026-01-01"));
/// Asset::new("daily_traffic", |ctx: OpCtx| async move {
///     // one entry per hour of the day this key is for
///     let hours = ctx.input("hourly_traffic").cloned().unwrap_or(json!({}));
///     Ok(json!({ "hits": hours.as_object().map_or(0, |h| h.len()) }))
/// })
/// .reads(&hourly, PartitionMapping::covering())
/// .partitioned(Partitions::daily("2026-01-01"));
/// ```
///
/// [`identity`](Self::identity) is the default and the only shape that reads
/// an unpartitioned dep. the rest need a partitioned one, and a pairing that
/// could never resolve (a day covering a static key set, an offset along a
/// set with no order) fails the build rather than quietly reading nothing.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PartitionMapping {
    shape: Shape,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum Shape {
    #[default]
    Identity,
    All,
    Covering,
    Offset(i64),
}

impl PartitionMapping {
    /// the same key, which is what every dep between two partitioned assets
    /// meant before there was anything else to mean. the default, and the only
    /// shape that says anything about an unpartitioned dep, whose whole value
    /// arrives at every key.
    pub fn identity() -> PartitionMapping {
        PartitionMapping {
            shape: Shape::Identity,
        }
    }

    /// every key the dep has, as one object keyed by them. this is the read an
    /// unpartitioned asset makes of a partitioned one, and the only shape that
    /// pairs any two key sets: a set that grows leaves the consumer stale
    /// whenever it grows, which is what an aggregation of everything means.
    pub fn all() -> PartitionMapping {
        PartitionMapping { shape: Shape::All }
    }

    /// the dep's keys that fall inside this one: a daily key reading the 24
    /// hourly keys of its day, as one object keyed by them. the consumer's keys
    /// have to span at least as much time as the dep's, so daily covers hourly
    /// and hourly does not cover daily.
    ///
    /// a window promises its whole range: a key whose dep does not hold every
    /// hour of it is refused at the build that names it, rather than
    /// materialized from the part that happens to be there. the exception is
    /// the range the dep's clock has not reached (the hours left in today),
    /// which is not missing but not yet due, so a rollup of the day so far
    /// builds and goes stale as each hour lands.
    pub fn covering() -> PartitionMapping {
        PartitionMapping {
            shape: Shape::Covering,
        }
    }

    /// the key `n` steps back or forward along the dep's order:
    /// `offset(-1)` is yesterday's key on a daily set and the previous hour on
    /// an hourly one. both sets have to be the same kind, and a set with no
    /// order to step along is refused at the build.
    ///
    /// off either end the mapping reads nothing rather than failing: the first
    /// key of a set has no key before it, and that is a fact about the edge of
    /// history and not a broken dependency.
    pub fn offset(n: i64) -> PartitionMapping {
        PartitionMapping {
            shape: Shape::Offset(n),
        }
    }

    pub(crate) fn is_identity(&self) -> bool {
        self.shape == Shape::Identity
    }

    pub(crate) fn is_all(&self) -> bool {
        self.shape == Shape::All
    }

    pub(crate) fn is_covering(&self) -> bool {
        self.shape == Shape::Covering
    }

    pub(crate) fn is_offset(&self) -> bool {
        matches!(self.shape, Shape::Offset(_))
    }

    /// whether this shape names at most one key, which is what decides how the
    /// value and the recorded fingerprint are shaped: one dep partition
    /// arrives as itself, a set of them as an object keyed by partition.
    pub(crate) fn reads_one(&self) -> bool {
        matches!(self.shape, Shape::Identity | Shape::Offset(_))
    }

    /// what the api and the ui call this mapping.
    pub(crate) fn label(&self) -> String {
        match self.shape {
            Shape::Identity => "identity".into(),
            Shape::All => "all".into(),
            Shape::Covering => "covering".into(),
            Shape::Offset(n) => format!("offset {n:+}"),
        }
    }

    /// the dep keys one partition of `own` reads, oldest first. `key` is
    /// `None` when the consumer is not partitioned, which only
    /// [`all`](Self::all) has an answer for.
    pub(crate) fn reads(&self, own: Option<&Partitions>, key: Option<&str>, dep: &KeySet) -> Reads {
        let here = own.zip(key);
        match self.shape {
            Shape::Identity => Reads::at(key),
            Shape::All => Reads {
                keys: dep.keys().to_vec(),
                missing: Vec::new(),
            },
            Shape::Covering => {
                let mut reads = Reads::default();
                let covered = here
                    .map(|(own, key)| covered(&own.kind, key, &dep.kind))
                    .unwrap_or_default();
                // a key the dep's set has not reached yet is not missing, it
                // is not due: a generated set grows with the clock, and what
                // it holds up to now is all there is to read. a key *before*
                // what it holds is the one that never arrives
                let last = dep.keys().last();
                for k in covered {
                    match (dep.holds(&k), last) {
                        (true, _) => reads.keys.push(k),
                        (false, Some(last)) if &k > last => {}
                        (false, _) => reads.missing.push(k),
                    }
                }
                reads
            }
            Shape::Offset(n) => Reads {
                keys: here
                    .and_then(|(own, key)| shifted(&own.kind, key, n))
                    .filter(|k| dep.holds(k))
                    .into_iter()
                    .collect(),
                missing: Vec::new(),
            },
        }
    }
}

/// what one partition reads from one dep: the dep keys it takes, and the ones
/// it promised that the dep does not hold and never will. `missing` is empty
/// for every shape but [`covering`](PartitionMapping::covering), which
/// promises a whole range: the others name keys that may or may not exist,
/// which is a different claim.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Reads {
    pub keys: Vec<String>,
    pub missing: Vec<String>,
}

impl Reads {
    /// what [identity](PartitionMapping::identity) reads, without asking the
    /// dep what it holds: a key it does not hold is a dep that never
    /// materializes, not a key that goes unread. worth having on its own: a
    /// dep of ten thousand keys is not worth walking to be told the answer is
    /// the key you started with.
    pub(crate) fn at(key: Option<&str>) -> Reads {
        Reads {
            keys: key.map(|k| vec![k.to_string()]).unwrap_or_default(),
            missing: Vec::new(),
        }
    }
}

/// one asset's keys, prepared once. resolving a mapping asks "does the dep
/// hold this key" over and over, and a generated set is in order, so that is a
/// search rather than a scan.
pub(crate) struct KeySet {
    kind: Kind,
    keys: Vec<String>,
}

impl KeySet {
    pub(crate) fn of(spec: &Partitions) -> KeySet {
        KeySet {
            kind: spec.kind.clone(),
            keys: spec.keys_now(),
        }
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys
    }

    pub(crate) fn holds(&self, key: &str) -> bool {
        match self.kind {
            // a static set is in the order it was declared, which is nothing
            // to search along
            Kind::Static { .. } => self.keys.iter().any(|k| k == key),
            // both generated formats are fixed-width, so their time order is
            // their string order
            _ => self.keys.binary_search_by(|k| k.as_str().cmp(key)).is_ok(),
        }
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
    fn a_range_is_the_keys_between_its_ends() {
        let p = Partitions::keys(["emea", "amer", "apac"]);
        assert_eq!(p.range("emea", "apac").unwrap(), ["emea", "amer", "apac"]);
        assert_eq!(p.range("amer", "amer").unwrap(), ["amer"]);
        let err = p.range("apac", "emea").unwrap_err();
        assert!(err.to_string().contains("runs backwards"), "{err}");
        let err = p.range("emea", "nowhere").unwrap_err();
        assert!(err.to_string().contains("is not a key"), "{err}");
    }

    #[test]
    fn the_build_limit_defaults_and_floors_at_one() {
        assert_eq!(Partitions::daily("2026-01-01").limit(), 31);
        assert_eq!(Partitions::daily("2026-01-01").build_limit(7).limit(), 7);
        assert_eq!(Partitions::daily("2026-01-01").build_limit(0).limit(), 1);
    }

    // the clock is an input to a generated key set, so the tests below pin it
    // rather than resolve against whatever today happens to be
    fn set(spec: &Partitions) -> KeySet {
        KeySet {
            kind: spec.kind.clone(),
            keys: spec.keys_until(at("2026-03-09T00:00:00Z")),
        }
    }

    fn reads(own: &Partitions, key: &str, dep: &Partitions, m: &PartitionMapping) -> Reads {
        m.reads(Some(own), Some(key), &set(dep))
    }

    #[test]
    fn identity_reads_its_own_key_and_says_nothing_about_the_dep() {
        let day = Partitions::daily("2026-01-01");
        let dep = Partitions::daily("2026-03-01");
        let m = PartitionMapping::identity();
        assert_eq!(
            reads(&day, "2026-03-02", &dep, &m),
            Reads {
                keys: vec!["2026-03-02".into()],
                missing: Vec::new(),
            }
        );
        // a key the dep's set starts after is still the key it reads: nothing
        // materializes there, which is a dep that never arrives rather than a
        // read that resolved somewhere else
        assert_eq!(
            reads(&day, "2026-01-05", &dep, &m).keys,
            ["2026-01-05"],
            "identity resolved away from its own key"
        );
    }

    #[test]
    fn a_daily_key_covers_the_twenty_four_hours_inside_it() {
        let day = Partitions::daily("2026-01-01");
        let hour = Partitions::hourly("2020-01-01T00");
        let got = reads(&day, "2026-03-02", &hour, &PartitionMapping::covering());
        assert_eq!(got.keys.len(), 24);
        assert_eq!(got.keys[0], "2026-03-02T00");
        assert_eq!(got.keys[23], "2026-03-02T23");
        assert!(got.missing.is_empty());
        // the hours of the next day belong to the next key
        assert!(!got.keys.iter().any(|k| k.starts_with("2026-03-03")));

        // hours the dep's set starts after are named as missing rather than
        // dropped: a window promises its whole range
        let late = Partitions::hourly("2026-03-02T06");
        let got = reads(&day, "2026-03-02", &late, &PartitionMapping::covering());
        assert_eq!(got.keys.len(), 18);
        assert_eq!(got.missing.len(), 6);
        assert_eq!(got.missing[0], "2026-03-02T00");

        // an hour the dep's set has not reached is not one it is missing: the
        // pinned clock is midnight, so that day holds exactly one hour so far
        let got = reads(&day, "2026-03-09", &hour, &PartitionMapping::covering());
        assert_eq!(got.keys, ["2026-03-09T00"]);
        assert!(
            got.missing.is_empty(),
            "an hour that has not happened yet reads as missing: {:?}",
            got.missing
        );

        // and two sets of the same grain cover key for key
        let got = reads(
            &day,
            "2026-03-02",
            &Partitions::daily("2020-01-01"),
            &PartitionMapping::covering(),
        );
        assert_eq!(got.keys, ["2026-03-02"]);
    }

    #[test]
    fn an_offset_steps_along_the_set_and_stops_at_its_first_key() {
        let day = Partitions::daily("2026-03-01");
        let dep = Partitions::daily("2026-03-01");
        assert_eq!(
            reads(&day, "2026-03-05", &dep, &PartitionMapping::offset(-1)).keys,
            ["2026-03-04"]
        );
        assert_eq!(
            reads(&day, "2026-03-05", &dep, &PartitionMapping::offset(-3)).keys,
            ["2026-03-02"]
        );
        // the first key has nothing before it, which reads as nothing rather
        // than as a failure
        assert!(
            reads(&day, "2026-03-01", &dep, &PartitionMapping::offset(-1))
                .keys
                .is_empty()
        );
        // and so does a key past the end of the set
        assert!(
            reads(&day, "2026-03-05", &dep, &PartitionMapping::offset(9_000))
                .keys
                .is_empty()
        );
        let hour = Partitions::hourly("2026-03-01T00");
        assert_eq!(
            reads(&hour, "2026-03-01T05", &hour, &PartitionMapping::offset(-1)).keys,
            ["2026-03-01T04"]
        );
    }

    #[test]
    fn all_reads_every_key_the_dep_has() {
        let dep = Partitions::keys(["emea", "amer", "apac"]);
        let m = PartitionMapping::all();
        let day = Partitions::daily("2026-03-01");
        assert_eq!(
            reads(&day, "2026-03-02", &dep, &m).keys,
            ["emea", "amer", "apac"]
        );
        // an unpartitioned consumer has no key of its own, and all is the one
        // shape that still answers
        assert_eq!(
            m.reads(None, None, &set(&dep)).keys,
            ["emea", "amer", "apac"]
        );
        assert!(
            PartitionMapping::covering()
                .reads(None, None, &set(&dep))
                .keys
                .is_empty()
        );
    }

    #[test]
    fn a_key_set_knows_what_it_holds_however_it_is_ordered() {
        let generated = set(&Partitions::daily("2026-03-01"));
        assert!(generated.holds("2026-03-02") && !generated.holds("2020-01-01"));
        assert!(!generated.holds("nonsense"));
        // and nothing past the day the clock has reached
        assert!(!generated.holds("2026-03-10"));
        let statics = set(&Partitions::keys(["emea", "amer"]));
        assert!(statics.holds("amer") && !statics.holds("apac"));
        assert_eq!(statics.keys(), ["emea", "amer"]);
    }

    #[test]
    fn a_mapping_says_what_it_is_and_how_many_keys_it_names() {
        assert_eq!(PartitionMapping::identity().label(), "identity");
        assert_eq!(PartitionMapping::all().label(), "all");
        assert_eq!(PartitionMapping::covering().label(), "covering");
        assert_eq!(PartitionMapping::offset(-1).label(), "offset -1");
        assert_eq!(PartitionMapping::offset(2).label(), "offset +2");
        assert_eq!(PartitionMapping::default(), PartitionMapping::identity());
        assert!(PartitionMapping::identity().is_identity());
        assert!(!PartitionMapping::covering().is_identity());
        for one in [PartitionMapping::identity(), PartitionMapping::offset(-1)] {
            assert!(one.reads_one(), "{} names one key", one.label());
        }
        for many in [PartitionMapping::all(), PartitionMapping::covering()] {
            assert!(!many.reads_one(), "{} names a set", many.label());
        }
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
