//! a param a caller declares secret, and what hestan does and does not do
//! with it.
//!
//! a launch's params are written to `runs.params`, shown on the run page,
//! returned by `GET /api/runs`, printed by the cli and kept until
//! [retention](crate::Retention) prunes them. that is the right shape for a
//! date or a region and the wrong shape for a deploy token, so an op may
//! declare which of its params are credentials:
//!
//! ```
//! # use hestan::{Op, OpCtx};
//! # use serde_json::json;
//! Op::new("deploy", |ctx: OpCtx| async move {
//!     let token = ctx.params()["token"].as_str().unwrap_or_default();
//!     # let _ = token;
//!     Ok(json!(null))
//! })
//! .secret_params(["token"]);
//! ```
//!
//! # Where the redaction happens
//!
//! **in the store, not in any renderer.** [`Store`](crate::Store) holds the
//! declarations and applies them to every params column it writes:
//! `runs.params`, `schedules.params` and `presets.params`. a declared-secret
//! param is replaced with [`REDACTED`] there, before the insert, so the value
//! is not in the database at all. the api, the ui, the event log, the cli, a
//! `psql` session and a route somebody adds next month are all reading a row
//! that never held it, which is the only arrangement a new reader cannot
//! bypass by existing.
//!
//! the ops still get the value. the process that took the launch keeps it in
//! memory, keyed by run id, and puts it back into the params of the run it is
//! about to execute. it is never written down on the way.
//!
//! # What a secret means for a replay
//!
//! **a replay, a resume and a retry of a run that carried a secret param are
//! refused, and the refusal names the param.** the store holds [`REDACTED`]
//! where the token was, and re-launching from that row would run the deploy
//! with the literal string `[hestan:redacted]` as its credential: a run that
//! fails confusingly at best and authenticates as something unintended at
//! worst. so the marker is refused as a param value at
//! [launch](crate::Runner::launch), which is the one funnel every run in
//! hestan goes through, and a re-run means launching again and supplying the
//! value again.
//!
//! that is the cost of the feature and it is the point of it: hestan is not a
//! secret store, and the credential is somewhere that can hand it over again.
//!
//! # What is only best effort
//!
//! the declaration is exact: a declared name is redacted whatever its value.
//! two things around it are not.
//!
//! - **an op that copies its own secret somewhere.** `ctx.params()["token"]`
//!   is a `&str` like any other, and an op is free to log it or put it in
//!   metadata. so, as a **second line and not the first**, while a run holding
//!   secret values is executing in this process, every string bound to every
//!   statement the store issues is scanned for those values and any that
//!   appears is replaced. that catches a copy; it does not catch a
//!   transformation, and it only scans for values of at least
//!   [`SCANNED_FROM`] characters, because hunting a shorter string through
//!   every write would rewrite ids, timestamps and job names across the
//!   database.
//! - **nothing guesses.** hestan does not match param names against a pattern
//!   like `token|secret|password`: a pattern misses the credential somebody
//!   called `key2` and redacts the innocent column named `password_column`,
//!   and a redaction that is sometimes wrong is one nobody can reason about.
//!   a param nobody declared is stored.
//!
//! # The limits, plainly
//!
//! - **top-level keys only.** `{"token": "…"}` is redacted;
//!   `{"db": {"password": "…"}}` is not. the declaration names a param, and a
//!   param is a key of the object a launch was given.
//! - **one process.** the value lives in the memory of the process that took
//!   the launch. a worker in another process that claims the run finds the
//!   marker, refuses to execute on it, and fails the run saying so. secret
//!   params therefore work on a single-process deployment and on a
//!   multi-process one only when whatever launched also executes. if that is
//!   not the shape, put the credential in a
//!   [resource](crate::Hestan::resource): a resource is process
//!   configuration, it is built where the op runs, and it is never run data.
//! - **not a lifetime.** the value is dropped when the run finishes here, or
//!   when [`HELD_CAP`] later runs have pushed it out, whichever comes first.
//!   nothing persists it and nothing recovers it.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};

use serde_json::Value;

/// what a stored param says where a declared secret was.
///
/// deliberately not `[redacted]`: this is refused as a param value at launch
/// (see the [module docs](crate::secret)), so it has to be a string nobody
/// types by accident.
pub const REDACTED: &str = "[hestan:redacted]";

/// the shortest secret value the second line scans writes for.
///
/// below this a value is still kept out of the params column by its
/// declaration; what it is not is hunted through every other string the store
/// writes, because a six-character needle matches inside run ids and
/// timestamps and would corrupt them.
pub const SCANNED_FROM: usize = 16;

/// how many runs' secret values one process keeps at once.
///
/// each is dropped when its run finishes here, so this only bounds the runs
/// this process launched and something else claimed. the oldest goes first,
/// and a run whose values have gone fails rather than executing on the
/// marker.
pub const HELD_CAP: usize = 4_096;

/// the declarations, and the values this process is holding for runs it took.
///
/// it lives on the [`Store`](crate::Store) because the store is the thing
/// that must never write one down, and because a store handle is what every
/// part of hestan already has.
#[derive(Default)]
pub(crate) struct Vault {
    /// job name to the params that job's ops declared secret.
    declared: RwLock<HashMap<String, BTreeSet<String>>>,
    /// whether anything at all is declared, so an ordinary deployment pays one
    /// atomic load per write and no lock.
    declares: AtomicBool,
    held: Mutex<Held>,
    /// whether `held` has a value long enough to scan for, for the same
    /// reason.
    scanning: AtomicBool,
}

#[derive(Default)]
struct Held {
    /// run id to the secret params that run was launched with.
    by_run: HashMap<String, BTreeMap<String, Value>>,
    /// insertion order, so the cap drops the oldest run rather than an
    /// arbitrary one.
    order: VecDeque<String>,
    /// every distinct value long enough to scan for, and how many runs hold
    /// it.
    scan: BTreeMap<String, usize>,
}

impl Vault {
    /// record what `job`'s ops declared secret. called once per job when a
    /// [`Runner`](crate::Runner) is built, and additive: two runners over one
    /// store both have jobs, and both sets are true.
    pub(crate) fn declare(&self, job: &str, names: BTreeSet<String>) {
        if names.is_empty() {
            return;
        }
        let mut declared = self.declared.write().expect("the declarations");
        declared.entry(job.to_string()).or_default().extend(names);
        self.declares.store(true, Ordering::Release);
    }

    /// what `job` declared secret, or nothing.
    pub(crate) fn declared_by(&self, job: &str) -> BTreeSet<String> {
        if !self.declares.load(Ordering::Acquire) {
            return BTreeSet::new();
        }
        self.declared
            .read()
            .expect("the declarations")
            .get(job)
            .cloned()
            .unwrap_or_default()
    }

    /// `params` with every param `job` declared secret replaced by
    /// [`REDACTED`]. this is what goes in a params column.
    pub(crate) fn redact<'a>(&self, job: &str, params: &'a Value) -> Cow<'a, Value> {
        redact_with(&self.declared_by(job), params)
    }

    /// the values `redact` would replace, to be held while the run executes.
    pub(crate) fn secrets_in(&self, job: &str, params: &Value) -> BTreeMap<String, Value> {
        secrets_with(&self.declared_by(job), params)
    }

    /// the top-level params of `params` that are already the marker: a launch
    /// reading back a run's stored params rather than being given the values.
    pub(crate) fn marked(params: &Value) -> Vec<String> {
        params
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, v)| v.as_str() == Some(REDACTED))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// keep `values` for `run_id` until the run finishes here.
    pub(crate) fn hold(&self, run_id: &str, values: BTreeMap<String, Value>) {
        if values.is_empty() {
            return;
        }
        let mut held = self.held.lock().expect("the held values");
        if let Some(old) = held.by_run.remove(run_id) {
            held.unscan(&old);
            held.order.retain(|id| id != run_id);
        }
        held.scan_for(&values);
        held.by_run.insert(run_id.to_string(), values);
        held.order.push_back(run_id.to_string());
        while held.order.len() > HELD_CAP {
            let Some(oldest) = held.order.pop_front() else {
                break;
            };
            if let Some(gone) = held.by_run.remove(&oldest) {
                held.unscan(&gone);
            }
        }
        self.scanning
            .store(!held.scan.is_empty(), Ordering::Release);
    }

    /// let go of what was held for `run_id`. nothing recovers it afterwards,
    /// which is what makes a replay of that run a refusal rather than a run on
    /// a stale credential.
    pub(crate) fn release(&self, run_id: &str) {
        if !self.scanning.load(Ordering::Acquire) {
            return;
        }
        let mut held = self.held.lock().expect("the held values");
        if let Some(gone) = held.by_run.remove(run_id) {
            held.unscan(&gone);
            held.order.retain(|id| id != run_id);
        }
        self.scanning
            .store(!held.scan.is_empty(), Ordering::Release);
    }

    /// put this run's secret values back into the params the store handed
    /// back, and say which markers had nothing to put there.
    ///
    /// the second half is the whole of what a worker in another process sees:
    /// the marker, and no way to turn it back into the credential.
    pub(crate) fn restore(&self, run_id: &str, params: &mut Value) -> Vec<String> {
        let marked = Vault::marked(params);
        if marked.is_empty() {
            return Vec::new();
        }
        let values = self
            .held
            .lock()
            .expect("the held values")
            .by_run
            .get(run_id)
            .cloned()
            .unwrap_or_default();
        let object = params.as_object_mut().expect("marked params are an object");
        let mut missing = Vec::new();
        for name in marked {
            match values.get(&name) {
                Some(value) => {
                    object.insert(name, value.clone());
                }
                None => missing.push(name),
            }
        }
        missing
    }

    /// `s` with every value this process is holding replaced by [`REDACTED`].
    ///
    /// the second line: what an op copied out of its params into a log line,
    /// a metadata value or an error message. borrowed and untouched when
    /// nothing is held, which is every deployment that declares no secrets and
    /// every moment of one that does with no such run in flight.
    pub(crate) fn scrub<'a>(&self, s: &'a str) -> Cow<'a, str> {
        if !self.scanning.load(Ordering::Acquire) {
            return Cow::Borrowed(s);
        }
        let held = self.held.lock().expect("the held values");
        let mut out = Cow::Borrowed(s);
        for value in held.scan.keys() {
            if out.contains(value.as_str()) {
                out = Cow::Owned(out.replace(value.as_str(), REDACTED));
            }
        }
        out
    }
}

impl Held {
    /// count the scannable strings in `values` in.
    fn scan_for(&mut self, values: &BTreeMap<String, Value>) {
        for value in values.values() {
            if let Some(text) = scannable(value) {
                *self.scan.entry(text).or_default() += 1;
            }
        }
    }

    /// and back out, dropping the ones nothing holds any more.
    fn unscan(&mut self, values: &BTreeMap<String, Value>) {
        for value in values.values() {
            let Some(text) = scannable(value) else {
                continue;
            };
            if let Some(count) = self.scan.get_mut(&text) {
                *count -= 1;
                if *count == 0 {
                    self.scan.remove(&text);
                }
            }
        }
    }
}

/// `params` with every one of `names` replaced by [`REDACTED`].
///
/// the store reaches this through [`Vault::redact`], which is the only path
/// that matters for what is written down. it is also what the cli's
/// `--dry-run` prints through, since that renders params the store never
/// sees and would otherwise be the one place a declared secret was echoed
/// back.
pub(crate) fn redact_with<'a>(names: &BTreeSet<String>, params: &'a Value) -> Cow<'a, Value> {
    let Some(object) = params.as_object() else {
        return Cow::Borrowed(params);
    };
    if !names.iter().any(|n| object.contains_key(n)) {
        return Cow::Borrowed(params);
    }
    let mut out = object.clone();
    for name in names {
        if let Some(slot) = out.get_mut(name) {
            *slot = Value::String(REDACTED.to_string());
        }
    }
    Cow::Owned(Value::Object(out))
}

/// the values `redact_with` would replace, by name.
pub(crate) fn secrets_with(names: &BTreeSet<String>, params: &Value) -> BTreeMap<String, Value> {
    let Some(object) = params.as_object() else {
        return BTreeMap::new();
    };
    names
        .iter()
        .filter_map(|name| object.get(name).map(|v| (name.clone(), v.clone())))
        .collect()
}

/// `text` with each of `values` replaced by [`REDACTED`], whatever its
/// length.
///
/// for the one string hestan produces that quotes a param back at whoever
/// sent it: the reason an op's params check gave for refusing a launch. serde
/// says `invalid type: string "hunter2", expected u64`, which is the classic
/// leak, and the values are known exactly here rather than guessed at, so
/// there is no floor to apply: both the value and its json spelling go.
pub(crate) fn hide(values: &BTreeMap<String, Value>, text: &str) -> String {
    let mut out = text.to_string();
    for value in values.values() {
        for form in [value.as_str().map(str::to_string), Some(value.to_string())]
            .into_iter()
            .flatten()
            .filter(|f| !f.is_empty())
        {
            out = out.replace(&form, REDACTED);
        }
    }
    out
}

/// a secret value the second line will look for: a string, long enough that
/// finding it inside another write means something.
///
/// a number or an object is not scanned for. `{"port": 5432}` appears inside
/// half the timestamps in the database, and a json rendering of an object is
/// not the text an op would have written anyway.
fn scannable(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|s| s.chars().count() >= SCANNED_FROM)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn vault() -> Vault {
        let vault = Vault::default();
        vault.declare("deploy", BTreeSet::from(["token".to_string()]));
        vault
    }

    #[test]
    fn a_declared_param_is_the_marker_and_everything_else_is_untouched() {
        let vault = vault();
        let params = json!({"token": "hunter2", "env": "prod", "wait": 30});
        let stored = vault.redact("deploy", &params);
        assert_eq!(
            *stored,
            json!({"token": REDACTED, "env": "prod", "wait": 30})
        );
        // byte for byte the same value, not a re-serialization of it
        assert_eq!(stored["env"], params["env"]);
        assert_eq!(stored["wait"], params["wait"]);
    }

    #[test]
    fn a_job_that_declared_nothing_gets_its_params_back_unchanged() {
        let vault = vault();
        let params = json!({"token": "hunter2"});
        assert!(matches!(vault.redact("report", &params), Cow::Borrowed(_)));
        // and a declared job that was passed nothing secret is untouched too
        assert!(matches!(
            vault.redact("deploy", &json!({"env": "prod"})),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn the_marker_is_what_a_stored_param_reads_back_as() {
        assert_eq!(Vault::marked(&json!({"token": REDACTED})), ["token"]);
        assert!(Vault::marked(&json!({"token": "hunter2"})).is_empty());
        assert!(Vault::marked(&json!("not an object")).is_empty());
    }

    #[test]
    fn a_held_value_comes_back_and_a_released_one_is_named_as_gone() {
        let vault = vault();
        let params = json!({"token": "a-token-long-enough", "env": "prod"});
        vault.hold("r1", vault.secrets_in("deploy", &params));

        let mut stored = vault.redact("deploy", &params).into_owned();
        assert_eq!(vault.restore("r1", &mut stored), Vec::<String>::new());
        assert_eq!(stored, params);

        // another process, or this one after the run ended: the marker and no
        // way back to the value
        let mut stored = vault.redact("deploy", &params).into_owned();
        assert_eq!(vault.restore("r2", &mut stored), ["token"]);
        assert_eq!(stored["token"], REDACTED);

        vault.release("r1");
        let mut stored = vault.redact("deploy", &params).into_owned();
        assert_eq!(vault.restore("r1", &mut stored), ["token"]);
    }

    #[test]
    fn a_held_value_is_scrubbed_out_of_any_string_while_the_run_is_live() {
        let vault = vault();
        let params = json!({"token": "a-token-long-enough"});
        assert_eq!(
            vault.scrub("logging a-token-long-enough"),
            "logging a-token-long-enough"
        );
        vault.hold("r1", vault.secrets_in("deploy", &params));
        assert_eq!(
            vault.scrub("logging a-token-long-enough now"),
            format!("logging {REDACTED} now")
        );
        vault.release("r1");
        assert_eq!(
            vault.scrub("logging a-token-long-enough"),
            "logging a-token-long-enough"
        );
    }

    // the second line has a floor, and the floor is why an id is not rewritten
    #[test]
    fn a_short_value_is_kept_out_of_params_and_not_hunted_through_writes() {
        let vault = vault();
        let params = json!({"token": "short"});
        assert_eq!(vault.redact("deploy", &params)["token"], REDACTED);
        vault.hold("r1", vault.secrets_in("deploy", &params));
        assert_eq!(
            vault.scrub("a run of the short job"),
            "a run of the short job"
        );
        // and it still comes back for the ops of its own run
        let mut stored = vault.redact("deploy", &params).into_owned();
        assert!(vault.restore("r1", &mut stored).is_empty());
        assert_eq!(stored["token"], "short");
    }

    // the message a failed params check produces quotes the value back, which
    // is how a credential reaches a log without anything ever storing it
    #[test]
    fn a_refusal_that_quotes_the_params_quotes_the_marker_instead() {
        let values = BTreeMap::from([("token".to_string(), json!("hunter2"))]);
        assert_eq!(
            hide(&values, r#"invalid type: string "hunter2", expected u64"#),
            format!("invalid type: string \"{REDACTED}\", expected u64")
        );
        // and a value that is not a string is hidden in its json spelling
        let numeric = BTreeMap::from([("pin".to_string(), json!(9182))]);
        assert_eq!(
            hide(&numeric, "invalid type: integer 9182, expected a string"),
            format!("invalid type: integer {REDACTED}, expected a string")
        );
    }

    #[test]
    fn two_runs_holding_one_value_keep_it_until_both_are_done() {
        let vault = vault();
        let params = json!({"token": "a-token-long-enough"});
        vault.hold("r1", vault.secrets_in("deploy", &params));
        vault.hold("r2", vault.secrets_in("deploy", &params));
        vault.release("r1");
        assert_eq!(
            vault.scrub("a-token-long-enough"),
            REDACTED,
            "the other run still holds it"
        );
        vault.release("r2");
        assert_eq!(vault.scrub("a-token-long-enough"), "a-token-long-enough");
    }
}
