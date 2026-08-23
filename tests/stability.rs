//! the enums that are still a closed set, matched with no `_` arm.
//!
//! this file is an integration test, which is to say a separate crate, and
//! that is the whole point of it: `#[non_exhaustive]` does not restrict the
//! crate that defines the type, so the same matches written inside `src/`
//! would compile whether the attribute were there or not and would prove
//! nothing at all. out here they compile only while the sets below stay
//! closed, so adding a variant to any of them, or marking one
//! `#[non_exhaustive]`, fails this file rather than somebody's build.
//!
//! it says nothing about the types that did get the attribute. the matches
//! that would prove those are the ones that no longer compile, and a test
//! cannot contain code that does not compile. `Trigger`'s own rustdoc carries
//! one as a `compile_fail` doc example instead, which `cargo test` runs.
//!
//! two further checks are here because they are the same promise made
//! another way: that a public enum added later cannot land without a
//! decision, and that the nine exit codes `docs/cli.md` publishes are the
//! nine the type carries.

use hestan::{
    Access, BackfillStatus, CancelOutcome, Catchup, CheckStatus, DeliveryState, EventLevel,
    Freshness, LateKind, LogStream, OpStatus, Overlap, Reclaim, Role, RunStatus, SensorOutcome,
    Severity,
};

fn late_kind(k: LateKind) -> &'static str {
    match k {
        LateKind::Asset => "asset",
        LateKind::Job => "job",
    }
}

fn access(a: Access) -> &'static str {
    match a {
        Access::Viewer => "viewer",
        Access::Operator => "operator",
        Access::Admin => "admin",
    }
}

fn run_status(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Success => "success",
        RunStatus::Failed => "failed",
        RunStatus::Canceled => "canceled",
    }
}

fn op_status(s: OpStatus) -> &'static str {
    match s {
        OpStatus::Pending => "pending",
        OpStatus::Running => "running",
        OpStatus::Success => "success",
        OpStatus::Failed => "failed",
        OpStatus::Skipped => "skipped",
        OpStatus::Canceled => "canceled",
    }
}

fn event_level(l: EventLevel) -> &'static str {
    match l {
        EventLevel::Info => "info",
        EventLevel::Warn => "warn",
        EventLevel::Error => "error",
    }
}

fn freshness(f: Freshness) -> &'static str {
    match f {
        Freshness::Fresh => "fresh",
        Freshness::Late { by: _ } => "late",
        Freshness::Never => "never",
    }
}

fn overlap(o: Overlap) -> &'static str {
    match o {
        Overlap::Allow => "allow",
        Overlap::Skip => "skip",
        Overlap::Queue => "queue",
    }
}

fn catchup(c: Catchup) -> &'static str {
    match c {
        Catchup::Skip => "skip",
        Catchup::One => "one",
        Catchup::All { limit: _ } => "all",
    }
}

fn role(r: Role) -> &'static str {
    match r {
        Role::All => "all",
        Role::Scheduler => "scheduler",
        Role::Worker => "worker",
    }
}

fn delivery_state(d: DeliveryState) -> &'static str {
    match d {
        DeliveryState::Pending => "pending",
        DeliveryState::Failed => "failed",
        DeliveryState::Delivered => "delivered",
    }
}

fn log_stream(s: LogStream) -> &'static str {
    match s {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn severity(s: Severity) -> &'static str {
    match s {
        Severity::Warn => "warn",
        Severity::Error => "error",
    }
}

fn check_status(s: CheckStatus) -> &'static str {
    match s {
        CheckStatus::Passed => "passed",
        CheckStatus::Failed => "failed",
    }
}

fn backfill_status(s: BackfillStatus) -> &'static str {
    match s {
        BackfillStatus::Running => "running",
        BackfillStatus::Complete => "complete",
        BackfillStatus::Failed => "failed",
        BackfillStatus::Canceled => "canceled",
    }
}

fn sensor_outcome(o: SensorOutcome) -> &'static str {
    match o {
        SensorOutcome::Fired => "fired",
        SensorOutcome::Error => "error",
        SensorOutcome::Skipped => "skipped",
    }
}

fn cancel_outcome(o: CancelOutcome) -> &'static str {
    match o {
        CancelOutcome::Requested => "requested",
        CancelOutcome::AlreadyFinished => "already_finished",
        CancelOutcome::Unknown => "unknown",
    }
}

fn reclaim_is_not_here(_: Reclaim) {
    // Reclaim is `#[non_exhaustive]`, and it is imported so that this file
    // names every enum the decision was made about: the ones with a match
    // above are the closed sets, and this one is here to be conspicuously
    // without one.
}

/// the assertion is that this file compiled. the calls are so that each match
/// above is reachable code rather than something the compiler is entitled to
/// discard, and the strings are the words the api and the store already use.
#[test]
fn the_closed_sets_still_match_without_a_wildcard() {
    assert_eq!(late_kind(LateKind::Job), "job");
    assert_eq!(access(Access::Admin), "admin");
    assert_eq!(run_status(RunStatus::Queued), "queued");
    assert_eq!(op_status(OpStatus::Skipped), "skipped");
    assert_eq!(event_level(EventLevel::Warn), "warn");
    assert_eq!(freshness(Freshness::Never), "never");
    assert_eq!(overlap(Overlap::Queue), "queue");
    assert_eq!(catchup(Catchup::All { limit: 24 }), "all");
    assert_eq!(role(Role::Worker), "worker");
    assert_eq!(delivery_state(DeliveryState::Delivered), "delivered");
    assert_eq!(log_stream(LogStream::Stderr), "stderr");
    assert_eq!(severity(Severity::Error), "error");
    assert_eq!(check_status(CheckStatus::Passed), "passed");
    assert_eq!(backfill_status(BackfillStatus::Complete), "complete");
    assert_eq!(sensor_outcome(SensorOutcome::Skipped), "skipped");
    assert_eq!(cancel_outcome(CancelOutcome::Unknown), "unknown");
    reclaim_is_not_here(Reclaim::Requeue);
}

/// the exit codes, which are the closed set with a published table behind it:
/// `docs/cli.md` prints these nine and a cron line is a `case` over them.
#[cfg(feature = "cli")]
#[test]
fn the_exit_codes_still_match_without_a_wildcard() {
    use hestan::cli::Exit;

    fn code(e: Exit) -> u8 {
        match e {
            Exit::Ok => 0,
            Exit::Failed => 1,
            Exit::Usage => 2,
            Exit::Canceled => 3,
            Exit::Timeout => 4,
            Exit::Unreachable => 5,
            Exit::Unsupported => 6,
            Exit::Actionable => 7,
            Exit::Denied => 8,
        }
    }

    // and the number each one carries is the number the table publishes
    assert_eq!(code(Exit::Ok), Exit::Ok as u8);
    assert_eq!(code(Exit::Denied), 8);
    assert_eq!(code(Exit::Unreachable), Exit::Unreachable as u8);
}

/// the seventeen closed sets by name, so that the check below can tell a
/// public enum nobody decided about from one that was decided and left
/// matchable on purpose. the same list as the matches above, written twice
/// because a `match` arm is not something a test can read back.
const CLOSED: &[&str] = &[
    "Access",
    "BackfillStatus",
    "CancelOutcome",
    "Catchup",
    "CheckStatus",
    "DeliveryState",
    "EventLevel",
    "Exit",
    "Freshness",
    "LateKind",
    "LogStream",
    "OpStatus",
    "Overlap",
    "Role",
    "RunStatus",
    "SensorOutcome",
    "Severity",
];

/// a public enum that is neither `#[non_exhaustive]` nor named in `CLOSED` is
/// one nobody made the call about, which is the state every enum in the crate
/// was in until this file existed. so it fails here rather than becoming
/// somebody's break later.
///
/// it reads `src/` as text, which is crude and is the point: an enum added
/// tomorrow is invisible to a test written against the types of today.
#[test]
fn every_public_enum_is_either_marked_or_listed_here() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = 0usize;
    let mut undecided: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&src).expect("src/") {
        let path = entry.expect("a directory entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("a source file");
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let Some(rest) = line.strip_prefix("pub enum ") else {
                continue;
            };
            let name = rest.split([' ', '<']).next().unwrap_or(rest);
            found += 1;
            let marked = lines[..i]
                .iter()
                .rev()
                .take_while(|l| l.starts_with("#["))
                .any(|l| *l == "#[non_exhaustive]");
            if !marked && !CLOSED.contains(&name) {
                undecided.push(format!("{}: {name}", path.display()));
            }
        }
    }
    assert!(
        undecided.is_empty(),
        "public enums with nothing recorded about them: mark one \
         `#[non_exhaustive]`, or add it to CLOSED and to a match above, and \
         say which in its rustdoc: {undecided:?}"
    );
    // and something was actually read, so a scraper that stopped matching
    // anything cannot pass over an empty set
    assert!(found >= 20, "only {found} public enums found in src/");
    assert_eq!(
        CLOSED.len(),
        17,
        "CLOSED and the matches above have drifted"
    );
}

/// the exit codes are the closed set with a table published behind it, so the
/// table and the type are asserted against each other rather than kept in step
/// by hand.
#[cfg(feature = "cli")]
#[test]
fn the_published_exit_codes_are_the_ones_the_type_carries() {
    use hestan::cli::Exit;

    let md = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/cli.md"),
    )
    .expect("docs/cli.md");
    let published: Vec<u8> = md
        .split_once("## The exit codes")
        .expect("the exit code section")
        .1
        .lines()
        .skip_while(|l| !l.starts_with("| code |"))
        .skip(2)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            l.trim_start_matches("| ")
                .split(' ')
                .next()
                .expect("a first cell")
                .parse()
                .expect("a number in the first column")
        })
        .collect();
    let carried = [
        Exit::Ok,
        Exit::Failed,
        Exit::Usage,
        Exit::Canceled,
        Exit::Timeout,
        Exit::Unreachable,
        Exit::Unsupported,
        Exit::Actionable,
        Exit::Denied,
    ]
    .map(|e| e as u8);
    assert_eq!(
        published,
        carried.to_vec(),
        "docs/cli.md publishes exit codes the Exit variants do not carry"
    );
}
