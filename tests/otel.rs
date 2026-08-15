//! the shape a run has on the trace bus, against a real subscriber.
//!
//! a binary of its own for the reason `tests/capture.rs` is one, and it is
//! worth knowing: `tracing` caches a callsite's interest the first time that
//! callsite is hit, using whatever subscriber the thread that hit it had. in a
//! binary where hundreds of other tests run ops with no subscriber installed,
//! the executor's spans would be registered as "nobody is interested" by
//! whichever thread got there first, and these cases would fail about one run
//! in three.
//!
//! nothing here exports anything. what is being asserted is the tree hestan
//! opens (which span is whose parent, what each carries, and where an event
//! lands) because that tree is the whole of what hestan contributes to a
//! trace. turning it into otel spans is `tracing-opentelemetry`'s job, and
//! testing that would be testing somebody else's crate.

use std::sync::{Arc, Mutex};

use hestan::prelude::*;
use hestan::{Runner, Store, Trigger};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone)]
struct Span {
    id: u64,
    name: String,
    parent: Option<u64>,
    fields: Vec<(String, String)>,
}

impl Span {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, Clone)]
struct Recorded {
    target: String,
    message: String,
    kind: Option<String>,
    /// the span it was emitted inside, if any.
    span: Option<u64>,
}

#[derive(Default)]
struct Seen {
    spans: Vec<Span>,
    events: Vec<Recorded>,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Seen>>);

impl<S> Layer<S> for Recorder
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        self.0.lock().unwrap().spans.push(Span {
            id: id.into_u64(),
            name: attrs.metadata().name().to_string(),
            // the parent hestan asked for, or the one it was opened inside
            parent: attrs
                .parent()
                .cloned()
                .or_else(|| ctx.current_span().id().cloned())
                .map(|p| p.into_u64()),
            fields: fields.0,
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let get = |name: &str| {
            fields
                .0
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        self.0.lock().unwrap().events.push(Recorded {
            target: event.metadata().target().to_string(),
            message: get("message").unwrap_or_default(),
            kind: get("kind"),
            span: ctx.event_span(event).map(|s| s.id().into_u64()),
        });
    }
}

#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

fn flaky_job(fails: Arc<std::sync::atomic::AtomicU32>) -> Job {
    Job::builder("etl")
        .op(Op::new("extract", |_| async { Ok(json!({ "rows": 3 })) }))
        .op(Op::new("load", move |ctx: OpCtx| {
            let fails = fails.clone();
            async move {
                ctx.info("loading");
                ctx.meta("rows", 3_i64);
                if fails.fetch_sub(0, std::sync::atomic::Ordering::SeqCst) > 0
                    && fails.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) > 0
                {
                    return Err("warehouse said no".into());
                }
                Ok(json!(null))
            }
        })
        .after(["extract"])
        .retries(1))
        .build()
        .unwrap()
}

/// a current-thread runtime inside `with_default`, so every task hestan spawns
/// polls on the thread the subscriber is the default for. hestan installs no
/// subscriber anywhere (that is the claim) so a test has to install one to
/// see anything at all, and a multi-threaded runtime would put half the run on
/// threads that have none.
#[test]
fn a_run_is_a_tree_of_spans_with_its_events_on_them() {
    let seen = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(seen.clone());
    let fails = Arc::new(std::sync::atomic::AtomicU32::new(1));
    let runner = Runner::new([flaky_job(fails)], Store::open(":memory:").unwrap()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let run = tracing::subscriber::with_default(subscriber, || {
        rt.block_on(runner.run("etl", json!({}), Trigger::Manual))
            .unwrap()
    });
    assert_eq!(run.status, RunStatus::Success);

    let seen = seen.0.lock().unwrap();
    let runs: Vec<&Span> = seen
        .spans
        .iter()
        .filter(|s| s.name == "hestan.run")
        .collect();
    assert_eq!(runs.len(), 1, "a run is one span");
    let root = runs[0];
    assert_eq!(root.field("run_id"), Some(run.id.as_str()));
    assert_eq!(root.field("job"), Some("etl"));
    assert_eq!(root.field("trigger"), Some("manual"));

    // every attempt is a span, and every one of them hangs off the run
    let ops: Vec<&Span> = seen
        .spans
        .iter()
        .filter(|s| s.name == "hestan.op")
        .collect();
    assert!(
        ops.iter().all(|s| s.parent == Some(root.id)),
        "an op span floated free of its run"
    );
    assert_eq!(
        ops.iter()
            .map(|s| (s.field("op").unwrap(), s.field("attempt").unwrap()))
            .collect::<Vec<_>>(),
        [("extract", "1"), ("load", "1"), ("load", "2")],
        "a retry is a span of its own, not an annotation on the first"
    );

    // hestan's own run log lands on the trace as span events, on the run
    let narration: Vec<&Recorded> = seen
        .events
        .iter()
        .filter(|e| e.target == "hestan::events")
        .collect();
    let kinds: Vec<&str> = narration.iter().filter_map(|e| e.kind.as_deref()).collect();
    for expected in [
        "run_queued",
        "run_started",
        "op_started",
        "op_retry",
        "run_success",
    ] {
        assert!(
            kinds.contains(&expected),
            "no {expected} on the trace: {kinds:?}"
        );
    }
    assert!(
        narration
            .iter()
            .filter(|e| e.kind.as_deref() == Some("run_started"))
            .all(|e| e.span == Some(root.id)),
        "the run's own narration did not land on the run's span"
    );

    // and what the op body said lands on that attempt's span rather than on
    // the run's, which is the difference between a trace and a list
    let load = ops
        .iter()
        .find(|s| s.field("attempt") == Some("2"))
        .unwrap();
    let said = seen
        .events
        .iter()
        .find(|e| e.message == "loading" && e.span == Some(load.id));
    assert!(
        said.is_some(),
        "ctx.info did not land on the attempt's span"
    );
}
