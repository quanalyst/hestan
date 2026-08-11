//! the `capture` feature: a [`Layer`] that stores the `tracing` events an op
//! emits, so a run page shows what the libraries an op called had to say.
//!
//! hestan is a library inside somebody else's binary. it does not install a
//! global subscriber, it does not touch stdout, and it does not redirect a
//! file descriptor: all three would be hestan taking over output that belongs
//! to the host application. what it offers instead is a layer the host
//! composes into the subscriber it was going to build anyway — see
//! [`capture_layer`].
//!
//! the host's own logging carries on exactly as it did — this layer stores an
//! event only when the span it was emitted inside carries hestan's `run_id`,
//! `op` and `attempt` fields, which is a span only the executor opens. an
//! event from the host's http server, its startup, or a background task of its
//! own is not hestan's to record and is not recorded.
//!
//! two limits are worth knowing before you rely on it, both real and neither
//! hideable:
//!
//! - **`println!` in an in-process op is not captured.** fd 1 belongs to the
//!   whole process, and redirecting it would take the host's output with it.
//!   an [isolated op](crate::Op::isolated) is a process of its own, so there
//!   everything is captured, verbatim.
//! - **an event from a task the op spawned is not captured**, because
//!   `tokio::spawn` does not carry the span into the new task.
//!   `.instrument(tracing::Span::current())` on the spawned future puts it
//!   back, and then it is.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::logs::{Attempt, Budget, HESTAN, Source, TRACE_TARGET};
use crate::model::EventLevel;
use crate::store::Store;

/// how many captured events wait to be written before the layer starts
/// dropping them.
///
/// the emitting thread is an op doing its work and must never wait on a
/// database write, so this queue is bounded and a full one is a drop rather
/// than a stall.
/// four thousand is several seconds of a very chatty op at the rate a single
/// writer sustains.
const BUFFER: usize = 4_096;

/// how long the writer waits for the next event before looking for drops to
/// account for. a burst that ends without another event still gets its
/// "so many dropped" line, within this.
const IDLE: Duration = Duration::from_millis(100);

/// how many attempts the writer keeps a [`Budget`] for.
///
/// one entry per attempt that emitted anything, evicted oldest-first past
/// this. an attempt evicted while still running would get a fresh budget, so
/// this is set far above any plausible number of ops running at once rather
/// than at a number that trades memory for the cap.
const TRACKED: usize = 1_024;

/// a captured event on its way to the store.
struct Record {
    at: Attempt,
    level: EventLevel,
    target: String,
    message: String,
}

/// the layer [`capture_layer`] returns.
///
/// composable into any `tracing_subscriber` registry, and inert until an
/// event arrives from inside an op.
pub struct CaptureLayer {
    events: SyncSender<Record>,
    /// what the buffer was too full to take, per attempt. counted rather than
    /// silently lost: the run page says how many events it is missing.
    dropped: Arc<Mutex<HashMap<Attempt, u64>>>,
}

/// a [`Layer`] that stores the `tracing` events hestan's ops emit into
/// `store`, and nothing else.
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use tracing_subscriber::prelude::*;
///
/// let store = hestan::Store::open("hestan.db")?;
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::fmt::layer())
///     .with(hestan::capture_layer(&store))
///     .init();
/// # Ok(()) }
/// ```
///
/// the host's subscriber, composed by the host: hestan installs none. see the
/// [module docs](self) for what this deliberately does not capture, and
/// [`Hestan::log_limit`](crate::Hestan::log_limit) for the caps it writes
/// under.
///
/// filtering is the host's, as it is for every layer: add
/// `.with_filter(LevelFilter::INFO)` and hestan stores what survives it.
/// hestan's own run log has three levels, so a `TRACE` or `DEBUG` event is
/// stored as `info` — its `target` and message say the rest.
pub fn capture_layer(store: &Store) -> CaptureLayer {
    let (events, queue) = sync_channel(BUFFER);
    let dropped = Arc::new(Mutex::new(HashMap::new()));
    let writer = Writer {
        store: store.clone(),
        dropped: dropped.clone(),
        budgets: HashMap::new(),
        order: VecDeque::new(),
    };
    // a thread rather than a task: a layer is built wherever the host builds
    // its subscriber, which is usually before there is a runtime at all
    let spawned = std::thread::Builder::new()
        .name("hestan-capture".to_string())
        .spawn(move || writer.run(queue));
    if let Err(e) = spawned {
        // the layer stays, and drops everything into a channel nobody reads:
        // a host that cannot spawn a thread has worse problems than a missing
        // run log, and this is not the place to take its process down
        tracing::warn!("could not start the capture writer, op events will not be stored: {e}");
    }
    CaptureLayer { events, dropped }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// remember which attempt a span stands for, if it stands for one.
    ///
    /// read once here rather than at every event: the fields are on the span
    /// and the span outlives its events by definition.
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = SpanFields::default();
        attrs.record(&mut fields);
        let (Some(run_id), Some(op), Some(attempt)) = (fields.run_id, fields.op, fields.attempt)
        else {
            return;
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Attempt {
                run_id,
                op,
                attempt,
            });
        }
    }

    /// store one event, if it belongs to an op.
    ///
    /// the walk is outward from the event's own span to the root and stops at
    /// the first attempt it finds, so an op that opens spans of its own inside
    /// its body still attributes to the attempt those spans are under.
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // hestan narrating its own run log onto the trace bus. it is already
        // stored, as an event; storing it again as captured output would put
        // every line of the run log twice on the run page
        if event.metadata().target() == TRACE_TARGET {
            return;
        }
        let Some(mut scope) = ctx.event_scope(event) else {
            return;
        };
        let Some(at) = scope.find_map(|span| span.extensions().get::<Attempt>().cloned()) else {
            // an event outside every op: the host's, and none of hestan's
            // business
            return;
        };
        let mut visitor = Message::default();
        event.record(&mut visitor);
        let record = Record {
            at,
            level: level_of(event.metadata().level()),
            target: event.metadata().target().to_string(),
            message: visitor.finish(),
        };
        // never blocks: the thread emitting this is an op doing its work, and
        // making it wait on a database write to say something would be a worse
        // bug than the missing line
        if let Err(TrySendError::Full(record) | TrySendError::Disconnected(record)) =
            self.events.try_send(record)
        {
            *self.dropped.lock().unwrap().entry(record.at).or_insert(0) += 1;
        }
    }
}

/// the thread that writes what the layer collected.
struct Writer {
    store: Store,
    dropped: Arc<Mutex<HashMap<Attempt, u64>>>,
    /// how much of each attempt's cap is spent. bounded by [`TRACKED`].
    budgets: HashMap<Attempt, Budget>,
    /// insertion order, for evicting the oldest attempt when full.
    order: VecDeque<Attempt>,
}

impl Writer {
    fn run(mut self, queue: Receiver<Record>) {
        loop {
            match queue.recv_timeout(IDLE) {
                Ok(record) => {
                    self.account_for_drops();
                    let source = Source::Event {
                        level: record.level,
                        target: &record.target,
                    };
                    budget(&mut self.budgets, &mut self.order, &record.at).line(
                        &self.store,
                        &record.at,
                        source,
                        &record.message,
                    );
                }
                // a burst that stopped: say what was lost even though nothing
                // has arrived since
                Err(RecvTimeoutError::Timeout) => self.account_for_drops(),
                // every layer has been dropped, so there will be no more
                Err(RecvTimeoutError::Disconnected) => {
                    self.account_for_drops();
                    return;
                }
            }
        }
    }

    /// one line per attempt that lost events, under the same cap everything
    /// else is written under. a gap in a log that says it is a gap is worth
    /// something; one that does not is worse than nothing.
    fn account_for_drops(&mut self) {
        let lost: Vec<(Attempt, u64)> = self.dropped.lock().unwrap().drain().collect();
        for (at, n) in lost {
            let msg = format!(
                "{n} event{} dropped: hestan's capture buffer was full, and an op is never made \
                 to wait on it",
                if n == 1 { " was" } else { "s were" }
            );
            let source = Source::Event {
                level: EventLevel::Warn,
                target: HESTAN,
            };
            budget(&mut self.budgets, &mut self.order, &at).line(&self.store, &at, source, &msg);
        }
    }
}

/// one attempt's budget, evicting the oldest attempt when there are too many.
///
/// a free function over the two fields rather than a method, so the caller can
/// still hand the store to the budget it just took out.
///
/// the evicted attempt is all but certainly finished; one still running would
/// get a fresh allowance, which is why [`TRACKED`] is generous rather than
/// tight.
fn budget<'a>(
    budgets: &'a mut HashMap<Attempt, Budget>,
    order: &mut VecDeque<Attempt>,
    at: &Attempt,
) -> &'a mut Budget {
    if !budgets.contains_key(at) {
        budgets.insert(at.clone(), Budget::new());
        order.push_back(at.clone());
        while order.len() > TRACKED {
            if let Some(old) = order.pop_front() {
                budgets.remove(&old);
            }
        }
    }
    budgets.get_mut(at).expect("just inserted")
}

/// tracing has five levels and a run log has three. trace and debug are
/// stored as info rather than dropped: the `target` says where a line came
/// from, and a level nobody can see says nothing at all.
fn level_of(level: &Level) -> EventLevel {
    match *level {
        Level::ERROR => EventLevel::Error,
        Level::WARN => EventLevel::Warn,
        _ => EventLevel::Info,
    }
}

/// the three fields that make a span an op attempt.
///
/// both `record_str` and `record_debug` are implemented because `%value`
/// arrives as the second and a plain `&str` as the first, and a host writing
/// the span itself should not have to know which.
#[derive(Default)]
struct SpanFields {
    run_id: Option<String>,
    op: Option<String>,
    attempt: Option<u32>,
}

impl SpanFields {
    fn set(&mut self, field: &Field, value: String) {
        match field.name() {
            "run_id" => self.run_id = Some(value),
            "op" => self.op = Some(value),
            _ => {}
        }
    }
}

impl Visit for SpanFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.set(field, format!("{value:?}"));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "attempt" {
            self.attempt = Some(value as u32);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "attempt" && value >= 0 {
            self.attempt = Some(value as u32);
        }
    }
}

/// an event's message, with whatever else it carried after it.
///
/// `tracing::info!(rows = 12, "loaded")` is stored as `loaded rows=12`: one
/// line, in the order it reads, rather than a message column and a json blob
/// nobody looks at.
#[derive(Default)]
struct Message {
    message: String,
    fields: String,
}

impl Message {
    fn finish(self) -> String {
        match (self.message.is_empty(), self.fields.is_empty()) {
            (true, _) => self.fields.trim_start().to_string(),
            (false, true) => self.message,
            (false, false) => format!("{}{}", self.message, self.fields),
        }
    }

    fn put(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = value;
        } else {
            self.fields.push(' ');
            self.fields.push_str(field.name());
            self.fields.push('=');
            self.fields.push_str(&value);
        }
    }
}

impl Visit for Message {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.put(field, format!("{value:?}"));
    }
}
