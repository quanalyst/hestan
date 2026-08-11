//! the `otel` feature: a run as a distributed trace.
//!
//! hestan already knows a run is a causal tree — a run, its ops, an attempt
//! each, and for an [isolated op](crate::Op::isolated) a subprocess under that.
//! that is what a trace is, and emitting it as one puts a pipeline in the same
//! waterfall as the services it calls.
//!
//! **hestan installs nothing.** no subscriber, no tracer provider, no
//! exporter. it opens `tracing` spans with the right shape and the right
//! fields, and the host composes
//! [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) into the
//! subscriber it was going to build anyway — the same arrangement
//! [`capture_layer`][cap] uses, for the same reason: the
//! subscriber belongs to the application, not to a library inside it.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use tracing_subscriber::prelude::*;
//!
//! // your provider, your exporter, your sampling
//! let tracer = my_tracer();
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(tracing_opentelemetry::layer().with_tracer(tracer))
//!     .init();
//! # Ok(()) }
//! # fn my_tracer() -> opentelemetry_sdk::trace::SdkTracer { unimplemented!() }
//! ```
//!
//! everything below is what hestan does *on top of* that, which is the part
//! nothing else can do for it: carrying the trace context across the process
//! boundary an isolated op puts in the middle of a run.
//!
//! # The shape
//!
//! | hestan | span |
//! | --- | --- |
//! | a run | `hestan.run`, the root, with `run_id`, `job` and `trigger` |
//! | an attempt of an op | `hestan.op` beneath it, with `run_id`, `op` and `attempt` |
//! | a retry | another `hestan.op`, with the next `attempt` — a span of its own, not an annotation on the first |
//! | an event | a span event on the `hestan.op` it belongs to, or on `hestan.run` for a run-level one |
//!
//! the field names are exactly the ones
//! [`capture_layer`][cap] reads, because they are the same
//! spans. a build with both features composes both layers and each takes what
//! it wants.
//!
//! # What this cannot do, stated rather than papered over
//!
//! **an isolated op's subprocess is only in the trace if the host puts it
//! there.** hestan hands the child a `traceparent` in its environment and the
//! child parents its span to it — that part works, and it is the part nothing
//! else does. but a span is only exported by a provider, and the provider in
//! the child process is the *host's*, built by the host's `main`. so:
//!
//! - a child whose binary composes an otel layer produces spans that nest
//!   correctly under the parent's `hestan.op`. this is the case worth having
//!   and it is the case hestan can create.
//! - a child that composes no layer produces no spans. the parent's
//!   `hestan.op` still covers the whole of the child's execution — hestan
//!   times the subprocess — so the trace is complete at op granularity and
//!   missing only what happened *inside* the child.
//! - **hestan cannot flush the child's exporter.** a batch exporter that has
//!   not shipped its spans when the child exits loses them, and the provider
//!   belongs to the host. a child role that exports must flush before
//!   returning from `main`. hestan will not reach into a provider it does not
//!   own to do it, and a library that did would be taking over the
//!   application's telemetry.
//!
//! **nothing is sampled, tagged or named by hestan beyond the above.** there
//! is no `hestan::otel::init`, no exporter helper and no environment variable
//! hestan reads. those are the host's, and a library that shipped its own
//! would be one more thing to fight with the host's.
//!
#![cfg_attr(feature = "capture", doc = "[cap]: crate::capture_layer")]
#![cfg_attr(not(feature = "capture"), doc = "[cap]: crate")]

use std::collections::HashMap;

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// the environment variable an [isolated op](crate::Op::isolated)'s child is
/// handed its parent's trace context in.
///
/// the [w3c name](https://www.w3.org/TR/trace-context/), because that is what
/// it holds: `00-{trace_id}-{span_id}-{flags}`. anything else reading this
/// process's environment for a traceparent gets a correct one.
pub const TRACEPARENT: &str = "traceparent";

/// and the state header beside it, for a vendor that uses one.
pub const TRACESTATE: &str = "tracestate";

/// this span's trace context, as the environment a child should be given.
///
/// empty when the host has composed no otel layer, which is the ordinary case
/// for a build that merely turned the feature on: there is no context to
/// carry, and an invented one would be a trace id that leads nowhere.
pub(crate) fn carry(span: &Span) -> Vec<(String, String)> {
    let mut carrier = Carrier::default();
    TraceContextPropagator::new().inject_context(&span.context(), &mut carrier);
    carrier.0.into_iter().collect()
}

/// make `span` a child of the trace context in this process's environment.
///
/// called by an isolated op's child before it runs the body. a process started
/// by anything other than hestan has no such variable and this does nothing,
/// which is the same as saying the span is a root.
pub(crate) fn adopt(span: &Span) {
    let mut carrier = Carrier::default();
    for key in [TRACEPARENT, TRACESTATE] {
        if let Ok(value) = std::env::var(key) {
            carrier.0.insert(key.to_string(), value);
        }
    }
    if carrier.0.is_empty() {
        return;
    }
    // errs only when there is no otel layer to set it on, which is the same
    // "nothing composed" case `carry` answers with nothing
    let _ = span.set_parent(TraceContextPropagator::new().extract(&carrier));
}

/// a string map, which is all the w3c propagator wants of a carrier.
#[derive(Default)]
struct Carrier(HashMap<String, String>);

impl Injector for Carrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

impl Extractor for Carrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider;
    use tracing_subscriber::prelude::*;

    /// a provider that exports nowhere. what is being tested is the context a
    /// span carries and the parent a second span adopts from it, and both of
    /// those are decided long before anything is exported — so a test that
    /// needed an exporter would be testing the sdk rather than hestan.
    fn subscriber() -> tracing::subscriber::DefaultGuard {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("hestan-test"));
        tracing::subscriber::set_default(tracing_subscriber::registry().with(layer))
    }

    #[test]
    fn a_span_carries_a_w3c_traceparent_a_child_can_adopt() {
        let _guard = subscriber();
        let parent = tracing::info_span!("hestan.op", run_id = "r1", op = "a", attempt = 1u64);
        let carried: HashMap<String, String> = carry(&parent).into_iter().collect();

        let traceparent = carried
            .get(TRACEPARENT)
            .expect("a span under an otel layer has a context to carry")
            .clone();
        // `00-{32 hex}-{16 hex}-{2 hex}`, and the trace id is this span's
        let parts: Vec<String> = traceparent.split('-').map(str::to_string).collect();
        assert_eq!(parts.len(), 4, "{traceparent}");
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
        assert_ne!(parts[1], "0".repeat(32), "a trace id that leads nowhere");

        // and a span in what stands for the child process joins that trace
        // rather than starting one of its own
        for (key, value) in carried {
            unsafe { std::env::set_var(key, value) };
        }
        let child = tracing::info_span!("hestan.op", run_id = "r1", op = "a", attempt = 1u64);
        adopt(&child);
        let joined: HashMap<String, String> = carry(&child).into_iter().collect();
        let child_parent = joined.get(TRACEPARENT).unwrap();
        assert_eq!(
            child_parent.split('-').nth(1),
            Some(parts[1].as_str()),
            "the child started a trace of its own"
        );
        assert_ne!(
            child_parent.split('-').nth(2),
            Some(parts[2].as_str()),
            "the child reused its parent's span id rather than nesting under it"
        );
        for key in [TRACEPARENT, TRACESTATE] {
            unsafe { std::env::remove_var(key) };
        }
    }

    // the ordinary case for a build that turned the feature on and composed no
    // layer: nothing to carry, and nothing invented to carry instead
    #[test]
    fn no_layer_means_no_context_rather_than_a_made_up_one() {
        let span = tracing::info_span!("hestan.op", run_id = "r1", op = "a", attempt = 1u64);
        assert!(carry(&span).is_empty());
    }
}
