//! isolated ops: one attempt, one **op subprocess**.
//!
//! an op subprocess is not a [queue worker](crate::Hestan::work), and the two
//! are worth keeping apart in your head because both spawn processes and both
//! used to be called workers. an op subprocess runs **one op of one run and
//! exits**; it claims nothing, owns nothing, and its parent holds the retry
//! policy, the pool permit and the clock. a queue worker is a **long-lived
//! process that claims whole runs** off the queue and executes them. a queue
//! worker spawns op subprocesses, exactly as any other hestan process does.
//!
//! the child is this same binary re-executed with two environment variables
//! set. it rebuilds the same jobs because it runs the same `main`, and it
//! reads everything else out of the store (the run's params, the op's inputs,
//! its committed state), so nothing is serialized between the two processes
//! that was not already a row. what it produces goes back the same way: the
//! output through its io manager, the terminal status onto its own op run row,
//! its log lines into the run's events. there is no protocol between the two,
//! which is why this costs a process spawn and nothing else.
//!
//! the one thing that does travel down a pipe is what the child *printed*.
//! stdout and stderr are the child's, whole, and a `println!` or a linked c
//! library writing to fd 2 is output nobody else in this process can claim,
//! so the parent pipes both, reads them concurrently, and stores each line
//! under this attempt. an in-process op gets no such thing, and
//! [`docs/logs.md`](https://docs.rs/hestan) says plainly why: redirecting fd 1
//! process-wide would hijack the host application's own output.
//!
//! both halves live here: [`attempt`] is what the parent's executor calls
//! instead of the op body, and [`work`] is the whole of what the child does.

use std::collections::{BTreeMap, HashMap};
use std::os::unix::process::ExitStatusExt;
use std::panic::AssertUnwindSafe;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tracing::Instrument;

use crate::error::Error;
use crate::executor::{CANCEL_GRACE, Ended, panic_payload};
use crate::io::{Io, IoKey};
use crate::job::Job;
use crate::logs::{Attempt, capture_child};
use crate::model::{EventKind, EventLevel, OpStatus};
use crate::op::{self, Cancel, Op, OpCtx};
use crate::resource::Resources;
use crate::store::{Store, note};

/// the run an op subprocess is part of.
pub(crate) const RUN_VAR: &str = "HESTAN_ISOLATED_RUN";
/// the one op of it the subprocess is there to run.
pub(crate) const OP_VAR: &str = "HESTAN_ISOLATED_OP";

/// what the environment is asking this process to be.
pub(crate) struct Request {
    pub(crate) run_id: String,
    pub(crate) op: String,
}

/// the op-subprocess request in this process's environment, if there is one.
///
/// every entry point asks this before it does anything else at all: `serve`,
/// `work`, `run_once`, `build_asset`. an op subprocess that reached ordinary
/// boot behaviour would sync schedules, sweep, bind a listener and start
/// claiming runs off the queue, none of which is its business: it is here to
/// run one op.
pub(crate) fn requested() -> Option<Request> {
    let run_id = std::env::var(RUN_VAR).ok()?;
    let op = std::env::var(OP_VAR).ok()?;
    (!run_id.is_empty() && !op.is_empty()).then_some(Request { run_id, op })
}

/// what an op subprocess did, which is what its exit code says.
pub(crate) enum Worked {
    Success,
    Failed,
    /// the body [skipped itself](crate::OpCtx::skip). the row it wrote says
    /// `skipped` and carries the reason, which is what the parent reads back:
    /// the exit code is only how the child says whether it got that far.
    Skipped,
    /// the body ran and the store would not take the row that says what it
    /// did. the parent reads the op's row rather than this process's exit
    /// code, so what this changes is what gets *said* about an attempt that
    /// recorded nothing.
    Unrecorded,
}

// ---------------------------------------------------------------- the parent

/// run one attempt of `op` in a child process.
///
/// the parent's whole job is to start the child, watch it, read what it
/// printed, and read the row it wrote. a child that exits without writing one
/// (killed, aborted, out of memory) is recorded here instead, with what
/// killed it, because that containment is the entire point of running it
/// elsewhere. what it printed before dying is kept either way, and is usually
/// the only thing that says what it was doing.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn attempt(
    op: &Op,
    run_id: &str,
    name: &str,
    attempt: u32,
    invocation: &Value,
    store: &Store,
    cancel: &watch::Receiver<bool>,
    span: &tracing::Span,
) -> Ended {
    // this op may have spent the last minute waiting for a pool permit, and the
    // run may have been canceled in it. starting a process now would be work
    // nobody is waiting for
    if *cancel.borrow() {
        return Ended::Killed("canceled before its process started".to_string());
    }
    // written before the child exists, because the child reads its inputs
    // rather than being told them. the error itself is in this process's log,
    // where `landed` put it; what belongs on the op run is that the child was
    // never given anything to read
    if !store
        .landed("set_op_inputs", || {
            store.set_op_inputs(run_id, name, invocation)
        })
        .await
    {
        return Ended::Failed("could not record the op's inputs".to_string());
    }
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return Ended::Failed(format!("could not find this binary to re-execute: {e}")),
    };
    let mut command = Command::new(&exe);
    // the trace context of the attempt that is spawning this, so the child's
    // spans nest under it rather than starting a trace of their own. empty
    // unless the `otel` feature is on *and* the host composed a layer, and an
    // empty carrier is nothing to pass; see `crate::otel`
    #[cfg(feature = "otel")]
    for (key, value) in crate::otel::carry(span) {
        command.env(key, value);
    }
    #[cfg(not(feature = "otel"))]
    let _ = span;
    let mut child = match command
        .env(RUN_VAR, run_id)
        .env(OP_VAR, name)
        // both pipes, and both drained below: what the child prints is the op's
        // output and belongs on the run page rather than on whatever terminal
        // the orchestrator happens to have
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // the backstop for the one case this function does not get to finish:
        // the run's cancellation drain aborts this task, and a dropped child
        // left running would be an orphan nobody is waiting for
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            return Ended::Failed(format!(
                "could not start an op subprocess ({}): {e}",
                exe.display()
            ));
        }
    };
    let pid = child.id();
    let capture = capture_child(&mut child, store, &Attempt::new(run_id, name, attempt));
    if let Some(pid) = pid {
        note(store.op_spawned(run_id, name, pid));
        note(store.append_event(
            run_id,
            Some(name),
            EventLevel::Info,
            EventKind::Log,
            &format!("isolated: running in process {pid}"),
            Some(&json!({ "pid": pid })),
        ));
    }

    // three ways this ends, and only the first is the child's own doing
    let expiry = async {
        match op.timeout_after() {
            Some(limit) => tokio::time::sleep(limit).await,
            None => std::future::pending().await,
        }
    };
    let stop = op::flipped(cancel.clone());
    tokio::pin!(expiry, stop);
    let exited = tokio::select! {
        exited = child.wait() => exited,
        () = &mut stop => {
            // the kill first and the drain after it: a pipe reaches its end
            // when the process holding the far side of it is gone
            let ended = stopped(&mut child, pid, None).await;
            capture.finish(CANCEL_GRACE).await;
            return ended;
        }
        () = &mut expiry => {
            let limit = op.timeout_after().expect("the expiry arm cannot fire without a limit");
            let ended = stopped(&mut child, pid, Some(limit)).await;
            capture.finish(CANCEL_GRACE).await;
            return ended;
        }
    };
    // before the row is read, so a run page showing a finished op is showing
    // everything that op printed
    capture.finish(CANCEL_GRACE).await;
    let status = match exited {
        Ok(status) => status,
        Err(e) => return Ended::Failed(format!("could not wait for the op subprocess: {e}")),
    };
    recorded(store, run_id, name).unwrap_or_else(|| Ended::Failed(no_result(op, &status)))
}

/// what the child recorded, if it recorded anything.
///
/// the row comes first and the exit status second: the child is the process
/// that ran the body, so if it got as far as writing a result, that result is
/// what happened, however it exited afterwards.
fn recorded(store: &Store, run_id: &str, name: &str) -> Option<Ended> {
    let row = match store.op_run(run_id, name) {
        Ok(row) => row?,
        Err(e) => {
            return Some(Ended::Failed(format!(
                "could not read back what the op subprocess recorded: {e}"
            )));
        }
    };
    match row.status {
        OpStatus::Success => match row.output {
            Some(handle) => Some(Ended::Handle(handle)),
            // unreachable: the child writes the status and the output in one
            // statement. saying so beats seeding downstream with nothing
            None => Some(Ended::Failed(
                "the op subprocess recorded success without an output".to_string(),
            )),
        },
        OpStatus::Failed => {
            Some(Ended::Failed(row.error.unwrap_or_else(|| {
                "the op failed without recording why".to_string()
            })))
        }
        // the child wrote its own terminal row for this too, reason and all,
        // so the parent has nothing to add: it only has to not read `skipped`
        // as `failed`, which is what a missing arm here would have done
        OpStatus::Skipped => {
            Some(Ended::Skipped(row.error.unwrap_or_else(|| {
                "the op skipped itself without recording why".to_string()
            })))
        }
        // not a terminal row the parent can adopt. named rather than caught
        // by a `_`, so a new `OpStatus` has to be classified here instead of
        // silently reading as "the child never finished"
        OpStatus::Pending | OpStatus::Running | OpStatus::Canceled => None,
    }
}

/// stop a child for real: SIGTERM, a short grace, then SIGKILL.
///
/// SIGTERM arrives inside the child as ordinary cancellation, so an op that
/// polls `ctx.is_cancelled()` gets to stop cleanly. one that does not is
/// killed, and that is the whole difference from the in-process path: hestan
/// is not asking.
async fn stopped(child: &mut Child, pid: Option<u32>, timeout: Option<Duration>) -> Ended {
    let asked = match pid {
        // SAFETY: kill(2) with the pid of a child this process spawned and has
        // not reaped, so it names this child or nothing at all
        Some(pid) => unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) == 0 },
        None => false,
    };
    let went_quietly = asked
        && tokio::time::timeout(CANCEL_GRACE, child.wait())
            .await
            .is_ok();
    if !went_quietly {
        let _ = child.start_kill();
        // and reaped, so the pid this row carried names nothing by the time
        // anyone reads it back
        let _ = child.wait().await;
    }
    let how = match (asked, went_quietly) {
        (true, true) => "it stopped when asked".to_string(),
        (true, false) => format!("it ignored SIGTERM for {CANCEL_GRACE:?} and was killed"),
        (false, _) => "it was killed".to_string(),
    };
    match timeout {
        // a timeout is an ordinary attempt failure, and retries like one
        Some(limit) => Ended::Failed(format!("timed out after {limit:?}: {how}")),
        None => Ended::Killed(format!("canceled: {how}")),
    }
}

/// the containment message: a child that died without recording anything.
///
/// this is the sentence someone reads at 3am about an op that segfaulted, so it
/// says what happened to the process rather than that something went wrong. a
/// declared limit is named after it, because "signal 24" is the encoding and
/// "the cpu limit" is the fact.
fn no_result(op: &Op, status: &ExitStatus) -> String {
    let how = match status.signal() {
        Some(sig) => format!("exited with signal {sig} ({})", signal_name(sig)),
        None => match status.code() {
            Some(code) => format!("exited with status {code}"),
            None => "exited".to_string(),
        },
    };
    let mut msg = format!("op {how} without recording a result");
    if let Some(limit) = limit_hit(op, status) {
        msg.push_str("; ");
        msg.push_str(&limit);
    }
    msg
}

/// which declared limit explains a death like this one.
///
/// SIGXCPU comes from nowhere else, so a cpu limit is named as the cause. a
/// memory limit is offered rather than asserted: an allocation past `RLIMIT_AS`
/// aborts the process, and so does an ordinary `abort()`, and the two are the
/// same signal.
fn limit_hit(op: &Op, status: &ExitStatus) -> Option<String> {
    let sig = status.signal()?;
    match (op.declared_cpu_limit(), op.declared_memory_limit()) {
        (Some(cpu), _) if sig == libc::SIGXCPU => {
            Some(format!("it exceeded its cpu limit of {cpu:?}"))
        }
        (_, Some(bytes)) if matches!(sig, libc::SIGABRT | libc::SIGKILL | libc::SIGSEGV) => {
            Some(format!(
                "it was running under a memory limit of {}, which an allocation past the limit \
                 aborts on",
                bytes_human(bytes)
            ))
        }
        // the hard limit, a second past the soft one: it was told with SIGXCPU
        // and did not go
        (Some(cpu), None) if sig == libc::SIGKILL => Some(format!(
            "it was running under a cpu limit of {cpu:?}, which kills a process that ignores \
             its SIGXCPU"
        )),
        _ => None,
    }
}

/// a byte count the way a limit is written down, since `536870912` in an error
/// message is a number someone has to go and divide.
fn bytes_human(bytes: u64) -> String {
    for (unit, size) in [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if bytes >= size {
            return format!("{:.0} {unit}", bytes as f64 / size as f64);
        }
    }
    format!("{bytes} bytes")
}

/// what a signal means to whoever is reading the run page, rather than what it
/// is called in `signal.h`.
fn signal_name(sig: i32) -> &'static str {
    match sig {
        libc::SIGABRT => "aborted",
        libc::SIGBUS => "bus error",
        libc::SIGFPE => "arithmetic error",
        libc::SIGHUP => "hung up",
        libc::SIGILL => "illegal instruction",
        libc::SIGINT => "interrupted",
        libc::SIGKILL => "killed",
        libc::SIGPIPE => "broken pipe",
        libc::SIGQUIT => "quit",
        libc::SIGSEGV => "segmentation fault",
        libc::SIGTERM => "terminated",
        libc::SIGXCPU => "cpu limit exceeded",
        libc::SIGXFSZ => "file size limit exceeded",
        _ => "unknown",
    }
}

// ----------------------------------------------------------------- the child

/// run one op of one run in this process, and record it exactly as the
/// in-process path would.
///
/// there is nothing here about being a subprocess: it loads what the op needs
/// out of the store, calls the body once, and writes the result back. the
/// parent owns the retry policy, the pool permit and the clock, so an op
/// subprocess runs one attempt and stops.
pub(crate) async fn run_one_op(
    req: &Request,
    jobs: &[Job],
    store: &Store,
    io: &Io,
    resources: &Resources,
) -> Result<Worked, Error> {
    let run = store
        .run(&req.run_id)?
        .ok_or_else(|| Error::UnknownRun(req.run_id.clone()))?;
    let job = jobs
        .iter()
        .find(|j| j.name() == run.job)
        .ok_or_else(|| Error::UnknownJob(run.job.clone()))?;
    let op = job.op(&req.op).ok_or_else(|| {
        Error::Graph(format!(
            "job {}: no op {}. this binary does not build the registry that launched run {}",
            run.job, req.op, req.run_id
        ))
    })?;
    // an op subprocess exists to contain an isolated op. anything else reaching
    // here means parent and child disagree about the job, which is the one
    // thing that must not pass quietly
    if !op.is_isolated() {
        return Err(Error::Graph(format!(
            "job {}: op {} is not .isolated() in this binary",
            run.job, req.op
        )));
    }

    let (inputs, dep_statuses) = handed_over(op, job, io, store, &req.run_id).await?;
    let state = Arc::new(store.op_state(job.name(), &req.op)?);
    let new_state = Arc::new(Mutex::new(None));
    let new_meta = Arc::new(Mutex::new(BTreeMap::new()));
    let built = Arc::new(Mutex::new(Vec::new()));
    // the parent's SIGTERM lands here as ordinary cancellation, so an op that
    // polls `ctx.is_cancelled()` stops cleanly and one that ignores it is
    // killed a few seconds later. the attempt half never flips: the parent owns
    // the timeout, and enforces it with the same signal
    let (asked_to_stop, on_cancel) = watch::channel(false);
    let (_never, on_expiry) = watch::channel(false);
    if let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        tokio::spawn(async move {
            term.recv().await;
            let _ = asked_to_stop.send(true);
        });
    }
    let ctx = OpCtx {
        cancel: Cancel {
            run: on_cancel,
            attempt: on_expiry,
        },
        run_id: req.run_id.clone(),
        job: run.job.clone(),
        op: req.op.clone(),
        params: run.params.clone(),
        scheduled_for: run.scheduled_for,
        // both belong to a fan-out instance, which an isolated op may not be
        element: None,
        partition: None,
        inputs: Arc::new(inputs),
        dep_statuses: Arc::new(dep_statuses),
        resources: resources.clone(),
        state,
        new_state: new_state.clone(),
        new_fingerprint: Arc::new(Mutex::new(None)),
        new_meta: new_meta.clone(),
        new_per_asset: Arc::new(Mutex::new(BTreeMap::new())),
        // an asset op is never isolated, so this is empty, but it is read
        // where the parent reads it, so it stays true if one ever is
        built: built.clone(),
        store: store.clone(),
        io: io.clone(),
        // the parent holds this op's pool slot for as long as this process
        // lives, which is the whole of the child's work
        slot: None,
    };

    // the same span the parent opens around an in-process attempt, and (with
    // the `otel` feature) a child of the parent's, taken from the trace
    // context in this process's environment. that is the whole of what makes a
    // subprocess's spans land under the op that spawned it rather than in a
    // trace of their own; `crate::otel` says what it does not do.
    let span = tracing::info_span!(
        "hestan.op",
        run_id = %req.run_id,
        op = %req.op,
        // which attempt this child is: the parent wrote it on the op run row
        // before spawning, and the child has no other way to know
        attempt = store
            .op_run(&req.run_id, &req.op)
            .ok()
            .flatten()
            .map_or(1, |o| o.attempts),
    );
    #[cfg(feature = "otel")]
    crate::otel::adopt(&span);

    // last, so what the limits cap is the body and not the loading of its
    // inputs, and refused outright if they cannot be applied, since an op that
    // ran uncapped believing otherwise is the one outcome worth nobody's time
    let produced = match apply_limits(op) {
        Err(e) => Err(e),
        Ok(()) => {
            let called = AssertUnwindSafe(async { op.call(ctx).await })
                .catch_unwind()
                .instrument(span)
                .await;
            match called {
                Ok(Ok(output)) => Ok(output),
                // a body that [skipped itself](crate::OpCtx::skip) is terminal
                // here, before anything is persisted: it produced no output, so
                // there is nothing for the io manager to store and nothing for
                // a materialization to point at. the row it writes is what the
                // parent reads back, so the two processes agree without a
                // protocol, the same way they agree about a success
                Ok(Err(e)) if op::skip_reason(&*e).is_some() => {
                    let reason = e.to_string();
                    let meta = op::staged_meta(&new_meta);
                    return Ok(skipped_in_child(store, req, meta.as_ref(), &reason).await);
                }
                Ok(Err(e)) => Err(e.to_string()),
                Err(panic) => Err(match panic_payload(panic.as_ref()) {
                    Some(s) => format!("op panicked: {s}"),
                    None => "op panicked".to_string(),
                }),
            }
        }
    };
    // persisted before the success is recorded, in the order the run's own task
    // uses: a row claiming an output that was never stored is a lie the next
    // run trips over
    let produced = match produced {
        Err(e) => Err(e),
        Ok(output) => {
            let key = IoKey {
                run_id: req.run_id.clone(),
                job: run.job.clone(),
                op: req.op.clone(),
            };
            crate::io::put(io, op.io_name(), key, output)
                .await
                .map_err(|e| format!("could not persist the output: {e}"))
        }
    };
    match produced {
        Ok(handle) => {
            // what the manager knows about what it stored, beside what the op
            // staged, the same rule the parent applies to an in-process op
            let meta = crate::io::handle_meta(&handle, op::staged_meta(&new_meta));
            let built = crate::store::stored_as(op::staged_builds(&built), &handle);
            if !store
                .landed("op_finished", || {
                    store.op_finished(
                        &req.run_id,
                        &req.op,
                        OpStatus::Success,
                        Some(&handle),
                        meta.as_ref(),
                        None,
                        &built,
                    )
                })
                .await
            {
                return Ok(Worked::Unrecorded);
            }
            // state second: a crash between the writes re-runs the op, never
            // skips it. taken out of the mutex first, because a lock held
            // across a retry is a lock held for as long as the store is slow
            let state = new_state.lock().unwrap().take();
            if let Some(state) = state
                && !store
                    .landed("set_op_state", || {
                        store.set_op_state(job.name(), &req.op, &state)
                    })
                    .await
            {
                return Ok(Worked::Unrecorded);
            }
            let data = op.output_type().map(|t| json!({ "output_type": t }));
            note(store.append_event(
                &req.run_id,
                Some(&req.op),
                EventLevel::Info,
                EventKind::OpSuccess,
                "finished",
                data.as_ref(),
            ));
            Ok(Worked::Success)
        }
        // the row carries the message home; the parent decides whether this was
        // the last attempt and writes the event that says so
        Err(msg) => {
            match store
                .landed("op_finished", || {
                    store.op_finished(
                        &req.run_id,
                        &req.op,
                        OpStatus::Failed,
                        None,
                        None,
                        Some(&msg),
                        &[],
                    )
                })
                .await
            {
                true => Ok(Worked::Failed),
                false => Ok(Worked::Unrecorded),
            }
        }
    }
}

/// the child's terminal write for a body that
/// [skipped itself](crate::OpCtx::skip).
///
/// it writes the event as well as the row, unlike the failure path above,
/// because the parent writes no event of its own for a skip: a skip is
/// terminal on the attempt that reached it, so there is no "was that the last
/// attempt" for the parent to decide.
async fn skipped_in_child(
    store: &Store,
    req: &crate::isolate::Request,
    meta: Option<&Value>,
    reason: &str,
) -> Worked {
    note(store.append_event(
        &req.run_id,
        Some(&req.op),
        EventLevel::Warn,
        EventKind::OpSkipped,
        reason,
        Some(&json!({ "reason": reason, "upstream": Value::Null })),
    ));
    match store
        .landed("op_finished", || {
            store.op_finished(
                &req.run_id,
                &req.op,
                OpStatus::Skipped,
                None,
                meta,
                Some(reason),
                &[],
            )
        })
        .await
    {
        true => Worked::Skipped,
        false => Worked::Unrecorded,
    }
}

/// the [limits](crate::Op::memory_limit) this op declared, applied to this
/// process before its body runs.
///
/// this is the whole reason a limit needs `.isolated()`: `setrlimit` caps a
/// process, and in-process that process is the orchestrator. it caps this
/// child, which is a few megabytes of hestan plus the op: near enough the op
/// alone, and honest about not being exactly it.
fn apply_limits(op: &Op) -> Result<(), String> {
    if let Some(bytes) = op.declared_memory_limit() {
        let limit = libc::rlimit {
            rlim_cur: bytes,
            rlim_max: bytes,
        };
        // SAFETY: setrlimit(2) against this process's own limits, with a fully
        // initialized struct that outlives the call. lowering needs no
        // privilege, so the only failures here are a value the kernel refuses.
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } != 0 {
            return Err(format!(
                "could not apply the {} memory limit: {}",
                bytes_human(bytes),
                std::io::Error::last_os_error()
            ));
        }
    }
    if let Some(cpu) = op.declared_cpu_limit() {
        // whole seconds are the kernel's granularity, and nothing is no limit
        // at all, so anything under a second is one second.
        //
        // the hard limit sits one second above the soft one on purpose: at the
        // soft limit the kernel sends SIGXCPU, which says what happened, and at
        // the hard limit it sends SIGKILL, which does not. equal limits would
        // collapse the two into the second one and lose the diagnosis.
        let secs = cpu.as_secs().max(1);
        let limit = libc::rlimit {
            rlim_cur: secs,
            rlim_max: secs + 1,
        };
        // SAFETY: as above, RLIMIT_CPU rather than RLIMIT_AS
        if unsafe { libc::setrlimit(libc::RLIMIT_CPU, &limit) } != 0 {
            return Err(format!(
                "could not apply the {cpu:?} cpu limit: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

/// what the parent recorded on this op's row, turned back into the inputs and
/// dep statuses an [`OpCtx`] carries. `executor::invocation` writes it.
type HandedOver = (HashMap<String, Value>, HashMap<String, OpStatus>);

async fn handed_over(
    op: &Op,
    job: &Job,
    io: &Io,
    store: &Store,
    run_id: &str,
) -> Result<HandedOver, Error> {
    let recorded = store
        .op_inputs(run_id, op.name())?
        .unwrap_or_else(|| json!({}));
    let held = recorded.get("held").and_then(Value::as_object);
    let deps = recorded.get("deps").and_then(Value::as_object);
    let mut inputs = HashMap::new();
    let mut dep_statuses = HashMap::new();
    for dep in op.deps() {
        // the name this body calls the dep, which differs from the job-level
        // one only inside a flattened graph instance
        let seen = op.dep_alias(dep).to_string();
        if let Some(handle) = held.and_then(|h| h.get(dep)).cloned() {
            // resolved here rather than by the parent: an op reading a gigabyte
            // should read it in the process that wants it
            let name = job.op(dep).and_then(Op::io_name);
            let value = crate::io::get(io, name, io_key(run_id, job, dep), handle)
                .await
                .map_err(|e| Error::Graph(format!("could not read the output of {dep}: {e}")))?;
            inputs.insert(seen.clone(), value);
        }
        if let Some(status) = deps
            .and_then(|d| d.get(dep))
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
        {
            dep_statuses.insert(seen, status);
        }
    }
    Ok((inputs, dep_statuses))
}

fn io_key(run_id: &str, job: &Job, op: &str) -> IoKey {
    IoKey {
        run_id: run_id.to_string(),
        job: job.name().to_string(),
        op: op.to_string(),
    }
}
