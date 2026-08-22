//! stopping on purpose: what a long-lived process does between the signal and
//! the exit.
//!
//! **only a process that serves listens for anything.** [`listen`] is called by
//! `Hestan::serve` and `Hestan::work` and by nothing else. `run_once` and
//! `build_asset` install no handler and are byte for byte the processes they
//! were: a headless one-shot exists to execute the run it was asked for, and a
//! handler that made it wait for a drain, or swallowed the signal that would
//! have ended it, would be a worse bug than the one this module is here for.
//!
//! the other half is `crate::isolate`, which handles SIGTERM inside an **op
//! subprocess** and has since isolated ops existed. that is a different process
//! with a different job: it runs one op and exits, and the signal reaches it as
//! ordinary cancellation. this module is about the process that spawned it.
//!
//! # what a stop is
//!
//! 1. the signal arrives and the http server stops accepting, finishing the
//!    connections it already has.
//! 2. nothing new is claimed: the runner is told it is stopping, and every
//!    dispatch pass after that returns without looking at the queue.
//! 3. the loops that decide are stopped, so this process launches nothing else.
//! 4. what is already in flight is given until the deadline to finish.
//! 5. whatever the deadline did not cover is handed back rather than left to
//!    expire.
//!
//! a **second signal** cuts step 4 short. somebody pressing ctrl-c twice is
//! saying something, and the second one is not swallowed.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::executor::Runner;

/// how long a stopping process waits for what it is already doing.
///
/// **eight seconds, because ten is what a container gives.** `docker stop`
/// waits ten seconds and then sends SIGKILL; kubernetes' default
/// `terminationGracePeriodSeconds` is thirty. the deadline has to be the
/// smaller of those minus what the release and the exit cost, or the process
/// spends its whole grace period waiting and is killed in the middle of
/// handing its claims back, which is the failure it was fixing. eight leaves
/// two seconds for a handful of statements, and the deployment that wants
/// longer runs on kubernetes and can say so.
pub(crate) const WITHIN: Duration = Duration::from_secs(8);

/// how often the drain looks at what is still in flight. small, because the
/// number this phase exists to shrink is how long a stop takes.
const LOOK: Duration = Duration::from_millis(20);

/// the stop signals this process has been sent, and how many.
///
/// cheap to clone: every clone reads the same count, so the http server's
/// shutdown future and the drain's "was that a second one" are one fact
/// rather than two subscriptions that can disagree.
#[derive(Clone)]
pub(crate) struct Stop {
    /// the sender, held rather than dropped, and that is the point of the
    /// `Arc`. a dropped sender closes the channel, and a closed channel makes
    /// [`asked`](Stop::asked) return at once: a process that exits the instant
    /// it starts serving. holding it means a build where no handler could be
    /// installed waits forever instead, which is exactly what it did before
    /// there was one.
    seen: Arc<watch::Sender<u64>>,
}

/// listen for SIGTERM and SIGINT, and count them.
///
/// a signal hestan cannot install a handler for is logged and left alone: the
/// kernel's default action for it is what the process had before this existed,
/// which is worse than stopping cleanly and better than not stopping at all.
pub(crate) fn listen() -> Stop {
    let seen = Arc::new(watch::Sender::new(0u64));
    #[cfg(unix)]
    for kind in [
        tokio::signal::unix::SignalKind::terminate(),
        tokio::signal::unix::SignalKind::interrupt(),
    ] {
        match tokio::signal::unix::signal(kind) {
            Ok(mut signals) => {
                let seen = seen.clone();
                tokio::spawn(async move {
                    while signals.recv().await.is_some() {
                        seen.send_modify(|n| *n += 1);
                    }
                });
            }
            Err(e) => tracing::warn!(
                "this process could not listen for {kind:?} and will not stop cleanly \
                 when it is sent one: {e}"
            ),
        }
    }
    #[cfg(not(unix))]
    {
        let seen = seen.clone();
        tokio::spawn(async move {
            while tokio::signal::ctrl_c().await.is_ok() {
                seen.send_modify(|n| *n += 1);
            }
        });
    }
    Stop { seen }
}

impl Stop {
    /// resolves when this process has been asked to stop, and at once if it
    /// already has.
    pub(crate) async fn asked(&self) {
        self.counted(1).await
    }

    /// resolves on the second one: stop now, and stop waiting for whatever the
    /// first one started.
    pub(crate) async fn asked_again(&self) {
        self.counted(2).await
    }

    /// whether a signal has arrived, asked rather than waited for.
    ///
    /// this is how "did the http server end because it was asked to stop, or
    /// on its own" is decided. it cannot be decided by racing the two: when a
    /// signal is what ended the server, both are ready in the same instant and
    /// a `select!` is free to pick either, which makes a stop that drains and
    /// a stop that does not the same coin toss.
    pub(crate) fn was_asked(&self) -> bool {
        *self.seen.borrow() > 0
    }

    async fn counted(&self, n: u64) {
        let mut rx = self.seen.subscribe();
        // `wait_for` reads the current value before it waits, so a signal that
        // arrived before this was called is not one this misses
        let _ = rx.wait_for(|seen| *seen >= n).await;
    }
}

/// wait until this process is executing nothing.
///
/// polled rather than woken, and deliberately: the run that settles last is
/// the one being waited for, and 20ms of coarseness on a drain measured in
/// seconds is not worth a second notification path that has to be kept correct
/// on every way a run can end.
pub(crate) async fn settled(runner: &Runner) {
    while !runner.executing().is_empty() {
        tokio::time::sleep(LOOK).await;
    }
}
