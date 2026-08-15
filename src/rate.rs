//! the other shape an external limit comes in: n calls per period, rather than
//! n at once.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

/// a named rate limit shared by every job in the process, declared with
/// `Hestan::rate` and taken from by [`Op::rate`](crate::Op::rate).
pub(crate) struct Rate {
    limit: usize,
    per: Duration,
    bucket: Mutex<Bucket>,
}

pub(crate) type Rates = Arc<HashMap<String, Rate>>;

/// what one declared rate is doing, from [`Runner::rates`](crate::Runner::rates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateStatus {
    /// the name it was declared under, and the name an op takes from.
    pub name: String,
    /// how many tokens each period holds.
    pub limit: usize,
    /// the period those tokens are spread over.
    pub per: Duration,
    /// how many ops are waiting for a token **in this process**, right now.
    pub waiting: usize,
}

impl Rate {
    pub(crate) fn new(limit: usize, per: Duration) -> Rate {
        Rate {
            limit,
            per,
            bucket: Mutex::new(Bucket::new(limit, per)),
        }
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) fn per(&self) -> Duration {
        self.per
    }

    pub(crate) fn waiting(&self) -> usize {
        self.locked().queue.len()
    }

    /// take a token, reserving a later one when the bucket is empty.
    ///
    /// the reservation is made here rather than when the wait ends, and that is
    /// the whole of how the queue stays in order: an op is given the next token
    /// nobody else has been given, so the one that asked first goes first and
    /// nothing has to be woken in turn.
    pub(crate) fn take(&self) -> Ticket<'_> {
        let now = Instant::now();
        let mut bucket = self.locked();
        let at = bucket.take(now);
        match at <= now {
            true => Ticket::Ready,
            false => Ticket::Waiting(Reserved {
                rate: self,
                waiter: bucket.enqueue(at),
                spent: false,
            }),
        }
    }

    /// the bucket, whatever a panicking holder left behind. a poisoned lock
    /// would refuse every token this rate has for the life of the process,
    /// which is a worse answer than arithmetic that was interrupted once.
    fn locked(&self) -> MutexGuard<'_, Bucket> {
        self.bucket.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// what [`Rate::take`] came back with.
pub(crate) enum Ticket<'a> {
    /// there was a token, and it is already spent.
    Ready,
    /// there was not, and this is the reservation for a later one.
    Waiting(Reserved<'a>),
}

/// a token reserved from a bucket, waiting to be spent.
///
/// dropping it before [`spend`](Reserved::spend) resolves gives the token to
/// the op behind it in the queue, which is what a canceled run does. a token
/// is spent rather than returned, so one taken by an op that is already dying
/// is a call nobody makes, and a call the op behind it should have been
/// making.
pub(crate) struct Reserved<'a> {
    rate: &'a Rate,
    waiter: Arc<Notify>,
    spent: bool,
}

impl Reserved<'_> {
    /// when this reservation's token arrives, as the bucket currently has it.
    pub(crate) fn at(&self) -> Option<Instant> {
        self.rate.locked().deadline(&self.waiter)
    }

    /// wait for the reserved token.
    ///
    /// cancel-safe: dropping this future before it resolves is what hands the
    /// token on, so an op abandoned by a canceled run costs the queue nothing.
    pub(crate) async fn spend(mut self) {
        // re-read on every wake rather than sleeping out the instant this
        // started with: an op ahead in the queue that leaves hands its token
        // down, and arriving early is the whole point of being handed one
        while let Some(at) = self.at() {
            tokio::select! {
                () = tokio::time::sleep_until(at) => break,
                () = self.waiter.notified() => {}
            }
        }
        self.spent = true;
    }
}

impl Drop for Reserved<'_> {
    fn drop(&mut self) {
        self.rate.locked().leave(&self.waiter, self.spent);
    }
}

/// a token bucket: `limit` tokens accrue over `per`, and up to `limit` of them
/// may be spent at once.
///
/// a fixed window is less code and is the classic wrong answer: five a second,
/// five at 0.99s and five more at 1.01s, and the api sees ten inside fifty
/// milliseconds. tokens accrue continuously here, so a span that short is worth
/// what a span that short is worth wherever it falls.
///
/// the schedule is one instant, `next`: when the token after everything taken
/// so far accrues. a caller may draw up to `burst` ahead of it, which is what
/// makes the first `limit` of them immediate: an api that says "5 a second"
/// generally tolerates 5 at once and then a second of quiet, and dribbling them
/// out one every 200ms would be slower than the thing being protected asked
/// for.
struct Bucket {
    /// how long one token takes to accrue: `per / limit`.
    spacing: Duration,
    /// how far ahead of `next` a caller may draw: `spacing * (limit - 1)`.
    burst: Duration,
    next: Instant,
    /// who is holding a reservation, in the order they took one, and when each
    /// one's token arrives.
    queue: VecDeque<(Arc<Notify>, Instant)>,
}

impl Bucket {
    fn new(limit: usize, per: Duration) -> Bucket {
        let limit = u32::try_from(limit.max(1)).unwrap_or(u32::MAX);
        let spacing = per / limit;
        Bucket {
            spacing,
            burst: spacing * (limit - 1),
            next: Instant::now(),
            queue: VecDeque::new(),
        }
    }

    /// take a token and say when it may be spent: `now` or earlier for one
    /// that was already in the bucket.
    ///
    /// every call takes one, whether or not it has to wait: the n+1th caller is
    /// told about the n+1th token and nobody else can be given it.
    fn take(&mut self, now: Instant) -> Instant {
        // `now` on a bucket nobody has touched for a while: idle time accrues
        // no credit past `burst`, which is what having a capacity means
        let at = self.next.checked_sub(self.burst).unwrap_or(now).max(now);
        self.next = self
            .next
            .max(now)
            .checked_add(self.spacing)
            .unwrap_or(self.next);
        at
    }

    fn enqueue(&mut self, at: Instant) -> Arc<Notify> {
        let waiter = Arc::new(Notify::new());
        self.queue.push_back((waiter.clone(), at));
        waiter
    }

    fn deadline(&self, who: &Arc<Notify>) -> Option<Instant> {
        self.queue
            .iter()
            .find(|(waiter, _)| Arc::ptr_eq(waiter, who))
            .map(|(_, at)| *at)
    }

    /// take a waiter out of the queue, having spent its token or not.
    ///
    /// an unspent token is neither lost nor left in the schedule as a hole:
    /// every op behind this one moves up a place and is woken to find out, so a
    /// canceled run costs the ops queued behind it nothing. the queue is in
    /// token order and stays in it, which is what keeps this from being a way
    /// to overtake.
    fn leave(&mut self, who: &Arc<Notify>, spent: bool) {
        let Some(gone) = self
            .queue
            .iter()
            .position(|(waiter, _)| Arc::ptr_eq(waiter, who))
        else {
            return;
        };
        self.queue.remove(gone);
        if spent {
            return;
        }
        let spacing = self.spacing;
        for (waiter, at) in self.queue.iter_mut().skip(gone) {
            *at = at.checked_sub(spacing).unwrap_or(*at);
            waiter.notify_one();
        }
        self.next = self.next.checked_sub(spacing).unwrap_or(self.next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// a bucket whose clock starts exactly at `start`, so a case can talk in
    /// milliseconds from there.
    fn bucket(limit: usize, per: Duration, start: Instant) -> Bucket {
        Bucket {
            next: start,
            ..Bucket::new(limit, per)
        }
    }

    /// what `take` says to a run of arrivals, as millisecond offsets from
    /// `start`, the times the api sees the calls at.
    fn admissions(bucket: &mut Bucket, start: Instant, arrivals_ms: &[u64]) -> Vec<u128> {
        arrivals_ms
            .iter()
            .map(|ms| {
                bucket
                    .take(start + Duration::from_millis(*ms))
                    .duration_since(start)
                    .as_millis()
            })
            .collect()
    }

    #[test]
    fn a_burst_goes_at_once_and_the_rest_are_spaced() {
        let start = Instant::now();
        let mut bucket = bucket(5, Duration::from_secs(1), start);
        // five arrive together: five go, and the sixth waits for one token to
        // accrue rather than for the whole second
        assert_eq!(
            admissions(&mut bucket, start, &[0; 7]),
            vec![0, 0, 0, 0, 0, 200, 400]
        );
    }

    #[test]
    fn a_window_boundary_does_not_admit_twice_the_rate() {
        let start = Instant::now();
        let mut bucket = bucket(5, Duration::from_secs(1), start);
        // the shape a fixed window gets wrong: a batch just before a second
        // ticks over and another just after
        let mut at = admissions(&mut bucket, start, &[990; 5]);
        at.extend(admissions(&mut bucket, start, &[1010; 5]));

        // no fifty-millisecond span holds more than a whole second's worth. a
        // fixed window puts all ten inside twenty of them
        for (i, from) in at.iter().enumerate() {
            let together = at[i..].iter().take_while(|t| **t <= from + 50).count();
            assert!(
                together <= 5,
                "{together} calls inside 50ms from {from}ms: {at:?}"
            );
        }
    }

    #[test]
    fn a_drained_bucket_refills_at_the_declared_rate() {
        let start = Instant::now();
        let mut drained = bucket(2, Duration::from_secs(1), start);
        assert_eq!(admissions(&mut drained, start, &[0, 0]), vec![0, 0]);
        // half a period later there is one token and not two
        assert_eq!(
            admissions(&mut drained, start, &[500, 500]),
            vec![500, 1000]
        );

        // and a bucket nobody has touched for two periods is full, not fuller:
        // three at once would be credit for the idle time
        let mut idle = bucket(2, Duration::from_secs(1), start);
        assert_eq!(
            admissions(&mut idle, start, &[2000; 3]),
            vec![2000, 2000, 2500]
        );
    }

    #[tokio::test]
    async fn the_op_that_asked_first_is_given_the_token_that_comes_first() {
        let rate = Rate::new(1, Duration::from_secs(30));
        assert!(matches!(rate.take(), Ticket::Ready));
        let (Ticket::Waiting(first), Ticket::Waiting(second)) = (rate.take(), rate.take()) else {
            panic!("a second token inside the period was not a wait");
        };
        assert_eq!(rate.waiting(), 2);
        assert!(
            first.at() < second.at(),
            "the op that asked second was given the earlier token"
        );
    }

    #[tokio::test]
    async fn a_waiter_that_leaves_hands_its_token_to_the_one_behind_it() {
        let rate = Rate::new(1, Duration::from_secs(30));
        assert!(matches!(rate.take(), Ticket::Ready));
        let (Ticket::Waiting(leaving), Ticket::Waiting(behind)) = (rate.take(), rate.take()) else {
            panic!("a second token inside the period was not a wait");
        };
        let (freed, was) = (leaving.at(), behind.at());
        // the shape of a canceled run: the wait is dropped where it stands
        drop(leaving);
        assert_eq!(rate.waiting(), 1);
        assert_eq!(
            behind.at(),
            freed,
            "the op behind waited out a token that was spent on nobody"
        );
        // and the token the queue gave up is the one at the back of it rather
        // than one out of the middle: the next op to ask gets what the op
        // behind used to hold
        let next = rate.take();
        let Ticket::Waiting(next) = next else {
            panic!("a token appeared out of a reservation nobody spent");
        };
        assert_eq!(next.at(), was);
    }

    #[tokio::test]
    async fn a_waiter_moved_up_the_queue_is_woken_rather_than_sleeping_it_out() {
        // a paused clock: it only moves when everything is parked, so "was it
        // woken" is answered by the instant the op went at rather than by how
        // long the test took on a busy machine
        tokio::time::pause();
        let start = Instant::now();
        let rate = Arc::new(Rate::new(1, Duration::from_secs(10)));
        assert!(matches!(rate.take(), Ticket::Ready));
        let Ticket::Waiting(leaving) = rate.take() else {
            panic!("a second token inside the period was not a wait");
        };

        let behind = tokio::spawn({
            let rate = rate.clone();
            async move {
                let Ticket::Waiting(behind) = rate.take() else {
                    panic!("a third token inside the period was not a wait");
                };
                behind.spend().await;
                Instant::now()
            }
        });
        // parked on the token it was given, which is the one after `leaving`'s
        while rate.waiting() < 2 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(11)).await;
        drop(leaving);

        assert_eq!(
            behind.await.unwrap().duration_since(start),
            Duration::from_secs(11),
            "it slept out the token it was given instead of the one it inherited"
        );
    }

    #[tokio::test]
    async fn a_token_is_spent_the_moment_it_is_taken() {
        let rate = Rate::new(2, Duration::from_secs(30));
        // the difference from a pool: nothing gives one back, so the third op
        // waits however briefly the first two were about their work
        assert!(matches!(rate.take(), Ticket::Ready));
        assert!(matches!(rate.take(), Ticket::Ready));
        let third = rate.take();
        assert!(matches!(third, Ticket::Waiting(_)));
        assert_eq!(rate.waiting(), 1);
    }
}
