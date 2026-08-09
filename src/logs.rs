//! what an op *printed*, as opposed to what it
//! [said](crate::OpCtx::info) — and the cap that keeps a `println!` loop from
//! filling a disk.
//!
//! two mechanisms fill the `op_logs` table and this module is the throat both
//! go through: an [isolated op](crate::Op::isolated)'s subprocess capture,
//! which is always on, and the `capture` feature's tracing layer. neither one
//! decides for itself how much it may write.
//!
//! the cap is per **attempt**, not per op or per run: a retry starts from a
//! full budget, because the interesting output is usually the attempt that
//! failed last. past either limit capture stops for that attempt and one line
//! says so — hestan speaking rather than the op, which is what the `hestan`
//! target on a row with no stream means.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::executor::note;
use crate::model::{EventLevel, LogStream};
use crate::store::Store;

/// how much of one attempt's output is stored before capture stops.
/// [`Hestan::log_limit`](crate::Hestan::log_limit) moves it.
pub(crate) const DEFAULT_BYTES: u64 = 1 << 20;

/// how many lines of one attempt's output are stored before capture stops.
/// [`Hestan::log_lines`](crate::Hestan::log_lines) moves it.
pub(crate) const DEFAULT_LINES: u64 = 10_000;

/// the longest single line stored whole. a line past this is stored clipped
/// to it with [`CLIPPED`] on the end, rather than being dropped: the front of
/// a long line is nearly always the part worth reading.
pub(crate) const LINE_MAX: usize = 8 * 1024;

/// what a clipped line ends with.
pub(crate) const CLIPPED: &str = "… [truncated]";

/// the `target` on a row hestan wrote about the capture itself. no tracing
/// event carries it — an event's target is a module path, so the shortest one
/// hestan can emit is `hestan::something` — which is what makes it a usable
/// marker for the ui.
pub(crate) const HESTAN: &str = "hestan";

/// the caps a [`Store`] writes captured output under, shared by every clone
/// of it and by every writer that holds one.
///
/// atomics rather than a plain pair because the store is built before the
/// builder's limits are known and cloned everywhere immediately after, so a
/// value set on one clone has to be the value every clone reads.
#[derive(Debug)]
pub(crate) struct Caps {
    bytes: AtomicU64,
    lines: AtomicU64,
}

impl Default for Caps {
    fn default() -> Caps {
        Caps {
            bytes: AtomicU64::new(DEFAULT_BYTES),
            lines: AtomicU64::new(DEFAULT_LINES),
        }
    }
}

impl Caps {
    pub(crate) fn read(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.lines.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn set_bytes(&self, bytes: u64) {
        self.bytes.store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn set_lines(&self, lines: u64) {
        self.lines.store(lines, Ordering::Relaxed);
    }
}

/// the one attempt a captured line belongs to. every row is written under
/// this, so a retry's output is separable from the attempt before it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Attempt {
    pub run_id: String,
    pub op: String,
    pub attempt: u32,
}

impl Attempt {
    pub(crate) fn new(run_id: &str, op: &str, attempt: u32) -> Attempt {
        Attempt {
            run_id: run_id.to_string(),
            op: op.to_string(),
            attempt,
        }
    }
}

/// where a line came from, which is exactly which of `stream` and
/// `level`/`target` the row carries.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Source<'a> {
    /// a pipe of an isolated op's process.
    Stream(LogStream),
    /// a tracing event emitted inside the op's span.
    Event { level: EventLevel, target: &'a str },
}

impl Source<'_> {
    /// `(stream, level, target)`, the three columns as stored.
    pub(crate) fn columns(&self) -> (Option<&'static str>, Option<&'static str>, Option<&str>) {
        match self {
            Source::Stream(s) => (Some(s.as_str()), None, None),
            Source::Event { level, target } => (None, Some(level.as_str()), Some(target)),
        }
    }

    /// the line hestan writes about capture itself.
    fn hestan() -> Source<'static> {
        Source::Event {
            level: EventLevel::Warn,
            target: HESTAN,
        }
    }
}

/// how much of one attempt's budget is spent, and whether it ran out.
///
/// one budget per attempt, shared by everything writing for it — an isolated
/// op's two pipes spend the same allowance, because the limit is on what the
/// attempt produced and not on which pipe it came out of.
#[derive(Debug, Default)]
pub(crate) struct Budget {
    bytes: u64,
    lines: u64,
    /// set by the line that hit a cap, and never unset: after it this attempt
    /// stores nothing, which is what makes the marker appear exactly once.
    spent: bool,
}

impl Budget {
    pub(crate) fn new() -> Budget {
        Budget::default()
    }

    /// whether this attempt has stopped storing.
    #[cfg(test)]
    pub(crate) fn is_spent(&self) -> bool {
        self.spent
    }

    /// store one line, or store the one line that says why this was the last.
    ///
    /// the caps are read per line rather than held, so a store whose limits
    /// were set after a writer was built still writes under them.
    pub(crate) fn line(&mut self, store: &Store, at: &Attempt, source: Source<'_>, message: &str) {
        if self.spent {
            return;
        }
        let (max_bytes, max_lines) = store.log_caps();
        let message = clip(message);
        let cost = message.len() as u64;
        let hit = if self.lines + 1 > max_lines {
            Some(format!("{max_lines} lines"))
        } else if self.bytes + cost > max_bytes {
            Some(format!("{} of output", bytes_human(max_bytes)))
        } else {
            None
        };
        if let Some(what) = hit {
            self.spent = true;
            let msg = format!(
                "capture stopped: this attempt reached its cap of {what}. everything it \
                 printed after this line was dropped"
            );
            note(store.append_op_log(at, Source::hestan(), &msg));
            return;
        }
        self.lines += 1;
        self.bytes += cost;
        note(store.append_op_log(at, source, &message));
    }
}

/// a line past [`LINE_MAX`], clipped on a char boundary and marked.
fn clip(message: &str) -> Cow<'_, str> {
    if message.len() <= LINE_MAX {
        return Cow::Borrowed(message);
    }
    let mut end = LINE_MAX;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(format!("{}{CLIPPED}", &message[..end]))
}

/// a byte stream cut into lines, holding at most one capped line at a time.
///
/// `read_until(b'\n')` would be four lines shorter and would also let a child
/// that never prints a newline grow the parent's heap until it dies. this
/// keeps at most [`LINE_MAX`] bytes: the line that reaches the limit is handed
/// over there and then, and the rest of it is dropped up to the next newline
/// rather than buffered. that also makes the clip *per line* rather than a
/// long line arriving as a hundred 8 KiB ones.
#[derive(Default)]
pub(crate) struct Split {
    pending: Vec<u8>,
    /// past the limit, waiting for the newline that ends the line we gave up
    /// on. the line itself has already been emitted, clipped.
    dropping: bool,
}

impl Split {
    /// feed one read's worth of bytes, calling `line` for each line completed.
    pub(crate) fn feed(&mut self, chunk: &[u8], mut line: impl FnMut(&str)) {
        for byte in chunk {
            if *byte == b'\n' {
                if self.dropping {
                    self.dropping = false;
                } else {
                    emit(&mut self.pending, &mut line);
                }
                continue;
            }
            if self.dropping {
                continue;
            }
            self.pending.push(*byte);
            // clipped here so the rest of this line costs nothing to skip
            if self.pending.len() >= LINE_MAX {
                emit(&mut self.pending, &mut line);
                self.dropping = true;
            }
        }
    }

    /// whatever was still buffered at end of stream: a child that died
    /// mid-line still said what it managed to say.
    pub(crate) fn finish(&mut self, mut line: impl FnMut(&str)) {
        if !self.pending.is_empty() {
            emit(&mut self.pending, &mut line);
        }
    }
}

/// one line out of the buffer: `\r` dropped so a CRLF child reads right, and
/// invalid utf-8 replaced rather than dropping the line — a pipe carries
/// bytes, and a mangled character is worth more than nothing at all.
fn emit(pending: &mut Vec<u8>, line: &mut impl FnMut(&str)) {
    if pending.last() == Some(&b'\r') {
        pending.pop();
    }
    line(&String::from_utf8_lossy(pending));
    pending.clear();
}

/// a byte count the way a limit is written down, since `1048576` in a log line
/// is a number someone has to go and divide.
fn bytes_human(bytes: u64) -> String {
    for (unit, size) in [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if bytes >= size {
            return format!("{:.0} {unit}", bytes as f64 / size as f64);
        }
    }
    format!("{bytes} bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store::open(":memory:").unwrap()
    }

    fn lines(store: &Store, run: &str) -> Vec<crate::model::OpLog> {
        store.op_logs(run, None, 0, 10_000).unwrap()
    }

    #[test]
    fn a_captured_line_round_trips_with_its_stream_or_its_level() {
        let store = store();
        let at = Attempt::new("r1", "load", 1);
        let mut budget = Budget::new();
        budget.line(&store, &at, Source::Stream(LogStream::Stdout), "on stdout");
        budget.line(&store, &at, Source::Stream(LogStream::Stderr), "on stderr");
        budget.line(
            &store,
            &at,
            Source::Event {
                level: EventLevel::Warn,
                target: "orders::load",
            },
            "an event",
        );

        let rows = lines(&store, "r1");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].stream, Some(LogStream::Stdout));
        assert_eq!(rows[0].message, "on stdout");
        assert_eq!(rows[0].attempt, 1);
        assert_eq!(rows[0].level, None);
        assert_eq!(rows[0].target, None);
        assert_eq!(rows[1].stream, Some(LogStream::Stderr));
        // a pipe has no levels and an event was never on a pipe: exactly one
        // half of the three columns is filled, whichever wrote the row
        assert_eq!(rows[2].stream, None);
        assert_eq!(rows[2].level, Some(EventLevel::Warn));
        assert_eq!(rows[2].target.as_deref(), Some("orders::load"));
        // ids are the cursor, so they only go up
        assert!(rows[0].id < rows[1].id && rows[1].id < rows[2].id);
    }

    #[test]
    fn the_line_cap_stops_capture_and_says_so_exactly_once() {
        let store = store();
        store.set_log_caps(Some(1 << 20), Some(3));
        let at = Attempt::new("r1", "chatty", 1);
        let mut budget = Budget::new();
        for i in 0..100 {
            budget.line(
                &store,
                &at,
                Source::Stream(LogStream::Stdout),
                &format!("line {i}"),
            );
        }

        let rows = lines(&store, "r1");
        assert_eq!(rows.len(), 4, "three lines and one explanation");
        assert_eq!(rows[2].message, "line 2");
        let last = &rows[3];
        assert_eq!(last.target.as_deref(), Some(HESTAN));
        assert_eq!(last.stream, None);
        assert!(last.message.contains("cap of 3 lines"), "{}", last.message);
        assert!(budget.is_spent());
    }

    #[test]
    fn the_byte_cap_stops_capture_and_says_so_exactly_once() {
        let store = store();
        store.set_log_caps(Some(25), None);
        let at = Attempt::new("r1", "chatty", 1);
        let mut budget = Budget::new();
        for i in 0..100 {
            budget.line(
                &store,
                &at,
                Source::Stream(LogStream::Stdout),
                &format!("0123456789 {i}"),
            );
        }

        let rows = lines(&store, "r1");
        assert_eq!(rows.len(), 3, "two lines of 13 bytes and one explanation");
        let last = &rows[2];
        assert_eq!(last.target.as_deref(), Some(HESTAN));
        assert!(last.message.contains("cap of 25 bytes"), "{}", last.message);
    }

    #[test]
    fn an_over_long_line_is_stored_clipped_with_its_marker() {
        let store = store();
        let at = Attempt::new("r1", "verbose", 1);
        let mut budget = Budget::new();
        // multi-byte on purpose: the clip lands mid-character unless it looks
        let long = "é".repeat(LINE_MAX);
        budget.line(&store, &at, Source::Stream(LogStream::Stdout), &long);
        budget.line(&store, &at, Source::Stream(LogStream::Stdout), "after it");

        let rows = lines(&store, "r1");
        assert_eq!(rows.len(), 2, "a clipped line is stored, not dropped");
        assert!(rows[0].message.ends_with(CLIPPED));
        assert!(rows[0].message.len() <= LINE_MAX + CLIPPED.len());
        assert!(rows[0].message.starts_with("éé"));
        // and capture carries on, because one long line is not a cap
        assert_eq!(rows[1].message, "after it");
    }

    /// what a `Split` makes of a stream arriving in these chunks.
    fn split(chunks: &[&[u8]]) -> Vec<String> {
        let mut out = Vec::new();
        let mut split = Split::default();
        for chunk in chunks {
            split.feed(chunk, |line| out.push(line.to_string()));
        }
        split.finish(|line| out.push(line.to_string()));
        out
    }

    #[test]
    fn lines_are_cut_wherever_the_reads_landed() {
        // a line split across three reads is still one line
        assert_eq!(
            split(&[b"one\ntw", b"o\nthr", b"ee\n"]),
            ["one", "two", "three"]
        );
        // a child that died mid-line still gets what it wrote
        assert_eq!(split(&[b"finished\nhalf a li"]), ["finished", "half a li"]);
        // and one that wrote nothing at all produces nothing, not an empty line
        assert!(split(&[]).is_empty());
        assert!(split(&[b""]).is_empty());
        // an empty line the child really did print is a line
        assert_eq!(split(&[b"\n\n"]), ["", ""]);
        // crlf reads as a line ending rather than as a character
        assert_eq!(split(&[b"windows\r\nnext\r\n"]), ["windows", "next"]);
    }

    #[test]
    fn a_child_that_never_prints_a_newline_cannot_grow_the_parent() {
        let mut split = Split::default();
        let mut lines = Vec::new();
        // a megabyte with no newline in it, arriving 4 KiB at a time
        for _ in 0..256 {
            split.feed(&[b'x'; 4096], |line| lines.push(line.to_string()));
            assert!(
                split.pending.len() <= LINE_MAX,
                "the buffer grew past one line"
            );
        }
        split.feed(b"\nafter it\n", |line| lines.push(line.to_string()));
        split.finish(|line| lines.push(line.to_string()));
        // one clipped line, not two hundred and fifty-six of them
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].len(), LINE_MAX);
        assert_eq!(lines[1], "after it");
    }

    #[test]
    fn each_attempt_gets_its_own_budget() {
        let store = store();
        store.set_log_caps(None, Some(1));
        let mut first = Budget::new();
        first.line(
            &store,
            &Attempt::new("r1", "flaky", 1),
            Source::Stream(LogStream::Stdout),
            "attempt one",
        );
        first.line(
            &store,
            &Attempt::new("r1", "flaky", 1),
            Source::Stream(LogStream::Stdout),
            "dropped",
        );
        let mut second = Budget::new();
        second.line(
            &store,
            &Attempt::new("r1", "flaky", 2),
            Source::Stream(LogStream::Stdout),
            "attempt two",
        );

        let rows = lines(&store, "r1");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2].message, "attempt two");
        assert_eq!(rows[2].attempt, 2);
        assert!(!second.is_spent(), "a retry starts from a full budget");
    }
}
