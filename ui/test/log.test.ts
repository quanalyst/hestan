// the run page's log pane draws two sources at once: what hestan said about
// the run, and what its ops printed. what goes in the pane is a decision — a
// source filter, the level and op filters over both, and one order — so it is
// a function, and this is its test.
//
// run with `npm test` (vite bundles this for node, node runs it).
import assert from "node:assert/strict";
import test from "node:test";
import { logRows, marks } from "../src/log";
import type { Filters } from "../src/log";
import type { EventKind, EventLevel, LogStream, OpLog, RunEvent } from "../src/types";

const event = (
  seq: number,
  ts: string,
  op: string | null,
  level: EventLevel,
  kind: EventKind,
  message: string,
): RunEvent => ({
  seq,
  run_id: "r1",
  subject_kind: "run",
  subject: null,
  op,
  level,
  kind,
  message,
  data: null,
  ts,
});

const printed = (
  id: number,
  at: string,
  op: string,
  stream: LogStream,
  message: string,
  attempt = 1,
): OpLog => ({
  id,
  run_id: "r1",
  op,
  attempt,
  at,
  stream,
  level: null,
  target: null,
  message,
});

const traced = (
  id: number,
  at: string,
  op: string,
  level: EventLevel,
  target: string,
  message: string,
): OpLog => ({ id, run_id: "r1", op, attempt: 1, at, stream: null, level, target, message });

const filters = (over: Partial<Filters> = {}): Filters => ({
  source: "both",
  kind: "all",
  level: "all",
  op: null,
  find: "",
  only: false,
  ...over,
});

const events: RunEvent[] = [
  event(1, "2026-08-08T10:00:00Z", null, "info", "run_started", "started"),
  event(2, "2026-08-08T10:00:02Z", "load", "info", "log", "asked for a page"),
  event(3, "2026-08-08T10:00:05Z", "load", "error", "op_failed", "attempt 1 failed: no"),
];

const output: OpLog[] = [
  printed(1, "2026-08-08T10:00:01Z", "load", "stdout", "connecting"),
  printed(2, "2026-08-08T10:00:03Z", "load", "stderr", "timed out"),
  traced(3, "2026-08-08T10:00:04Z", "load", "warn", "orders::load", "retrying"),
];

const messages = (f: Partial<Filters>) => logRows(events, output, filters(f)).map((r) => r.message);

test("both sources interleave by time, and either one alone is itself", () => {
  assert.deepEqual(messages({}), [
    "started",
    "connecting",
    "asked for a page",
    "timed out",
    "retrying",
    "attempt 1 failed: no",
  ]);
  assert.deepEqual(messages({ source: "events" }), [
    "started",
    "asked for a page",
    "attempt 1 failed: no",
  ]);
  assert.deepEqual(messages({ source: "output" }), ["connecting", "timed out", "retrying"]);
});

test("ties keep each source's own order rather than shuffling it", () => {
  const same = "2026-08-08T10:00:00Z";
  const rows = logRows(
    [event(1, same, "load", "info", "log", "one"), event(2, same, "load", "info", "log", "two")],
    [printed(1, same, "load", "stdout", "printed one"), printed(2, same, "load", "stdout", "printed two")],
    filters(),
  );
  assert.deepEqual(rows.map((r) => r.message), ["one", "two", "printed one", "printed two"]);
});

test("the op filter narrows both sources at once", () => {
  const other = printed(9, "2026-08-08T10:00:06Z", "clean", "stdout", "not this one");
  const rows = logRows(events, [...output, other], filters({ op: "clean" }));
  assert.deepEqual(rows.map((r) => r.message), ["not this one"]);
  // a run-level event belongs to no op, so an op filter excludes it
  assert.equal(logRows(events, [], filters({ op: "load" })).length, 2);
});

test("a level filter hides a line off a pipe rather than inventing a level for it", () => {
  // stderr is where plenty of perfectly ordinary programs write progress, so
  // "stderr means error" would be a guess, and a wrong one
  assert.deepEqual(messages({ level: "error" }), ["attempt 1 failed: no"]);
  assert.deepEqual(messages({ level: "warn" }), ["retrying"]);
  assert.deepEqual(messages({ level: "info" }), ["started", "asked for a page"]);
});

test("the event kind filter still narrows events and leaves output alone", () => {
  assert.deepEqual(messages({ kind: "logs" }), [
    "connecting",
    "asked for a page",
    "timed out",
    "retrying",
  ]);
});

test("a row says which op, which stream, and which attempt when there was more than one", () => {
  const rows = logRows(
    [],
    [
      printed(1, "2026-08-08T10:00:00Z", "load", "stdout", "first go"),
      printed(2, "2026-08-08T10:00:01Z", "load", "stderr", "second go", 2),
      traced(3, "2026-08-08T10:00:02Z", "load", "info", "orders::load", "an event"),
    ],
    filters(),
  );
  assert.deepEqual(
    rows.map((r) => [r.op, r.tag, r.attempt]),
    [
      ["load", "stdout", null],
      ["load", "stderr", 2],
      ["load", "info", null],
    ],
  );
});

test("a search marks where it hit, and narrows only when asked to", () => {
  // by default every line stays: the line above a match is often the point
  assert.deepEqual(messages({ find: "time" }), [
    "started",
    "connecting",
    "asked for a page",
    "timed out",
    "retrying",
    "attempt 1 failed: no",
  ]);
  assert.deepEqual(messages({ find: "time", only: true }), ["timed out"]);
  // an empty search is not a search, whatever the toggle says
  assert.equal(messages({ find: "", only: true }).length, 6);
  assert.deepEqual(messages({ find: "nothing says this", only: true }), []);
  // and it composes with the filters already there rather than replacing them
  assert.deepEqual(messages({ find: "e", only: true, source: "output" }), [
    "connecting",
    "timed out",
    "retrying",
  ]);
});

test("a marked message is pieces, so nothing is ever built out of html", () => {
  const pieces = (message: string, find: string) =>
    marks(message, find).map((p) => [p.text, p.hit]);
  assert.deepEqual(pieces("db locked", "lock"), [
    ["db ", false],
    ["lock", true],
    ["ed", false],
  ]);
  // every occurrence, case-insensitively, and the case that was printed is
  // the case that comes back
  assert.deepEqual(pieces("Retry, retry", "RETRY"), [
    ["Retry", true],
    [", ", false],
    ["retry", true],
  ]);
  assert.deepEqual(pieces("no match here", "zzz"), [["no match here", false]]);
  assert.deepEqual(pieces("anything", ""), [["anything", false]]);
  // a hit at either end leaves no empty piece beside it
  assert.deepEqual(pieces("abc", "abc"), [["abc", true]]);
  assert.deepEqual(pieces("<b>x</b>", "<b>"), [
    ["<b>", true],
    ["x</b>", false],
  ]);
});

test("hestan's own line about the capture is marked as hestan's", () => {
  const marker: OpLog = {
    id: 1,
    run_id: "r1",
    op: "load",
    attempt: 1,
    at: "2026-08-08T10:00:00Z",
    stream: null,
    level: "warn",
    target: "hestan",
    message: "capture stopped: this attempt reached its cap of 3 lines",
  };
  const rows = logRows([], [marker, traced(2, "2026-08-08T10:00:01Z", "load", "warn", "hestan::x", "not this")], filters());
  assert.deepEqual(rows.map((r) => r.marker), [true, false]);
});
