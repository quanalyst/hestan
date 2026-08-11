// the activity feed mixes a polled page with a live stream, so the decisions
// worth testing are what a row is about, whether a filter admits it, and what
// happens when the same event arrives twice.
import assert from "node:assert/strict";
import test from "node:test";
import type { FeedRow } from "../src/activity";
import { kindLabel, linkFor, matches, merge, subjectOf } from "../src/activity";
import type { EventKind, EventLevel, RunEvent, SubjectKind } from "../src/types";

const ev = (
  seq: number,
  subject_kind: SubjectKind,
  subject: string | null,
  kind: EventKind,
  message = "",
  level: EventLevel = "info",
  run_id: string | null = null,
): RunEvent => ({
  seq,
  run_id,
  subject_kind,
  subject,
  op: null,
  level,
  kind,
  message,
  data: null,
  ts: "2026-01-01T00:00:00Z",
  actor: null,
});

const row = (e: RunEvent): FeedRow => ({ kind: "event", event: e });

test("a run event is about its run, which lives in a different column", () => {
  assert.equal(subjectOf(ev(1, "asset", "sales", "asset_materialized")), "sales");
  assert.equal(subjectOf(ev(2, "run", null, "run_started", "", "info", "r1")), "r1");
  assert.equal(subjectOf(ev(3, "system", null, "log")), null);
});

test("a subject links to its own page, and to nothing when it has none", () => {
  assert.equal(linkFor(ev(1, "run", null, "run_started", "", "info", "r1")), "/runs/r1");
  assert.equal(linkFor(ev(2, "asset", "sales/orders", "asset_materialized")), "/assets/sales/orders");
  assert.equal(linkFor(ev(3, "schedule", "etl", "schedule_fired")), "/jobs/etl");
  assert.equal(linkFor(ev(4, "backfill", "12", "backfill_started")), "/backfills/12");
  // a notification's delivery is about nothing that has a page
  assert.equal(linkFor(ev(5, "system", "7", "notification_failed")), null);
});

test("filters narrow, compose, and an empty find admits everything", () => {
  const e = ev(1, "asset", "sales/orders", "asset_materialized", "sales/orders materialized");
  const all = { subjectKind: "all", level: "all", find: "" } as const;
  assert.equal(matches(e, all), true);
  assert.equal(matches(e, { ...all, subjectKind: "asset" }), true);
  assert.equal(matches(e, { ...all, subjectKind: "sensor" }), false);
  assert.equal(matches(e, { ...all, level: "error" }), false);
  // the find box reads the message, the kind and the subject
  assert.equal(matches(e, { ...all, find: "orders" }), true);
  assert.equal(matches(e, { ...all, find: "materialized" }), true);
  assert.equal(matches(e, { ...all, find: "  ORDERS " }), true);
  assert.equal(matches(e, { ...all, find: "sensor" }), false);
  // and they compose rather than replacing one another
  assert.equal(matches(e, { subjectKind: "asset", level: "info", find: "sales" }), true);
  assert.equal(matches(e, { subjectKind: "asset", level: "warn", find: "sales" }), false);
});

test("a live event joins the feed newest first, once, under the cap", () => {
  const feed = [row(ev(3, "asset", "a", "asset_materialized")), row(ev(1, "run", null, "run_queued"))];
  const merged = merge(feed, [row(ev(4, "sensor", "watch", "sensor_tick"))]);
  assert.deepEqual(
    merged.map((r) => (r.kind === "event" ? r.event.seq : r.gap.through)),
    [4, 3, 1],
  );

  // a resumed stream re-delivers what a poll had already shown, and the feed
  // must not show it twice
  const twice = merge(merged, [row(ev(3, "asset", "a", "asset_materialized"))]);
  assert.equal(twice.length, 3);

  // and it holds what it says it holds rather than growing all afternoon
  const many = Array.from({ length: 10 }, (_, i) => row(ev(100 + i, "asset", "a", "log")));
  assert.equal(merge(feed, many, 5).length, 5);
  assert.equal((merge(feed, many, 5)[0] as { event: RunEvent }).event.seq, 109);
});

test("a dropped-events marker sorts where the events it stands for would have been", () => {
  const feed = [row(ev(9, "asset", "a", "log")), row(ev(2, "asset", "a", "log"))];
  const merged = merge(feed, [{ kind: "gap", gap: { count: 6, through: 8 } }]);
  assert.deepEqual(
    merged.map((r) => (r.kind === "event" ? `e${r.event.seq}` : `gap${r.gap.through}`)),
    ["e9", "gap8", "e2"],
  );
});

test("a kind reads without saying its subject twice", () => {
  assert.equal(kindLabel("asset_materialized"), "asset materialized");
  assert.equal(kindLabel("run_started"), "started");
  assert.equal(kindLabel("op_retry"), "retry");
  assert.equal(kindLabel("schedule_caught_up"), "caught up");
  assert.equal(kindLabel("notification_failed"), "failed");
  assert.equal(kindLabel("sensor_tick"), "sensor tick");
  // a kind from a newer writer reads as itself rather than as nothing
  assert.equal(kindLabel("quantum_entangled"), "quantum entangled");
});
