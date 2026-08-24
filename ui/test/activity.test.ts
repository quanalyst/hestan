// the activity feed mixes a polled page with a live stream, so the decisions
// worth testing are what a row is about, whether a filter admits it, and what
// happens when the same event arrives twice.
import assert from "node:assert/strict";
import test from "node:test";
import type { FeedRow } from "../src/activity";
import { decidingLine, deploymentLine, kindLabel, linkFor, matches, merge, subjectOf } from "../src/activity";
import type { EventKind, EventLevel, Health, RunEvent, SubjectKind } from "../src/types";

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
  assert.equal(kindLabel("policy_launched"), "policy launched");
  // a kind from a newer writer reads as itself rather than as nothing
  assert.equal(kindLabel("quantum_entangled"), "quantum entangled");
});

// several processes may serve this ui and exactly one of them decides, so the
// line has to say which one this is rather than only what is happening
const hestan = {
  version: "0.1.0-beta.3",
  schema: 24,
  features: ["bundled", "cli", "postgres"],
  platform: "linux/aarch64",
  debug_assertions: false,
};

const health = (
  deciding: Health["deciding"],
  deployment: Health["deployment"] = { name: null, build: null, hestan },
): Health => ({
  ok: true,
  instance: "a1b2c3d4",
  holding: [],
  deciding,
  deployment,
});

test("the deciding line says whether this is the process that decides", () => {
  assert.equal(
    decidingLine(
      health({ leader: true, holder: "a1b2c3d4", term: 3, lease_secs: 8, decides: true }),
    ),
    "this process (a1b2c3d4) is deciding, on term 3",
  );
  assert.equal(
    decidingLine(
      health({ leader: false, holder: "e5f6a7b8", term: 3, lease_secs: 8, decides: true }),
    ),
    "e5f6a7b8 is deciding; this process (a1b2c3d4) is standing by",
  );
  // a worker is not standing by to decide: it never would
  assert.equal(
    decidingLine(
      health({ leader: false, holder: "e5f6a7b8", term: 3, lease_secs: 8, decides: false }),
    ),
    "e5f6a7b8 is deciding; this process (a1b2c3d4) is a worker and decides nothing",
  );
  // and a deployment with nothing in it that decides says so, because that is
  // the answer to the question this line exists for
  assert.match(
    decidingLine(health({ leader: false, holder: null, term: 3, lease_secs: null, decides: true }))!,
    /nothing is deciding/,
  );
  // a process that could not read its own lease says nothing rather than
  // guessing
  assert.equal(decidingLine(health(null)), null);
});

test("the deployment line separates what was declared from what hestan knows", () => {
  assert.equal(
    deploymentLine(health(null, { name: "prod-eu", build: "9f2c1ab", hestan })),
    "prod-eu · build 9f2c1ab · hestan 0.1.0-beta.3 on linux/aarch64",
  );
  // hestan's own version is never offered in place of the application's build:
  // a deployment that declared none is told it declared none
  const undeclared = deploymentLine(health(null));
  assert.match(undeclared, /build not declared/);
  assert.doesNotMatch(undeclared, /build 0\.1\.0/);
  // and a build with no name still says the half that matters
  assert.equal(
    deploymentLine(health(null, { name: null, build: "9f2c1ab", hestan })),
    "build 9f2c1ab · hestan 0.1.0-beta.3 on linux/aarch64",
  );
});
