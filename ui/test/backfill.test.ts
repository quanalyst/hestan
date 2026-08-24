// a backfill is the one action in this ui that costs real money if it is
// wrong, so the three things that could lie about it (what a range covers,
// what it will cost, and why the button is off) are functions with a test.
import assert from "node:assert/strict";
import test from "node:test";
import {
  backfillPlan,
  chunksOf,
  estimateMs,
  keyStates,
  keysInRange,
  medianMs,
  rangeOf,
} from "../src/backfill";
import type { Backfill, OpStatSample, PartitionEntry, PartitionState, Run } from "../src/types";

// the api hands the grid its keys newest first
const shown: PartitionEntry[] = ["05", "04", "03", "02", "01"].map((key, i) => ({
  key: `2026-01-${key}`,
  state: (i < 2 ? "missing" : "materialized") as PartitionState,
  fingerprint: null,
  built_at: null,
  run_id: null,
  reads: [],
  reasons: [],
  waiting: null,
}));

const day = (d: string) => `2026-01-${d}`;
const keys = (list: PartitionEntry[]) => list.map((p) => p.key);

test("a drag reads the same either way round, and comes back oldest first", () => {
  assert.deepEqual(rangeOf(shown, day("04"), day("02")), { from: day("02"), to: day("04") });
  assert.deepEqual(rangeOf(shown, day("02"), day("04")), { from: day("02"), to: day("04") });
  // one cell is a range of one
  assert.deepEqual(rangeOf(shown, day("03"), day("03")), { from: day("03"), to: day("03") });
  assert.equal(rangeOf(shown, day("03"), day("99")), null);
});

test("a range covers the keys the key set actually holds", () => {
  assert.deepEqual(keysInRange(shown, { from: day("02"), to: day("04") }), [
    shown[3],
    shown[2],
    shown[1],
  ]);
  assert.deepEqual(keys(keysInRange(shown, { from: day("01"), to: day("01") })), [day("01")]);
  assert.deepEqual(keysInRange(shown, null), []);
  // a key nothing in the set matches covers nothing rather than everything
  assert.deepEqual(keysInRange(shown, { from: day("02"), to: "2026-02-30" }), []);
});

const running: Backfill = {
  id: 7,
  asset: "traffic",
  from_key: day("01"),
  to_key: day("05"),
  partitions: [],
  run_ids: [],
  total: 0,
  launched: 0,
  created_at: "2026-01-06T00:00:00Z",
  finished_at: null,
  status: "running",
};

test("the button says why it is off, before the click rather than after it", () => {
  const plan = (range: Parameters<typeof backfillPlan>[1], only = true, r: Backfill | null = null) =>
    backfillPlan(shown, range, only, r);

  assert.equal(plan(null).refused, "drag across the grid to pick a range");
  assert.equal(plan({ from: "nope", to: "nope" }).refused, "no partitions in that range");
  // everything in this range is already fresh, and only_missing would drop it
  assert.equal(
    plan({ from: day("01"), to: day("03") }).refused,
    "every partition in that range is already fresh",
  );
  // asked for regardless, the same range is launchable
  assert.equal(plan({ from: day("01"), to: day("03") }, false).refused, null);
  // and one already running takes precedence over all of it: the api refuses
  // a second backfill of one asset, so the ui does not offer one
  assert.match(plan({ from: day("04"), to: day("05") }, true, running).refused ?? "", /still running/);

  const ok = plan({ from: day("03"), to: day("05") });
  assert.equal(ok.refused, null);
  assert.deepEqual(keys(ok.covered), [day("03"), day("04"), day("05")]);
  assert.deepEqual(keys(ok.building), [day("04"), day("05")], "the fresh key is skipped");
});

const sample = (ms: number | null, status: OpStatSample["status"] = "success"): OpStatSample => ({
  run_id: "r",
  status,
  ms,
});

test("the estimate is a median of what a partition has actually taken, or nothing", () => {
  assert.equal(medianMs([sample(100), sample(300), sample(200)]), 200);
  // even counts take the middle of the two, and a timeout does not decide it
  assert.equal(medianMs([sample(100), sample(200), sample(300), sample(400)]), 250);
  assert.equal(medianMs([sample(100), sample(90_000, "failed")]), 100);

  // nothing to go on: no estimate rather than a plausible number
  assert.equal(medianMs([]), null);
  assert.equal(medianMs([sample(null), sample(500, "running")]), null);
  assert.equal(estimateMs(400, null), null);
  assert.equal(estimateMs(400, 1200), 480_000);
  assert.equal(estimateMs(0, 1200), 0);
});

const backfill = (over: Partial<Backfill>): Backfill => ({
  ...running,
  partitions: ["a", "b", "c", "d", "e"],
  total: 5,
  ...over,
});

const run = (id: string, status: Run["status"]): Run => ({
  id,
  job: "assets",
  status,
  trigger: "build",
  params: {},
  created_at: "2026-01-06T00:00:00Z",
  started_at: null,
  finished_at: null,
  error: null,
  resumed_from: null,
  replay_of: null,
  scheduled_for: null,
  tags: {},
  priority: 0,
  claimed_by: null,
  claimed_at: null,
  lease_until: null,
  actor: null,
  build: null,
});

test("which run built which key is arithmetic on the chunk size", () => {
  // two chunks of two out of five, so the build limit is two, which is not on
  // the wire, and is exactly `launched` over the runs it took
  const b = backfill({ run_ids: ["r1", "r2"], launched: 4 });
  const chunks = chunksOf(b, [run("r1", "success"), run("r2", "running")]);
  assert.deepEqual(
    chunks.map((c) => [c.run_id, c.status, c.keys]),
    [
      ["r1", "success", ["a", "b"]],
      ["r2", "running", ["c", "d"]],
      [null, "not launched", ["e"]],
    ],
  );

  const states = keyStates(chunks);
  assert.equal(states.get("a"), "success");
  assert.equal(states.get("d"), "running");
  assert.equal(states.get("e"), "not launched");

  // a short last chunk does not make the arithmetic drift
  const done = backfill({ run_ids: ["r1", "r2", "r3"], launched: 5 });
  assert.deepEqual(
    chunksOf(done, []).map((c) => c.keys),
    [["a", "b"], ["c", "d"], ["e"]],
  );

  // nothing launched yet is the whole range still to go
  assert.deepEqual(
    chunksOf(backfill({ run_ids: [], launched: 0 }), []).map((c) => [c.status, c.keys.length]),
    [["not launched", 5]],
  );
});
