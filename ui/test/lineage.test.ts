// the asset page's claim is that staleness is provable: this dep changed, in
// this build, at this time. the reads that turn materialization rows into that
// claim are pure, so they are tested against a fixture rather than the dom.
import assert from "node:assert/strict";
import test from "node:test";
import { downstreamOf, linkKind, movedInputs, whenChanged } from "../src/lineage";
import type { AssetSummary, MaterializationEntry } from "../src/types";

const asset = (name: string, deps: string[]): AssetSummary => ({
  name,
  kind: "derived",
  deps,
  auto: false,
  op: name,
  partitions: null,
  fingerprint: null,
  built_at: null,
  run_id: null,
  stale: false,
  reasons: [],
  checks: { passed: 0, failed: 0, last_run_at: null },
  freshness: null,
});

// newest first, the order the api hands history back in
const build = (
  id: number,
  fingerprint: string,
  inputs: Record<string, string | null> = {},
  changed = true,
): MaterializationEntry => ({
  id,
  partition: null,
  fingerprint,
  changed,
  inputs,
  run_id: `run${id}`,
  built_at: `2026-08-0${id}T00:00:00Z`,
  metadata: null,
  deltas: {},
});

test("downstream is the reverse of every asset's own deps", () => {
  const assets = [
    asset("docs_dir", []),
    asset("doc_stats", ["docs_dir"]),
    asset("doc_totals", ["doc_stats"]),
    asset("long_docs", ["doc_stats"]),
  ];
  assert.deepEqual(downstreamOf(assets, "doc_stats"), ["doc_totals", "long_docs"]);
  assert.deepEqual(downstreamOf(assets, "doc_totals"), []);
  assert.deepEqual(downstreamOf(assets, "nothing_named_this"), []);
});

test("the build a fingerprint arrived in is the oldest one holding it", () => {
  // rebuilt twice at the same content, so the change is three builds back and
  // not the newest one
  const history = [
    build(5, "bbb", { docs_dir: "z" }, false),
    build(4, "bbb", { docs_dir: "z" }, false),
    build(3, "bbb", { docs_dir: "z" }),
    build(2, "aaa", { docs_dir: "y" }),
    build(1, "aaa", { docs_dir: "x" }),
  ];
  const at = whenChanged(history, "bbb");
  assert.equal(at?.built.id, 3);
  assert.equal(at?.before?.id, 2);
});

test("a fingerprint the history does not reach names no build at all", () => {
  const history = [build(2, "bbb"), build(1, "aaa")];
  // the newest build does not hold it: the history was capped, or it came from
  // somewhere this list cannot see. naming the wrong build would be worse
  assert.equal(whenChanged(history, "aaa"), null);
  assert.equal(whenChanged(history, "ccc"), null);
  assert.equal(whenChanged(history, null), null);
  assert.equal(whenChanged([], "bbb"), null);

  // the very first build recorded has nothing before it
  const first = whenChanged([build(1, "aaa")], "aaa");
  assert.equal(first?.built.id, 1);
  assert.equal(first?.before, null);
});

test("a dep that is merely stale itself is not a dep that changed", () => {
  // the api records a reason for both, and they are different claims: one has
  // moved, the other will move when somebody rebuilds it
  assert.equal(linkKind({ dep: "orders", had: "aaa", now: "bbb" }), "changed");
  assert.equal(linkKind({ dep: "orders", had: "aaa", now: "aaa" }), "pending");
  assert.equal(linkKind({ dep: "orders", had: null, now: null }), "absent");
  assert.equal(linkKind({ dep: "orders", had: "aaa", now: null }), "absent");
});

test("what moved under a build is the inputs whose fingerprints differ", () => {
  const built = build(2, "bbb", { docs_dir: "z", config: "c", fresh: "n" });
  const before = build(1, "aaa", { docs_dir: "y", config: "c" });
  // config held still, and an input with nothing before it to compare against
  // is not a change: the first recorded build changed nothing
  assert.deepEqual(movedInputs(built, before), ["docs_dir"]);
  assert.deepEqual(movedInputs(built, null), []);

  // a source records null for its own input, and null moving to a hash is a move
  assert.deepEqual(movedInputs(build(2, "b", { src: "h" }), build(1, "a", { src: null })), ["src"]);
});
