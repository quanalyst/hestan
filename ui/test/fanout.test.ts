// a mapped op writes no op_runs row of its own, so everything the run page
// says about a fan-out it works out from the instances' names. a fan-out
// inside a fan-out puts a second group in the name, and reading that back
// wrong is a whole level of a run silently absent from the page.
import assert from "node:assert/strict";
import test from "node:test";
import { fanOut, instanceDeps, instanceTree, labelsOf, rollup } from "../src/fanout";
import type { JobSummary, OpRun, OpSummary } from "../src/types";

const op = (name: string, mapped_over: string | null, deps: string[] = []): OpSummary =>
  ({ name, deps, mapped_over }) as OpSummary;

const job = (...ops: OpSummary[]) => ({ ops }) as JobSummary;

const row = (name: string, status = "success"): OpRun => ({ op: name, status }) as OpRun;

// regions -> sites (one fan-out) -> probe (a fan-out inside it)
const nested = job(
  op("regions", null),
  op("sites", "regions", ["regions"]),
  op("probe", "sites", ["sites"]),
);

test("an instance name reads back as one label per level of fan-out", () => {
  assert.deepEqual(labelsOf("sites[0]"), ["0"]);
  assert.deepEqual(labelsOf("probe[0][3]"), ["0", "3"]);
  assert.deepEqual(labelsOf("probe[2026-01-05][3]"), ["2026-01-05", "3"]);
  // a name no expansion could have written is not one
  assert.equal(labelsOf("probe"), null);
  assert.equal(labelsOf("probe[0"), null);
  assert.equal(labelsOf("probe[a[b]"), null);
});

test("instances group under the op they belong to, at either depth", () => {
  const rows = [
    row("probe[1][0]"),
    row("regions"),
    row("probe[0][1]"),
    row("sites[1]"),
    row("probe[0][0]"),
    row("sites[0]"),
    row("probe[1][1]"),
  ];
  const fan = fanOut(nested, rows);
  assert.deepEqual([...fan.keys()].sort(), ["probe", "sites"]);
  // element order at every level, whatever order the rows arrived in
  assert.deepEqual(
    fan.get("probe")!.map((r) => r.op),
    ["probe[0][0]", "probe[0][1]", "probe[1][0]", "probe[1][1]"],
  );
  assert.deepEqual(
    fan.get("sites")!.map((r) => r.op),
    ["sites[0]", "sites[1]"],
  );
});

test("an op whose name has brackets is not an instance of anything", () => {
  // `regions` is what a fan-out expands over rather than a fan-out
  const fan = fanOut(nested, [row("regions[extra]"), row("sites[0]")]);
  assert.deepEqual([...fan.keys()], ["sites"]);
});

test("a nested fan-out is a tree of groups and a flat one is not", () => {
  const flat = instanceTree([row("sites[0]"), row("sites[1]")]);
  assert.deepEqual(
    flat.map((n) => n.kind),
    ["instance", "instance"],
  );

  const tree = instanceTree([
    row("probe[0][0]"),
    row("probe[0][1]"),
    row("probe[1][0]"),
  ]);
  assert.deepEqual(
    tree.map((n) => (n.kind === "group" ? n.label : n.row.op)),
    ["0", "1"],
  );
  const first = tree[0];
  assert.equal(first.kind, "group");
  assert.deepEqual(
    first.kind === "group" ? first.children.map((c) => (c.kind === "instance" ? c.row.op : "")) : [],
    ["probe[0][0]", "probe[0][1]"],
  );
});

test("an instance waited for its own outer instance, not for every one", () => {
  const fan = fanOut(nested, [
    row("sites[0]"),
    row("sites[1]"),
    row("probe[0][0]"),
    row("probe[1][0]"),
  ]);
  // the inner fan-out follows the outer instance it belongs to
  assert.deepEqual(instanceDeps(nested.ops[2], "probe[1][0]", fan), ["sites[1]"]);
  // and the outer one follows the ordinary op it expanded over
  assert.deepEqual(instanceDeps(nested.ops[1], "sites[1]", fan), ["regions"]);
});

test("the worst thing any instance is doing is what the mapped op is doing", () => {
  assert.equal(rollup([row("probe[0][0]"), row("probe[0][1]")]), "success");
  assert.equal(rollup([row("probe[0][0]"), row("probe[1][0]", "failed")]), "failed");
  assert.equal(rollup([row("probe[0][0]"), row("probe[1][0]", "running")]), "running");
});
