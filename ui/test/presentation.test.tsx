import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { displayName, matchesName, hierarchy, subgroupKey, readSet, writeSet, summary } from "../src/presentation";
import { lanesOf, rowsOf } from "../src/timeline";
import TimelineGutter from "../src/TimelineGutter";
import { collapseHierarchy } from "../src/dag";

const items = [
  { name: "id1", display_name: "Readable one", group: "a", subgroup: "shared", group_hue: 10, last_run: { status: "failed" }, freshness: { status: "late" } },
  { name: "id2", group: "b", subgroup: "shared", group_hue: 20, last_run: { status: "running" } },
  { name: "id3", group: "a", group_hue: 10 },
];
test("readable names and identifiers are both searchable with a legacy fallback", () => {
  assert.equal(displayName(items[0]), "Readable one");
  assert.equal(displayName(items[1]), "id2");
  assert.ok(matchesName(items[0], " READABLE "));
  assert.ok(matchesName(items[0], "ID1"));
});
test("subgroups belong to their parent, and unassigned members are explicit", () => {
  const sections = hierarchy(items);
  assert.notEqual(sections[0].children[0].key, sections[1].children[0].key);
  assert.equal(sections[0].children[1].name, "no subgroup");
  assert.equal(hierarchy([{ name: "a/b/c", group: "a" }])[0].children.length, 0);
  const lanes = lanesOf(items, new Set(["a", subgroupKey("a", "shared")]));
  assert.deepEqual(lanes.filter((l) => l.kind === "job").map((l) => l.jobs[0]), ["id1"]);
  assert.equal(lanes.find((l) => l.jobs[0] === "id1" && l.kind === "job")?.label, "Readable one");
});
test("collapsed summaries distinguish execution, checks and freshness", () => {
  assert.match(summary(items), /1 failed · 1 running · 1 late/);
  const markup = renderToStaticMarkup(<svg><TimelineGutter rows={rowsOf(lanesOf(items, new Set()), new Map())} onToggle={() => {}} /></svg>);
  assert.match(markup, /1 failed/);
  assert.match(markup, /1 late/);
  assert.match(markup, /aria-expanded="false"/);
});
test("hierarchy URL keys round trip punctuation and same-named subgroups", () => {
  const keys = new Set(["a,b", subgroupKey("a,b", "shared"), subgroupKey("b", "shared")]);
  const url = new URLSearchParams({ open: writeSet(keys), q: "Readable one", namespace: "n" });
  assert.deepEqual(readSet(new URLSearchParams(url.toString()).get("open")), keys);
  assert.deepEqual(readSet("a,b"), new Set(["a", "b"]));
});
test("collapsed dependencies keep both directions and avoid identifier collisions", () => {
  const nodes = [{ name: "id1", deps: ["id2"] }, { name: "id2", deps: [] }, { name: "id3", deps: ["id1"] }, { name: "group:a", deps: ["id3"] }];
  const folded = collapseHierarchy(nodes, hierarchy(items), new Set(["a"]));
  const group = folded.groups[0].node;
  assert.notEqual(group, "group:a");
  assert.deepEqual(folded.nodes.find((n) => n.name === group)?.deps, ["id2"]);
  assert.deepEqual(folded.nodes.find((n) => n.name === "group:a")?.deps, [group]);
  assert.deepEqual(nodes[2].deps, ["id1"]);
});

test("label views are immutable projections with distinct missing values", async () => {
  const { viewFrom, viewParams, matchesLabel, dimensionsOf } = await import("../src/presentation");
  const data = items.map((i, n) => ({ ...i, labels: (n === 0 ? { arbitrary: "value" } : n === 1 ? { arbitrary: "no arbitrary label" } : {}) as Record<string, string> }));
  const before = JSON.stringify(data);
  const view = ["label:arbitrary", "subgroup"] as const;
  const tree = hierarchy(data, view);
  assert.equal(tree.length, 3);
  assert.notEqual(tree[1].key, tree[2].key, "literal and absent values must not merge");
  assert.deepEqual(tree.flatMap((s) => s.members.map((m) => m.name)).sort(), items.map((i) => i.name).sort());
  assert.equal(tree[0].members[0], data[0]);
  const params = viewParams(new URLSearchParams({ namespace: "scope", q: "Readable", open: "a", state: "failed" }), view);
  assert.deepEqual(viewFrom(new URLSearchParams(params.toString())), view);
  assert.equal(params.get("namespace"), "scope");
  assert.equal(params.get("state"), "failed");
  assert.equal(params.get("q"), "Readable");
  assert.equal(params.has("open"), false);
  assert.deepEqual(viewFrom(new URLSearchParams("by=bad&then=group")), ["group", "subgroup"]);
  assert.deepEqual(viewFrom(new URLSearchParams("by=group&then=group")), ["group", "subgroup"]);
  assert.deepEqual(dimensionsOf(data), ["group", "subgroup", "label:arbitrary"]);
  assert.ok(matchesLabel(data[2], new URLSearchParams("label=arbitrary&missing=1")));
  assert.ok(!matchesLabel(data[0], new URLSearchParams("label=arbitrary&missing=1")));
  assert.ok(matchesLabel(data[0], new URLSearchParams("label=arbitrary&value=value")));
  assert.equal(JSON.stringify(data), before);
});
test("projected timelines preserve names, group marks and every member", () => {
  const data = items.map((i) => ({ ...i, labels: { arbitrary: "shared" } }));
  const view = ["label:arbitrary", "group"] as const;
  const tree = hierarchy(data, view);
  const open = new Set(tree.flatMap((s) => [s.key, ...s.children.map((c) => c.key)]));
  const lanes = lanesOf(data, open, view);
  assert.equal(lanes[0].hue, null, "mixed groups must not acquire a member's colour");
  const leaves = lanes.filter((l) => l.kind === "job");
  assert.equal(leaves.length, 3);
  assert.deepEqual(leaves.map((l) => l.jobs[0]).sort(), ["id1", "id2", "id3"]);
  assert.equal(leaves.find((l) => l.jobs[0] === "id2")?.hue, 20);
  assert.equal(leaves.find((l) => l.jobs[0] === "id1")?.label, "Readable one");
});
test("subgroup-first projections retain parent identity", () => {
  const tree = hierarchy(items, ["subgroup", "group"]);
  assert.equal(tree.length, 3);
  assert.notEqual(tree[0].key, tree[1].key);
  assert.equal(tree[0].name, "a / shared");
  assert.equal(tree[1].name, "b / shared");
});
test("filtering preserves an unassigned member's containing subgroup row", async () => {
  const { filterHierarchy, labelValue } = await import("../src/presentation");
  const sections = filterHierarchy(hierarchy(items), (i) => i.name === "id3");
  assert.equal(sections[0].children[0].name, "no subgroup");
  assert.deepEqual(sections[0].children[0].members.map((i) => i.name), ["id3"]);
  assert.equal(labelValue({ name: "a", group: null, labels: {} }, "constructor"), undefined);
});
test("collapsed same-named subgroups include their parents on the graph", () => {
  const collapsed = collapseHierarchy(items.map((i) => ({ name: i.name, deps: [] })), hierarchy(items), new Set([subgroupKey("a", "shared"), subgroupKey("b", "shared")]));
  assert.deepEqual(collapsed.nodes.filter((n) => n.badge).map((n) => n.display_name), ["a / shared", "b / shared"]);
});
