// the catalog's four decisions (search, state filter, grouping, sort) and
// the two ways the graph is made small enough to draw. all of them are pure
// functions over a fixture, because a table of three hundred assets is not a
// thing to assert through the dom.
import assert from "node:assert/strict";
import test from "node:test";
import {
  filterAssets,
  groupAssets,
  groupOf,
  neverBuilt,
  policySays,
  sortAssets,
} from "../src/catalog";
import { collapseGroups, neighbourhood } from "../src/dag";
import type { DagNode } from "../src/DagView";
import type { AssetPolicy, AssetSummary, Origin, PartitionCounts } from "../src/types";

interface Over {
  stale?: boolean;
  built_at?: string | null;
  failed?: number;
  partitions?: PartitionCounts | null;
  within_secs?: number;
  last_success?: string | null;
  group?: string | null;
  provenance?: Origin[];
}

// the group falls back to the name prefix on the way out of the api, not here.
// this fixture stands in for what the api sent, so it applies the same
// fallback: everything before the first separator, and nothing where there is
// nothing before it
const prefixGroup = (name: string): string | null => {
  const cut = name.indexOf("/");
  return cut > 0 ? name.slice(0, cut) : null;
};

export const asset = (name: string, over: Over = {}): AssetSummary => ({
  name,
  group: over.group === undefined ? prefixGroup(name) : over.group,
  group_hue: 200,
  provenance: over.provenance ?? [],
  kind: "derived",
  deps: [],
  auto: false,
  policy: null,
  op: name,
  partitions: over.partitions ?? null,
  fingerprint: "fp",
  built_at: over.built_at === undefined ? "2026-08-08T00:00:00Z" : over.built_at,
  run_id: null,
  stale: over.stale ?? false,
  reasons: [],
  mappings: [],
  checks: { passed: 1, failed: over.failed ?? 0, last_run_at: null },
  freshness:
    over.within_secs === undefined
      ? null
      : {
          status: "fresh",
          within_secs: over.within_secs,
          late_by_secs: null,
          last_success: over.last_success ?? null,
        },
});

const names = (list: AssetSummary[]) => list.map((a) => a.name);

const catalog = [
  asset("sales/orders"),
  asset("sales/returns", { stale: true }),
  asset("sales/customers", { built_at: null, stale: true }),
  asset("marketing/spend", { failed: 1 }),
  asset("heartbeat"),
];

test("the four states are four different questions, not one", () => {
  // never built and stale are the same verdict to the engine and different
  // things to look at, so the stale filter does not answer for both
  assert.deepEqual(names(filterAssets(catalog, "", "stale")), ["sales/returns"]);
  assert.deepEqual(names(filterAssets(catalog, "", "never")), ["sales/customers"]);
  assert.deepEqual(names(filterAssets(catalog, "", "fresh")), [
    "sales/orders",
    "marketing/spend",
    "heartbeat",
  ]);
  // a failed check is about the data rather than about the build, so it cuts
  // across the other three
  assert.deepEqual(names(filterAssets(catalog, "", "failed")), ["marketing/spend"]);
  assert.equal(filterAssets(catalog, "", "all").length, 5);

  // a partitioned asset has been built when any key has
  const none = { total: 4, materialized: 0, stale: 0, missing: 4 };
  const some = { total: 4, materialized: 0, stale: 2, missing: 2 };
  assert.equal(neverBuilt(asset("a", { partitions: none, built_at: null })), true);
  assert.equal(neverBuilt(asset("a", { partitions: some, built_at: null })), false);
});

test("search is a substring of the name, and it composes with the state", () => {
  assert.deepEqual(names(filterAssets(catalog, "sales", "all")), [
    "sales/orders",
    "sales/returns",
    "sales/customers",
  ]);
  assert.deepEqual(names(filterAssets(catalog, "  ORD  ", "all")), ["sales/orders"]);
  assert.deepEqual(names(filterAssets(catalog, "sales", "stale")), ["sales/returns"]);
  assert.deepEqual(names(filterAssets(catalog, "nothing", "all")), []);
});

test("grouping follows the group the api resolved, not the name", () => {
  const groups = groupAssets(catalog);
  assert.deepEqual(
    groups.map((g) => [g.name, g.assets.length]),
    [
      ["sales", 3],
      ["marketing", 1],
      ["", 1],
    ],
  );
  // no group anywhere: one flat list, not one group per asset
  const flat = groupAssets([asset("orders"), asset("returns")]);
  assert.deepEqual(
    flat.map((g) => [g.name, g.assets.length]),
    [["", 2]],
  );
  assert.deepEqual(groupAssets([]), []);
  // a declared group wins over the prefix in the name, and the row goes with
  // the group rather than with the letters it starts with
  const moved = groupAssets([
    asset("sales/orders", { group: "finance" }),
    asset("sales/returns"),
  ]);
  assert.deepEqual(
    moved.map((g) => [g.name, g.assets.map((a) => a.name)]),
    [
      ["finance", ["sales/orders"]],
      ["sales", ["sales/returns"]],
    ],
  );
  assert.equal(groupOf(asset("sales/eu/orders")), "sales");
  assert.equal(groupOf(asset("heartbeat")), "");
});

test("the group filter is exact, and it composes with the other two", () => {
  assert.deepEqual(names(filterAssets(catalog, "", "all", "sales")), [
    "sales/orders",
    "sales/returns",
    "sales/customers",
  ]);
  // exact, so it is not the search under another name
  assert.deepEqual(names(filterAssets(catalog, "", "all", "sale")), []);
  assert.deepEqual(names(filterAssets(catalog, "", "stale", "sales")), ["sales/returns"]);
  assert.deepEqual(names(filterAssets(catalog, "cust", "all", "sales")), ["sales/customers"]);
  // no group named is every group, which is not the same as the ungrouped ones
  assert.equal(filterAssets(catalog, "", "all", null).length, 5);
});

test("every column sorts both ways and leaves equal rows where they were", () => {
  assert.deepEqual(names(sortAssets(catalog, "name", "asc")), [
    "heartbeat",
    "marketing/spend",
    "sales/customers",
    "sales/orders",
    "sales/returns",
  ]);
  // fresh, then stale, then never built, and the two fresh ones keep the
  // order the api sent them in
  assert.deepEqual(names(sortAssets(catalog, "state", "asc")), [
    "sales/orders",
    "marketing/spend",
    "heartbeat",
    "sales/returns",
    "sales/customers",
  ]);
  assert.deepEqual(names(sortAssets(catalog, "state", "desc")).slice(0, 1), ["sales/customers"]);

  const built = [
    asset("old", { built_at: "2026-08-01T00:00:00Z" }),
    asset("new", { built_at: "2026-08-09T00:00:00Z" }),
    asset("never", { built_at: null }),
  ];
  assert.deepEqual(names(sortAssets(built, "built", "desc")), ["new", "old", "never"]);

  // an unpartitioned asset has no coverage and sorts past a covered one
  const cover = [
    asset("half", { partitions: { total: 4, materialized: 2, stale: 1, missing: 1 } }),
    asset("full", { partitions: { total: 4, materialized: 4, stale: 0, missing: 0 } }),
    asset("plain"),
  ];
  assert.deepEqual(names(sortAssets(cover, "coverage", "asc")), ["half", "full", "plain"]);
});

test("freshness sorts by how much of its own window is spent", () => {
  const now = Date.now();
  const ago = (secs: number) => new Date(now - secs * 1000).toISOString();
  // half an hour into a six hour window is fresher than half an hour into one
  const list = [
    asset("wide", { within_secs: 21600, last_success: ago(1800) }),
    asset("tight", { within_secs: 3600, last_success: ago(1800) }),
    asset("none"),
  ];
  assert.deepEqual(names(sortAssets(list, "freshness", "desc")), ["tight", "wide", "none"]);
});

// the group is on the node, resolved, exactly as `AssetsPage` puts it there
const node = (name: string, deps: string[] = []): DagNode => ({
  name,
  deps,
  group: prefixGroup(name),
});

const graph: DagNode[] = [
  node("raw"),
  node("sales/orders", ["raw"]),
  node("sales/returns", ["raw"]),
  node("sales/customers", ["sales/orders"]),
  node("finance/revenue", ["sales/orders", "sales/returns"]),
  node("elsewhere"),
];

test("a policy says what it is waiting for, and which key is waiting", () => {
  const policy = (over: Partial<AssetPolicy> = {}): AssetPolicy => ({
    rule: "stale",
    cron: null,
    tz: null,
    upstream_ready: true,
    says: "when stale, once upstream is ready",
    waiting: null,
    ...over,
  });
  assert.equal(policySays(policy()), "when stale, once upstream is ready");

  // an unpartitioned asset has no key to name, so the sentence is what it is
  // waiting on and nothing else
  assert.equal(
    policySays(policy({ says: "when stale", waiting: { key: null, for: "orders", keys: 1 } })),
    "when stale · waiting for orders",
  );

  // and a partitioned one names the newest key waiting, then how many others
  // are in the same position
  const wait = { key: "2026-08-14", for: "hours[2026-08-14T23]", keys: 1 };
  assert.equal(
    policySays(policy({ says: "when stale", waiting: wait })),
    "when stale · 2026-08-14 waiting for hours[2026-08-14T23]",
  );
  assert.equal(
    policySays(policy({ says: "when stale", waiting: { ...wait, keys: 3 } })),
    "when stale · 2026-08-14 and 2 more waiting for hours[2026-08-14T23]",
  );
});

test("a neighbourhood reaches both ways, and stops at the depth asked for", () => {
  const at = (focus: string, depth: number) =>
    neighbourhood(graph, focus, depth).map((n) => n.name);
  // one hop from orders is what feeds it and what it feeds
  assert.deepEqual(at("sales/orders", 1), [
    "raw",
    "sales/orders",
    "sales/customers",
    "finance/revenue",
  ]);
  // two hops picks up the sibling, through the source they share
  assert.deepEqual(at("sales/orders", 2), [
    "raw",
    "sales/orders",
    "sales/returns",
    "sales/customers",
    "finance/revenue",
  ]);
  assert.deepEqual(at("elsewhere", 3), ["elsewhere"]);
  // a focus that is not in the graph draws nothing rather than everything
  assert.deepEqual(at("gone", 2), []);
});

// the fold used to slice the prefix off the name; it now reads the group the
// api resolved. on a graph where the two agree it has to rewire exactly the
// same edges, which is what every existing deployment sees
test("folding by the declared group rewires what folding by the prefix did", () => {
  const folded = collapseGroups(graph, new Set(["sales"]));
  assert.deepEqual(
    folded.map((n) => [n.name, n.deps, n.badge]),
    [
      ["raw", [], undefined],
      ["sales/", ["raw"], "×3"],
      ["finance/revenue", ["sales/"], undefined],
      ["elsewhere", [], undefined],
    ],
  );
  // orders -> customers was inside the group, and is not an edge any more
  assert.equal(
    folded.some((n) => n.deps.includes("sales/") && n.name === "sales/"),
    false,
  );
  // and a search for something it swallowed still finds where it went
  assert.match(folded[1].find ?? "", /sales\/orders/);
  assert.match(folded[1].find ?? "", /sales\/customers/);
  // folding nothing is the graph itself, not a copy of it with the same shape
  assert.equal(collapseGroups(graph, new Set()), graph);

  // and a node whose group is not its prefix folds with the group. the prefix
  // fold could not have done this at all, which is the point of declaring one
  const moved: DagNode[] = [
    { name: "raw", deps: [], group: null },
    { name: "sales/orders", deps: ["raw"], group: "finance" },
    { name: "finance/revenue", deps: ["sales/orders"], group: "finance" },
    { name: "elsewhere", deps: [], group: null },
  ];
  assert.deepEqual(
    collapseGroups(moved, new Set(["finance"])).map((n) => [n.name, n.deps, n.badge]),
    [
      ["raw", [], undefined],
      ["finance/", ["raw"], "×2"],
      ["elsewhere", [], undefined],
    ],
  );
});
