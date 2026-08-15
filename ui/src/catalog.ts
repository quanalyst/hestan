// what the assets table shows, out of everything registered: the search, the
// state filter, the grouping and the sort.
//
// a module of its own because all four are decisions rather than markup, and
// because one flat table is fine at twelve assets and useless at three
// hundred, which is exactly the size at which testing them through the dom
// stops being possible.
import type { AssetSummary } from "./types";

// the separator a name uses to say which group it is in. one character, and
// the one every catalog in the world already uses
export const SEPARATOR = "/";

export type StateFilter = "all" | "fresh" | "stale" | "never" | "failed";

export const STATE_FILTERS = ["all", "fresh", "stale", "never", "failed"] as const;

export type SortKey = "name" | "state" | "built" | "freshness" | "coverage";

export type Dir = "asc" | "desc";

export interface Group {
  // "" for the assets whose names carry no separator at all
  prefix: string;
  assets: AssetSummary[];
}

// never built and stale are the same verdict to the engine (nothing to
// compare against is why it is stale) and different things to look at
export function neverBuilt(a: AssetSummary): boolean {
  return a.partitions ? a.partitions.materialized + a.partitions.stale === 0 : a.built_at === null;
}

export function matchesState(a: AssetSummary, filter: StateFilter): boolean {
  switch (filter) {
    case "all":
      return true;
    case "fresh":
      return !a.stale;
    case "stale":
      return a.stale && !neverBuilt(a);
    case "never":
      return neverBuilt(a);
    case "failed":
      return a.checks.failed > 0;
  }
}

// substring on the name, case-insensitively, as you type. no fuzzy matching:
// the names are a namespace, and `sales` should not find `stale_orders`
export function filterAssets(
  assets: AssetSummary[],
  query: string,
  filter: StateFilter,
): AssetSummary[] {
  const needle = query.trim().toLowerCase();
  return assets.filter(
    (a) => matchesState(a, filter) && (needle === "" || a.name.toLowerCase().includes(needle)),
  );
}

// the group a name declares by carrying a separator: everything up to the
// first one. `sales/orders` and `sales/returns` are one group; `heartbeat` is
// in no group at all
export function groupOf(name: string): string {
  const cut = name.indexOf(SEPARATOR);
  return cut < 0 ? "" : name.slice(0, cut);
}

// groups in the order their first member appears, so the api's dependency
// order still shows through, except the unprefixed ones, which go last:
// sitting between two named groups they read as belonging to one of them.
// with no separator anywhere there is no grouping to be had, and inventing
// one out of common substrings would be a guess
export function groupAssets(assets: AssetSummary[]): Group[] {
  if (!assets.some((a) => a.name.includes(SEPARATOR))) {
    return assets.length === 0 ? [] : [{ prefix: "", assets }];
  }
  const groups: Group[] = [];
  for (const a of assets) {
    const prefix = groupOf(a.name);
    const held = groups.find((g) => g.prefix === prefix);
    if (held) held.assets.push(a);
    else groups.push({ prefix, assets: [a] });
  }
  return groups.sort((a, b) => Number(a.prefix === "") - Number(b.prefix === ""));
}

const stateRank = (a: AssetSummary) => (neverBuilt(a) ? 2 : a.stale ? 1 : 0);

const builtAt = (a: AssetSummary) => (a.built_at === null ? 0 : new Date(a.built_at).getTime());

// how much of the declared window is spent; no policy has no answer, and sorts
// below one that is brand new
const windowUsed = (a: AssetSummary) => {
  const f = a.freshness;
  if (f === null) return -1;
  if (f.last_success === null) return Number.MAX_SAFE_INTEGER;
  return (Date.now() - new Date(f.last_success).getTime()) / 1000 / Math.max(f.within_secs, 1);
};

// an unpartitioned asset has no coverage, so it sorts past a fully covered
// one rather than mixed in among the empty ones
const coverage = (a: AssetSummary) =>
  a.partitions === null ? 2 : a.partitions.materialized / Math.max(a.partitions.total, 1);

const COMPARE: Record<SortKey, (a: AssetSummary, b: AssetSummary) => number> = {
  name: (a, b) => a.name.localeCompare(b.name),
  state: (a, b) => stateRank(a) - stateRank(b),
  built: (a, b) => builtAt(a) - builtAt(b),
  freshness: (a, b) => windowUsed(a) - windowUsed(b),
  coverage: (a, b) => coverage(a) - coverage(b),
};

// stable, so a column of equal values keeps the dependency order the api sent
export function sortAssets(assets: AssetSummary[], key: SortKey, dir: Dir): AssetSummary[] {
  const compare = COMPARE[key];
  return [...assets].sort((a, b) => (dir === "asc" ? compare(a, b) : -compare(a, b)));
}
