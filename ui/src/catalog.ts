// what the assets table shows, out of everything registered: the search, the
// state filter, the grouping and the sort.
//
// a module of its own because all four are decisions rather than markup, and
// because one flat table is fine at twelve assets and useless at three
// hundred, which is exactly the size at which testing them through the dom
// stops being possible.
import type { AssetPolicy, AssetSummary } from "./types";

// the separator a folded group node's name ends in. it is also the character a
// name falls back to when nothing declared a group, but that fallback is the
// server's to apply: what arrives here is already resolved
export const SEPARATOR = "/";

export type StateFilter = "all" | "fresh" | "stale" | "never" | "failed";

export const STATE_FILTERS = ["all", "fresh", "stale", "never", "failed"] as const;

export type SortKey = "name" | "state" | "built" | "freshness" | "coverage";

export type Dir = "asc" | "desc";

export interface Group {
  // "" for the assets that are in no group at all
  name: string;
  assets: AssetSummary[];
}

// what a policy says, and what it is waiting for where it wants a build it
// cannot have yet: "when stale · 2026-08-14 waiting for hours[2026-08-14T23]"
// is the sentence somebody needs at 2am. one line, because it goes beside the
// name rather than under it, and the two halves are separated the way the rest
// of a header line is: the rule already has commas in it
export function policySays(policy: AssetPolicy): string {
  const wait = policy.waiting;
  if (wait === null) return policy.says;
  const more = wait.keys > 1 ? ` and ${wait.keys - 1} more` : "";
  const which = wait.key === null ? "" : `${wait.key}${more} `;
  return `${policy.says} · ${which}waiting for ${wait.for}`;
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
// the names are a namespace, and `sales` should not find `stale_orders`.
// `group` is exact and is a different question from the search: it is which
// group, not which letters
export function filterAssets(
  assets: AssetSummary[],
  query: string,
  filter: StateFilter,
  group: string | null = null,
): AssetSummary[] {
  const needle = query.trim().toLowerCase();
  return assets.filter(
    (a) =>
      matchesState(a, filter) &&
      (group === null || a.group === group) &&
      (needle === "" || a.name.toLowerCase().includes(needle)),
  );
}

// the group an asset belongs to, which the api resolved: what it declared,
// else the part of its name before the first separator, else none. "" here is
// "no group", which is the bucket the ungrouped ones share
export function groupOf(a: AssetSummary): string {
  return a.group ?? "";
}

// groups in the order their first member appears, so the api's dependency
// order still shows through, except the ungrouped ones, which go last: sitting
// between two named groups they read as belonging to one of them. with no
// group anywhere there is no grouping to be had, and inventing one out of
// common substrings would be a guess
export function groupAssets(assets: AssetSummary[]): Group[] {
  if (!assets.some((a) => a.group !== null)) {
    return assets.length === 0 ? [] : [{ name: "", assets }];
  }
  const groups: Group[] = [];
  for (const a of assets) {
    const name = groupOf(a);
    const held = groups.find((g) => g.name === name);
    if (held) held.assets.push(a);
    else groups.push({ name, assets: [a] });
  }
  return groups.sort((a, b) => Number(a.name === "") - Number(b.name === ""));
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
