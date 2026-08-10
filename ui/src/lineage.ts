// why an asset is stale, as a chain of things that provably moved.
//
// every build records its own fingerprint and the fingerprints of the inputs
// it consumed, so the answer is on disk rather than inferred from clocks: the
// dep whose content changed, the build it changed in, and what had moved
// under *that* build. these are the reads that turn those rows into a chain,
// and they are pure so the walk can be tested against a fixture.
import type { AssetSummary, MaterializationEntry, StaleReason } from "./types";

// how far the walk goes before it stops asking. four levels is more than
// anybody reads, and each one costs a request
export const CHAIN_DEPTH = 4;

// the three things a reason can be, which are not the same claim:
// `changed` is content that provably moved, `pending` is an upstream that is
// itself stale but has not been rebuilt — so nothing has moved here *yet* —
// and `absent` is one that has never been built at all
export type ChainKind = "changed" | "pending" | "absent";

// one step of the chain: an upstream, what it did, and what did it in turn
export interface ChainLink {
  asset: string;
  kind: ChainKind;
  // what this asset consumed last time, and what the upstream holds now
  had: string | null;
  now: string | null;
  // the build in which the upstream took `now`; null when its recorded history
  // does not reach back that far, which is a thing to say rather than guess at
  built: MaterializationEntry | null;
  // the build before that one, which is what `built` is a change against
  before: MaterializationEntry | null;
  causes: ChainLink[];
}

// the api records a reason both for a dep whose content moved and for one that
// is merely stale itself, and calling the second one "changed" would be a lie:
// nothing has moved there until it is rebuilt
export function linkKind(reason: StaleReason): ChainKind {
  if (reason.now === null) return "absent";
  return reason.had === reason.now ? "pending" : "changed";
}

// assets that name `name` as a dep. every asset carries its own deps, so the
// reverse edges are one pass over the list the page already has
export function downstreamOf(assets: AssetSummary[], name: string): string[] {
  return assets.filter((a) => a.deps.includes(name)).map((a) => a.name);
}

// the build at which an asset's content became `fingerprint`, and the build
// before it.
//
// history is newest first, so that is the *oldest* consecutive entry holding
// the fingerprint: a rebuild that produced the same bytes is not when it
// changed. null when the newest entry does not hold it at all — the history
// was capped, or the value came from somewhere this list cannot see — because
// naming the wrong build is worse than naming none.
export function whenChanged(
  history: MaterializationEntry[],
  fingerprint: string | null,
): { built: MaterializationEntry; before: MaterializationEntry | null } | null {
  if (fingerprint === null || history.length === 0) return null;
  if (history[0].fingerprint !== fingerprint) return null;
  let i = 0;
  while (i + 1 < history.length && history[i + 1].fingerprint === fingerprint) i++;
  return { built: history[i], before: history[i + 1] ?? null };
}

// which of a build's inputs held a different fingerprint than they did for the
// build before it — the reason that build produced something new. an input
// with nothing before it to compare against is not a change: the first
// recorded build changed nothing, it started everything
export function movedInputs(
  built: MaterializationEntry,
  before: MaterializationEntry | null,
): string[] {
  if (before === null) return [];
  return Object.keys(built.inputs).filter(
    (dep) => dep in before.inputs && built.inputs[dep] !== before.inputs[dep],
  );
}
