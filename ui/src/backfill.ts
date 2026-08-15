// what a backfill would do, before it is started, and what it did once it is
// running.
//
// the arithmetic is all here rather than in the markup: a range that covers
// nothing, an estimate with no history behind it and a partition nobody can
// say the state of are the three ways this feature would lie, and each of
// them is a function with a test.
import type { Backfill, OpStatSample, PartitionEntry, Run, RunStatus } from "./types";

// the two ends of a drag over the grid, as keys of the asset's key set
export interface KeyRange {
  from: string;
  to: string;
}

// the grid draws newest key first, so a drag made either way round covers the
// same span, and the api takes it oldest first, which is what this returns
export function rangeOf(shown: PartitionEntry[], a: string, b: string): KeyRange | null {
  const i = shown.findIndex((p) => p.key === a);
  const j = shown.findIndex((p) => p.key === b);
  if (i < 0 || j < 0) return null;
  const [newest, oldest] = i <= j ? [i, j] : [j, i];
  return { from: shown[oldest].key, to: shown[newest].key };
}

// the keys a range covers, oldest first. taken from the grid rather than
// generated: what the key set holds is the api's answer, and inventing keys
// between two dates would be this ui's guess at a partition scheme
export function keysInRange(shown: PartitionEntry[], range: KeyRange | null): PartitionEntry[] {
  if (range === null) return [];
  const from = shown.findIndex((p) => p.key === range.from);
  const to = shown.findIndex((p) => p.key === range.to);
  if (from < 0 || to < 0) return [];
  return shown.slice(Math.min(from, to), Math.max(from, to) + 1).reverse();
}

export interface Plan {
  // every key the range covers
  covered: PartitionEntry[];
  // the ones a launch would actually build, which `only_missing` narrows to
  // the ones that are not already fresh
  building: PartitionEntry[];
  // why the button is disabled, in words, or null when it is not
  refused: string | null;
}

// the plan and, when there is one, the reason it cannot be launched. said
// before the click: an error afterwards is a click that should not have been
// offered
export function backfillPlan(
  shown: PartitionEntry[],
  range: KeyRange | null,
  onlyMissing: boolean,
  running: Backfill | null,
): Plan {
  const covered = keysInRange(shown, range);
  const building = onlyMissing ? covered.filter((p) => p.state !== "materialized") : covered;
  const refused =
    running !== null
      ? `backfill ${running.id} of this asset is still running`
      : range === null
        ? "drag across the grid to pick a range"
        : covered.length === 0
          ? "no partitions in that range"
          : building.length === 0
            ? "every partition in that range is already fresh"
            : null;
  return { covered, building, refused };
}

// the middle of what a build of one partition has actually taken. the median
// rather than the mean, since one build that hit a timeout should not decide
// what four hundred of them will cost
export function medianMs(samples: OpStatSample[]): number | null {
  // successes only: a failure's duration is how long it took to break, which
  // is not how long the work takes
  const ms = samples
    .filter((s) => s.status === "success" && s.ms !== null)
    .map((s) => s.ms as number)
    .sort((a, b) => a - b);
  if (ms.length === 0) return null;
  const half = Math.floor(ms.length / 2);
  return ms.length % 2 === 1 ? ms[half] : (ms[half - 1] + ms[half]) / 2;
}

// what a backfill of `count` partitions costs at that median, as work rather
// than as wall clock: chunks go out one after another and a chunk's own
// partitions run at whatever concurrency the deployment allows, so this is
// the total and not the wait. null in, null out: an estimate with nothing
// behind it is worse than none
export function estimateMs(count: number, median: number | null): number | null {
  return median === null ? null : count * median;
}

export type ChunkStatus = RunStatus | "not launched";

// one launched chunk of a backfill, or the tail that has not gone out yet
export interface Chunk {
  run_id: string | null;
  status: ChunkStatus;
  keys: string[];
}

// a backfill launches its keys in chunks of the asset's build limit, in
// order, one run each, so which run built which key is arithmetic rather
// than a stored fact. the limit is not on the wire, but `launched` over the
// runs it took to launch them is exactly it
export function chunksOf(backfill: Backfill, runs: Run[]): Chunk[] {
  const { partitions, run_ids, launched } = backfill;
  const size = run_ids.length === 0 ? 0 : Math.ceil(launched / run_ids.length);
  const chunks: Chunk[] = run_ids.map((run_id, i) => ({
    run_id,
    status: runs.find((r) => r.id === run_id)?.status ?? "queued",
    keys: partitions.slice(i * size, Math.min((i + 1) * size, launched)),
  }));
  const left = partitions.slice(launched);
  if (left.length > 0) chunks.push({ run_id: null, status: "not launched", keys: left });
  return chunks;
}

// what one partition's cell says: the state of the run that was launched for
// it, or that nothing has been launched for it yet
export function keyStates(chunks: Chunk[]): Map<string, ChunkStatus> {
  const out = new Map<string, ChunkStatus>();
  for (const chunk of chunks) for (const key of chunk.keys) out.set(key, chunk.status);
  return out;
}
