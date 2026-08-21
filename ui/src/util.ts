import type { Metadata, OpSummary, ReplayPreview, ResumePreview, RunStatus, When } from "./types";

const OUTPUT_CAP = 160;

export function shortId(id: string): string {
  return id.slice(0, 8);
}

// what a run says about the run it came from. a resume continues one from
// where it broke and a replay re-runs ops of one on the inputs it had, which
// are opposite things, so the two are separate fields and this reads whichever
// is set. most runs came from nothing and say nothing
export function lineage(run: {
  resumed_from: string | null;
  replay_of: string | null;
}): { verb: string; id: string } | null {
  if (run.replay_of) return { verb: "replay of", id: run.replay_of };
  if (run.resumed_from) return { verb: "continues", id: run.resumed_from };
  return null;
}

// what a resume would do, on one line
export function resumeLine(p: ResumePreview): string {
  return `${p.rerun.length} to re-run · ${p.reuse.length} reused`;
}

// and a replay: what it executes, and how much of what it reads comes from the
// run being replayed. an op with no deps seeds nothing, and saying "0 inputs"
// there would read as a value that went missing
export function replayLine(p: ReplayPreview): string {
  const ops = `${p.ops.length} to replay`;
  if (p.inputs.length === 0) return ops;
  return `${ops} · ${p.inputs.length} input${p.inputs.length === 1 ? "" : "s"} seeded`;
}

// an asset's permanent address. the name is a path segment per separator, so
// `sales/orders` is a url you can read rather than one escape sequence
export function assetPath(name: string): string {
  return `/assets/${name.split("/").map(encodeURIComponent).join("/")}`;
}

// how a trigger rule reads on a node; the default rule earns no marker,
// because every op used to have it
export function whenLabel(when: When): string | null {
  return when === "always" ? "always" : when === "any_failed" ? "if failed" : null;
}

// an op output on one line. an `$io` handle is a reference to somewhere the
// value actually lives, so it reads as that rather than as pretty-printed json
export function outputLine(output: unknown): string | null {
  if (output === null || output === undefined) return null;
  if (typeof output === "object" && "$io" in (output as object)) {
    const handle = output as Record<string, unknown>;
    const where = typeof handle.path === "string" ? handle.path : JSON.stringify(handle);
    return `${String(handle.$io)} · ${where}`;
  }
  const json = JSON.stringify(output);
  return json.length > OUTPUT_CAP ? json.slice(0, OUTPUT_CAP - 1) + "…" : json;
}

// a dag node's muted suffix: an instance count, a trigger rule, whether the op
// runs in a process of its own, or any combination of them
export function opBadge(op: OpSummary, count?: string | null): string | undefined {
  return (
    [count, whenLabel(op.when), op.isolated ? "isolated" : null].filter(Boolean).join(" ") ||
    undefined
  );
}

// a byte cap the way it was written down, rather than as the number of bytes
export function fmtBytes(bytes: number): string {
  for (const [unit, size] of [
    ["GiB", 1024 ** 3],
    ["MiB", 1024 ** 2],
    ["KiB", 1024],
  ] as const) {
    if (bytes >= size) return `${Math.round(bytes / size)} ${unit}`;
  }
  return `${bytes} B`;
}

// a data volume in decimal units (1.2 GB) which is how storage, warehouses
// and file sizes are quoted. fmtBytes above stays binary because a memory
// rlimit genuinely is, and the two are never showing the same kind of number.
// signed, since a byte delta comes through here too
export function fmtDataSize(bytes: number): string {
  for (const [unit, size] of [
    ["TB", 1e12],
    ["GB", 1e9],
    ["MB", 1e6],
    ["kB", 1e3],
  ] as const) {
    if (Math.abs(bytes) >= size) {
      const n = bytes / size;
      return `${Math.abs(n) < 10 ? n.toFixed(1) : Math.round(n)} ${unit}`;
    }
  }
  return `${Math.round(bytes)} B`;
}

// the five tags the api computes deltas and trends over: everything else is
// not a number and has neither
const NUMERIC_META = ["int", "float", "bytes", "duration_secs", "count"] as const;

export function numericMetaKeys(metadata: Metadata | null): string[] {
  if (!metadata) return [];
  return Object.entries(metadata)
    .filter(([, value]) => NUMERIC_META.some((tag) => tag in value))
    .map(([name]) => name);
}

// an interval the way it was declared: `6h`, not `6h 0m`. for a window or a
// period somebody wrote down, where fmtDuration is for a span that was measured
export function fmtEvery(secs: number): string {
  if (secs >= 86400 && secs % 86400 === 0) return `${secs / 86400}d`;
  if (secs >= 3600 && secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs >= 60 && secs % 60 === 0) return `${secs / 60}m`;
  return `${secs}s`;
}

export function isTerminal(status: RunStatus): boolean {
  return status === "success" || status === "failed" || status === "canceled";
}

export function relTime(iso: string | null): string {
  if (!iso) return "none";
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

export function untilTime(iso: string): string {
  const s = Math.max(0, (new Date(iso).getTime() - Date.now()) / 1000);
  if (s < 60) return `in ${Math.floor(s)}s`;
  if (s < 3600) return `in ${Math.floor(s / 60)}m`;
  if (s < 86400) return `in ${Math.floor(s / 3600)}h`;
  return `in ${Math.floor(s / 86400)}d`;
}

export function durationMs(run: { started_at: string | null; finished_at: string | null }): number | null {
  if (!run.started_at || !run.finished_at) return null;
  return new Date(run.finished_at).getTime() - new Date(run.started_at).getTime();
}

export function fmtDuration(ms: number | null): string {
  if (ms === null) return "none";
  ms = Math.max(0, ms); // clock skew can put finished_at before started_at
  if (ms < 999.5) return `${Math.round(ms)}ms`; // 999.5+ rounds to "1.0s", not "1000ms"
  if (ms < 60_000) {
    const s = (ms / 1000).toFixed(1);
    if (s !== "60.0") return `${s}s`;
  }
  const total = Math.round(ms / 1000);
  if (total >= 3600) return `${Math.floor(total / 3600)}h ${Math.floor((total % 3600) / 60)}m`;
  return `${Math.floor(total / 60)}m ${total % 60}s`;
}

// how a declared rate reads: "5 per second", "100 per 5m". the period is a
// round number somebody typed, so it is written the way they would say it
// rather than as a duration to be read off a stopwatch
export function fmtPeriod(secs: number): string {
  if (secs === 1) return "second";
  if (secs === 60) return "minute";
  if (secs === 3600) return "hour";
  if (secs < 1) return `${Math.round(secs * 1000)}ms`;
  if (secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs % 60 === 0) return `${secs / 60}m`;
  return `${secs}s`;
}

export function fmtRate(limit: number, perSecs: number): string {
  return `${limit} per ${fmtPeriod(perSecs)}`;
}

// a series timestamp, in utc exactly as it was stored. everything else in
// this ui prints times in the reader's zone; a series is the exception on
// purpose, because its points are indexed by the clock the data was written
// against and an axis has to agree with the numbers under it
export function stamp(iso: string, seconds = false): string {
  return iso.slice(0, seconds ? 19 : 16).replace("T", " ");
}

export function clockTime(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour12: false });
}
