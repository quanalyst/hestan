import type { Metadata, OpSummary, RunStatus, When } from "./types";

const OUTPUT_CAP = 160;

export function shortId(id: string): string {
  return id.slice(0, 8);
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

// a data volume in decimal units — 1.2 GB — which is how storage, warehouses
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

export function isTerminal(status: RunStatus): boolean {
  return status === "success" || status === "failed" || status === "canceled";
}

export function relTime(iso: string | null): string {
  if (!iso) return "—";
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
  if (ms === null) return "—";
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

export function clockTime(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour12: false });
}
