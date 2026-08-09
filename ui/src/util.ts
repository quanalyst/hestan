import type { OpSummary, RunStatus, When } from "./types";

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

// a dag node's muted suffix: an instance count, a trigger rule, or both
export function opBadge(op: OpSummary, count?: string | null): string | undefined {
  return [count, whenLabel(op.when)].filter(Boolean).join(" ") || undefined;
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
