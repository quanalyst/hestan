// the activity feed is a page of the log plus whatever the stream has pushed
// since. these are the decisions in that (what a row is about, whether a
// filter admits it, and how a live event joins a list that is already there)
// and they are pure so they can be tested without a browser or a server.
import type { EventKind, EventLevel, Health, RunEvent, SubjectKind } from "./types";

// how many rows the feed holds. the api pages at 100; live events push onto
// the front, and past this the oldest fall off the bottom rather than the tab
// growing without bound for as long as it is left open
export const FEED_CAP = 500;

// a gap the stream reported: it dropped events because this consumer was not
// reading fast enough. rendered in the feed where the events would have been,
// because a gap nobody can see is worse than no gap at all
export interface Dropped {
  count: number;
  through: number;
}

export type FeedRow = { kind: "event"; event: RunEvent } | { kind: "gap"; gap: Dropped };

// what an event is about, as one string. a run event carries the run in
// `run_id` and leaves `subject` null (the v17 migration says why) so this is
// where the two become one answer, exactly as `Event::about` does in rust
export function subjectOf(e: RunEvent): string | null {
  return e.subject ?? e.run_id;
}

// where clicking a row goes, or null when the subject has no page of its own.
// a system event is about a notification and a job event about retention;
// neither has anywhere better to be than where it already is
export function linkFor(e: RunEvent): string | null {
  const subject = subjectOf(e);
  if (subject === null) return null;
  switch (e.subject_kind) {
    case "run":
      return `/runs/${subject}`;
    case "asset":
      return `/assets/${subject.split("/").map(encodeURIComponent).join("/")}`;
    case "job":
    case "schedule":
      return `/jobs/${encodeURIComponent(subject)}`;
    case "backfill":
      return `/backfills/${subject}`;
    default:
      return null;
  }
}

export interface Filters {
  subjectKind: "all" | SubjectKind;
  level: "all" | EventLevel;
  // a substring matched against the message, the kind and the subject: one box
  // rather than three, because at 3am you type a name and not a field
  find: string;
}

export function matches(e: RunEvent, f: Filters): boolean {
  if (f.subjectKind !== "all" && e.subject_kind !== f.subjectKind) return false;
  if (f.level !== "all" && e.level !== f.level) return false;
  const needle = f.find.trim().toLowerCase();
  if (needle === "") return true;
  const hay = [e.message, e.kind, subjectOf(e) ?? "", e.op ?? ""].join(" ").toLowerCase();
  return hay.includes(needle);
}

// merge what the stream pushed into what the page already has.
//
// newest first, deduplicated on seq, capped. the dedupe is not paranoia: a
// resumed stream delivers the gap from its cursor, and a page that had already
// polled part of that gap would otherwise show it twice.
export function merge(feed: FeedRow[], incoming: FeedRow[], cap = FEED_CAP): FeedRow[] {
  const seen = new Set<number>();
  const rows: FeedRow[] = [];
  for (const row of [...incoming, ...feed]) {
    if (row.kind === "event") {
      if (seen.has(row.event.seq)) continue;
      seen.add(row.event.seq);
    }
    rows.push(row);
  }
  rows.sort((a, b) => order(b) - order(a));
  return rows.slice(0, cap);
}

// a gap sorts at the seq it ran through, which is where the events it stands
// for would have been
function order(row: FeedRow): number {
  return row.kind === "event" ? row.event.seq : row.gap.through;
}

// the words a kind reads as in the margin. the stored word with its subject
// prefix dropped, since the subject is already in the row: `asset_materialized`
// beside `sales/orders` says "asset" twice
export function kindLabel(kind: EventKind): string {
  const prefixes = ["run_", "op_", "schedule_", "backfill_", "notification_", "check_"];
  const prefix = prefixes.find((p) => kind.startsWith(p));
  const rest = prefix ? kind.slice(prefix.length) : kind;
  return rest.replace(/_/g, " ");
}

// what to say about who is deciding, in one line, or null when there is
// nothing worth a line.
//
// said on this page and nowhere else: this is the log of what the *deployment*
// did, so it is where somebody asking "why has nothing fired" already is, and
// saying it on every page would be a fact about the deployment repeated at
// somebody reading about one run.
export function decidingLine(health: Health): string | null {
  const d = health.deciding;
  if (d === null) return null;
  if (d.leader) return `this process (${health.instance}) is deciding, on term ${d.term}`;
  if (d.holder === null) {
    return "nothing is deciding: no schedule is being fired and no sensor evaluated";
  }
  if (!d.decides) {
    return `${d.holder} is deciding; this process (${health.instance}) is a worker and decides nothing`;
  }
  return `${d.holder} is deciding; this process (${health.instance}) is standing by`;
}
