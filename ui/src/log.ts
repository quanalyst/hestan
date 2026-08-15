// the run page's log pane draws two sources: the run's own events, which are
// hestan narrating, and what the ops printed. this turns either or both into
// one list of rows, filtered and interleaved.
//
// a module of its own because it is the only part of the pane that is a
// decision rather than markup, and the only part worth a test.
import type { EventLevel, OpLog, RunEvent } from "./types";

// the marker hestan writes when it stops capturing: a row with that target and
// no stream is hestan speaking, not the op, and the pane says so.
export const HESTAN = "hestan";

export type Source = "events" | "output" | "both";

export interface LogRow {
  key: string;
  ts: string;
  op: string | null;
  // null on a line from a pipe, which has no levels at all
  level: EventLevel | null;
  message: string;
  // what the row says about itself in the margin: an event kind, a stream, or
  // a captured event's level
  tag: string | null;
  source: "event" | "output";
  // an op's second attempt onwards; 1 is not worth the ink
  attempt: number | null;
  // hestan speaking about the capture rather than the op speaking
  marker: boolean;
}

export interface Filters {
  source: Source;
  // the existing event filter: every event, or only what an op said
  kind: "all" | "logs";
  level: "all" | EventLevel;
  op: string | null;
  // a substring to find in what was printed; "" is not a search
  find: string;
  // narrow to the lines that match, rather than marking them where they are
  only: boolean;
}

// one piece of a message: matched, or the text around it. always at least one
// piece, so a message with no match renders through the same path as one with
export interface Piece {
  text: string;
  hit: boolean;
}

// a message split around every occurrence of `find`, case-insensitively.
// pieces rather than html: nothing in this ui builds an element from a string
export function marks(message: string, find: string): Piece[] {
  const needle = find.toLowerCase();
  if (needle === "") return [{ text: message, hit: false }];
  const pieces: Piece[] = [];
  const hay = message.toLowerCase();
  let at = 0;
  for (let cut = hay.indexOf(needle, at); cut >= 0; cut = hay.indexOf(needle, at)) {
    if (cut > at) pieces.push({ text: message.slice(at, cut), hit: false });
    pieces.push({ text: message.slice(cut, cut + needle.length), hit: true });
    at = cut + needle.length;
  }
  if (at < message.length) pieces.push({ text: message.slice(at), hit: false });
  return pieces;
}

export const hits = (row: LogRow, find: string) =>
  find !== "" && row.message.toLowerCase().includes(find.toLowerCase());

function fromEvent(e: RunEvent): LogRow {
  return {
    key: `e${e.seq}`,
    ts: e.ts,
    op: e.op,
    level: e.level,
    message: e.message,
    tag: e.kind === "log" ? null : e.kind,
    source: "event",
    attempt: null,
    marker: false,
  };
}

function fromOutput(l: OpLog): LogRow {
  return {
    key: `o${l.id}`,
    ts: l.at,
    op: l.op,
    level: l.level,
    message: l.message,
    tag: l.stream ?? l.level,
    source: "output",
    attempt: l.attempt > 1 ? l.attempt : null,
    marker: l.stream === null && l.target === HESTAN,
  };
}

// what the pane shows, oldest first.
//
// interleaved by timestamp, with ties left in source order: both lists arrive
// in the order they were written, so a stable sort keeps each source's own
// order exactly however coarse the clock is.
export function logRows(events: RunEvent[], output: OpLog[], f: Filters): LogRow[] {
  const rows: LogRow[] = [];
  if (f.source !== "output")
    for (const e of events) if (f.kind === "all" || e.kind === "log") rows.push(fromEvent(e));
  if (f.source !== "events") for (const l of output) rows.push(fromOutput(l));
  return rows
    .filter((r) => f.op === null || r.op === f.op)
    // a line off a pipe has no level, so a level filter hides it rather than
    // guessing one: stderr is where plenty of programs write their progress
    .filter((r) => f.level === "all" || r.level === f.level)
    // a search marks where it hit by default; narrowing to the hits is a
    // second decision, because the line above a match is often the point
    .filter((r) => !f.only || f.find === "" || hits(r, f.find))
    .sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0));
}
