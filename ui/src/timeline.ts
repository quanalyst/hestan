// which rows the timeline draws, and what happens to a row when a group of
// them folds into one.
//
// a module of its own for the reason `catalog.ts` is one: folding is a
// decision rather than markup, and the claim worth making about it is one
// nobody can check by looking. **a folded group draws every run its members
// have**, packed into as many sub-lanes as the overlap needs, so a busy group
// comes out taller rather than quieter. that is asserted here, where it can
// be, rather than described in the component where it cannot.
import type { Run } from "./types";

// the geometry a row is laid out with. here rather than in the component
// because a fold is what changes a row's height: a group three runs deep at
// once is three sub-lanes tall, and that is arithmetic
export const TOP = 8;
export const BAR_H = 12;
export const LANE_GAP = 3;
export const ROW_PAD = 8;
export const MIN_ROW_H = 34;

// as much of a job as the timeline reads. `JobSummary` satisfies it, so a page
// hands over what it already fetched rather than mapping it into a shape
export interface TimelineJob {
  name: string;
  group: string | null;
  group_hue: number | null;
}

// one row of the plot: one job, or one folded group standing for several
export interface Lane {
  // what react and the fold state go by
  key: string;
  // the group this row stands for, and null on a row that is one job
  group: string | null;
  // every job whose runs are drawn on this row, in the order they arrived
  jobs: string[];
  // the angle the row's bars are drawn at, and null on a row that is one job.
  // a hue here means which group, so a row that is not standing for one is
  // drawn in the ink everything else is
  hue: number | null;
}

export interface Bar {
  run: Run;
  start: number;
  end: number;
  // which sub-lane inside the row it was packed into
  lane: number;
}

export interface Row {
  lane: Lane;
  y: number;
  h: number;
  bars: Bar[];
  laneCount: number;
}

// nothing folded, which is what a page that offers no folding passes
export const NONE: ReadonlySet<string> = new Set<string>();

// what the gutter writes beside a row. a folded one says how many jobs it
// swallowed: a row standing for six jobs and a row standing for one are
// different rows, and a reader cannot see the difference from the bars
export function laneLabel(lane: Lane): string {
  return lane.group === null ? lane.jobs[0] : `${lane.group} · ${lane.jobs.length}`;
}

// the rows, in the order the jobs arrived, with each folded group collapsed
// onto the row its first member would have had. that keeps the api's order
// showing through instead of sorting the folded ones somewhere else.
//
// a job in no group never folds. **nothing folds in a deployment that declares
// no group**: there is no grouping to be had, and inventing one out of a
// naming convention would be a guess.
export function lanesOf(jobs: TimelineJob[], folded: ReadonlySet<string>): Lane[] {
  const lanes: Lane[] = [];
  const open = new Map<string, Lane>();
  for (const job of jobs) {
    const group = job.group;
    if (group === null || group === "" || !folded.has(group)) {
      lanes.push({ key: `job:${job.name}`, group: null, jobs: [job.name], hue: null });
      continue;
    }
    const held = open.get(group);
    if (held) {
      held.jobs.push(job.name);
      continue;
    }
    const lane: Lane = { key: `group:${group}`, group, jobs: [job.name], hue: job.group_hue };
    open.set(group, lane);
    lanes.push(lane);
  }
  return lanes;
}

// every group anything on this plot declares, in the order its first member
// appears, for the fold chips. the ungrouped contribute nothing: there is no
// chip for "no group" and no fold behind it either
export function groupsOf(jobs: TimelineJob[]): string[] {
  const seen: string[] = [];
  for (const job of jobs) {
    if (job.group !== null && job.group !== "" && !seen.includes(job.group)) seen.push(job.group);
  }
  return seen;
}

// the greedy sub-lane packing, and the reason a fold can be trusted: **every
// bar gets a sub-lane of its own, and two that overlap in time never share
// one.** a folded group runs this over every member's runs at once, which is
// the same packing one job's row has always had.
//
// returns how many sub-lanes it needed, which is what the row's height is
// made of. a row with nothing on it is still one sub-lane tall.
export function pack(bars: Bar[]): number {
  bars.sort((a, b) => a.start - b.start);
  const ends: number[] = [];
  for (const bar of bars) {
    let lane = ends.findIndex((end) => end <= bar.start);
    if (lane === -1) {
      lane = ends.length;
      ends.push(bar.end);
    } else {
      ends[lane] = bar.end;
    }
    bar.lane = lane;
  }
  return Math.max(1, ends.length);
}

// the rows, top to bottom, each as tall as its own packing needs
export function rowsOf(lanes: Lane[], byJob: Map<string, Bar[]>): Row[] {
  const rows: Row[] = [];
  let y = TOP;
  for (const lane of lanes) {
    const bars = lane.jobs.flatMap((job) => byJob.get(job) ?? []);
    const laneCount = pack(bars);
    const block = laneCount * BAR_H + (laneCount - 1) * LANE_GAP;
    const h = Math.max(MIN_ROW_H, ROW_PAD * 2 + block);
    rows.push({ lane, y, h, bars, laneCount });
    y += h;
  }
  return rows;
}

// where a bar lands on screen. the component draws one rect per entry in here
// and filters nothing out of it, so this list is what the plot shows
export interface Placed extends Bar {
  bx: number;
  bw: number;
  by: number;
  hue: number | null;
}

// the plot's horizontal scale, as much of it as placing a bar needs
export interface Scale {
  // a moment to an x
  x: (t: number) => number;
  // where the now line is: a bar is never drawn to the right of it
  nowX: number;
  // where the plot starts: a bar is never drawn to the left of it
  gutter: number;
  // the narrowest a bar may be, so a run that took a second is still a mark
  minBarW: number;
}

// every bar of every row, positioned. **one entry per run, folded or not**:
// the sub-lane a bar was packed into decides its y, so a folded row spreads
// its members down the row rather than stacking them onto one line
export function place(rows: Row[], scale: Scale): Placed[] {
  return rows.flatMap((row) => {
    const block = row.laneCount * BAR_H + (row.laneCount - 1) * LANE_GAP;
    const blockTop = row.y + (row.h - block) / 2;
    return row.bars.map((bar) => {
      const bx = Math.max(scale.x(bar.start), scale.gutter);
      const bw = Math.max(scale.minBarW, Math.min(scale.x(bar.end), scale.nowX) - bx);
      const by = blockTop + bar.lane * (BAR_H + LANE_GAP);
      return { ...bar, bx, bw, by, hue: row.lane.hue };
    });
  });
}

// what the strip under the plot draws a glyph for. it reads the placed bars
// rather than the runs, so a failure inside a folded group is exactly as loud
// as it was on the row it had before the fold
export function failuresIn(placed: Placed[]): Placed[] {
  return placed.filter((bar) => bar.run.status === "failed");
}

// the hue a bar is actually drawn in: its row's, and **null on a failed one**.
// shape carries state everywhere in this ui, and a hue an outcome could
// overwrite would be a hue meaning two things, so a failure keeps its hatch
// and the group it belongs to is read off the row's label instead
export function hueOf(bar: Placed): number | null {
  return bar.run.status === "failed" ? null : bar.hue;
}

// the fold state as it travels in the url, beside the filters already there,
// so a folded view is a link somebody can send. a blank parameter is nothing
// folded, which is what an absent one means too
export function foldedFrom(raw: string | null): Set<string> {
  return new Set((raw ?? "").split(",").filter(Boolean));
}

export function foldParam(folded: ReadonlySet<string>): string {
  return [...folded].join(",");
}

// one group folded or unfolded, which is what a chip beside the plot does
export function toggleFold(folded: ReadonlySet<string>, group: string): Set<string> {
  const next = new Set(folded);
  if (!next.delete(group)) next.add(group);
  return next;
}
