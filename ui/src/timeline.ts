// which rows the timeline draws, and what a group does to them.
//
// a module of its own for the reason `catalog.ts` is one: the outline is a
// decision rather than markup, and the claim worth making about it is one
// nobody can check by looking. **a group's own row draws every run every one
// of its members had**, packed into as many sub-lanes as the overlap needs, so
// a busy group comes out taller rather than quieter, open or shut. that is
// asserted here, where it can be, rather than described in the component where
// it cannot.
import type { Run } from "./types";

// the geometry a row is laid out with. here rather than in the component
// because what a row's height is made of is arithmetic: a group three runs
// deep at once is three sub-lanes tall
export const TOP = 8;
export const BAR_H = 12;
export const LANE_GAP = 3;
export const ROW_PAD = 8;
export const MIN_ROW_H = 34;
// where the plot starts. the names, the blocks and the disclosures are left of
// this line and no bar is ever drawn left of it
export const GUTTER = 168;

// as much of a job as the timeline reads. `JobSummary` satisfies it, so a page
// hands over what it already fetched rather than mapping it into a shape
export interface TimelineJob {
  name: string;
  group: string | null;
  group_hue: number | null;
}

// a group's own row, or one job's. the outline is two deep and never more:
// a job belongs to one group or to none
export type LaneKind = "group" | "job";

// one row of the plot
export interface Lane {
  // what react and the open set go by
  key: string;
  kind: LaneKind;
  // the group this row is inside: the one it stands for on a group row, the
  // one it is a member of on a job row, and null on a job in no group. **a
  // row with a group here is a row that carries a block of that group's hue**,
  // which is what makes the shade a band down the gutter rather than a mark
  // per row
  group: string | null;
  // the angle that group is drawn at, and null wherever the group is
  hue: number | null;
  // every job whose runs are drawn on this row, in the order they arrived:
  // every member on a group row, and the one job on a job row
  jobs: string[];
  // whether this group's job rows are underneath it. always false on a job
  // row, which has nothing to open
  open: boolean;
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

// nothing open, which is the default and what a page offering no outline
// passes
export const NONE: ReadonlySet<string> = new Set<string>();

// what the gutter writes beside a row. a group row says how many jobs it
// stands for: a row aggregating six jobs and a row aggregating one are
// different rows, and a reader cannot see the difference from the bars
export function laneLabel(lane: Lane): string {
  return lane.kind === "group" ? `${lane.group} · ${lane.jobs.length}` : lane.jobs[0];
}

// the rows, top to bottom. a group takes the place its first member would have
// had, so the api's order still shows through, and **the group is a row
// whether it is open or shut**: shut, that row is all there is of the group;
// open, its jobs are rows directly underneath it and the group row goes on
// drawing the aggregate. a run inside an open group is therefore drawn twice,
// once as part of its group's load and once on its own row, which is the whole
// point of opening one.
//
// a job in no group is a row of its own and nothing else, always. **a
// deployment that declares no group gets exactly the rows it always had**:
// there is no grouping to be had, and inventing one out of a naming convention
// would be a guess.
export function lanesOf(jobs: TimelineJob[], open: ReadonlySet<string>): Lane[] {
  // the whole gutter is alphabetical, groups and ungrouped jobs together on
  // one list. a reader scanning for a word finds it where the word falls
  // rather than where the deployment happened to declare it, and a group and
  // a job that is in none are the same kind of thing to scan past
  const members = new Map<string, TimelineJob[]>();
  const loose: TimelineJob[] = [];
  for (const job of jobs) {
    const group = job.group === "" ? null : job.group;
    if (group === null) {
      loose.push(job);
      continue;
    }
    const held = members.get(group);
    if (held) held.push(job);
    else members.set(group, [job]);
  }

  const rows: { name: string; group: string | null }[] = [
    ...[...members.keys()].map((group) => ({ name: group, group })),
    ...loose.map((job) => ({ name: job.name, group: null })),
  ];
  rows.sort((a, b) => byName(a.name, b.name));

  const lanes: Lane[] = [];
  for (const row of rows) {
    if (row.group === null) {
      const job = loose.find((j) => j.name === row.name)!;
      lanes.push({ key: `job:${job.name}`, kind: "job", group: null, hue: null, jobs: [job.name], open: false });
      continue;
    }
    const held = [...(members.get(row.group) ?? [])].sort((a, b) => byName(a.name, b.name));
    // one group, one angle: the server hands the same one to every member, and
    // a member that arrived without one does not get to blank the row
    const hue = held.find((job) => job.group_hue !== null)?.group_hue ?? null;
    const shown = open.has(row.group);
    lanes.push({
      key: `group:${row.group}`,
      kind: "group",
      group: row.group,
      hue,
      jobs: held.map((job) => job.name),
      open: shown,
    });
    if (!shown) continue;
    for (const job of held)
      lanes.push({ key: `job:${job.name}`, kind: "job", group: row.group, hue, jobs: [job.name], open: false });
  }
  return lanes;
}

// two names in the order a reader scans them: case is not a sort key, and a
// tie on the lowercased form falls back to the raw one so the order is total
function byName(a: string, b: string): number {
  const la = a.toLowerCase();
  const lb = b.toLowerCase();
  if (la !== lb) return la < lb ? -1 : 1;
  return a < b ? -1 : a > b ? 1 : 0;
}

// the greedy sub-lane packing, and the reason a group row can be trusted:
// **every bar gets a sub-lane of its own, and two that overlap in time never
// share one.** a group row runs this over every member's runs at once, which
// is the same packing one job's row has always had.
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

// the rows, top to bottom, each as tall as its own packing needs.
//
// **each row gets its own copy of the bars it draws.** a run inside an open
// group is on two rows at once, `pack` writes the sub-lane it chose onto the
// bar, and a shared bar would leave the group row laid out by its members'
// packing instead of its own.
export function rowsOf(lanes: Lane[], byJob: Map<string, Bar[]>): Row[] {
  const rows: Row[] = [];
  let y = TOP;
  for (const lane of lanes) {
    const bars = lane.jobs.flatMap((job) => (byJob.get(job) ?? []).map((bar) => ({ ...bar })));
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
  // this drawing of this run: a run on an open group's row and the same run on
  // its job's row are two entries, and react needs to tell them apart
  key: string;
  bx: number;
  bw: number;
  by: number;
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

// every bar of every row, positioned. **one entry per run per row it is on**:
// the sub-lane a bar was packed into decides its y, so a group row spreads its
// members down the row rather than stacking them onto one line
export function place(rows: Row[], scale: Scale): Placed[] {
  return rows.flatMap((row) => {
    const block = row.laneCount * BAR_H + (row.laneCount - 1) * LANE_GAP;
    const blockTop = row.y + (row.h - block) / 2;
    return row.bars.map((bar) => {
      const bx = Math.max(scale.x(bar.start), scale.gutter);
      const bw = Math.max(scale.minBarW, Math.min(scale.x(bar.end), scale.nowX) - bx);
      const by = blockTop + bar.lane * (BAR_H + LANE_GAP);
      return { ...bar, key: `${row.lane.key}|${bar.run.id}`, bx, bw, by };
    });
  });
}

// what the strip under the plot draws a glyph for. it reads the placed bars
// rather than the runs, so a failure inside a shut group is exactly as loud as
// it was on the row it had before, and **one glyph per failed run**: an open
// group draws its runs twice and a failure counted twice would read as two
// failures.
export function failuresIn(placed: Placed[]): Placed[] {
  const seen = new Set<string>();
  return placed.filter((bar) => {
    if (bar.run.status !== "failed" || seen.has(bar.run.id)) return false;
    seen.add(bar.run.id);
    return true;
  });
}


// which groups are open, as it travels in the url beside the filters already
// there, so an opened view is a link somebody can send. **the parameter holds
// what is open**, because shut is the default: a blank one is nothing open,
// which is what an absent one means too
export function openFrom(raw: string | null): Set<string> {
  return new Set((raw ?? "").split(",").filter(Boolean));
}

export function openParam(open: ReadonlySet<string>): string {
  return [...open].join(",");
}

// one group opened or shut, which is what the disclosure in the gutter does
export function toggleOpen(open: ReadonlySet<string>, group: string): Set<string> {
  const next = new Set(open);
  if (!next.delete(group)) next.add(group);
  return next;
}
