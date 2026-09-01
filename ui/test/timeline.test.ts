// what the timeline draws a row for, and what opening a group does to those
// rows.
//
// the claims worth asserting here are the ones nobody can check by looking.
// **a group's row draws every run its members have, open or shut.** a group
// row that drew the first run and hid the rest would make a busy period look
// quiet, which is a worse lie than showing no group at all, and it would look
// correct on a screen. **and an open group draws its runs twice on purpose**,
// once as the group's load and once on the job's own row, which only holds if
// the two rows are packed independently of each other.
import assert from "node:assert/strict";
import test from "node:test";
import {
  BAR_H,
  LANE_GAP,
  MIN_ROW_H,
  ROW_PAD,
  failuresIn,
  hueOf,
  laneLabel,
  lanesOf,
  openFrom,
  openParam,
  place,
  rowsOf,
  toggleOpen,
} from "../src/timeline";
import type { Bar, Row, Scale, TimelineJob } from "../src/timeline";
import type { Run, RunStatus } from "../src/types";

const job = (name: string, group: string | null = null, group_hue: number | null = null): TimelineJob => ({
  name,
  group,
  group_hue,
});

// a bar as the plot builds one: minutes from an arbitrary zero, so an overlap
// is readable in the fixture rather than in a timestamp
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const min = (n: number) => T0 + n * 60_000;

let seq = 0;
const bar = (job: string, from: number, to: number, status: RunStatus = "success"): Bar => {
  seq += 1;
  return {
    run: { id: `r${seq}`, job, status } as Run,
    start: min(from),
    end: min(to),
    lane: 0,
  };
};

// runs grouped the way the component groups them before laying rows out
const byJob = (bars: Bar[]): Map<string, Bar[]> => {
  const held = new Map<string, Bar[]>();
  for (const b of bars) {
    const list = held.get(b.run.job);
    if (list) list.push(b);
    else held.set(b.run.job, [b]);
  }
  return held;
};

const ids = (bars: { run: Run }[]): string[] => bars.map((b) => b.run.id).sort();
const labels = (rows: Row[]): string[] => rows.map((r) => laneLabel(r.lane));

// the plot's scale, at a width wide enough that nothing is clamped: one x per
// minute, with the now line past the last run. this is what the component
// hands `place`, so what comes back is one entry per rect it draws
const SCALE: Scale = {
  x: (t) => 168 + (t - min(0)) / 60_000,
  nowX: 168 + 200,
  gutter: 168,
  minBarW: 4,
};

const drawn = (rows: Row[]) => place(rows, SCALE);

// three jobs in one group, one outside it. `pull` and `clean` overlap each
// other and `derive` sits inside `pull` as well, so the group's own row has to
// find three sub-lanes at once and not one
const WEATHER: TimelineJob[] = [
  job("weather_pull", "weather", 200),
  job("weather_clean", "weather", 200),
  job("weather_derive", "weather", 200),
  job("billing", null, null),
];

const RUNS = [
  bar("weather_pull", 0, 30),
  bar("weather_pull", 40, 70),
  bar("weather_clean", 10, 50, "failed"),
  bar("weather_derive", 20, 25),
  bar("weather_derive", 55, 80),
  bar("billing", 0, 5),
];

const SHUT = new Set<string>();
const OPEN = new Set(["weather"]);

test("a shut group is drawn a bar per member run, not a bar for the first member", () => {
  const rows = rowsOf(lanesOf(WEATHER, SHUT), byJob(RUNS));
  // shut is the default, and a deployment with groups opens on one row per
  // group rather than one per job
  assert.deepEqual(labels(rows), ["weather · 3", "billing"]);

  // the whole of it: the group is a row rather than a row per member, and
  // nothing at all changed about how many bars get drawn. `place` is the last
  // thing between a run and a rect, and it filters nothing
  assert.equal(drawn(rows).length, RUNS.length);
  assert.deepEqual(ids(drawn(rows)), ids(RUNS));

  const weather = rows[0];
  assert.equal(weather.bars.length, 5);
  assert.deepEqual(weather.lane.jobs, ["weather_pull", "weather_clean", "weather_derive"]);
  assert.equal(weather.lane.open, false);

  // and the density is in the height rather than lost: at minute 20 three runs
  // are live at once, so the row is three sub-lanes tall
  assert.equal(weather.laneCount, 3);
  assert.deepEqual([...new Set(weather.bars.map((b) => b.lane))].sort(), [0, 1, 2]);
  assert.ok(
    weather.h > MIN_ROW_H,
    `a three-deep row is taller than the minimum: ${weather.h} vs ${MIN_ROW_H}`,
  );
  assert.equal(weather.h, ROW_PAD * 2 + 3 * BAR_H + 2 * LANE_GAP);

  // two bars that overlap never share a sub-lane, which is what keeps the
  // count a count of runs rather than of whatever survived a collision
  for (const a of weather.bars) {
    for (const b of weather.bars) {
      if (a === b || a.lane !== b.lane) continue;
      assert.ok(a.end <= b.start || b.end <= a.start, `${a.run.id} and ${b.run.id} overlap in one sub-lane`);
    }
  }

  // three distinct rows of pixels on the group's row, and the group's hue on
  // every bar of it. a row that stacked them would have one
  const ys = new Set(drawn(rows).filter((b) => b.hue !== null).map((b) => b.by));
  assert.equal(ys.size, 3);
  assert.deepEqual([...new Set(drawn(rows).map((b) => b.hue))].sort(), [200, null]);
});

test("an open group draws its aggregate and its job rows under it", () => {
  const rows = rowsOf(lanesOf(WEATHER, OPEN), byJob(RUNS));
  // the group keeps its row and the jobs are added beneath it, in the order
  // they were listed, with the ungrouped job left where it was
  assert.deepEqual(labels(rows), [
    "weather · 3",
    "weather_pull",
    "weather_clean",
    "weather_derive",
    "billing",
  ]);
  assert.deepEqual(
    rows.map((r) => r.lane.kind),
    ["group", "job", "job", "job", "job"],
  );
  assert.equal(rows[0].lane.open, true);

  // **every run of the group is drawn twice**: once on the group's row and
  // once on its job's. that is what opening one is for, and it is the thing a
  // reader would otherwise have to take on trust
  const weatherRuns = RUNS.filter((b) => b.run.job !== "billing");
  assert.equal(drawn(rows).length, RUNS.length + weatherRuns.length);
  const twice = drawn(rows).filter((b) => b.run.id === weatherRuns[0].run.id);
  assert.equal(twice.length, 2);
  // and the two drawings of one run are told apart by their row, so nothing
  // downstream has to guess which is which
  assert.deepEqual(
    twice.map((b) => b.key).sort(),
    [`group:weather|${weatherRuns[0].run.id}`, `job:weather_pull|${weatherRuns[0].run.id}`],
  );

  // the group's row is still packed as the group: three sub-lanes, three rows
  // of pixels. the job rows pack themselves, and a bar shared between the two
  // would leave the group laid out by whichever row packed it last
  const group = rows[0];
  assert.equal(group.laneCount, 3);
  const groupBars = drawn(rows).filter((b) => b.key.startsWith("group:"));
  assert.equal(groupBars.length, 5);
  assert.equal(new Set(groupBars.map((b) => b.by)).size, 3);

  // while a job's own row is as tall as that one job needs: two runs that do
  // not overlap are one sub-lane
  assert.deepEqual(
    rows.slice(1).map((r) => r.laneCount),
    [1, 1, 1, 1],
  );

  // every row inside the group carries the group's hue, which is what the
  // gutter draws the block from and what the bars are drawn in
  assert.deepEqual(
    rows.map((r) => r.lane.hue),
    [200, 200, 200, 200, null],
  );
  assert.deepEqual(
    rows.map((r) => r.lane.group),
    ["weather", "weather", "weather", "weather", null],
  );
});

test("opening a group and shutting it again gives back the rows it started with", () => {
  const before = rowsOf(lanesOf(WEATHER, SHUT), byJob(RUNS));
  const opened = toggleOpen(SHUT, "weather");
  assert.deepEqual([...opened], ["weather"]);
  const after = rowsOf(lanesOf(WEATHER, toggleOpen(opened, "weather")), byJob(RUNS));
  assert.deepEqual(
    after.map((r) => [r.lane.key, r.y, r.h, r.laneCount]),
    before.map((r) => [r.lane.key, r.y, r.h, r.laneCount]),
  );
  assert.deepEqual(ids(drawn(after)), ids(drawn(before)));
});

test("a deployment that declares no group has one row per job and no block on any of them", () => {
  const plain = [job("etl"), job("billing"), job("cron_sweep")];

  // shut, opened, and even asked to open something nothing is in: there is no
  // grouping to be had, so there is nothing here that any of those can change
  for (const open of [SHUT, OPEN, new Set(["etl"])]) {
    const rows = rowsOf(lanesOf(plain, open), byJob([bar("etl", 0, 10)]));
    assert.deepEqual(labels(rows), ["etl", "billing", "cron_sweep"]);
    // a row is a job's, so there is no disclosure to draw
    assert.deepEqual(
      rows.map((r) => r.lane.kind),
      ["job", "job", "job"],
    );
    // and no group on any of them, which is what the gutter draws a block from
    assert.deepEqual(
      rows.map((r) => r.lane.group),
      [null, null, null],
    );
    assert.deepEqual(
      rows.map((r) => r.lane.hue),
      [null, null, null],
    );
    // the rows still stack from the top in the order they were listed
    assert.deepEqual(
      rows.map((r) => r.y),
      [8, 8 + MIN_ROW_H, 8 + 2 * MIN_ROW_H],
    );
  }
});

test("a job in no group gets no block in a deployment that has one", () => {
  const rows = rowsOf(lanesOf(WEATHER, OPEN), byJob(RUNS));
  const billing = rows[rows.length - 1];
  assert.equal(laneLabel(billing.lane), "billing");
  assert.equal(billing.lane.kind, "job");
  assert.equal(billing.lane.group, null, "no group, so no block behind the name");
  assert.equal(billing.lane.hue, null);
  // and its bars are drawn in the ink everything else is
  assert.deepEqual(
    drawn(rows)
      .filter((b) => b.run.job === "billing")
      .map((b) => b.hue),
    [null],
  );
});

test("a group is a row where its first member was, and gathers the members listed apart from it", () => {
  const mixed = [job("a", "one", 10), job("b", null), job("c", "one", 10), job("d", "two", 20)];
  const shut = rowsOf(lanesOf(mixed, SHUT), new Map());
  // `one` stands where `a` was, `c` is gathered onto it from further down, and
  // `b` keeps the place it had between them
  assert.deepEqual(labels(shut), ["one · 2", "b", "two · 1"]);
  assert.equal(shut[1].lane.group, null);

  // and opening one group leaves the other where it was
  const open = rowsOf(lanesOf(mixed, new Set(["one"])), new Map());
  assert.deepEqual(labels(open), ["one · 2", "a", "c", "b", "two · 1"]);
  assert.deepEqual(
    open.map((r) => r.lane.hue),
    [10, 10, 10, null, 20],
  );
});

test("a failed run inside a shut group still reaches the strip under the plot", () => {
  const shut = rowsOf(lanesOf(WEATHER, SHUT), byJob(RUNS));
  // `failuresIn` is what the strip maps over, so a failure that comes back
  // from it is a failure that gets its glyph, shut or open
  assert.deepEqual(
    failuresIn(drawn(shut)).map((b) => b.run.job),
    ["weather_clean"],
  );

  // and opening the group does not turn one failure into two: the run is
  // drawn on two rows, and two × in the strip would read as two failures
  const open = rowsOf(lanesOf(WEATHER, OPEN), byJob(RUNS));
  assert.deepEqual(ids(failuresIn(drawn(open))), ids(failuresIn(drawn(shut))));
  assert.equal(failuresIn(drawn(open)).length, 1);

  // and it is drawn in no hue, so the hatch its status has is what it wears:
  // shape carries state here, and a colour where an outcome already is would
  // be a colour meaning two things
  const bars = drawn(shut);
  assert.equal(hueOf(failuresIn(bars)[0]), null);
  // while everything else on the same row is the group's colour
  const rest = bars.filter((b) => b.hue === 200 && b.run.status !== "failed");
  assert.equal(rest.length, 4);
  assert.deepEqual([...new Set(rest.map(hueOf))], [200]);
});

test("the open set round-trips through the url", () => {
  assert.deepEqual([...openFrom(null)], [], "an absent parameter is nothing open");
  assert.deepEqual([...openFrom("")], []);
  assert.deepEqual([...openFrom("weather,eia")], ["weather", "eia"]);
  assert.equal(openParam(openFrom("weather,eia")), "weather,eia");
  // a parameter somebody hand-edited into commas and blanks is the groups it
  // names, rather than a group called ""
  assert.deepEqual([...openFrom(",weather,,")], ["weather"]);

  // and a disclosure turns one around without disturbing the rest
  const one = toggleOpen(openFrom("weather"), "eia");
  assert.equal(openParam(one), "weather,eia");
  assert.equal(openParam(toggleOpen(one, "weather")), "eia");
  assert.equal(openParam(toggleOpen(new Set(["eia"]), "eia")), "");

  // the link is the whole of the state: opening from a url gives the rows
  // opening by hand gives
  assert.deepEqual(
    labels(rowsOf(lanesOf(WEATHER, openFrom("weather")), byJob(RUNS))),
    labels(rowsOf(lanesOf(WEATHER, OPEN), byJob(RUNS))),
  );
});
