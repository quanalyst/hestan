// what the timeline draws a row for, and what a fold does to it.
//
// the one claim worth asserting here is the one nobody can check by looking:
// **a folded group keeps every run its members have.** a fold that drew the
// first run and hid the rest would make a busy period look quiet, which is a
// worse lie than not folding at all, and it would look correct on a screen.
import assert from "node:assert/strict";
import test from "node:test";
import {
  BAR_H,
  LANE_GAP,
  MIN_ROW_H,
  ROW_PAD,
  failuresIn,
  foldParam,
  foldedFrom,
  groupsOf,
  hueOf,
  laneLabel,
  lanesOf,
  place,
  rowsOf,
  toggleFold,
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

const ids = (bars: Bar[]): string[] => bars.map((b) => b.run.id).sort();

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
// other and `derive` sits inside `pull` as well, so folding the three has to
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

test("a folded group is drawn a bar per member run, not a bar for the first member", () => {
  const open = rowsOf(lanesOf(WEATHER, new Set()), byJob(RUNS));
  assert.equal(open.length, 4, "one row per job while nothing is folded");

  const shut = rowsOf(lanesOf(WEATHER, new Set(["weather"])), byJob(RUNS));
  assert.equal(shut.length, 2, "the three grouped rows became one, billing kept its own");

  // the whole of it: the fold changed how many rows there are and nothing at
  // all about how many bars get drawn on them. `place` is the last thing
  // between a run and a rect, and it filters nothing
  assert.equal(drawn(shut).length, RUNS.length);
  assert.deepEqual(ids(drawn(shut)), ids(drawn(open)));

  const weather = shut[0];
  assert.equal(weather.bars.length, 5);

  // and the density is in the height rather than lost: at minute 20 three
  // runs are live at once, so the row is three sub-lanes tall
  assert.equal(weather.laneCount, 3);
  assert.deepEqual([...new Set(weather.bars.map((b) => b.lane))].sort(), [0, 1, 2]);
  assert.ok(
    weather.h > MIN_ROW_H,
    `a three-deep row is taller than the minimum: ${weather.h} vs ${MIN_ROW_H}`,
  );
  assert.equal(weather.h, ROW_PAD * 2 + 3 * BAR_H + 2 * LANE_GAP);

  // two bars that overlap never share a sub-lane, which is what keeps the
  // count above a count of runs rather than of whatever survived a collision
  for (const a of weather.bars) {
    for (const b of weather.bars) {
      if (a === b || a.lane !== b.lane) continue;
      assert.ok(a.end <= b.start || b.end <= a.start, `${a.run.id} and ${b.run.id} overlap in one sub-lane`);
    }
  }

  // three distinct rows of pixels on the folded row, and the group's hue on
  // every bar of it. a fold that stacked them would have one
  const ys = new Set(drawn(shut).filter((b) => b.hue !== null).map((b) => b.by));
  assert.equal(ys.size, 3);
  assert.deepEqual([...new Set(drawn(shut).map((b) => b.hue))].sort(), [200, null]);
});

test("a failed run inside a folded group still reaches the strip under the plot", () => {
  const shut = rowsOf(lanesOf(WEATHER, new Set(["weather"])), byJob(RUNS));
  // `failuresIn` is what the strip maps over, so a failure that comes back
  // from it is a failure that gets its glyph, folded or not
  assert.deepEqual(
    failuresIn(drawn(shut)).map((b) => b.run.job),
    ["weather_clean"],
  );
  const open = rowsOf(lanesOf(WEATHER, new Set()), byJob(RUNS));
  assert.deepEqual(ids(failuresIn(drawn(shut))), ids(failuresIn(drawn(open))));

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

test("a folded row says which group it is and how many jobs it swallowed", () => {
  const [weather, billing] = rowsOf(lanesOf(WEATHER, new Set(["weather"])), byJob(RUNS));
  assert.equal(laneLabel(weather.lane), "weather · 3");
  assert.deepEqual(weather.lane.jobs, ["weather_pull", "weather_clean", "weather_derive"]);
  assert.equal(weather.lane.hue, 200);
  // and an unfolded row is a job's name and is drawn in no hue at all: a
  // colour here means which group, and a row that is one job is not one
  assert.equal(laneLabel(billing.lane), "billing");
  assert.equal(billing.lane.hue, null);
});

test("a deployment that declares no group has one row per job and nothing to fold", () => {
  const plain = [job("etl"), job("billing"), job("cron_sweep")];
  assert.deepEqual(groupsOf(plain), [], "no chips, so no row of chips");

  // folded or not, and even asked to fold something nothing is in
  for (const folded of [new Set<string>(), new Set(["weather"])]) {
    const rows = rowsOf(lanesOf(plain, folded), byJob([bar("etl", 0, 10)]));
    assert.deepEqual(
      rows.map((r) => laneLabel(r.lane)),
      ["etl", "billing", "cron_sweep"],
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

test("the fold state round-trips through the url", () => {
  assert.deepEqual([...foldedFrom(null)], []);
  assert.deepEqual([...foldedFrom("")], []);
  assert.deepEqual([...foldedFrom("weather,eia")], ["weather", "eia"]);
  assert.equal(foldParam(foldedFrom("weather,eia")), "weather,eia");
  // a parameter somebody hand-edited into commas and blanks is the groups it
  // names, rather than a group called ""
  assert.deepEqual([...foldedFrom(",weather,,")], ["weather"]);

  // and a chip turns one around without disturbing the rest
  const one = toggleFold(foldedFrom("weather"), "eia");
  assert.equal(foldParam(one), "weather,eia");
  assert.equal(foldParam(toggleFold(one, "weather")), "eia");
  assert.equal(foldParam(toggleFold(new Set(["eia"]), "eia")), "");

  // the link is the whole of the state: folding from a url gives the rows
  // folding by hand gives
  const fromLink = rowsOf(lanesOf(WEATHER, foldedFrom("weather")), byJob(RUNS));
  const byHand = rowsOf(lanesOf(WEATHER, new Set(["weather"])), byJob(RUNS));
  assert.deepEqual(
    fromLink.map((r) => laneLabel(r.lane)),
    byHand.map((r) => laneLabel(r.lane)),
  );
});

test("a group is folded where its first member was, and the chips list it once", () => {
  const mixed = [job("a", "one", 10), job("b", null), job("c", "one", 10), job("d", "two", 20)];
  assert.deepEqual(groupsOf(mixed), ["one", "two"]);
  const rows = rowsOf(lanesOf(mixed, new Set(["one"])), new Map());
  // `one` folds onto the row `a` would have had; `d` is in a group nobody
  // folded, so it is still its own row under its own name
  assert.deepEqual(
    rows.map((r) => laneLabel(r.lane)),
    ["one · 2", "b", "d"],
  );
  // a job in no group is never folded into anything, whatever is folded
  assert.equal(rows[1].lane.group, null);
  assert.equal(rows[2].lane.hue, null, "an unfolded row carries no hue");
});
