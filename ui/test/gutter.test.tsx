// what the timeline's gutter actually draws, in markup rather than in a claim
// about it.
//
// two things are worth asserting at this level. **a deployment that declares
// no group comes out of here with nothing new on it**: no block, no
// disclosure, one name per job, which is the promise the outline was added
// under. and **every colour in the gutter arrives as an angle from `at`**,
// which is the only thing in this ui that emits one, so the block cannot
// quietly become a second palette.
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { at } from "../src/Swatch";
import TimelineGutter, { roomFor, truncate } from "../src/TimelineGutter";
import { lanesOf, rowsOf } from "../src/timeline";
import type { Row, TimelineJob } from "../src/timeline";

const job = (name: string, group: string | null = null, group_hue: number | null = null): TimelineJob => ({
  name,
  group,
  group_hue,
});

const WEATHER: TimelineJob[] = [
  job("weather_pull", "weather", 200),
  job("weather_clean", "weather", 200),
  job("billing", null, null),
];

const rows = (jobs: TimelineJob[], open: string[]): Row[] =>
  rowsOf(lanesOf(jobs, new Set(open)), new Map());

const draw = (jobs: TimelineJob[], open: string[], onToggle = () => {}) =>
  renderToStaticMarkup(
    <svg>
      <TimelineGutter rows={rows(jobs, open)} onToggle={onToggle} />
    </svg>,
  );

// every `--h` in the markup, in the order it was drawn
const angles = (markup: string) => [...markup.matchAll(/--h:\s*(\d+)/g)].map((m) => Number(m[1]));
// the angle `at` hands an element, as the style it sets it in
const angleOf = (hue: number) => (at(hue) as Record<string, string>)["--h"];
const count = (markup: string, needle: RegExp) => (markup.match(needle) ?? []).length;

test("a deployment that declares no group gets a gutter of names and nothing else", () => {
  const markup = draw([job("etl"), job("billing")], []);
  assert.deepEqual(angles(markup), [], "no group, so no hue anywhere in the gutter");
  assert.equal(count(markup, /tl-block/g), 0, "no block behind a name that is in no group");
  assert.equal(count(markup, /<button/g), 0, "nothing to open, so no disclosure");
  assert.ok(markup.includes(">etl<"), markup);
  assert.ok(markup.includes(">billing<"), markup);
  // and both names end where they always did, against the bars they belong to
  assert.equal(count(markup, /x="158"/g), 2);
});

test("a group carries a block of its own hue, and the hue comes from at()", () => {
  const shut = draw(WEATHER, []);
  // one row in the group while it is shut, so one block
  assert.deepEqual(angles(shut), [200]);
  assert.equal(count(shut, /tl-block/g), 1);

  const open = draw(WEATHER, ["weather"]);
  // the group's row and both of its jobs' rows, all the same angle: the band
  // is continuous down the gutter rather than a mark on the group alone
  assert.deepEqual(angles(open), [200, 200, 200]);
  assert.equal(count(open, /tl-block/g), 3);

  // the angle is `at`'s and only `at`'s. nothing here writes a colour of its
  // own: the block is a var(--h) rule in the stylesheet, and this is the
  // element that hands it the angle
  assert.equal(angleOf(200), "200");
  assert.ok(open.includes(`--h:${angleOf(200)}`), open);
  assert.equal(count(open, /hsl\(|rgb\(|#[0-9a-f]{3}/gi), 0, "no colour is written here");

  // and the job in no group is left alone: three rows in the markup, three
  // names, and only the two inside the group are backed
  assert.ok(open.includes(">billing<"), open);
});

test("the disclosure is a button that says which way the group goes", () => {
  const shut = draw(WEATHER, []);
  assert.equal(count(shut, /<button/g), 1, "one group, one disclosure");
  assert.ok(shut.includes('aria-expanded="false"'), shut);
  assert.ok(shut.includes('aria-label="weather"'), shut);
  assert.ok(shut.includes(">weather · 2<"), shut);
  // the row has room for the count and not for the names, so the names are on
  // the hover, on the control that covers them
  assert.ok(shut.includes('title="weather_pull, weather_clean"'), shut);
  // shut, the caret points at the row it would add
  assert.equal(count(shut, /tl-caret-open/g), 0);

  const open = draw(WEATHER, ["weather"]);
  assert.ok(open.includes('aria-expanded="true"'), open);
  assert.equal(count(open, /tl-caret-open/g), 1);
  // opening adds job rows and no second disclosure: the outline is two deep
  assert.equal(count(open, /<button/g), 1);

  // a plot with no way to open a group draws no disclosure rather than one
  // that does nothing
  const idle = renderToStaticMarkup(
    <svg>
      <TimelineGutter rows={rows(WEATHER, [])} />
    </svg>,
  );
  assert.equal(count(idle, /<button/g), 0);
  assert.ok(idle.includes(">weather · 2<"), idle);
});

test("a job row is indented inside its group, and an ungrouped one is not", () => {
  const markup = draw(WEATHER, ["weather"]);
  const xs = [...markup.matchAll(/<text class="tl-label" x="(\d+)"/g)].map((m) => Number(m[1]));
  // the group's name ends against the bars, its jobs step back from that edge,
  // and the job in no group is where it always was
  assert.deepEqual(xs, [158, 146, 146, 158]);
});

test("a name too long for the room it has is cut, and the cut is marked", () => {
  const [group, member, plain] = rows(
    [job("a_very_long_job_name_indeed", "a_long_group_name_here", 30), job("an_ungrouped_job_with_a_long_name")],
    ["a_long_group_name_here"],
  );
  // a group's row gives up characters to the disclosure beside it, a job's
  // row to its indent, and a row in no group has the whole gutter
  assert.deepEqual([roomFor(group), roomFor(member), roomFor(plain)], [20, 21, 23]);
  assert.equal(truncate("short", 20), "short");
  assert.equal(truncate("a_long_group_name_here · 1", 20).length, 20);
  assert.ok(truncate("a_long_group_name_here · 1", 20).endsWith("…"));
});
