// what the run page's saved section collects, and what a series says about
// itself when it is a sample of a much longer one.
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import SavedList from "../src/SavedList";
import type { MetaValue, OpRun } from "../src/types";

const TAKEN = "2026-08-20T09:00:00+00:00";

const op = (name: string, metadata: OpRun["metadata"]): OpRun => ({
  run_id: "r1",
  op: name,
  status: "success",
  attempts: 1,
  started_at: "2026-08-20T09:00:00Z",
  finished_at: "2026-08-20T09:00:01Z",
  output: null,
  metadata,
  deltas: {},
  error: null,
  pid: null,
});

const saved = (value: MetaValue): MetaValue => ({ saved: { taken_at: TAKEN, value } });

const at = (hour: number) => new Date(Date.UTC(2026, 0, 1) + hour * 3_600_000).toISOString();

const html = (ops: OpRun[]) => renderToStaticMarkup(<SavedList ops={ops} />);

test("the section collects every op's samples, labelled and in the order the ops come back", () => {
  const markup = html([
    op("alpha", { head: saved({ text: "from alpha" }) }),
    op("beta", { count: { count: 4242 } }),
    op("gamma", { head: saved({ text: "from gamma" }), tail: saved({ text: "also gamma" }) }),
  ]);
  assert.deepEqual(
    [...markup.matchAll(/saved-op">([^<]+)</g)].map((m) => m[1]),
    ["alpha", "gamma", "gamma"],
  );
  assert.deepEqual(
    [...markup.matchAll(/saved-name">([^<]+)</g)].map((m) => m[1]),
    ["head", "head", "tail"],
  );
  assert.ok(markup.includes("from alpha"), markup);
  assert.ok(markup.includes("also gamma"), markup);
  // the op that reported facts and saved nothing contributes nothing
  assert.ok(!markup.includes("4,242") && !markup.includes("4242"), markup);
});

test("the section says it is a snapshot and when each one was taken", () => {
  const markup = html([op("alpha", { head: saved({ text: "x" }) })]);
  assert.ok(markup.includes("snapshot"), markup);
  assert.ok(/taken \d+[smhd] ago/.test(markup), markup);
  // the exact moment is on the hover, since the line reads in relative time
  assert.ok(markup.includes(`title="${TAKEN}"`), markup);
});

test("a run that saved nothing renders no section at all", () => {
  assert.equal(html([]), "");
  assert.equal(html([op("alpha", null), op("beta", { rows: { count: 1 } })]), "");
});

test("a sampled series draws its range on the axis and says what it stands for", () => {
  // two hundred points spread across a year of hourly ones, which is what
  // Meta::series produces for that year
  const points = Array.from(
    { length: 200 },
    (_, i) => [at(Math.floor((i * 8759) / 199)), Math.floor((i * 8759) / 199)] as [string, number],
  );
  const markup = html([op("load", { hourly: saved({ series: { points, of: 8760 } }) })]);

  assert.ok(markup.includes("200 of 8,760 points"), markup);
  // both ends of the time range, on the axis
  assert.ok(markup.includes("2026-01-01 00:00"), markup);
  assert.ok(markup.includes("2026-12-31 23:00"), markup);
  // and both ends of the value range
  assert.ok(markup.includes(">0</text>"), markup);
  assert.ok(markup.includes(">8,759</text>"), markup);
  // the numbers behind the shape, one row per point plus the heading
  assert.equal((markup.match(/<tr>/g) ?? []).length, 201);
  assert.ok(!markup.includes("NaN"), markup);
  // monochrome: no hue is introduced, and no inline colour of any kind
  assert.ok(!/hsl\(|#[0-9a-fA-F]{3}|style=/.test(markup), markup);

  // a series that is all of itself says so plainly rather than "200 of 200"
  const whole = html([op("load", { hourly: saved({ series: { points, of: 200 } }) })]);
  assert.ok(whole.includes(">200 points<"), whole);
});

test("a series with no range to spread across still draws", () => {
  // one point: no time range, no value range, and no NaN in the geometry
  const one = html([op("a", { s: saved({ series: { points: [[at(0), 5]], of: 1 } }) })]);
  assert.ok(!one.includes("NaN"), one);
  assert.ok(one.includes(">1 point<"), one);

  // flat: a value range of nothing draws down the middle, because along the
  // bottom would read as zero
  const flat = html([
    op("a", { s: saved({ series: { points: [[at(0), 5], [at(1), 5]], of: 2 } }) }),
  ]);
  assert.ok(!flat.includes("NaN"), flat);

  // empty: the op looked and found nothing, which is not the same as an op
  // that saved no series at all
  const none = html([op("a", { s: saved({ series: { points: [], of: 0 } }) })]);
  assert.ok(none.includes("no points"), none);
  assert.ok(!none.includes("<svg"), none);
});
