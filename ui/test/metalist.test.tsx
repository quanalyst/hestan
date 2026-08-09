// what a metadata row draws: the value in its unit, the delta the api
// computed beside it, and a sparkline under it once there is enough history
// to be one.
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import MetaList from "../src/MetaList";
import type { MetaPoint, Metadata, Trends } from "../src/types";

const html = (metadata: Metadata, deltas = {}, trends: Trends = {}) =>
  renderToStaticMarkup(<MetaList metadata={metadata} deltas={deltas} trends={trends} />);

const points = (n: number): MetaPoint[] =>
  Array.from({ length: n }, (_, i) => ({
    at: `2026-08-0${i + 1}T00:00:00Z`,
    value: 10 + i,
    run_id: `r${i}`,
  }));

const bars = (markup: string) => (markup.match(/<rect/g) ?? []).length;

test("fewer than three points is not a trend and draws nothing", () => {
  const metadata: Metadata = { rows: { count: 12 } };
  for (const n of [0, 1, 2]) {
    const markup = html(metadata, {}, { rows: points(n) });
    assert.ok(!markup.includes("<svg"), `${n} points drew a chart`);
  }
  const markup = html(metadata, {}, { rows: points(3) });
  assert.ok(markup.includes("<svg"), markup);
  assert.equal(bars(markup), 3);
  // and only for the key it was fetched for
  assert.equal(bars(html({ rows: { count: 12 }, note: { text: "x" } }, {}, { rows: points(4) })), 4);
});

test("units render as units and a delta always carries a sign", () => {
  // a count reads as itself; a size reads as the percentage, with the
  // absolute change on the hover
  const markup = html(
    { rows: { count: 1240 }, size: { bytes: 1_152_000_000 }, took: { duration_secs: 3.4 } },
    {
      rows: { delta: 37, delta_pct: 3.08 },
      size: { delta: -48_000_000, delta_pct: -4 },
      took: { delta: 0, delta_pct: 0 },
    },
  );
  assert.ok(markup.includes("1,240"), markup);
  assert.ok(markup.includes("1.2 GB"), markup);
  assert.ok(markup.includes("3.4s"), markup);
  assert.ok(markup.includes("+37"), markup);
  assert.ok(markup.includes("−4%"), markup);
  // measured and did not move, which is not the same as nothing to compare
  assert.ok(markup.includes("±0"), markup);
  assert.ok(markup.includes('title="−48 MB"'), markup);

  // a key with no delta shows none at all, and no colour is ever involved
  const plain = html({ rows: { count: 1240 } });
  assert.ok(!plain.includes("meta-delta"), plain);
  assert.ok(!/style=|color/.test(plain), plain);
});
