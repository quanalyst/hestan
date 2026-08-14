// a declared rate is two numbers somebody typed, and the ui reads them back the
// way they would say them out loud rather than as a duration off a stopwatch.
//
// run with `npm test` (vite bundles this for node, node runs it).
import assert from "node:assert/strict";
import test from "node:test";
import { fmtPeriod, fmtRate } from "../src/util";

test("a period reads as the word for it where there is one", () => {
  assert.equal(fmtPeriod(1), "second");
  assert.equal(fmtPeriod(60), "minute");
  assert.equal(fmtPeriod(3600), "hour");
  // and as the unit it was written in where there is not
  assert.equal(fmtPeriod(0.2), "200ms");
  assert.equal(fmtPeriod(5), "5s");
  assert.equal(fmtPeriod(300), "5m");
  assert.equal(fmtPeriod(7200), "2h");
  // 90 seconds is a minute and a half and neither unit says so: seconds is the
  // one that is not wrong
  assert.equal(fmtPeriod(90), "90s");
});

test("a rate reads as the limit an api published", () => {
  assert.equal(fmtRate(5, 1), "5 per second");
  assert.equal(fmtRate(1000, 3600), "1000 per hour");
  assert.equal(fmtRate(2, 0.5), "2 per 500ms");
});
