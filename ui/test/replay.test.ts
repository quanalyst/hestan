// the two things the run page says about a replay: where a run came from, and
// what a replay of it would do. a resume and a replay mean opposite things and
// are one letter apart, so the line that tells them apart is asserted rather
// than eyeballed.
import assert from "node:assert/strict";
import test from "node:test";
import type { ReplayPreview, ResumePreview } from "../src/types";
import { lineage, replayLine, resumeLine } from "../src/util";

const from = (resumed_from: string | null, replay_of: string | null) => ({
  resumed_from,
  replay_of,
});

test("a run says which of the two it came from, or nothing at all", () => {
  assert.deepEqual(lineage(from("r1", null)), { verb: "continues", id: "r1" });
  assert.deepEqual(lineage(from(null, "r1")), { verb: "replay of", id: "r1" });
  assert.equal(lineage(from(null, null)), null);
});

test("a replay line counts what would run and what would be seeded", () => {
  const p = (ops: string[], inputs: string[]): ReplayPreview => ({ ops, inputs });
  assert.equal(replayLine(p(["load"], ["extract"])), "1 to replay · 1 input seeded");
  assert.equal(replayLine(p(["a", "b"], ["x", "y"])), "2 to replay · 2 inputs seeded");
  // an op with no deps reads nothing back, and "0 inputs" there would read as
  // something that went missing
  assert.equal(replayLine(p(["extract"], [])), "1 to replay");
});

test("a resume line stays what it was, so the two never read alike", () => {
  const p: ResumePreview = { rerun: ["b", "c"], reuse: ["a"] };
  assert.equal(resumeLine(p), "2 to re-run · 1 reused");
  assert.notEqual(resumeLine(p), replayLine({ ops: p.rerun, inputs: p.reuse }));
});
