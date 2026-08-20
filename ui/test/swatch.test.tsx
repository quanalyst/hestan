// what a swatch actually draws, in markup rather than in a claim about it.
//
// the two things worth asserting at this level are that a multi-source swatch
// is split into one stripe per source and never averaged into a third colour,
// and that turning colour off leaves markup with no hue in it at all.
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import DagView from "../src/DagView";
import type { DagNode } from "../src/DagView";
import Swatch from "../src/Swatch";
import { stripesFor } from "../src/colour";
import { asset } from "./catalog.test";

const stripes = [
  { label: "feed", hue: 12 },
  { label: "vendor", hue: 105 },
  { label: "warehouse", hue: 274 },
  { label: "zulu", hue: 300 },
];

// every `--h` in the markup, in the order it was drawn
const angles = (markup: string) =>
  [...markup.matchAll(/--h:\s*(\d+)/g)].map((m) => Number(m[1]));

test("a split swatch draws each source's own angle and averages nothing", () => {
  const markup = renderToStaticMarkup(<Swatch stripes={stripes.slice(0, 3)} />);
  assert.deepEqual(angles(markup), [12, 105, 274]);
  // three stripes, three elements: nothing here mixes two hues into a third
  assert.equal((markup.match(/swatch-stripe/g) ?? []).length, 3);
  assert.ok(markup.includes("feed, vendor, warehouse"), markup);

  // past the cap the rest become a count rather than a fourth stripe
  const capped = renderToStaticMarkup(<Swatch stripes={stripes} />);
  assert.deepEqual(angles(capped), [12, 105, 274]);
  assert.ok(capped.includes("+1"), capped);
  // and the one that was counted away is still named on the hover
  assert.ok(capped.includes("zulu"), capped);

  // nothing to colour draws nothing at all, not an empty box
  assert.equal(renderToStaticMarkup(<Swatch stripes={[]} />), "");
});

const graph = (hues: DagNode["hues"]): DagNode[] => [
  { name: "orders", deps: [], group: "warehouse", hues },
  { name: "margin", deps: ["orders"], group: "finance", hues },
];

test("colour off leaves the graph with no hue in its markup", () => {
  const margin = asset("margin", {
    group: "finance",
    provenance: [
      { name: "vendor", hue: 105 },
      { name: "warehouse", hue: 274 },
    ],
  });

  const coloured = renderToStaticMarkup(
    <DagView nodes={graph(stripesFor(margin, "origin"))} />,
  );
  assert.deepEqual(angles(coloured), [105, 274, 105, 274]);
  assert.equal((coloured.match(/dag-swatch/g) ?? []).length, 4);

  const off = renderToStaticMarkup(<DagView nodes={graph(stripesFor(margin, "off"))} />);
  assert.deepEqual(angles(off), []);
  assert.equal(off.includes("dag-swatch"), false);
  assert.equal(off.includes("hsl"), false);
  // the node names are still there, which is the whole of what colour was
  // adding: it carries nothing on its own
  assert.ok(off.includes("orders") && off.includes("margin"), off);
});
