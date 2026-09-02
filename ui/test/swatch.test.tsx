// what a swatch actually draws, in markup rather than in a claim about it.
//
// the two things worth asserting at this level are that a multi-source swatch
// is split into one stripe per source and never averaged into a third colour,
// and that turning colour off leaves markup with no hue in it at all.
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { at } from "../src/Swatch";
import DagView from "../src/DagView";
import type { DagNode } from "../src/DagView";
import Swatch from "../src/Swatch";
import { HueLegend, OriginCell } from "../src/AssetsPage";
import { stripesFor } from "../src/colour";
import { asset } from "./catalog.test";

const stripes = [
  { label: "feed", hue: 12 },
  { label: "vendor", hue: 105 },
  { label: "warehouse", hue: 274 },
  { label: "zulu", hue: 300 },
];

// every `--shade` in the markup, in the order it was drawn. the palette is
// black, white and grey: a label's angle only ever picks which shade of the
// page's own ink its mark is drawn at
const angles = (markup: string) =>
  [...markup.matchAll(/--shade:\s*([\d.]+)/g)].map((m) => Number(m[1]));
// what `at` hands an element for a given angle
const shade = (hue: number) => Number((at(hue) as Record<string, string>)["--shade"]);

test("a split swatch draws each source's own shade and averages nothing", () => {
  const markup = renderToStaticMarkup(<Swatch stripes={stripes.slice(0, 3)} />);
  assert.deepEqual(angles(markup), [shade(12), shade(105), shade(274)]);
  // three stripes, three elements: nothing here mixes two hues into a third
  assert.equal((markup.match(/swatch-stripe/g) ?? []).length, 3);
  assert.ok(markup.includes("feed, vendor, warehouse"), markup);

  // past the cap the rest become a count rather than a fourth stripe
  const capped = renderToStaticMarkup(<Swatch stripes={stripes} />);
  assert.deepEqual(angles(capped), [shade(12), shade(105), shade(274)]);
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

test("colour off leaves the graph with no shade in its markup", () => {
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
  assert.deepEqual(angles(coloured), [shade(105), shade(274), shade(105), shade(274)]);
  assert.equal((coloured.match(/dag-swatch/g) ?? []).length, 4);

  const off = renderToStaticMarkup(<DagView nodes={graph(stripesFor(margin, "off"))} />);
  assert.deepEqual(angles(off), []);
  assert.equal(off.includes("dag-swatch"), false);
  assert.equal(off.includes("hsl"), false);
  // the node names are still there, which is the whole of what colour was
  // adding: it carries nothing on its own
  assert.ok(off.includes("orders") && off.includes("margin"), off);
});

// the third place a hue is drawn. `Swatch.tsx` exports the only function that
// emits one, and `Swatch` and `DagView` are asserted above; the legend and the
// origin cell reach for it directly, so the rule that a colour is never the
// only carrier is asserted here too rather than left to the two that were
// convenient to render
test("every shade in the legend and the origin cell is drawn beside its own name", () => {
  const legend = renderToStaticMarkup(<HueLegend stripes={stripes} says="by group" />);
  // one shade drawn, one name written, for every stripe: no bare mark
  assert.deepEqual(angles(legend), [shade(12), shade(105), shade(274), shade(300)]);
  for (const s of stripes) assert.ok(legend.includes(s.label), `${s.label} missing: ${legend}`);
  // the block of colour is decoration and says so, so a screen reader is not
  // told about a swatch it cannot use
  assert.equal((legend.match(/aria-hidden="true"/g) ?? []).length, stripes.length);

  // and the cell: the words do not depend on the mode, which is what makes
  // turning colour off cost a reader nothing
  const a = asset("margin", {
    provenance: [
      { name: "vendor", hue: 105 },
      { name: "warehouse", hue: 274 },
    ],
  });
  for (const mode of ["group", "origin", "off"] as const) {
    const cell = renderToStaticMarkup(<OriginCell asset={a} mode={mode} />);
    assert.ok(cell.includes("vendor") && cell.includes("warehouse"), `${mode}: ${cell}`);
  }
  // off draws the words and no angle at all
  assert.deepEqual(angles(renderToStaticMarkup(<OriginCell asset={a} mode="off" />)), []);
});
