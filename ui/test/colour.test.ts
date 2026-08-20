// the colour channel's three rules, asserted rather than described.
//
// colour means where an asset belongs or where it came from and never how it
// is doing; a colour is never the only thing carrying an answer; and one hue
// meaning at a time. the first is a rule about what the code may not do, and
// the other two are checkable over a fixture, which is what this is.
import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_HUE_MODE,
  HUE_MODES,
  MAX_STRIPES,
  groupWords,
  hueMode,
  legendFor,
  originWords,
  shownAndMore,
  stripesFor,
} from "../src/colour";
import { asset } from "./catalog.test";
import type { AssetSummary } from "../src/types";

const warehouse = { name: "warehouse", hue: 274 };
const vendor = { name: "vendor", hue: 105 };
const ledger = { name: "ledger", hue: 40 };
const feed = { name: "feed", hue: 12 };

const catalog: AssetSummary[] = [
  asset("sales/orders", { provenance: [warehouse] }),
  asset("sales/returns", { provenance: [warehouse] }),
  asset("margin", { group: "finance", provenance: [vendor, warehouse] }),
  asset("heartbeat", { provenance: [] }),
];

const labels = (a: AssetSummary, mode: (typeof HUE_MODES)[number]) =>
  stripesFor(a, mode).map((s) => s.label);

test("a hue means the group or the origin, one at a time, or nothing", () => {
  assert.equal(DEFAULT_HUE_MODE, "group");
  // an unknown mode in the url is the default rather than a blank page
  assert.equal(hueMode("sideways"), "group");
  assert.equal(hueMode(null), "group");
  assert.equal(hueMode("origin"), "origin");

  const margin = catalog[2];
  assert.deepEqual(labels(margin, "group"), ["finance"]);
  assert.deepEqual(labels(margin, "origin"), ["vendor", "warehouse"]);
  // never both at once: whichever mode is live, the other's labels are absent
  assert.equal(labels(margin, "group").includes("warehouse"), false);
  assert.equal(labels(margin, "origin").includes("finance"), false);
});

test("colour off draws no hue at all, anywhere, and loses no words", () => {
  for (const a of catalog) {
    assert.deepEqual(stripesFor(a, "off"), []);
    // the words are the same three sentences in every mode, which is what
    // makes turning colour off cost nothing
    assert.equal(groupWords(a), groupWords(a));
    assert.deepEqual(originWords(a), originWords(a));
  }
  assert.deepEqual(legendFor(catalog, "off"), []);
  // an asset in no group has no group hue in group mode either: null is not
  // an angle, and inventing one would colour "nothing" as a something
  assert.deepEqual(stripesFor(catalog[3], "group"), []);
});

test("an empty origin is an answer in words, not a blank", () => {
  assert.deepEqual(originWords(catalog[3]), ["no source"]);
  assert.equal(groupWords(catalog[3]), "no group");
  assert.equal(groupWords(catalog[2]), "finance");
  assert.deepEqual(originWords(catalog[0]), ["warehouse"]);
});

test("every hue drawn has its own name beside it and in the legend", () => {
  for (const mode of HUE_MODES) {
    const legend = legendFor(catalog, mode).map((s) => s.label);
    for (const a of catalog) {
      const drawn = labels(a, mode);
      // the legend turns any hue in the view back into a name
      for (const label of drawn) {
        assert.ok(legend.includes(label), `${label} is coloured and not in the ${mode} legend`);
      }
      // and the words on the row itself already say it: the group heading the
      // row sits under, or the origin cell on the row
      const beside = mode === "group" ? [groupWords(a)] : originWords(a);
      for (const label of drawn) {
        assert.ok(beside.includes(label), `${label} is coloured with nothing beside it`);
      }
    }
  }
});

test("no two rows are told apart by a colour alone", () => {
  for (const mode of HUE_MODES) {
    for (const a of catalog) {
      for (const b of catalog) {
        if (a === b) continue;
        const differ = labels(a, mode).join("|") !== labels(b, mode).join("|");
        if (!differ) continue;
        // a colour tells these two apart, so the words have to as well
        const words = (x: AssetSummary) => [groupWords(x), ...originWords(x)].join("|");
        assert.notEqual(
          words(a),
          words(b),
          `${a.name} and ${b.name} differ only by hue under ${mode}`,
        );
      }
    }
  }
});

test("the legend names each label once, in name order, with its own hue", () => {
  assert.deepEqual(legendFor(catalog, "origin"), [vendor, warehouse].map((o) => ({
    label: o.name,
    hue: o.hue,
  })));
  // one entry per label however many assets carry it
  assert.deepEqual(
    legendFor(catalog, "group").map((s) => s.label),
    ["finance", "sales"],
  );
});

test("a multi-source swatch is one stripe per source in name order, never blended", () => {
  const many = asset("everything", {
    provenance: [warehouse, feed, vendor, ledger],
  });
  // the api sends them sorted; this end sorts them too, so the order is its
  // own claim rather than a fact about the fixture
  assert.deepEqual(labels(many, "origin"), ["feed", "ledger", "vendor", "warehouse"]);
  const { shown, more } = shownAndMore(stripesFor(many, "origin"));
  // every stripe keeps its own hue: nothing here averages two of them into a
  // third that stands for a source nobody has
  assert.deepEqual(
    shown.map((s) => s.hue),
    [feed.hue, ledger.hue, vendor.hue],
  );
  assert.equal(shown.length, MAX_STRIPES);
  assert.equal(more, 1);
  // and the one past the cap is still named in the legend
  assert.ok(legendFor([many], "origin").some((s) => s.label === "warehouse"));

  // at or under the cap nothing is counted away
  assert.deepEqual(shownAndMore(stripesFor(catalog[2], "origin")).more, 0);
});
