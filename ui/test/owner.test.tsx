// who owns something, drawn: the one line beside a name, and the claim that
// an owner nobody declared is an absence on the page rather than a label with
// nothing after it.
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import AssetDetail from "../src/AssetDetail";
import { asset } from "./catalog.test";
import type { Owner } from "../src/types";
import { ownerLine } from "../src/util";

test("an owner says itself in one line, whichever half was declared", () => {
  assert.equal(
    ownerLine({ team: "data-platform", person: "ada", contact: "#data-alerts" }),
    "ada of data-platform (#data-alerts)",
  );
  assert.equal(ownerLine({ team: "data-platform" }), "data-platform");
  assert.equal(ownerLine({ person: "ada" }), "ada");
  assert.equal(ownerLine({ team: "data", contact: "ops@example.com" }), "data (ops@example.com)");
});

// the promise, and the reason this returns null rather than "": a caller can
// tell "nobody declared one" from "the name came back blank", and draws
// nothing at all for the first
test("nobody declared one is null, and null draws no line", () => {
  assert.equal(ownerLine(null), null);
  // an owner object with nothing in it is the same absence: it can only come
  // from a declaration somebody wrote by accident
  assert.equal(ownerLine({} as Owner), null);

  const unowned = renderToStaticMarkup(<AssetDetail asset={asset("orders")} />);
  assert.ok(!unowned.includes("owner"), `an unowned asset drew an owner line: ${unowned}`);

  const owned = renderToStaticMarkup(
    <AssetDetail
      asset={asset("orders", {
        owner: { team: "finance", contact: "#fin-alerts", escalates_to: "ops@example.com" },
      })}
    />,
  );
  assert.ok(owned.includes("finance (#fin-alerts)"), owned);
  // the second contact is beside the first and is not dressed up as a step
  // hestan is going to take
  assert.ok(owned.includes("ops@example.com"), owned);
});
