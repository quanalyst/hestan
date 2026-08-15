// what a role may, and the rule that keeps a control a role may not use off
// the page rather than on it and failing at 403.
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { renderToStaticMarkup } from "react-dom/server";
import SignIn from "../src/SignIn";
import type { Role } from "../src/identity";
import { may } from "../src/identity";

const SRC = join(dirname(fileURLToPath(import.meta.url)), "..", "src");

test("a role may whatever the roles below it may", () => {
  const roles: Role[] = ["viewer", "operator", "admin"];
  const table: Record<Role, Role[]> = {
    viewer: ["viewer"],
    operator: ["viewer", "operator"],
    admin: ["viewer", "operator", "admin"],
  };
  for (const role of roles) {
    for (const needs of roles) {
      assert.equal(may(role, needs), table[role].includes(needs), `${role} vs ${needs}`);
    }
  }
});

// the structural half of "a viewer's ui offers no launch control": every page
// that can change something has to have asked what the role may. a new button
// wired to a new endpoint fails this until somebody decides which role it
// belongs to, which is the point: the ui and the api are not allowed to
// disagree about who may do what.
test("every page that changes something asks what the role may first", () => {
  const pages = readdirSync(SRC).filter((f) => f.endsWith(".tsx"));
  const mutating = pages.filter((page) => {
    const source = readFileSync(join(SRC, page), "utf8");
    return /\b(post|put|del)</.test(source);
  });
  // the pages that launch, cancel, build, backfill, pause and reorder
  assert.ok(mutating.length >= 8, `only found ${mutating.length} mutating pages`);
  for (const page of mutating) {
    const source = readFileSync(join(SRC, page), "utf8");
    assert.ok(source.includes("useMay("), `${page} changes something without asking the role`);
  }
});

test("the token prompt says what holding a token in a browser costs", () => {
  const markup = renderToStaticMarkup(<SignIn refused={false} onToken={() => {}} />);
  // typed as a password, so a shared screen does not show it
  assert.ok(markup.includes('type="password"'), markup);
  // and the two facts somebody is entitled to before they paste a credential
  assert.ok(markup.includes("this tab only"), markup);
  assert.ok(markup.includes("javascript"), markup);
  // a refusal is a sentence, never a colour
  const refused = renderToStaticMarkup(<SignIn refused onToken={() => {}} />);
  assert.ok(refused.includes("refused"), refused);
  assert.ok(!/style=|color/.test(refused), refused);
});
