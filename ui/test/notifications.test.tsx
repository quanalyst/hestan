import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router-dom";
import { DeliveryRows } from "../src/NotificationsPage";
import { deliveryActions, deliveryQuery, deliveryStates } from "../src/notifications";
import type { Delivery } from "../src/notifications";

test("delivery actions respect both lifecycle and administrator access", () => {
  for (const state of deliveryStates) assert.deepEqual(deliveryActions(state, false), []);
  assert.deepEqual(deliveryActions("failed", true), ["retry", "dismiss"]);
  assert.deepEqual(deliveryActions("blocked", true), ["dismiss"]);
  assert.deepEqual(deliveryActions("pending", true), ["dismiss"]);
  for (const state of ["in_flight", "delivered", "dismissed"] as const) assert.deepEqual(deliveryActions(state, true), []);
});
test("delivery filters and pagination survive a shareable URL", () => {
  const original = new URLSearchParams({ state: "failed", destination: "shared/sink", job: "stable job", run: "r1", before: "cursor", since: "2026-01-01T00:00:00Z", until: "2026-02-01T00:00:00Z", ignored: "value" });
  const parsed = new URLSearchParams(deliveryQuery(original));
  for (const key of ["state", "destination", "job", "run", "before", "since", "until"]) assert.equal(parsed.get(key), original.get(key));
  assert.equal(parsed.get("limit"), "50"); assert.equal(parsed.has("ignored"), false);
});
test("delivery rows expose identity, separate outcomes, next attempts and errors", () => {
  const row: Delivery = { id: "delivery", destination: "receiver", state: "failed", attempts: 8, cycle_attempts: 8, cycle: 1, generation: 9, created_at: "2026-01-01T00:00:00Z", next_attempt_at: null, delivered_at: null, last_error: "HTTP 503 <untrusted>", event: { display_name: "Readable", event_type: "run.finished", event_id: "event", run_url: null, run: { run_id: "run-id", job: "persistent-id", status: "success" } } };
  const html = renderToStaticMarkup(<MemoryRouter><DeliveryRows rows={[row]} /></MemoryRouter>);
  assert.match(html, /Readable/); assert.match(html, /persistent-id/); assert.match(html, /\/notifications\/delivery/); assert.match(html, /\/runs\/run-id/); assert.match(html, /8 total/); assert.match(html, /HTTP 503 &lt;untrusted&gt;/); assert.match(html, /failed/); assert.match(html, /success/);
});
