export const deliveryStates = ["pending", "in_flight", "blocked", "delivered", "failed", "dismissed"] as const;
export type DeliveryState = typeof deliveryStates[number];
export interface Delivery {
  id: string;
  destination: string;
  state: DeliveryState;
  attempts: number;
  cycle_attempts: number;
  cycle: number;
  generation: number;
  created_at: string;
  next_attempt_at: string | null;
  delivered_at: string | null;
  last_error: string | null;
  event: { display_name: string; event_type: string; event_id: string; run_url: string | null; run: { run_id: string; job: string; status: string } };
}
export interface DeliveryAttempt {
  attempt: number;
  cycle: number;
  started_at: string;
  finished_at: string | null;
  outcome: string;
  status: number | null;
  error: string | null;
}
export function deliveryActions(state: DeliveryState, admin: boolean): string[] {
  if (!admin) return [];
  if (state === "failed") return ["retry", "dismiss"];
  return state === "pending" || state === "blocked" ? ["dismiss"] : [];
}
export function deliveryQuery(params: URLSearchParams): string {
  const q = new URLSearchParams();
  for (const key of ["state", "destination", "job", "run", "before", "since", "until"]) {
    const value = params.get(key);
    if (value) q.set(key, value);
  }
  q.set("limit", "50");
  return q.toString();
}
