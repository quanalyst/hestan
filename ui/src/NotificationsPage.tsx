import { createContext, useContext, useEffect, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { get, post } from "./api";
import { deliveryActions, deliveryQuery, deliveryStates } from "./notifications";
import type { Delivery, DeliveryAttempt } from "./notifications";
import { relTime, shortId } from "./util";
import { useMay } from "./role";

export const NotificationControlsContext = createContext(false);

export function DeliveryProblems({ run }: { run?: string }) {
  const [rows, setRows] = useState<Delivery[]>([]);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    const refresh = async () => {
      try {
        const pages = await Promise.all(["failed", "blocked", "pending"].map(state => {
          const q = new URLSearchParams({ state, limit: "5" });
          if (run) q.set("run", run);
          return get<{ deliveries: Delivery[] }>(`/api/notification-deliveries?${q}`);
        }));
        if (active) { setRows(pages.flatMap(p => p.deliveries)); setError(""); }
      } catch (e) { if (active) setError(String(e)); }
    };
    void refresh(); const timer = setInterval(() => void refresh(), 5000);
    return () => { active = false; clearInterval(timer); };
  }, [run]);
  return <section aria-label="Notification deliveries">
    <p><Link to={`/notifications${run ? `?run=${encodeURIComponent(run)}` : ""}`}>Notification deliveries</Link></p>
    {error && <p role="alert">Could not load notification deliveries: {error}</p>}
    {rows.length > 0 && <><p className="muted">Recent failed, blocked, and pending deliveries (up to five of each).</p><DeliveryRows rows={rows} /></>}
  </section>;
}

export function DeliveryRows({ rows }: { rows: Delivery[] }) {
  return <table><thead><tr><th>Destination</th><th>Delivery state</th><th>Run outcome</th><th>Job / run</th><th>Attempts</th><th>Next attempt</th><th>Last error</th></tr></thead>
    <tbody>{rows.map(row => <tr key={row.id}>
      <td><Link to={`/notifications/${row.id}`}>{row.destination}</Link></td>
      <td>{row.state.replaceAll("_", " ")}</td>
      <td>{row.event.run.status}</td>
      <td><Link to={`/runs/${row.event.run.run_id}`}>{row.event.display_name} · {shortId(row.event.run.run_id)}</Link><div className="muted">{row.event.run.job}</div></td>
      <td>{row.attempts} total · {row.cycle_attempts} this cycle</td>
      <td>{row.next_attempt_at ? relTime(row.next_attempt_at) : "—"}</td>
      <td>{row.last_error ?? "—"}</td>
    </tr>)}</tbody></table>;
}

export default function NotificationsPage() {
  const [params, setParams] = useSearchParams();
  const query = deliveryQuery(params);
  const [rows, setRows] = useState<Delivery[]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [loaded, setLoaded] = useState(false);
  useEffect(() => {
    let active = true;
    const refresh = async () => {
      try {
        const result = await get<{ deliveries: Delivery[]; next_cursor: string | null }>(`/api/notification-deliveries?${query}`);
        if (active) { setRows(result.deliveries); setNext(result.next_cursor); setError(""); setLoaded(true); }
      } catch (e) { if (active) { setError(String(e)); setLoaded(true); } }
    };
    void refresh();
    const timer = setInterval(() => void refresh(), 3000);
    return () => { active = false; clearInterval(timer); };
  }, [query]);
  function filter(key: string, value: string) {
    const next = new URLSearchParams(params);
    next.delete("before");
    if (value) next.set(key, value); else next.delete(key);
    setParams(next);
  }
  return <>
    <h1>Notification deliveries</h1>
    <p className="muted">Delivery is separate from execution. Delivered means the destination accepted submission; duplicates are possible after an interrupted attempt.</p>
    <div className="filter-row">
      <label>State <select value={params.get("state") ?? ""} onChange={e => filter("state", e.target.value)}><option value="">All</option>{deliveryStates.map(s => <option key={s} value={s}>{s.replaceAll("_", " ")}</option>)}</select></label>
      {["destination", "job", "run"].map(key => <label key={key}>{key} <input value={params.get(key) ?? ""} onChange={e => filter(key, e.target.value)} /></label>)}
      {(["since", "until"] as const).map(key => {
        const date = new Date(params.get(key) ?? "");
        return <label key={key}>{key === "since" ? "From (UTC)" : "Before (UTC)"} <input type="datetime-local" value={Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 16)} onChange={e => filter(key, e.target.value ? `${e.target.value}:00Z` : "")} /></label>;
      })}
    </div>
    {error && <p role="alert">{error}</p>}
    {!loaded ? <p>Loading deliveries…</p> : rows.length === 0 ? <p>No deliveries match these filters.</p> : <DeliveryRows rows={rows} />}
    {params.has("before") && <button onClick={() => filter("before", "")}>Newest</button>}
    {rows.length === 50 && next && <button onClick={() => { const q = new URLSearchParams(params); q.set("before", next); setParams(q); }}>Older</button>}
  </>;
}

export function NotificationPage() {
  const { id } = useParams();
  const allowed = useContext(NotificationControlsContext);
  const admin = useMay("admin") && allowed;
  const [row, setRow] = useState<Delivery | null>(null);
  const [attempts, setAttempts] = useState<DeliveryAttempt[]>([]);
  const [error, setError] = useState("");
  const [actionError, setActionError] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    let active = true;
    const refresh = async () => {
      try {
        const [detail, history] = await Promise.all([get<{ delivery: Delivery }>(`/api/notification-deliveries/${id}`), get<{ attempts: DeliveryAttempt[] }>(`/api/notification-deliveries/${id}/attempts`)]);
        if (active) { setRow(detail.delivery); setAttempts(history.attempts); setError(""); }
      } catch (e) { if (active) setError(String(e)); }
    };
    void refresh(); const timer = setInterval(() => void refresh(), 2000);
    return () => { active = false; clearInterval(timer); };
  }, [id]);
  async function act(action: string) {
    if (!row) return;
    setBusy(true); setActionError("");
    try { const result = await post<{ delivery: Delivery }>(`/api/notification-deliveries/${id}/${action}`, { generation: row.generation }); setRow(result.delivery); }
    catch (e) { setActionError(String(e)); }
    finally { setBusy(false); }
  }
  return <>
    <p><Link to="/notifications">Notification deliveries</Link></p>
    <h1>{row?.destination ?? "Notification delivery"}</h1>
    {(error || actionError) && <p role="alert">{actionError || error}</p>}
    {!row && !error && <p>Loading delivery…</p>}
    {row && <>
      <p className="mono">{row.id}</p>
      <DeliveryRows rows={[row]} />
      <p>Delivered means submission was acknowledged. A Teams workflow may still fail downstream; email acceptance does not confirm inbox delivery.</p>
      {row.state === "blocked" && <p>Restore a compatible destination registration to resume delivery.</p>}
      {deliveryActions(row.state, admin).map(action => <button disabled={busy} key={action} onClick={() => void act(action)}>{action === "retry" ? "Retry delivery" : "Dismiss delivery"}</button>)}
      <h2>Attempts</h2>
      {attempts.length === 0 ? <p>No send has been attempted.</p> : <table><thead><tr><th>Attempt</th><th>Cycle</th><th>Started</th><th>Finished</th><th>Outcome</th><th>HTTP status</th><th>Error</th></tr></thead><tbody>{attempts.map(a => <tr key={a.attempt}><td>{a.attempt}</td><td>{a.cycle}</td><td>{relTime(a.started_at)}</td><td>{a.finished_at ? relTime(a.finished_at) : "—"}</td><td>{a.outcome.replaceAll("_", " ")}</td><td>{a.status ?? "—"}</td><td>{a.error ?? "—"}</td></tr>)}</tbody></table>}
    </>}
  </>;
}
