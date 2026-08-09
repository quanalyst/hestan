import { useEffect, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { get, HttpError, post, usePoll } from "./api";
import StatusDot from "./StatusDot";
import type { Notification, QueueView, Run } from "./types";
import { durationMs, fmtDuration, isTerminal, relTime, shortId } from "./util";

const PAGE = 100;
const STATUSES = ["all", "queued", "running", "success", "failed", "canceled"] as const;
const TRIGGERS = ["all", "manual", "schedule", "retry", "resume", "build", "sensor"] as const;
const WINDOWS = [
  { label: "all", secs: null },
  { label: "1h", secs: 3600 },
  { label: "6h", secs: 21600 },
  { label: "24h", secs: 86400 },
] as const;

function FilterGroup<T extends string>({
  label,
  value,
  options,
  onPick,
}: {
  label: string;
  value: T;
  options: readonly T[];
  onPick: (v: T) => void;
}) {
  return (
    <span className="filter-group">
      <span className="filter-label">{label}</span>
      {options.map((o) => (
        <button key={o} className={value === o ? "text-btn active" : "text-btn"} onClick={() => onPick(o)}>
          {o}
        </button>
      ))}
    </span>
  );
}

const isActive = (r: Run) => r.status === "queued" || r.status === "running";

// muted key:value chips, in the stable order the api sends them in
function TagChips({ tags }: { tags: Record<string, string> }) {
  const pairs = Object.entries(tags);
  if (pairs.length === 0) return null;
  return (
    <>
      {pairs.map(([k, v]) => (
        <span key={k} className="run-tag mono">
          {k}:{v}
        </span>
      ))}
    </>
  );
}

// an alert nobody received, shown where the runs it is about are. only the
// undelivered ones: a delivered notification is not news, and the whole point
// of writing them down was that a lost one leaves a trace somebody can find
function Undelivered({ rows }: { rows: Notification[] }) {
  const nav = useNavigate();
  if (rows.length === 0) return null;
  return (
    <>
      <h2>
        undelivered notifications
        <span className="secondary"> — {rows.length}</span>
      </h2>
      <table>
        <thead>
          <tr>
            <th>state</th>
            <th>run</th>
            <th>job</th>
            <th className="num">attempts</th>
            <th>last error</th>
            <th>queued</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((n) => (
            <tr
              key={n.id}
              onClick={() => n.payload.run_id && nav(`/runs/${n.payload.run_id}`)}
            >
              <td>{n.state}</td>
              <td className="mono">{n.payload.run_id ? shortId(n.payload.run_id) : "—"}</td>
              <td>{n.payload.job ?? "—"}</td>
              <td className="num">{n.attempts}</td>
              <td className="muted">{n.last_error ?? (n.attempts === 0 ? "not tried yet" : "")}</td>
              <td className="muted">{relTime(n.created_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

export default function RunsPage() {
  const nav = useNavigate();
  const [search] = useSearchParams();
  const job = search.get("job");
  // head is the polled newest page; tail accumulates older load-more pages
  const [head, setHead] = useState<Run[] | null>(null);
  const [tail, setTail] = useState<Run[]>([]);
  const headRef = useRef<Run[] | null>(null);
  const [exhausted, setExhausted] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [status, setStatus] = useState<(typeof STATUSES)[number]>("all");
  const [trigger, setTrigger] = useState<(typeof TRIGGERS)[number]>("all");
  const [win, setWin] = useState<(typeof WINDOWS)[number]["label"]>("all");
  const [q, setQ] = useState("");
  // unlike the others this one is served: tags are not in the polled page
  // unless the server was asked for them
  const [tag, setTag] = useState("");
  const [tagQ, setTagQ] = useState("");
  const [tagErr, setTagErr] = useState<string | null>(null);
  const [queue, setQueue] = useState<QueueView | null>(null);
  const [undelivered, setUndelivered] = useState<Notification[]>([]);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [rowErr, setRowErr] = useState<{ id: string; msg: string } | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const jobQ = job ? `&job=${encodeURIComponent(job)}` : "";
  const tagParam = tagQ ? `&tag=${encodeURIComponent(tagQ)}` : "";

  useEffect(() => {
    headRef.current = null;
    setHead(null);
    setTail([]);
    setExhausted(false);
  }, [job, tagQ]);

  usePoll(
    () => {
      get<{ runs: Run[] }>(`/api/runs?limit=${PAGE}${jobQ}${tagParam}`)
        .then((r) => {
          // rows new runs push off the head page fall into the gap before the tail
          const prev = headRef.current;
          if (prev && r.runs.length) {
            const ids = new Set(r.runs.map((x) => x.id));
            const oldest = r.runs[r.runs.length - 1].created_at;
            const slid = prev.filter((x) => !ids.has(x.id) && x.created_at < oldest);
            if (slid.length)
              setTail((t) => {
                const seen = new Set(t.map((x) => x.id));
                return [...slid.filter((x) => !seen.has(x.id)), ...t];
              });
          }
          headRef.current = r.runs;
          setHead(r.runs);
          setTagErr(null);
        })
        // a refused tag has to say so: the alternative is a list that quietly
        // stays as it was and looks like an answer
        .catch((e) => {
          if (e instanceof HttpError && e.status === 400) setTagErr(e.message);
        });
    },
    5000,
    [job, tagParam],
  );

  usePoll(
    () => {
      get<QueueView>("/api/queue")
        .then(setQueue)
        // a queue that cannot be read is not a queue of nothing; leave the last
        // answer up rather than claiming it drained
        .catch(() => {});
    },
    5000,
    [],
  );

  usePoll(
    () => {
      // given up on first: those are the ones nobody will retry
      Promise.all(
        (["failed", "pending"] as const).map((state) =>
          get<{ notifications: Notification[] }>(`/api/notifications?state=${state}&limit=20`),
        ),
      )
        .then(([failed, pending]) => setUndelivered([...failed.notifications, ...pending.notifications]))
        // the table is empty on a build with no durable notifications, which is
        // most of them; a read that failed says nothing about that
        .catch(() => {});
    },
    5000,
    [],
  );

  const hasActive = (head ?? []).some(isActive) || tail.some(isActive);
  useEffect(() => {
    if (!hasActive) return;
    setNow(Date.now());
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [hasActive]);

  if (!head) return <p className="muted">loading…</p>;

  const headIds = new Set(head.map((r) => r.id));
  const runs = [...head, ...tail.filter((r) => !headIds.has(r.id))];

  const active = runs.filter(isActive);
  const elapsed = (r: Run) =>
    Math.max(0, now - new Date((r.status === "running" && r.started_at) || r.created_at).getTime());

  const winSecs = WINDOWS.find((w) => w.label === win)!.secs;
  const cutoff = winSecs === null ? null : Date.now() - winSecs * 1000;
  const needle = q.trim().toLowerCase();
  const shown = runs.filter(
    (r) =>
      (status === "all" || r.status === status) &&
      (trigger === "all" || r.trigger === trigger) &&
      (cutoff === null || new Date(r.created_at).getTime() >= cutoff) &&
      (needle === "" || r.job.toLowerCase().includes(needle) || r.id.toLowerCase().includes(needle)),
  );
  const maxDur = Math.max(0, ...shown.map((r) => durationMs(r) ?? 0));

  const loadMore = async () => {
    const oldest = runs[runs.length - 1];
    if (!oldest) return;
    setLoadingMore(true);
    try {
      // before_id breaks created_at ties so simultaneous runs never drop out
      const r = await get<{ runs: Run[] }>(
        `/api/runs?limit=${PAGE}&before=${encodeURIComponent(oldest.created_at)}` +
          `&before_id=${encodeURIComponent(oldest.id)}${jobQ}${tagParam}`,
      );
      setTail((t) => [...t, ...r.runs]);
      if (r.runs.length < PAGE) setExhausted(true);
    } catch {
      // keep the button; the next click retries
    } finally {
      setLoadingMore(false);
    }
  };

  // one step up the queue rather than a number to type: what somebody wants at
  // 3am is this run next, and the head of the queue is one above whatever is
  // there now
  const bump = async (e: ReactMouseEvent, id: string, priority: number) => {
    e.stopPropagation();
    const top = Math.max(...(queue?.queued ?? []).map((q) => q.run.priority), priority);
    setBusyId(id);
    setRowErr(null);
    try {
      await post(`/api/runs/${id}/priority`, { priority: top + 1 });
      setQueue(await get<QueueView>("/api/queue"));
    } catch (err) {
      setRowErr({ id, msg: err instanceof Error ? err.message : String(err) });
    } finally {
      setBusyId(null);
    }
  };

  // retry redoes the whole job, resume continues where the run broke
  const relaunch = async (e: ReactMouseEvent, id: string, kind: "retry" | "resume") => {
    e.stopPropagation();
    setBusyId(id);
    setRowErr(null);
    try {
      const r = await post<{ run_id: string }>(`/api/runs/${id}/${kind}`);
      nav(`/runs/${r.run_id}`);
    } catch (err) {
      setRowErr({ id, msg: err instanceof Error ? err.message : String(err) });
    } finally {
      setBusyId(null);
    }
  };

  return (
    <>
      <h1>
        Runs
        {job && (
          <span className="secondary">
            {" "}
            — {job} <Link className="head-link" to="/runs">clear</Link>
          </span>
        )}
      </h1>
      {tagErr && <p className="muted">{tagErr}</p>}
      {/* above the runs, and outside the filters: an alert that never arrived
          is not something to go looking for */}
      <Undelivered rows={undelivered} />
      {runs.length === 0 ? (
        <p className="muted">
          {tagQ ? `no runs tagged ${tagQ}` : "no runs yet — launch one from a job page"}
        </p>
      ) : (
        <>
          {queue && queue.depth > 0 && (
            <>
              <h2>
                queued
                <span className="secondary"> — {queue.depth} waiting</span>
              </h2>
              <table>
                <thead>
                  <tr>
                    <th className="num">#</th>
                    <th>run</th>
                    <th>job</th>
                    <th className="num">priority</th>
                    <th>waiting on</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {queue.queued.map((q) => (
                    <tr key={q.run.id} onClick={() => nav(`/runs/${q.run.id}`)}>
                      <td className="num muted">{q.position}</td>
                      <td className="mono">{shortId(q.run.id)}</td>
                      <td>
                        {q.run.job}
                        <TagChips tags={q.run.tags} />
                      </td>
                      <td className="num">{q.run.priority}</td>
                      <td className="muted">
                        {q.blocked_by ? q.blocked_by.reason : "starting now"}
                      </td>
                      <td className="row-action">
                        <button
                          className="text-btn"
                          disabled={busyId === q.run.id}
                          onClick={(e) => bump(e, q.run.id, q.run.priority)}
                        >
                          bump
                        </button>
                        {rowErr?.id === q.run.id && (
                          <span className="muted row-err">{rowErr.msg}</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {queue.depth > queue.queued.length && (
                <p className="muted">
                  showing the first {queue.queued.length} of {queue.depth}
                </p>
              )}
            </>
          )}
          {active.length > 0 && (
            <>
              <h2>running now</h2>
              {active.map((r) => (
                <div key={r.id} className="live-row" onClick={() => nav(`/runs/${r.id}`)}>
                  <StatusDot status={r.status} />
                  <span className="mono">{shortId(r.id)}</span>
                  <span>{r.job}</span>
                  <span className="muted live-dur">{fmtDuration(elapsed(r))}</span>
                </div>
              ))}
            </>
          )}
          <div className="filter-row">
            <FilterGroup label="status" value={status} options={STATUSES} onPick={setStatus} />
            <FilterGroup label="trigger" value={trigger} options={TRIGGERS} onPick={setTrigger} />
            <FilterGroup label="window" value={win} options={WINDOWS.map((w) => w.label)} onPick={setWin} />
            <span className="filter-group">
              <span className="filter-label">tag</span>
              <input
                className="filter-input"
                value={tag}
                placeholder="key:value"
                onChange={(e) => setTag(e.target.value)}
                onKeyDown={(e) => {
                  // the server owns this filter, so it applies on enter rather
                  // than on every keystroke
                  if (e.key === "Enter") setTagQ(tag.trim());
                  if (e.key === "Escape") {
                    setTag("");
                    setTagQ("");
                  }
                }}
                onBlur={() => setTagQ(tag.trim())}
              />
            </span>
            <span className="filter-group">
              <span className="filter-label">find</span>
              <input
                className="filter-input"
                value={q}
                placeholder="job or run id"
                onChange={(e) => setQ(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape") setQ("");
                }}
              />
            </span>
          </div>
          {shown.length === 0 ? (
            <p className="muted">no runs match the filter</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>status</th>
                  <th>run</th>
                  <th>job</th>
                  <th>trigger</th>
                  <th>started</th>
                  <th className="num">duration</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {shown.map((run) => {
                  const d = durationMs(run);
                  return (
                    <tr key={run.id} onClick={() => nav(`/runs/${run.id}`)}>
                      <td>
                        <StatusDot status={run.status} />
                      </td>
                      <td className="mono">{shortId(run.id)}</td>
                      <td>
                        {run.job}
                        <TagChips tags={run.tags} />
                      </td>
                      <td>{run.trigger}</td>
                      <td className="muted">{relTime(run.started_at ?? run.created_at)}</td>
                      <td className="num">
                        {fmtDuration(d)}
                        {d !== null && maxDur > 0 && (
                          <div
                            className={
                              run.status === "failed"
                                ? "dur-bar failed"
                                : run.status === "canceled"
                                  ? "dur-bar canceled"
                                  : "dur-bar"
                            }
                          >
                            <span style={{ width: `${(d / maxDur) * 100}%` }} />
                          </div>
                        )}
                      </td>
                      <td className="row-action">
                        {isTerminal(run.status) && (
                          <button
                            className="text-btn"
                            disabled={busyId === run.id}
                            onClick={(e) => relaunch(e, run.id, "retry")}
                          >
                            re-run
                          </button>
                        )}
                        {(run.status === "failed" || run.status === "canceled") && (
                          <button
                            className="text-btn"
                            disabled={busyId === run.id}
                            onClick={(e) => relaunch(e, run.id, "resume")}
                          >
                            resume
                          </button>
                        )}
                        {rowErr?.id === run.id && <span className="muted row-err">{rowErr.msg}</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
          {runs.length >= PAGE && !exhausted && (
            <div className="load-more">
              <button className="text-btn" onClick={loadMore} disabled={loadingMore}>
                {loadingMore ? "loading…" : "load more"}
              </button>
            </div>
          )}
        </>
      )}
    </>
  );
}
