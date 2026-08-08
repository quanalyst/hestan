import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { get, HttpError, post, usePoll } from "./api";
import DagView from "./DagView";
import type { NodeStatus } from "./DagView";
import GanttChart from "./GanttChart";
import StatusDot from "./StatusDot";
import type { EventLevel, JobSummary, OpRun, OpStatus, Run, RunEvent } from "./types";
import { clockTime, durationMs, fmtDuration, isTerminal, relTime, shortId } from "./util";

export default function RunPage() {
  const { id } = useParams();
  if (!id) return null;
  return <RunView key={id} id={id} />;
}

function RunView({ id }: { id: string }) {
  const nav = useNavigate();
  const [run, setRun] = useState<Run | null>(null);
  const [ops, setOps] = useState<OpRun[]>([]);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [filter, setFilter] = useState<"all" | "logs">("all");
  const [level, setLevel] = useState<"all" | EventLevel>("all");
  const [opSel, setOpSel] = useState<string | null>(null);
  const [job, setJob] = useState<JobSummary | null>(null);
  const [retrying, setRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const [missing, setMissing] = useState(false);
  const lastSeq = useRef(0);
  const doneRef = useRef(false);
  const logRef = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  const done = run !== null && isTerminal(run.status);

  usePoll(
    () => {
      // run before events, or the tick that sees a terminal status misses the
      // closing event the backend committed just before it
      get<{ run: Run; ops: OpRun[] }>(`/api/runs/${id}`)
        .then((r) => {
          const terminal = isTerminal(r.run.status);
          // ignore out-of-order responses once a terminal state has landed
          if (doneRef.current && !terminal) return;
          doneRef.current = terminal;
          setRun(r.run);
          setOps(r.ops);
        })
        .catch((e) => {
          if (e instanceof HttpError && e.status === 404) setMissing(true);
        })
        .then(() =>
          get<{ events: RunEvent[] }>(`/api/runs/${id}/events?after=${lastSeq.current}`)
            .then((r) => {
              if (r.events.length === 0) return;
              lastSeq.current = Math.max(lastSeq.current, r.events[r.events.length - 1].seq);
              setEvents((prev) => {
                const seen = prev.length ? prev[prev.length - 1].seq : 0;
                const fresh = r.events.filter((e) => e.seq > seen);
                return fresh.length ? [...prev, ...fresh] : prev;
              });
            })
            .catch(() => {}),
        );
    },
    done || missing ? null : 1500,
    [id, done, missing],
  );

  const jobName = run?.job;
  useEffect(() => {
    if (!jobName) return;
    get<JobSummary>(`/api/jobs/${encodeURIComponent(jobName)}`)
      .then(setJob)
      .catch(() => {});
  }, [jobName]);

  useEffect(() => {
    const el = logRef.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  }, [events]);

  const onScroll = () => {
    const el = logRef.current;
    if (el) stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  };

  const retry = async () => {
    setRetrying(true);
    setRetryError(null);
    try {
      const r = await post<{ run_id: string }>(`/api/runs/${id}/retry`);
      nav(`/runs/${r.run_id}`);
    } catch (e) {
      setRetryError(e instanceof Error ? e.message : String(e));
    } finally {
      setRetrying(false);
    }
  };

  const cancel = async () => {
    setCanceling(true);
    setCancelError(null);
    try {
      await post<{ ok: boolean }>(`/api/runs/${id}/cancel`);
      // left disabled: the next poll lands the canceled status and swaps in re-run
    } catch (e) {
      // 409 means the run beat us to a terminal state, which the poll will show
      if (e instanceof HttpError && e.status === 409) return;
      setCancelError(e instanceof Error ? e.message : String(e));
      setCanceling(false);
    }
  };

  if (missing) return <p className="muted">run not found</p>;
  if (!run) return <p className="muted">loading…</p>;

  const dur = durationMs(run);
  const statuses: Record<string, OpStatus> = Object.fromEntries(ops.map((o) => [o.op, o.status]));
  // a subset run (memoized asset build) writes no op_runs row for what it skipped
  const dagStatuses: Record<string, NodeStatus> = job
    ? Object.fromEntries(job.ops.map((o) => [o.name, statuses[o.name] ?? "absent"]))
    : {};
  const shown = events
    .filter((e) => filter === "all" || e.kind === "log")
    .filter((e) => level === "all" || e.level === level)
    .filter((e) => opSel === null || e.op === opSel);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>
            <Link to={`/jobs/${encodeURIComponent(run.job)}`}>{run.job}</Link>{" "}
            <span className="mono secondary">{shortId(run.id)}</span>
          </h1>
          <p className="muted">
            {run.trigger} · created {relTime(run.created_at)}
            {dur !== null && <> · took {fmtDuration(dur)}</>}
          </p>
        </div>
        <div className="run-actions">
          <div className="run-side">
            <span className="pill">
              <StatusDot status={run.status} />
            </span>
            {done ? (
              <button className="text-btn" onClick={retry} disabled={retrying}>
                re-run
              </button>
            ) : (
              <button className="text-btn" onClick={cancel} disabled={canceling}>
                cancel
              </button>
            )}
          </div>
          {retryError && <p className="muted">re-run failed: {retryError}</p>}
          {cancelError && <p className="muted">cancel failed: {cancelError}</p>}
        </div>
      </div>

      {job && (
        <DagView
          nodes={job.ops}
          statuses={dagStatuses}
          selected={opSel}
          onSelect={(op) => setOpSel((prev) => (prev === op ? null : op))}
        />
      )}

      {job && <GanttChart ops={job.ops.filter((o) => o.name in statuses)} opRuns={ops} />}

      <h2>
        log
        <span className="log-filter">
          {(["all", "logs"] as const).map((f) => (
            <button
              key={f}
              className={filter === f ? "text-btn active" : "text-btn"}
              onClick={() => setFilter(f)}
            >
              {f}
            </button>
          ))}
          <span className="filter-sep" />
          {(["all", "info", "warn", "error"] as const).map((l) => (
            <button
              key={l}
              className={level === l ? "text-btn active" : "text-btn"}
              onClick={() => setLevel(l)}
            >
              {l}
            </button>
          ))}
          {opSel && (
            <>
              <span className="filter-sep" />
              <button className="text-btn active" onClick={() => setOpSel(null)}>
                {opSel} ×
              </button>
            </>
          )}
        </span>
      </h2>
      <div className="log" ref={logRef} onScroll={onScroll}>
        {shown.length === 0 && (
          <span className="muted">{events.length === 0 ? "no events yet" : "no events match the filter"}</span>
        )}
        {shown.map((e) => (
          <div key={e.seq} className={e.kind === "log" ? `ev ev-${e.level}` : "ev ev-system"}>
            <span className="ev-ts">{clockTime(e.ts)}</span>{" "}
            <span className="ev-op">[{e.op ?? "run"}]</span>{" "}
            {e.kind !== "log" && <span className="ev-kind">{e.kind} </span>}
            {e.message}
          </div>
        ))}
      </div>
    </>
  );
}
