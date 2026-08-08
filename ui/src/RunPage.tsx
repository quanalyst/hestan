import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { get, HttpError, post, usePoll } from "./api";
import DagView from "./DagView";
import type { NodeStatus } from "./DagView";
import GanttChart from "./GanttChart";
import StatusDot from "./StatusDot";
import type { EventLevel, JobSummary, OpRun, OpStatus, ResumePreview, Run, RunEvent } from "./types";
import { clockTime, durationMs, fmtDuration, isTerminal, relTime, shortId } from "./util";

const plan = (p: ResumePreview) => `${p.rerun.length} to re-run · ${p.reuse.length} reused`;

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
  const [pending, setPending] = useState<null | "re-run" | "resume">(null);
  const [launchError, setLaunchError] = useState<string | null>(null);
  const [preview, setPreview] = useState<ResumePreview | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [fromPlan, setFromPlan] = useState<ResumePreview | null>(null);
  const [fromError, setFromError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const [missing, setMissing] = useState(false);
  const lastSeq = useRef(0);
  const doneRef = useRef(false);
  const logRef = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  const done = run !== null && isTerminal(run.status);
  // a run that ended badly is the one resume continues; any finished run can be
  // re-run from a chosen op
  const resumable = run !== null && (run.status === "failed" || run.status === "canceled");

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

  // what resume would do, before the click rather than after it
  useEffect(() => {
    if (!resumable) {
      setPreview(null);
      setPreviewError(null);
      return;
    }
    get<ResumePreview>(`/api/runs/${id}/resume_preview`)
      .then((p) => {
        setPreview(p);
        setPreviewError(null);
      })
      .catch((e) => {
        setPreview(null);
        setPreviewError(e instanceof Error ? e.message : String(e));
      });
  }, [id, resumable]);

  useEffect(() => {
    if (!done || !opSel) {
      setFromPlan(null);
      setFromError(null);
      return;
    }
    get<ResumePreview>(`/api/runs/${id}/resume_preview?from=${encodeURIComponent(opSel)}`)
      .then((p) => {
        setFromPlan(p);
        setFromError(null);
      })
      .catch((e) => {
        setFromPlan(null);
        setFromError(e instanceof Error ? e.message : String(e));
      });
  }, [id, done, opSel]);

  useEffect(() => {
    const el = logRef.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  }, [events]);

  const onScroll = () => {
    const el = logRef.current;
    if (el) stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  };

  // re-run redoes the whole job; resume continues this run, optionally from a
  // chosen op. both land on the new run's page
  const relaunch = async (kind: "re-run" | "resume", from?: string[]) => {
    setPending(kind);
    setLaunchError(null);
    try {
      const path = kind === "resume" ? "resume" : "retry";
      const r = await post<{ run_id: string }>(`/api/runs/${id}/${path}`, from && { from });
      nav(`/runs/${r.run_id}`);
    } catch (e) {
      setLaunchError(`${kind} failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setPending(null);
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
            {run.resumed_from && (
              <>
                {" "}
                · continues{" "}
                <Link className="head-link mono" to={`/runs/${run.resumed_from}`}>
                  {shortId(run.resumed_from)}
                </Link>
              </>
            )}
          </p>
        </div>
        <div className="run-actions">
          <div className="run-side">
            <span className="pill">
              <StatusDot status={run.status} />
            </span>
            {done ? (
              <>
                <button
                  className="text-btn"
                  onClick={() => relaunch("re-run")}
                  disabled={pending !== null}
                >
                  re-run
                </button>
                {resumable && (
                  <button
                    className="text-btn"
                    onClick={() => relaunch("resume")}
                    disabled={pending !== null}
                  >
                    resume
                  </button>
                )}
              </>
            ) : (
              <button className="text-btn" onClick={cancel} disabled={canceling}>
                cancel
              </button>
            )}
          </div>
          {resumable && preview && <p className="muted">resume: {plan(preview)}</p>}
          {resumable && previewError && <p className="muted">no resume: {previewError}</p>}
          {launchError && <p className="muted">{launchError}</p>}
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

      {done && opSel && (
        <p className="muted dag-action">
          {fromError ? (
            `cannot re-run from ${opSel}: ${fromError}`
          ) : (
            <>
              <button
                className="text-btn"
                onClick={() => relaunch("resume", [opSel])}
                disabled={pending !== null}
              >
                re-run from {opSel}
              </button>
              {fromPlan && <> · {plan(fromPlan)}</>}
            </>
          )}
        </p>
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
