import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { get, HttpError, post, usePoll } from "./api";
import DagView from "./DagView";
import type { NodeStatus } from "./DagView";
import GanttChart from "./GanttChart";
import MetaList from "./MetaList";
import StatusDot from "./StatusDot";
import { GlyphShape } from "./StatusGlyph";
import { hits, logRows, marks } from "./log";
import { useMay } from "./role";
import type { Source } from "./log";
import type {
  EventLevel,
  JobSummary,
  OpLog,
  OpRun,
  OpStatus,
  OpSummary,
  ResumePreview,
  Run,
  RunEvent,
} from "./types";
import {
  clockTime,
  durationMs,
  fmtDuration,
  isTerminal,
  opBadge,
  outputLine,
  relTime,
  shortId,
} from "./util";

const plan = (p: ResumePreview) => `${p.rerun.length} to re-run · ${p.reuse.length} reused`;

// an instance's label is its index, or the element itself on an op that names
// its instances by them — which is what makes a partitioned asset's instances
// read as `daily_orders[2026-01-05]`. so the label is whatever is in the
// brackets, and what makes it an instance is that its parent is mapped
const INSTANCE = /^(.*)\[(.+)\]$/;

// a mapped op writes no op_runs row of its own: its instances are the record,
// so the ui rebuilds the group from their bracketed names
function fanOut(job: JobSummary | null, ops: OpRun[]): Map<string, OpRun[]> {
  const mapped = new Set((job?.ops ?? []).filter((o) => o.mapped_over).map((o) => o.name));
  const out = new Map<string, OpRun[]>();
  for (const o of ops) {
    const m = INSTANCE.exec(o.op);
    if (!m || !mapped.has(m[1])) continue;
    const group = out.get(m[1]);
    if (group) group.push(o);
    else out.set(m[1], [o]);
  }
  // element order, which is what the collected output is in; a key-labelled
  // instance has no index to order by, so its label is the order
  const label = (o: OpRun) => INSTANCE.exec(o.op)![2];
  for (const group of out.values())
    group.sort((a, b) => {
      const [x, y] = [label(a), label(b)];
      const [i, j] = [Number(x), Number(y)];
      return Number.isNaN(i) || Number.isNaN(j) ? x.localeCompare(y) : i - j;
    });
  return out;
}

// the worst thing any instance is doing is what the mapped op is doing:
// it succeeds only if all of them did
function rollup(rows: OpRun[]): OpStatus {
  for (const st of ["failed", "canceled", "running", "pending", "skipped"] as const)
    if (rows.some((r) => r.status === st)) return st;
  return "success";
}

// how many instances a mapped op made, from the expansion event — the only
// place the count lives when it expanded over an empty array
function expansions(events: RunEvent[]): Map<string, number> {
  const out = new Map<string, number>();
  for (const e of events) {
    if (e.kind !== "op_expanded" || !e.op) continue;
    const n = (e.data as { instances?: number } | null)?.instances;
    if (typeof n === "number") out.set(e.op, n);
  }
  return out;
}

// captured output is paged, and a finished run's poll only fires once — so
// this drains the cursor rather than showing the first page and stopping.
// bounded: an op is capped at 10,000 lines and a run may hold several, and a
// page that keeps fetching for a minute is not a page anyone is reading
const LOG_PAGE = 2000;
const LOG_PAGES = 8;

async function pullOutput(
  id: string,
  cursor: { current: number },
  onRows: (fn: (prev: OpLog[]) => OpLog[]) => void,
) {
  for (let page = 0; page < LOG_PAGES; page++) {
    let batch: OpLog[];
    try {
      const r = await get<{ logs: OpLog[] }>(
        `/api/runs/${id}/logs?after=${cursor.current}&limit=${LOG_PAGE}`,
      );
      batch = r.logs;
    } catch {
      return;
    }
    if (batch.length === 0) return;
    cursor.current = batch[batch.length - 1].id;
    onRows((prev) => [...prev, ...batch]);
    if (batch.length < LOG_PAGE) return;
  }
}

export default function RunPage() {
  const { id } = useParams();
  if (!id) return null;
  return <RunView key={id} id={id} />;
}

function RunView({ id }: { id: string }) {
  // cancelling, re-running and resuming are driving what is happening now,
  // which is an operator's
  const mayDrive = useMay("operator");
  const nav = useNavigate();
  const [run, setRun] = useState<Run | null>(null);
  const [ops, setOps] = useState<OpRun[]>([]);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [output, setOutput] = useState<OpLog[]>([]);
  const [filter, setFilter] = useState<"all" | "logs">("all");
  // what an op printed and what hestan said about it, together by default:
  // hiding either one behind a click is how you miss the line that explains
  // the failure above it
  const [source, setSource] = useState<Source>("both");
  const [level, setLevel] = useState<"all" | EventLevel>("all");
  const [find, setFind] = useState("");
  const [only, setOnly] = useState(false);
  // pinned to the newest line while the run is live, and released the moment
  // you scroll up: a pane that yanks you back is worse than one that does not
  // follow at all
  const [follow, setFollow] = useState(true);
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
  const lastLog = useRef(0);
  const doneRef = useRef(false);
  const logRef = useRef<HTMLDivElement>(null);

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
        )
        .then(() => pullOutput(id, lastLog, setOutput));
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
    if (el && follow && !done) el.scrollTop = el.scrollHeight;
  }, [events, output, follow, done]);

  // scrolling away from the tail turns following off; scrolling back to it
  // does not turn it on, because that would be the yank again
  const onScroll = () => {
    const el = logRef.current;
    if (!el) return;
    const atEnd = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    if (!atEnd && follow) setFollow(false);
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
  const selectedOp = opSel === null ? undefined : ops.find((o) => o.op === opSel);
  // an io handle points at where the value lives; anything else is the value
  const selectedOutput = outputLine(selectedOp?.output);
  // what the op said about what it produced, rendered by type below the
  // output, with what each number did since the last run of this op beside it
  const selectedMeta = selectedOp?.metadata ?? null;
  // an isolated op carries the process it is running in, and only while it is:
  // the terminal write hands the pid back
  const selectedPid = selectedOp?.pid ?? null;
  const instances = fanOut(job, ops);
  const expanded = expansions(events);
  // a subset run (memoized asset build) writes no op_runs row for what it
  // skipped; a mapped op never has one, and reads through its instances
  const dagStatuses: Record<string, NodeStatus> = job
    ? Object.fromEntries(
        job.ops.map((o) => {
          const group = instances.get(o.name);
          if (group) return [o.name, rollup(group)];
          // expanded over an empty array: it ran, with nothing to do
          if (expanded.get(o.name) === 0) return [o.name, "success"];
          return [o.name, statuses[o.name] ?? "absent"];
        }),
      )
    : {};
  const dagNodes = (job?.ops ?? []).map((o) => {
    if (!o.mapped_over) return { ...o, badge: opBadge(o) };
    const n = expanded.get(o.name) ?? instances.get(o.name)?.length;
    return { ...o, badge: opBadge(o, n === undefined ? "×n" : `×${n}`) };
  });
  // the gantt lists instances as their own rows, and hangs a mapped op's
  // dependents off them, since the parent itself has no span to draw
  const ganttOps: OpSummary[] = job
    ? [
        ...job.ops
          .filter((o) => o.name in statuses)
          .map((o) => ({
            ...o,
            deps: o.deps.flatMap((d) => instances.get(d)?.map((r) => r.op) ?? [d]),
          })),
        ...[...instances].flatMap(([parent, rows]) => {
          const op = job.ops.find((o) => o.name === parent);
          return op ? rows.map((r) => ({ ...op, name: r.op })) : [];
        }),
      ]
    : [];
  const shown = logRows(events, output, { source, kind: filter, level, op: opSel, find, only });
  // counted over what the other filters left, since that is what is on screen
  const matches = find === "" ? 0 : shown.filter((r) => hits(r, find)).length;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>
            <Link to={`/jobs/${encodeURIComponent(run.job)}`}>{run.job}</Link>{" "}
            <span className="mono secondary">{shortId(run.id)}</span>
          </h1>
          <p className="muted">
            {run.trigger}
            {/* "manual, by whom": absent on everything a loop launched, and on
                every launch through a deployment that checks nobody */}
            {run.actor && <> by {run.actor}</>} · created {relTime(run.created_at)}
            {/* the hour this run stands for, which is not when it started once
                a schedule is catching up or a held fire drains */}
            {run.scheduled_for && (
              <> · for {relTime(run.scheduled_for)}</>
            )}
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
          {run.error && <p className="mono muted op-err">{run.error}</p>}
        </div>
        <div className="run-actions">
          <div className="run-side">
            <span className="pill">
              <StatusDot status={run.status} />
            </span>
            {/* not a launch: it opens the job's launchpad on this run's
                params and tags, because the commonest real launch is that run
                again with one field changed */}
            <button
              className="text-btn"
              onClick={() =>
                nav(`/jobs/${encodeURIComponent(run.job)}?from=${encodeURIComponent(run.id)}`)
              }
            >
              clone
            </button>
            {mayDrive &&
              (done ? (
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
              ))}
          </div>
          {resumable && preview && <p className="muted">resume: {plan(preview)}</p>}
          {resumable && previewError && <p className="muted">no resume: {previewError}</p>}
          {launchError && <p className="muted">{launchError}</p>}
          {cancelError && <p className="muted">cancel failed: {cancelError}</p>}
        </div>
      </div>

      {job && (
        <DagView
          nodes={dagNodes}
          statuses={dagStatuses}
          selected={opSel}
          onSelect={(op) => setOpSel((prev) => (prev === op ? null : op))}
        />
      )}

      {opSel && instances.has(opSel) && (
        <div className="op-instances">
          {instances.get(opSel)!.map((r) => (
            <span key={r.op} className="op-instance">
              <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
                <GlyphShape status={r.status} />
              </svg>
              <span className="mono">{r.op}</span>
              <span className="muted">{r.status}</span>
            </span>
          ))}
        </div>
      )}

      {selectedPid !== null && (
        <p className="muted dag-action">
          isolated · running in process <span className="mono">{selectedPid}</span>
        </p>
      )}

      {opSel && selectedOutput !== null && (
        <p className="muted dag-action">
          output <span className="mono">{selectedOutput}</span>
        </p>
      )}

      {selectedMeta && (
        <div className="dag-action">
          <MetaList metadata={selectedMeta} deltas={selectedOp?.deltas} />
        </div>
      )}

      {mayDrive && done && opSel && (
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

      {job && <GanttChart ops={ganttOps} opRuns={ops} />}

      <h2>
        log
        <span className="log-filter">
          {/* what an op printed is a different thing from what hestan said
              about it, and reading them together is usually the point */}
          {(["events", "output", "both"] as const).map((s) => (
            <button
              key={s}
              className={source === s ? "text-btn active" : "text-btn"}
              onClick={() => setSource(s)}
            >
              {s}
            </button>
          ))}
          {/* an event kind filter has nothing to filter when no events are shown */}
          {source !== "output" && (
            <>
              <span className="filter-sep" />
              {(["all", "logs"] as const).map((f) => (
                <button
                  key={f}
                  className={filter === f ? "text-btn active" : "text-btn"}
                  onClick={() => setFilter(f)}
                >
                  {f}
                </button>
              ))}
            </>
          )}
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
          {/* off once the run is over: there is no newest line to pin to */}
          {!done && (
            <>
              <span className="filter-sep" />
              <button
                className={follow ? "text-btn active" : "text-btn"}
                onClick={() => setFollow((f) => !f)}
              >
                follow
              </button>
            </>
          )}
        </span>
        <span className="log-find">
          <input
            className="filter-input"
            value={find}
            placeholder="find in log"
            onChange={(e) => setFind(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") setFind("");
            }}
          />
          {find !== "" && (
            <>
              <span className="muted">
                {matches} line{matches === 1 ? "" : "s"}
              </span>
              <button
                className={only ? "text-btn active" : "text-btn"}
                onClick={() => setOnly((o) => !o)}
                disabled={matches === 0}
              >
                only
              </button>
            </>
          )}
        </span>
        {output.length > 0 && (
          <a
            className="log-download"
            href={`/api/runs/${id}/logs${opSel ? `/download?op=${encodeURIComponent(opSel)}` : "/download"}`}
          >
            download output
          </a>
        )}
      </h2>
      <div className="log" ref={logRef} onScroll={onScroll}>
        {shown.length === 0 && (
          <span className="muted">
            {events.length === 0 && output.length === 0
              ? "nothing yet"
              : find !== "" && only
                ? `nothing says ${find}`
                : "nothing matches the filter"}
          </span>
        )}
        {shown.map((r) => (
          <div
            key={r.key}
            className={
              r.marker
                ? "ev ev-marker"
                : r.level === null
                  ? "ev"
                  : r.source === "event" && r.tag !== null
                    ? "ev ev-system"
                    : `ev ev-${r.level}`
            }
          >
            <span className="ev-ts">{clockTime(r.ts)}</span>{" "}
            <span className="ev-op">
              [{r.op ?? "run"}
              {/* the attempt only when there was more than one, since a retry's
                  output and the output it replaced read identically otherwise */}
              {r.attempt !== null && ` #${r.attempt}`}]
            </span>{" "}
            {r.tag !== null && <span className="ev-kind">{r.tag} </span>}
            {/* pieces, never an html string: a log line is whatever an op
                printed, and this ui builds no element out of one */}
            {marks(r.message, find).map((piece, i) =>
              piece.hit ? (
                <mark key={i}>{piece.text}</mark>
              ) : (
                <span key={i}>{piece.text}</span>
              ),
            )}
          </div>
        ))}
      </div>
    </>
  );
}
