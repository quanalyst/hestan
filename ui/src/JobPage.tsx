import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { get, HttpError, post, usePoll } from "./api";
import DagView from "./DagView";
import DurationBars from "./DurationBars";
import OpInspector from "./OpInspector";
import StatusDot from "./StatusDot";
import { GlyphShape } from "./StatusGlyph";
import type { Status } from "./StatusGlyph";
import TimelinePlot, { futureWindowSecs } from "./TimelinePlot";
import type { JobState, JobSummary, OpStat, Run, Tick, TickOutcome, UpcomingSchedule } from "./types";
import { durationMs, fmtDuration, relTime, shortId, untilTime } from "./util";

const STATS_RUNS = 50;

const TICK_GLYPH = {
  fired: "success",
  error: "failed",
  skipped: "skipped",
  deferred: "pending",
} as const satisfies Record<TickOutcome, Status>;

const paramsKey = (job: string) => `hestan.params.${job}`;

function loadParams(job: string): string {
  try {
    return localStorage.getItem(paramsKey(job)) ?? "";
  } catch {
    return "";
  }
}

function saveParams(job: string, text: string) {
  try {
    if (text.trim()) localStorage.setItem(paramsKey(job), text);
    else localStorage.removeItem(paramsKey(job));
  } catch {
    // storage unavailable; launching still works
  }
}

function validJson(text: string): boolean {
  if (!text.trim()) return true; // empty launches with {}
  try {
    JSON.parse(text);
    return true;
  } catch {
    return false;
  }
}

export default function JobPage() {
  const { name } = useParams();
  if (!name) return null;
  // keyed so a job switch remounts: no state leaks across it
  return <JobView key={name} name={name} />;
}

function JobView({ name }: { name: string }) {
  const nav = useNavigate();
  const [job, setJob] = useState<JobSummary | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [ticks, setTicks] = useState<Tick[]>([]);
  const [stats, setStats] = useState<OpStat[] | null>(null);
  const [states, setStates] = useState<JobState[]>([]);
  const [tlRuns, setTlRuns] = useState<Run[]>([]);
  const [upcoming, setUpcoming] = useState<UpcomingSchedule[]>([]);
  const [windowSecs, setWindowSecs] = useState(21600);
  const [opSel, setOpSel] = useState<string | null>(null);
  const [paramsOpen, setParamsOpen] = useState(false);
  const [paramsText, setParamsText] = useState(() => loadParams(name));
  const [launching, setLaunching] = useState(false);
  const [launchError, setLaunchError] = useState<string | null>(null);
  const [schedError, setSchedError] = useState<string | null>(null);
  const [missing, setMissing] = useState(false);
  const enc = encodeURIComponent(name);

  usePoll(
    () => {
      get<JobSummary>(`/api/jobs/${enc}`)
        .then(setJob)
        .catch((e) => {
          if (e instanceof HttpError && e.status === 404) setMissing(true);
        });
      get<{ runs: Run[] }>(`/api/runs?job=${enc}&limit=50`)
        .then((r) => setRuns(r.runs))
        .catch(() => {});
      get<{ ticks: Tick[] }>(`/api/schedules/ticks?job=${enc}&limit=5`)
        .then((r) => setTicks(r.ticks))
        .catch(() => {});
      get<{ ops: OpStat[] }>(`/api/jobs/${enc}/op_stats?runs=${STATS_RUNS}`)
        .then((r) => setStats(r.ops))
        .catch(() => {});
      get<{ states: JobState[] }>(`/api/jobs/${enc}/state`)
        .then((r) => setStates(r.states))
        .catch(() => {});
    },
    missing ? null : 5000,
    [name, missing],
  );

  // wider fetch for the timeline; the recent-runs table keeps its own latest-50
  usePoll(
    () => {
      const since = new Date(Date.now() - windowSecs * 1000).toISOString();
      get<{ runs: Run[] }>(`/api/runs?job=${enc}&since=${encodeURIComponent(since)}&limit=500`)
        .then((r) => setTlRuns(r.runs))
        .catch(() => {});
    },
    missing ? null : 10_000,
    [name, windowSecs, missing],
  );

  usePoll(
    () => {
      get<{ upcoming: UpcomingSchedule[] }>(`/api/schedules/upcoming?window=${futureWindowSecs(windowSecs)}`)
        .then((r) => setUpcoming(r.upcoming.filter((u) => u.job === name)))
        .catch(() => {});
    },
    missing ? null : 30_000,
    [name, windowSecs, missing],
  );

  const paramsValid = validJson(paramsText);

  const launch = async () => {
    setLaunching(true);
    setLaunchError(null);
    try {
      const text = paramsText.trim();
      saveParams(name, paramsText);
      const r = await post<{ run_id: string }>(
        `/api/jobs/${enc}/runs`,
        text ? { params: JSON.parse(text) } : undefined,
      );
      nav(`/runs/${r.run_id}`);
    } catch (e) {
      setLaunchError(e instanceof Error ? e.message : String(e));
    } finally {
      setLaunching(false);
    }
  };

  const setPaused = async (expr: string, paused: boolean) => {
    setSchedError(null);
    const flip = (j: JobSummary | null, p: boolean) =>
      j && { ...j, schedules: j.schedules.map((s) => (s.expr === expr ? { ...s, paused: p } : s)) };
    setJob((j) => flip(j, paused));
    try {
      await post<{ ok: boolean }>("/api/schedules/state", { job: name, expr, paused });
    } catch (e) {
      setJob((j) => flip(j, !paused));
      setSchedError(e instanceof Error ? e.message : String(e));
    }
  };

  if (missing) return <p className="muted">job not found</p>;
  if (!job) return <p className="muted">loading…</p>;

  const paramTypes = [...new Set(job.ops.map((o) => o.params_type).filter((t): t is string => t !== null))];
  // only what departs from the defaults, as with a schedule's tz
  const policy = [
    job.max_parallel === null ? null : `max_parallel ${job.max_parallel}`,
    job.overlap === "skip" ? null : `overlap ${job.overlap}`,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{job.name}</h1>
          {job.description && <p className="secondary">{job.description}</p>}
          {policy && <p className="muted">{policy}</p>}
        </div>
        <div className="run-actions">
          <button onClick={launch} disabled={launching || !paramsValid}>
            launch run
          </button>
          <button
            className={paramsOpen ? "text-btn active" : "text-btn"}
            onClick={() => setParamsOpen((o) => !o)}
          >
            params
          </button>
          {paramsOpen && (
            <div className="params-block">
              {paramTypes.length > 0 && (
                <div className="params-label">
                  params_type <span className="mono">{paramTypes.join(" · ")}</span>
                </div>
              )}
              <textarea
                className="params-input"
                value={paramsText}
                placeholder="{}"
                spellCheck={false}
                onChange={(e) => setParamsText(e.target.value)}
              />
              {!paramsValid && <p className="muted params-hint">invalid json</p>}
            </div>
          )}
          {!paramsValid && !paramsOpen && <p className="muted">saved params are invalid json — open params to fix</p>}
          {launchError && <p className="muted">launch failed: {launchError}</p>}
        </div>
      </div>

      <h2>graph</h2>
      <DagView
        nodes={job.ops}
        selected={opSel}
        onSelect={(op) => setOpSel((prev) => (prev === op ? null : op))}
      />
      {opSel && (
        <OpInspector
          ops={job.ops}
          name={opSel}
          stat={stats?.find((s) => s.op === opSel)}
          state={states.find((s) => s.op === opSel)}
          onClose={() => setOpSel(null)}
        />
      )}

      {job.ops.some((o) => o.output_type) && (
        <>
          <h2>ops</h2>
          <div className="op-list">
            {job.ops.map((op) => (
              <div key={op.name} className="op-row">
                <span className="mono">{op.name}</span>
                {op.output_type && <span className="mono muted">{` -> ${op.output_type}`}</span>}
              </div>
            ))}
          </div>
        </>
      )}

      {job.schedules.length > 0 && (
        <>
          <h2>schedules</h2>
          {job.schedules.map((s) => (
            <div key={s.expr} className="sched-row">
              <span className="mono">{s.expr}</span>
              {s.tz !== "UTC" && <span className="muted">{s.tz}</span>}
              <span className="muted">
                {s.paused ? "paused" : s.next_fire ? `next ${untilTime(s.next_fire)}` : "next —"}
              </span>
              <button className="text-btn" onClick={() => setPaused(s.expr, !s.paused)}>
                {s.paused ? "resume" : "pause"}
              </button>
            </div>
          ))}
          {schedError && <p className="muted">schedule update failed: {schedError}</p>}
          {ticks.length > 0 && (
            <>
              <div className="sub-label">recent ticks</div>
              {ticks.map((t) => (
                <div key={t.id} className="tick-row">
                  <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
                    <GlyphShape status={TICK_GLYPH[t.outcome]} />
                  </svg>
                  <span className="muted">{relTime(t.scheduled_for)}</span>
                  {t.run_id && (
                    <Link className="mono" to={`/runs/${t.run_id}`}>
                      {shortId(t.run_id)}
                    </Link>
                  )}
                  {t.error && <span className="muted">{t.error}</span>}
                </div>
              ))}
            </>
          )}
        </>
      )}

      <TimelinePlot
        jobs={[job.name]}
        runs={tlRuns}
        upcoming={upcoming}
        windowSecs={windowSecs}
        onWindow={setWindowSecs}
      />

      <h2>
        recent runs
        <Link className="head-link" to={`/runs?job=${encodeURIComponent(job.name)}`}>
          all runs
        </Link>
      </h2>
      <DurationBars runs={runs} />
      {runs.length === 0 ? (
        <p className="muted">no runs yet — launch one to get started</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>status</th>
              <th>run</th>
              <th>trigger</th>
              <th>started</th>
              <th className="num">duration</th>
            </tr>
          </thead>
          <tbody>
            {runs.slice(0, 10).map((run) => (
              <tr key={run.id} onClick={() => nav(`/runs/${run.id}`)}>
                <td>
                  <StatusDot status={run.status} />
                </td>
                <td className="mono">{shortId(run.id)}</td>
                <td>{run.trigger}</td>
                <td className="muted">{relTime(run.started_at ?? run.created_at)}</td>
                <td className="num">{fmtDuration(durationMs(run))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
