import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { get, usePoll } from "./api";
import MicroBars from "./MicroBars";
import type { MicroBar } from "./MicroBars";
import StatusDot from "./StatusDot";
import TimelinePlot, { futureWindowSecs } from "./TimelinePlot";
import type { JobSummary, Run, UpcomingSchedule } from "./types";
import { durationMs, fmtDuration, relTime } from "./util";

// newest first from the api, oldest on the left in the chart
const runBars = (runs: Run[]): MicroBar[] =>
  runs
    .flatMap((r) => {
      const ms = durationMs(r);
      return ms === null ? [] : [{ id: r.id, value: ms, status: r.status }];
    })
    .slice(0, 20)
    .reverse();

export default function JobsPage() {
  const nav = useNavigate();
  const [jobs, setJobs] = useState<JobSummary[] | null>(null);
  const [runs, setRuns] = useState<Run[] | null>(null);
  const [upcoming, setUpcoming] = useState<UpcomingSchedule[]>([]);
  const [windowSecs, setWindowSecs] = useState(21600);

  usePoll(
    () => {
      get<{ jobs: JobSummary[] }>("/api/jobs")
        .then((r) => setJobs(r.jobs))
        .catch(() => {});
    },
    5000,
    [],
  );

  // one window fetch feeds the statline, the timeline, and the sparklines
  usePoll(
    () => {
      const since = new Date(Date.now() - windowSecs * 1000).toISOString();
      get<{ runs: Run[] }>(`/api/runs?since=${encodeURIComponent(since)}&limit=2000`)
        .then((r) => setRuns(r.runs))
        .catch(() => {});
    },
    10_000,
    [windowSecs],
  );

  usePoll(
    () => {
      get<{ upcoming: UpcomingSchedule[] }>(`/api/schedules/upcoming?window=${futureWindowSecs(windowSecs)}`)
        .then((r) => setUpcoming(r.upcoming))
        .catch(() => {});
    },
    30_000,
    [windowSecs],
  );

  if (!jobs) return <p className="muted">loading…</p>;

  const winStart = Date.now() - windowSecs * 1000;
  const winRuns = (runs ?? []).filter((r) => new Date(r.created_at).getTime() >= winStart);
  // canceled excluded: its duration measures when someone hit stop
  const finished = winRuns.filter((r) => r.status === "success" || r.status === "failed");
  const durs = finished
    .map(durationMs)
    .filter((d): d is number => d !== null)
    .sort((a, b) => a - b);
  const running = winRuns.filter((r) => r.status === "running").length;

  const byJob = new Map<string, Run[]>();
  for (const r of winRuns) {
    const list = byJob.get(r.job);
    if (list) list.push(r);
    else byJob.set(r.job, [r]);
  }

  return (
    <>
      <h1>Jobs</h1>

      <div className="statline">
        {winRuns.length === 0 ? (
          <span>no runs in the last {windowSecs === 3600 ? "hour" : windowSecs === 21600 ? "6 hours" : "24 hours"}</span>
        ) : (
          <>
            <b>{winRuns.length}</b> {winRuns.length === 1 ? "run" : "runs"}
            {finished.length > 0 && (
              <>
                {" · "}
                <b>
                  {Math.round((100 * finished.filter((r) => r.status === "success").length) / finished.length)}%
                </b>{" "}
                success
              </>
            )}
            {durs.length > 0 && (
              <>
                {" · "}p95 <b>{fmtDuration(durs[Math.max(0, Math.ceil(durs.length * 0.95) - 1)])}</b>
              </>
            )}
            {" · "}
            <b>{running}</b> running
          </>
        )}
      </div>

      <TimelinePlot
        jobs={jobs.map((j) => j.name)}
        runs={winRuns}
        upcoming={upcoming}
        windowSecs={windowSecs}
        onWindow={setWindowSecs}
      />

      {jobs.length > 0 && (
        <>
          <h2>jobs</h2>
          <table>
            <thead>
              <tr>
                <th>name</th>
                <th>description</th>
                <th className="num">ops</th>
                <th>schedules</th>
                <th>recent</th>
                <th>last run</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => {
                const run = job.last_run;
                return (
                  <tr key={job.name} onClick={() => nav(`/jobs/${encodeURIComponent(job.name)}`)}>
                    <td>
                      {job.name}
                      {job.overdue && <span className="tag">overdue</span>}
                    </td>
                    <td className="secondary">{job.description ?? "—"}</td>
                    <td className="num">{job.ops.length}</td>
                    <td className="mono">
                      {job.schedules.length === 0
                        ? "—"
                        : job.schedules.map((s, i) => (
                            <span key={s.expr} className={s.paused ? "muted" : undefined}>
                              {i > 0 && ", "}
                              {s.expr}
                              {s.paused && <span className="tag">paused</span>}
                            </span>
                          ))}
                    </td>
                    <td>
                      <MicroBars bars={runBars(byJob.get(job.name) ?? [])} />
                    </td>
                    <td>
                      {run ? (
                        <span className="status-cell">
                          <StatusDot status={run.status} />
                          <span className="muted">{relTime(run.created_at)}</span>
                        </span>
                      ) : (
                        <span className="muted">no runs</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
