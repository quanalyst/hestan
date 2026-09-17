import GroupingControls from "./GroupingControls";
import type { Section } from "./presentation";
import { Fragment } from "react";
import { displayName, hierarchy, matchesName, summary, viewFrom, matchesLabel, filterHierarchy } from "./presentation";
import { useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { get, usePoll } from "./api";
import { namespacesOf, onlyNamespace } from "./catalog";
import MicroBars from "./MicroBars";
import type { MicroBar } from "./MicroBars";
import StatusDot from "./StatusDot";
import TimelinePlot, { futureWindowSecs } from "./TimelinePlot";
import { openFrom, openParam, toggleOpen as toggle } from "./timeline";
import type { JobSummary, LateEntry, Run, UpcomingSchedule } from "./types";
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
  // which slice of the deployment this page is showing, in the url like the
  // assets page's filters, so one team's view is a link
  const [params, setParams] = useSearchParams();
  const namespace = params.get("namespace");
  const query = params.get("q") ?? "";
  const view = viewFrom(params);
  const setNamespace = (want: string | null) =>
    setParams(
      (prev) => {
        if (want === null) prev.delete("namespace");
        else prev.set("namespace", want);
        return prev;
      },
      { replace: true },
    );
  // which groups on the timeline have their job rows showing, beside the
  // namespace filter and for the same reason: an opened view is a link
  // somebody can send. groups open shut, so the parameter carries what is
  // open, which is the shorter half of the answer
  const opened = openFrom(params.get("open"));
  const toggleOpen = (group: string) =>
    setParams(
      (prev) => {
        const next = openParam(toggle(opened, group));
        if (next === "") prev.delete("open");
        else prev.set("open", next);
        return prev;
      },
      { replace: true },
    );
  const [jobs, setJobs] = useState<JobSummary[] | null>(null);
  const [runs, setRuns] = useState<Run[] | null>(null);
  const [upcoming, setUpcoming] = useState<UpcomingSchedule[]>([]);
  const [late, setLate] = useState<LateEntry[]>([]);
  const requestedWindow = Number(params.get("window") ?? 21600);
  const windowSecs = [3600, 21600, 86400].includes(requestedWindow) ? requestedWindow : 21600;
  const setWindowSecs = (seconds: number) => setParams((previous) => {
    const next = new URLSearchParams(previous);
    if (seconds === 21600) next.delete("window"); else next.set("window", String(seconds));
    return next;
  }, { replace: true });

  usePoll(
    () => {
      get<{ jobs: JobSummary[] }>("/api/jobs")
        .then((r) => setJobs(r.jobs))
        .catch(() => {});
      get<{ late: LateEntry[] }>("/api/late")
        .then((r) => setLate(r.late))
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

  const namespaces = namespacesOf(jobs);
  const shown = onlyNamespace(jobs, namespace).filter((j) => matchesName(j, query) && matchesLabel(j, params));
  const shownNames = new Set(shown.map((j) => j.name));
  const sections = filterHierarchy(hierarchy(jobs, view), (j) => shownNames.has(j.name));
  const effectiveOpen = new Set(opened);
  if (query.trim()) for (const s of sections) { effectiveOpen.add(s.key); for (const c of s.children) effectiveOpen.add(c.key); }
  const rows = sections.flatMap<{ section: Section<JobSummary> | null; members: JobSummary[] }>((s) => {
    if (s.key === "") return [{ section: null, members: s.members }];
    return [{ section: s, members: effectiveOpen.has(s.key) && !s.children.length ? s.members : [] },
      ...(effectiveOpen.has(s.key) ? s.children.map((c) => ({ section: c, members: effectiveOpen.has(c.key) ? c.members : [] })) : [])];
  });
  const winStart = Date.now() - windowSecs * 1000;
  const winRuns = (runs ?? []).filter((r) => new Date(r.created_at).getTime() >= winStart &&
    (shownNames.has(r.job) || (!query && !namespace && !params.has("label") && !jobs.some((j) => j.name === r.job))));
  const shownLate = late.filter((l) => l.kind === "job" && shownNames.has(l.name));
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
        {/* a declared policy is a claim about the world, not about this
            window, so it is counted whether anything ran in the window or not */}
        {shownLate.length > 0 && (
          <>
            {" · "}
            <b>{shownLate.length}</b> late
          </>
        )}
      </div>

      <GroupingControls items={jobs} params={params} onChange={(p) => setParams(p, { replace: true })} />
      <label>find <input value={query} placeholder="name or identifier" onChange={(e) => setParams((p) => { if (e.target.value) p.set("q", e.target.value); else p.delete("q"); return p; }, { replace: true })} /></label>
      <TimelinePlot
        jobs={shown}
        view={view}
        runs={winRuns}
        upcoming={upcoming}
        windowSecs={windowSecs}
        onWindow={setWindowSecs}
        open={effectiveOpen}
        onOpen={toggleOpen}
      />

      {namespaces.length > 0 && (
        <div className="filter-row">
          <span className="filter-group">
            <span className="filter-label">namespace</span>
            <button
              className={namespace === null ? "text-btn active" : "text-btn"}
              onClick={() => setNamespace(null)}
            >
              all
            </button>
            {namespaces.map((ns) => (
              <button
                key={ns}
                className={namespace === ns ? "text-btn active" : "text-btn"}
                onClick={() => setNamespace(namespace === ns ? null : ns)}
              >
                {ns}
              </button>
            ))}
          </span>
        </div>
      )}

      {jobs.length > 0 && (
        <>
          <h2>
            jobs
            {shown.length !== jobs.length && (
              <span className="secondary">
                {" "}
                · {shown.length} of {jobs.length}
              </span>
            )}
          </h2>
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
              {rows.map(({ section, members }, i) => <Fragment key={section?.key ?? `loose:${i}`}>
                {section && <tr className="group-row"><td colSpan={6} style={{ paddingLeft: section.level * 20 }}>
                  <button className="text-btn" aria-expanded={effectiveOpen.has(section.key)} onClick={() => toggleOpen(section.key)}>
                    {effectiveOpen.has(section.key) ? "▾" : "▸"} {section.name}
                  </button> <span className="muted">{summary(section.members)}</span>
                </td></tr>}
              {members.map((job) => {
                const run = job.last_run;
                return (
                  <tr key={job.name} onClick={() => nav(`/jobs/${encodeURIComponent(job.name)}`)}>
                    <td>
                      <span title={job.name}>{displayName(job)}</span>
                      {job.overdue && <span className="tag">overdue</span>}
                      {job.freshness?.status === "late" && <span className="tag">late</span>}
                    </td>
                    <td className="secondary">{job.description ?? "none"}</td>
                    <td className="num">{job.ops.length}</td>
                    <td className="mono">
                      {job.schedules.length === 0
                        ? "none"
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
              })}</Fragment>)}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
