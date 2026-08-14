import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { del, get, HttpError, post, put, usePoll } from "./api";
import DagView from "./DagView";
import DurationBars from "./DurationBars";
import OpInspector from "./OpInspector";
import { useMay } from "./role";
import StatusDot from "./StatusDot";
import { GlyphShape } from "./StatusGlyph";
import type { Status } from "./StatusGlyph";
import TimelinePlot, { futureWindowSecs } from "./TimelinePlot";
import type {
  JobState,
  JobSummary,
  OpStat,
  OpSummary,
  Preset,
  Run,
  SchemaField,
  Tick,
  TickOutcome,
  UpcomingSchedule,
} from "./types";
import { durationMs, fmtDuration, fmtRate, opBadge, relTime, shortId, untilTime } from "./util";

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

// what a schema field is called in the legend. json schema can say far more
// than one word, and one word is what a legend has room for
function fieldType(f: SchemaField): string {
  if (typeof f.type === "string") return f.type;
  if (Array.isArray(f.type)) return f.type.join(" | ");
  if (typeof f.$ref === "string") return f.$ref.slice(f.$ref.lastIndexOf("/") + 1);
  if (Array.isArray(f.enum)) return "enum";
  return "—";
}

// keys the editor holds that the schema has never heard of: a typo, or a
// schema that has not caught up. worth pointing at, never worth refusing —
// the schema does not decide what launches
function unknownKeys(text: string, known: Record<string, SchemaField>): string[] {
  if (!text.trim()) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return [];
  return Object.keys(parsed).filter((k) => !(k in known));
}

// tags as one editable line: `env:prod, kind:smoke`. the same key:value the
// runs page filters on, so there is one spelling of a tag in the ui
function formatTags(tags: Record<string, string>): string {
  return Object.entries(tags)
    .map(([k, v]) => `${k}:${v}`)
    .join(", ");
}

// null for a line that is not tags at all, which is what disables the launch —
// dropping the fragment we could not read would launch something else
function parseTags(text: string): Record<string, string> | null {
  const out: Record<string, string> = {};
  for (const part of text.split(",")) {
    const piece = part.trim();
    if (!piece) continue;
    const at = piece.indexOf(":");
    if (at <= 0 || at === piece.length - 1) return null;
    out[piece.slice(0, at).trim()] = piece.slice(at + 1).trim();
  }
  return out;
}

// how many ops a "launch from here" covers, for the label only — whether the
// selection is launchable at all is the server's to say, and it says it
function downstreamOf(ops: OpSummary[], root: string): string[] {
  const out = new Set<string>();
  const stack = [root];
  while (stack.length) {
    const at = stack.pop()!;
    for (const o of ops)
      if (o.deps.includes(at) && !out.has(o.name)) {
        out.add(o.name);
        stack.push(o.name);
      }
  }
  return [...out];
}

// a schedule's params next to its expression: short mono json, full on hover
const SCHED_PARAMS_CAP = 44;

function paramsLabel(params: unknown): string | null {
  const json = JSON.stringify(params);
  return json === undefined || json === "{}" ? null : json;
}

export default function JobPage() {
  const { name } = useParams();
  if (!name) return null;
  // keyed so a job switch remounts: no state leaks across it
  return <JobView key={name} name={name} />;
}

function JobView({ name }: { name: string }) {
  // launching is an operator's; a preset and a paused schedule outlive the
  // launch and are an admin's
  const mayLaunch = useMay("operator");
  const mayConfigure = useMay("admin");
  const nav = useNavigate();
  const [search] = useSearchParams();
  // the run this launchpad was opened from, if it was cloned out of one
  const from = search.get("from");
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
  const [presets, setPresets] = useState<Preset[]>([]);
  const [presetName, setPresetName] = useState("");
  const [presetError, setPresetError] = useState<string | null>(null);
  const [tagsText, setTagsText] = useState("");
  const [cloneError, setCloneError] = useState<string | null>(null);
  const [launching, setLaunching] = useState(false);
  const [launchError, setLaunchError] = useState<string | null>(null);
  const [paramsError, setParamsError] = useState<string | null>(null);
  const [schedError, setSchedError] = useState<string | null>(null);
  const [subsetError, setSubsetError] = useState<string | null>(null);
  const [missing, setMissing] = useState(false);
  const enc = encodeURIComponent(name);

  const loadPresets = useCallback(
    () =>
      get<{ presets: Preset[] }>(`/api/jobs/${enc}/presets`)
        .then((r) => setPresets(r.presets))
        .catch(() => {}),
    [enc],
  );

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
      void loadPresets();
    },
    missing ? null : 5000,
    [name, missing, loadPresets],
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

  // a clone prefills rather than launches: editing is the whole point, so the
  // params and tags are fetched and dropped into the editor. fetched, not
  // carried in the url — a run's params do not belong in a query string
  useEffect(() => {
    if (!from) return;
    get<{ job: string; params: unknown; tags: Record<string, string> }>(
      `/api/runs/${encodeURIComponent(from)}/clone`,
    )
      .then((c) => {
        if (c.job !== name) {
          setCloneError(`run ${shortId(from)} belongs to job ${c.job}`);
          return;
        }
        setParamsText(JSON.stringify(c.params, null, 2));
        setTagsText(formatTags(c.tags));
        setParamsOpen(true);
        setParamsError(null);
        setCloneError(null);
      })
      .catch((e) => setCloneError(e instanceof Error ? e.message : String(e)));
  }, [from, name]);

  const paramsValid = validJson(paramsText);
  const tags = parseTags(tagsText);

  // the client-side parse is the first gate; the server owns the second, since
  // only it knows what the ops declared
  const checkParams = async () => {
    if (!paramsValid) return;
    const text = paramsText.trim();
    try {
      await post<{ ok: boolean }>(`/api/jobs/${enc}/validate_params`, {
        params: text ? JSON.parse(text) : {},
      });
      setParamsError(null);
    } catch (e) {
      setParamsError(e instanceof Error ? e.message : String(e));
    }
  };

  // `ops` runs only those and everything downstream; without it the whole job
  // runs, which is what the launch button has always done
  const launch = async (ops?: string[]) => {
    setLaunching(true);
    setLaunchError(null);
    setSubsetError(null);
    try {
      const text = paramsText.trim();
      saveParams(name, paramsText);
      const body: { params?: unknown; ops?: string[]; tags?: Record<string, string> } = {};
      if (text) body.params = JSON.parse(text);
      if (ops) body.ops = ops;
      if (tags && Object.keys(tags).length > 0) body.tags = tags;
      const r = await post<{ run_id: string }>(
        `/api/jobs/${enc}/runs`,
        text || ops || body.tags ? body : undefined,
      );
      nav(`/runs/${r.run_id}`);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      // the server owns whether a subset is launchable, so its refusal is the
      // message — there is no second copy of that rule here
      if (ops) setSubsetError(msg);
      else setLaunchError(msg);
    } finally {
      setLaunching(false);
    }
  };

  // picking a preset fills the editor rather than launching: the whole point
  // of a stored one is that it is a starting point you can still edit
  const fillFrom = (picked: string) => {
    const p = presets.find((x) => x.name === picked);
    if (!p) return;
    setPresetName(p.name);
    setParamsText(JSON.stringify(p.params, null, 2));
    setParamsError(null);
    setPresetError(null);
    setParamsOpen(true);
  };

  const savePreset = async () => {
    const preset = presetName.trim();
    if (!preset || !paramsValid) return;
    setPresetError(null);
    try {
      const text = paramsText.trim();
      await put<{ ok: boolean }>(`/api/jobs/${enc}/presets/${encodeURIComponent(preset)}`, {
        params: text ? JSON.parse(text) : {},
      });
      await loadPresets();
    } catch (e) {
      setPresetError(e instanceof Error ? e.message : String(e));
    }
  };

  const deletePreset = async () => {
    const preset = presetName.trim();
    setPresetError(null);
    try {
      await del<{ deleted: boolean }>(`/api/jobs/${enc}/presets/${encodeURIComponent(preset)}`);
      setPresetName("");
      await loadPresets();
    } catch (e) {
      setPresetError(e instanceof Error ? e.message : String(e));
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
  const fields = job.params_schema?.properties ?? {};
  const required = new Set(job.params_schema?.required ?? []);
  const strangers = unknownKeys(paramsText, fields);
  // only what departs from the defaults, as with a schedule's tz
  const policy = [
    job.max_parallel === null ? null : `max_parallel ${job.max_parallel}`,
    ...job.pools.map((p) => `pool ${p.name}${p.limit === null ? "" : ` ${p.limit}`}`),
    ...job.rates.map(
      (r) =>
        `rate ${r.name}${r.limit === null || r.per_secs === null ? "" : ` ${fmtRate(r.limit, r.per_secs)}`}`,
    ),
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
        {!mayLaunch ? (
          <p className="muted">launching needs an operator</p>
        ) : (
          <div className="run-actions">
            <div className="run-side">
              {presets.length > 0 && (
                <select
                  className="preset-select"
                  value={presets.some((p) => p.name === presetName) ? presetName : ""}
                  onChange={(e) => fillFrom(e.target.value)}
                >
                  <option value="">preset…</option>
                  {presets.map((p) => (
                    <option key={p.name} value={p.name}>
                      {p.name}
                    </option>
                  ))}
                </select>
              )}
              <button onClick={() => launch()} disabled={launching || !paramsValid || tags === null}>
                launch run
              </button>
            </div>
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
                  onChange={(e) => {
                    setParamsText(e.target.value);
                    setParamsError(null);
                  }}
                  onBlur={checkParams}
                />
                {!paramsValid && <p className="muted params-hint">invalid json</p>}
                {paramsValid && paramsError && <p className="muted params-hint">{paramsError}</p>}
                {/* a legend for the json above, not a replacement for it: the
                    schema says what the fields are, the editor still says what
                    they are set to */}
                {Object.keys(fields).length > 0 && (
                  <div className="params-fields">
                    {Object.entries(fields).map(([field, shape]) => (
                      <div key={field} className="params-field">
                        <span className="mono">{field}</span>
                        <span className="muted">{fieldType(shape)}</span>
                        {required.has(field) && <span className="muted">required</span>}
                        {shape.description && (
                          <span className="muted params-desc" title={shape.description}>
                            {shape.description}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                )}
                {strangers.length > 0 && (
                  <p className="muted params-hint">
                    not in the schema: <span className="mono">{strangers.join(", ")}</span>
                  </p>
                )}
                <div className="params-label">tags</div>
                <input
                  className="filter-input"
                  value={tagsText}
                  placeholder="env:prod, kind:smoke"
                  onChange={(e) => setTagsText(e.target.value)}
                />
                {tags === null && <p className="muted params-hint">tags are key:value, comma separated</p>}
                {mayConfigure && (
                  <>
                    <div className="preset-row">
                      <input
                        className="filter-input"
                        value={presetName}
                        placeholder="preset name"
                        onChange={(e) => {
                          setPresetName(e.target.value);
                          setPresetError(null);
                        }}
                      />
                      <button
                        className="text-btn"
                        onClick={savePreset}
                        disabled={!presetName.trim() || !paramsValid}
                      >
                        save
                      </button>
                      {presets.some((p) => p.name === presetName.trim()) && (
                        <button className="text-btn" onClick={deletePreset}>
                          delete
                        </button>
                      )}
                    </div>
                    {presetError && <p className="muted params-hint">{presetError}</p>}
                  </>
                )}
              </div>
            )}
            {!paramsValid && !paramsOpen && <p className="muted">saved params are invalid json — open params to fix</p>}
            {tags === null && !paramsOpen && <p className="muted">tags are not key:value — open params to fix</p>}
            {cloneError && <p className="muted">clone failed: {cloneError}</p>}
            {launchError && <p className="muted">launch failed: {launchError}</p>}
          </div>
        )}
      </div>

      <h2>graph</h2>
      <DagView
        // a mapped op is one node in the definition; how many instances it
        // makes is only known inside a run
        nodes={job.ops.map((o) => ({ ...o, badge: opBadge(o, o.mapped_over ? "×n" : null) }))}
        selected={opSel}
        onSelect={(op) => {
          setSubsetError(null);
          setOpSel((prev) => (prev === op ? null : op));
        }}
      />
      {mayLaunch && opSel && (
        <p className="muted dag-action">
          <button
            className="text-btn"
            onClick={() => launch([opSel])}
            disabled={launching || !paramsValid || tags === null}
          >
            launch from {opSel}
          </button>
          {` · ${downstreamOf(job.ops, opSel).length + 1} ops`}
          {subsetError && <> · {subsetError}</>}
        </p>
      )}
      {opSel && (
        <OpInspector
          ops={job.ops}
          job={name}
          name={opSel}
          pools={job.pools}
          rates={job.rates}
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
          {job.schedules.map((s) => {
            const params = paramsLabel(s.params);
            return (
            <div key={s.expr} className="sched-row">
              <span className="mono">{s.expr}</span>
              {s.tz !== "UTC" && <span className="muted">{s.tz}</span>}
              {/* only what departs from the default, like the tz above */}
              {s.catchup !== "skip" && (
                <span className="muted" title={s.cursor ? `cursor ${s.cursor}` : "no cursor yet"}>
                  catch up {s.catchup}
                </span>
              )}
              {params && (
                <span className="mono muted" title={params}>
                  {params.length > SCHED_PARAMS_CAP ? params.slice(0, SCHED_PARAMS_CAP - 1) + "…" : params}
                </span>
              )}
              <span className="muted">
                {s.paused ? "paused" : s.next_fire ? `next ${untilTime(s.next_fire)}` : "next —"}
              </span>
              {mayConfigure && (
                <button className="text-btn" onClick={() => setPaused(s.expr, !s.paused)}>
                  {s.paused ? "resume" : "pause"}
                </button>
              )}
            </div>
            );
          })}
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
