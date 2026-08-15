import { useEffect, useState } from "react";
import { get } from "./api";
import MetaList from "./MetaList";
import MicroBars from "./MicroBars";
import type { MicroBar } from "./MicroBars";
import type { JobPool, JobRate, JobState, MetaPoint, OpStat, OpSummary, Trends } from "./types";
import { fmtBytes, fmtDuration, fmtRate, numericMetaKeys, relTime } from "./util";

const TITLE_CAP = 2000;

// newest first from the api, oldest on the left in the chart. a mapped op's
// samples are its instances, so several of them share a run and the position
// is what tells two bars apart
const statBars = (stat: OpStat): MicroBar[] =>
  stat.recent
    .flatMap((s, i) => (s.ms === null ? [] : [{ id: `${s.run_id}:${i}`, value: s.ms, status: s.status }]))
    .reverse();

export default function OpInspector({
  ops,
  job,
  name,
  pools = [],
  rates = [],
  stat,
  state,
  onClose,
}: {
  ops: OpSummary[];
  job: string;
  name: string;
  pools?: JobPool[];
  rates?: JobRate[];
  stat?: OpStat;
  state?: JobState;
  onClose: () => void;
}) {
  const [trends, setTrends] = useState<Trends>({});

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  // one series per numeric key this op last reported, for the sparkline under
  // its value: refetched when the keys change or a run lands, rather than on
  // every poll of the stats
  const keys = numericMetaKeys(stat?.metadata ?? null).join(",");
  const newest = stat?.recent[0]?.run_id ?? null;
  useEffect(() => {
    if (keys === "") {
      setTrends({});
      return;
    }
    let live = true;
    const path = `/api/jobs/${encodeURIComponent(job)}/ops/${encodeURIComponent(name)}`;
    Promise.all(
      keys.split(",").map((key) =>
        get<{ points: MetaPoint[] }>(`${path}/metadata/${encodeURIComponent(key)}`)
          .then((r) => [key, r.points] as const)
          .catch(() => [key, []] as const),
      ),
    ).then((series) => {
      if (live) setTrends(Object.fromEntries(series));
    });
    return () => {
      live = false;
    };
  }, [job, name, keys, newest]);

  const op = ops.find((o) => o.name === name);
  if (!op) return null;
  const dependents = ops.filter((o) => o.deps.includes(name)).map((o) => o.name);
  // the limit belongs to the pool, not to this op: every job in the process
  // draws from the same one
  const poolLimit = pools.find((p) => p.name === op?.pool)?.limit ?? null;
  // and the rate is the same kind of fact: declared once, honoured by every job
  // in the process, and by this process alone
  const declaredRate = rates.find((r) => r.name === op?.rate) ?? null;
  const stateJson = state === undefined ? null : JSON.stringify(state.value);
  const stateClipped = stateJson !== null && stateJson.length > 120;
  // capped, or a multi-megabyte state would sit in the DOM as one attribute
  const stateTitle = !stateClipped
    ? undefined
    : stateJson.length > TITLE_CAP
      ? stateJson.slice(0, TITLE_CAP) + "… (truncated)"
      : stateJson;

  return (
    <aside className="op-panel">
      <div className="op-panel-head">
        <span className="mono op-title">{name}</span>
        <button className="text-btn" onClick={onClose} aria-label="close">
          ×
        </button>
      </div>

      {op.deps.length > 0 && (
        <>
          <div className="sub-label">deps</div>
          {op.deps.map((d) => (
            <div key={d} className="mono op-dep">
              {d}
            </div>
          ))}
        </>
      )}
      {dependents.length > 0 && (
        <>
          <div className="sub-label">dependents</div>
          {dependents.map((d) => (
            <div key={d} className="mono op-dep">
              {d}
            </div>
          ))}
        </>
      )}

      <div className="op-lines">
        {op.when !== "all_succeeded" && (
          <div className="op-line">
            <span className="op-line-label">runs</span>
            <span className="mono">{op.when === "always" ? "always" : "if a dep failed"}</span>
          </div>
        )}
        <div className="op-line">
          <span className="op-line-label">retries</span>
          <span className="num">{op.retries}</span>
        </div>
        {op.timeout_secs !== null && (
          <div className="op-line">
            <span className="op-line-label">timeout</span>
            <span className="num">{fmtDuration(op.timeout_secs * 1000)}</span>
          </div>
        )}
        {op.mapped_over && (
          <div className="op-line">
            <span className="op-line-label">mapped over</span>
            <span className="mono">{op.mapped_over}</span>
          </div>
        )}
        {op.requires.length > 0 && (
          <div className="op-line">
            <span className="op-line-label">resources</span>
            <span className="mono">{op.requires.join(", ")}</span>
          </div>
        )}
        {op.pool && (
          <div className="op-line">
            <span className="op-line-label">pool</span>
            <span className="mono">
              {op.pool}
              {poolLimit !== null && ` · ${poolLimit} at once`}
            </span>
          </div>
        )}
        {op.rate && (
          <div className="op-line">
            <span className="op-line-label">rate</span>
            <span className="mono">
              {op.rate}
              {declaredRate?.limit != null &&
                declaredRate.per_secs != null &&
                ` · ${fmtRate(declaredRate.limit, declaredRate.per_secs)}`}
            </span>
          </div>
        )}
        {/* the limits belong to the child, so they only ever appear with it */}
        {op.isolated && (
          <div className="op-line">
            <span className="op-line-label">isolated</span>
            <span className="mono">
              {[
                "own process",
                op.memory_limit_bytes === null ? null : fmtBytes(op.memory_limit_bytes),
                op.cpu_limit_secs === null ? null : `${fmtDuration(op.cpu_limit_secs * 1000)} cpu`,
              ]
                .filter(Boolean)
                .join(" · ")}
            </span>
          </div>
        )}
        {op.io && (
          <div className="op-line">
            <span className="op-line-label">io</span>
            <span className="mono">{op.io}</span>
          </div>
        )}
        {op.params_type && (
          <div className="op-line">
            <span className="op-line-label">params</span>
            <span className="mono">{op.params_type}</span>
          </div>
        )}
        {op.input_type && (
          <div className="op-line">
            <span className="op-line-label">input</span>
            <span className="mono">{op.input_type}</span>
          </div>
        )}
        {op.output_type && (
          <div className="op-line">
            <span className="op-line-label">output</span>
            <span className="mono">{op.output_type}</span>
          </div>
        )}
      </div>

      {state && stateJson !== null && (
        <>
          <div className="sub-label">state</div>
          <div className="mono op-state" title={stateTitle}>
            {stateClipped ? stateJson.slice(0, 119) + "…" : stateJson}
          </div>
          <div className="op-stat-line muted">updated {relTime(state.updated_at)}</div>
        </>
      )}

      {stat?.metadata && (
        <>
          <div className="sub-label">metadata</div>
          <MetaList metadata={stat.metadata} trends={trends} />
        </>
      )}

      {stat &&
        (stat.runs === 0 ? (
          <p className="muted op-noruns">no runs yet</p>
        ) : (
          <>
            <div className="sub-label">history</div>
            <div className="op-stat-line">
              avg {fmtDuration(stat.avg_ms)} · p95 {fmtDuration(stat.p95_ms)}
            </div>
            <div className="op-stat-line muted">
              {stat.failures}/{stat.runs} failed
            </div>
            <MicroBars bars={statBars(stat)} />
            {stat.last_error && (
              <>
                <div className="sub-label">last error</div>
                <div className="mono muted op-err">{stat.last_error}</div>
              </>
            )}
          </>
        ))}
    </aside>
  );
}
