import { useEffect } from "react";
import MicroBars from "./MicroBars";
import type { MicroBar } from "./MicroBars";
import type { JobPool, JobState, OpStat, OpSummary } from "./types";
import { fmtDuration, relTime } from "./util";

const TITLE_CAP = 2000;

// newest first from the api, oldest on the left in the chart
const statBars = (stat: OpStat): MicroBar[] =>
  stat.recent
    .flatMap((s) => (s.ms === null ? [] : [{ id: s.run_id, value: s.ms, status: s.status }]))
    .reverse();

export default function OpInspector({
  ops,
  name,
  pools = [],
  stat,
  state,
  onClose,
}: {
  ops: OpSummary[];
  name: string;
  pools?: JobPool[];
  stat?: OpStat;
  state?: JobState;
  onClose: () => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const op = ops.find((o) => o.name === name);
  if (!op) return null;
  const dependents = ops.filter((o) => o.deps.includes(name)).map((o) => o.name);
  // the limit belongs to the pool, not to this op: every job in the process
  // draws from the same one
  const poolLimit = pools.find((p) => p.name === op?.pool)?.limit ?? null;
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
        {op.pool && (
          <div className="op-line">
            <span className="op-line-label">pool</span>
            <span className="mono">
              {op.pool}
              {poolLimit !== null && ` · ${poolLimit} at once`}
            </span>
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
