import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { get, post } from "./api";
import { backfillPlan, estimateMs, medianMs } from "./backfill";
import type { KeyRange } from "./backfill";
import type { AssetSummary, Backfill, OpStat, PartitionEntry } from "./types";
import { fmtDuration } from "./util";

// asset builds are runs of one internal job, so its op history is where a
// partition's duration is recorded
const ASSETS_JOB = "assets";
const STATS_RUNS = 50;

// what a backfill of this range would cost, from what a build of one of this
// asset's partitions has actually taken. no history means no estimate, said
// in those words: a number with nothing behind it is worse than no number
function Estimate({ count, samples }: { count: number; samples: OpStat | undefined }) {
  const median = samples === undefined ? null : medianMs(samples.recent);
  const total = estimateMs(count, median);
  if (total === null) {
    return (
      <span className="muted">no build of this asset has been timed yet, so no estimate</span>
    );
  }
  return (
    <span className="muted" title={`${fmtDuration(median)} median per partition`}>
      about {fmtDuration(total)} of work · {fmtDuration(median)} a partition
    </span>
  );
}

// pick a range on the grid above, see exactly what it covers, and start it.
// every refusal is a disabled button with a reason rather than an error after
// the click
export default function BackfillLauncher({
  asset,
  shown,
  range,
  onRange,
  running,
}: {
  asset: AssetSummary;
  shown: PartitionEntry[];
  range: KeyRange | null;
  onRange: (r: KeyRange | null) => void;
  // a backfill of this asset that is still going; the api refuses a second one
  running: Backfill | null;
}) {
  const nav = useNavigate();
  const [onlyMissing, setOnlyMissing] = useState(true);
  const [stats, setStats] = useState<OpStat[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<{ ops: OpStat[] }>(`/api/jobs/${ASSETS_JOB}/op_stats?runs=${STATS_RUNS}`)
      .then((r) => setStats(r.ops))
      // no history is a thing the estimate says out loud, so a failed read
      // leaves it saying exactly that
      .catch(() => setStats([]));
  }, []);

  const plan = backfillPlan(shown, range, onlyMissing, running);
  const op = asset.op ?? asset.name;

  const start = async () => {
    if (range === null) return;
    setBusy(true);
    setError(null);
    try {
      const b = await post<Backfill>(`/api/assets/${encodeURIComponent(asset.name)}/backfill`, {
        from: range.from,
        to: range.to,
        only_missing: onlyMissing,
      });
      nav(`/backfills/${b.id}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="backfill-launch">
      <div className="backfill-row">
        <button onClick={start} disabled={busy || plan.refused !== null}>
          backfill
        </button>
        {plan.refused !== null ? (
          <span className="muted">{plan.refused}</span>
        ) : (
          <>
            <span className="mono">
              {plan.building[0].key} → {plan.building[plan.building.length - 1].key}
            </span>
            <span className="muted">
              {plan.building.length} of {plan.covered.length} selected
            </span>
            <Estimate
              count={plan.building.length}
              samples={stats?.find((s) => s.op === op)}
            />
          </>
        )}
      </div>
      <div className="backfill-row">
        <button className="text-btn" onClick={() => onRange(null)} disabled={range === null}>
          clear
        </button>
        {/* the api's own default, and the reason a repeat of a half-failed
            backfill is cheap */}
        <label className="muted backfill-only">
          <input
            type="checkbox"
            checked={onlyMissing}
            onChange={(e) => setOnlyMissing(e.target.checked)}
          />
          skip the ones already fresh
        </label>
      </div>
      {error && <p className="muted">backfill refused: {error}</p>}
    </div>
  );
}
