import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { get, post, usePoll } from "./api";
import AssetPanel from "./AssetPanel";
import DagView from "./DagView";
import { GlyphShape } from "./StatusGlyph";
import type { Status } from "./StatusGlyph";
import type {
  AssetSummary,
  Backfill,
  BackfillStatus,
  CheckSummary,
  SensorOutcome,
  SensorSummary,
} from "./types";
import { relTime } from "./util";

const SENSOR_GLYPH = {
  fired: "success",
  error: "failed",
  skipped: "skipped",
} as const satisfies Record<SensorOutcome, Status>;

// a backfill's states map onto the run states they are made of
const BACKFILL_GLYPH = {
  running: "running",
  complete: "success",
  failed: "failed",
  canceled: "canceled",
} as const satisfies Record<BackfillStatus, Status>;

// enough hex to tell fingerprints apart at a glance; the title carries the rest
const shortHash = (fp: string) => fp.slice(0, 12);

function fmtEvery(secs: number): string {
  if (secs >= 3600 && secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs >= 60 && secs % 60 === 0) return `${secs / 60}m`;
  return `${secs}s`;
}

// stale with no reasons means no materialization exists at all. a partitioned
// asset carries its evidence per key, so it counts keys instead
function staleSummary(a: AssetSummary): string {
  if (a.partitions) {
    const { stale, missing } = a.partitions;
    const parts = [];
    if (stale > 0) parts.push(`${stale} stale`);
    if (missing > 0) parts.push(`${missing} missing`);
    return parts.join(", ") || "stale";
  }
  if (a.reasons.length === 0) return "never built";
  if (a.reasons.length === 1) return `dep ${a.reasons[0].dep} changed`;
  return `${a.reasons.length} deps changed`;
}

function staleTitle(a: AssetSummary): string | undefined {
  if (a.partitions) return `${a.partitions.total} partitions`;
  if (a.reasons.length === 0) return undefined;
  return a.reasons
    .map((r) => `${r.dep}: ${r.had ? shortHash(r.had) : "—"} -> ${r.now ? shortHash(r.now) : "—"}`)
    .join("\n");
}

// the established shape vocabulary: a solid glyph when everything passed, an
// × when anything failed, and nothing at all when no check has ever run —
// an asset without checks should not read as an asset with silent ones
function Checks({ checks }: { checks: CheckSummary }) {
  const total = checks.passed + checks.failed;
  if (total === 0) return null;
  return (
    <span className="status" title={`${checks.passed}/${total} passed`}>
      <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
        <GlyphShape status={checks.failed > 0 ? "failed" : "success"} />
      </svg>
      {checks.failed > 0 ? `${checks.failed} failed` : `${checks.passed} passed`}
    </span>
  );
}

function Freshness({ stale }: { stale: boolean }) {
  return (
    <span className="status">
      <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
        <GlyphShape status={stale ? "pending" : "success"} />
      </svg>
      {stale ? "stale" : "fresh"}
    </span>
  );
}

export default function AssetsPage() {
  const nav = useNavigate();
  const [assets, setAssets] = useState<AssetSummary[] | null>(null);
  const [sensors, setSensors] = useState<SensorSummary[]>([]);
  const [backfills, setBackfills] = useState<Backfill[]>([]);
  const [backfillErr, setBackfillErr] = useState<string | null>(null);
  const [building, setBuilding] = useState(false);
  const [buildNote, setBuildNote] = useState<string | null>(null);
  const [busyAsset, setBusyAsset] = useState<string | null>(null);
  const [rowMsg, setRowMsg] = useState<{ asset: string; msg: string } | null>(null);
  const [sensorErr, setSensorErr] = useState<string | null>(null);
  const [sel, setSel] = useState<string | null>(null);

  usePoll(
    () => {
      get<{ assets: AssetSummary[] }>("/api/assets")
        .then((r) => setAssets(r.assets))
        .catch(() => {});
      get<{ sensors: SensorSummary[] }>("/api/sensors")
        .then((r) => setSensors(r.sensors))
        .catch(() => {});
      get<{ backfills: Backfill[] }>("/api/backfills?limit=20")
        .then((r) => setBackfills(r.backfills))
        .catch(() => {});
    },
    5000,
    [],
  );

  const buildOne = async (asset: string) => {
    setBusyAsset(asset);
    setRowMsg(null);
    try {
      const r = await post<{ run_id?: string; up_to_date?: boolean }>(
        `/api/assets/${encodeURIComponent(asset)}/build`,
      );
      if (r.run_id) nav(`/runs/${r.run_id}`);
      else setRowMsg({ asset, msg: "up to date" });
    } catch (e) {
      setRowMsg({ asset, msg: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusyAsset(null);
    }
  };

  const buildStale = async () => {
    setBuilding(true);
    setBuildNote(null);
    try {
      const r = await post<{ run_ids?: string[]; up_to_date?: boolean }>("/api/assets/build");
      const ids = r.run_ids ?? [];
      if (ids.length === 1) nav(`/runs/${ids[0]}`);
      else if (ids.length > 1) nav("/runs");
      // nothing stale left: it got built between polls
      else setBuildNote("already up to date");
    } catch (e) {
      setBuildNote(`build failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBuilding(false);
    }
  };

  const cancelBackfill = async (id: number) => {
    setBackfillErr(null);
    try {
      await post<{ canceled: boolean }>(`/api/backfills/${id}/cancel`);
      const r = await get<{ backfills: Backfill[] }>("/api/backfills?limit=20");
      setBackfills(r.backfills);
    } catch (e) {
      setBackfillErr(e instanceof Error ? e.message : String(e));
    }
  };

  const setPaused = async (name: string, paused: boolean) => {
    setSensorErr(null);
    const flip = (list: SensorSummary[], p: boolean) =>
      list.map((s) => (s.name === name ? { ...s, paused: p } : s));
    setSensors((list) => flip(list, paused));
    try {
      await post<{ ok: boolean }>("/api/sensors/state", { name, paused });
    } catch (e) {
      setSensors((list) => flip(list, !paused));
      setSensorErr(e instanceof Error ? e.message : String(e));
    }
  };

  if (!assets) return <p className="muted">loading…</p>;

  const anyStale = assets.some((a) => a.stale);
  const selected = assets.find((a) => a.name === sel) ?? null;
  const staleness = Object.fromEntries(
    assets.map((a) => [a.name, a.stale ? "stale" : "fresh"] as const),
  );

  return (
    <>
      <div className="page-head">
        <h1>Assets</h1>
        {anyStale && (
          <div className="run-actions">
            <button onClick={buildStale} disabled={building}>
              build stale
            </button>
            {buildNote && <p className="muted">{buildNote}</p>}
          </div>
        )}
      </div>

      {assets.length === 0 ? (
        <p className="muted">no assets registered — declare them with Hestan::assets</p>
      ) : (
        <>
          <h2>graph</h2>
          <DagView
            label="asset dependency graph"
            nodes={assets.map((a) => ({
              name: a.name,
              deps: a.deps,
              note: a.kind === "source" ? "source" : undefined,
            }))}
            statuses={staleness}
            selected={sel}
            onSelect={(name) => setSel((prev) => (prev === name ? null : name))}
          />

          <h2>assets</h2>
          <table>
            <thead>
              <tr>
                <th>name</th>
                <th>kind</th>
                <th>deps</th>
                <th>fingerprint</th>
                <th>built</th>
                <th>state</th>
                <th>checks</th>
                <th>auto</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {assets.map((a) => (
                <tr
                  key={a.name}
                  onClick={() => setSel((prev) => (prev === a.name ? null : a.name))}
                >
                  <td>
                    {a.name}
                    {a.freshness?.status === "late" && <span className="tag">late</span>}
                  </td>
                  <td className="muted">{a.kind}</td>
                  <td className="muted">{a.deps.length === 0 ? "—" : a.deps.join(", ")}</td>
                  {/* a partitioned asset has no single fingerprint to show:
                      how much of its key set is built is the fact instead */}
                  <td
                    className="mono"
                    title={
                      a.partitions
                        ? `${a.partitions.materialized} of ${a.partitions.total} partitions built`
                        : (a.fingerprint ?? undefined)
                    }
                  >
                    {a.partitions
                      ? `${a.partitions.materialized}/${a.partitions.total}`
                      : a.fingerprint
                        ? shortHash(a.fingerprint)
                        : "—"}
                  </td>
                  <td className="muted" title={a.built_at ?? undefined}>
                    {a.partitions ? "—" : relTime(a.built_at)}
                  </td>
                  <td>
                    <span className="status-cell">
                      <Freshness stale={a.stale} />
                      {a.stale && (
                        <span className="muted" title={staleTitle(a)}>
                          {staleSummary(a)}
                        </span>
                      )}
                    </span>
                  </td>
                  <td>
                    <Checks checks={a.checks} />
                  </td>
                  <td>{a.auto && <span className="muted">auto</span>}</td>
                  <td className="row-action">
                    {/* sources are probed, never built — the endpoint 400s */}
                    {a.kind !== "source" && (
                      <button
                        className="text-btn"
                        disabled={busyAsset === a.name}
                        onClick={(e) => {
                          e.stopPropagation();
                          buildOne(a.name);
                        }}
                      >
                        build
                      </button>
                    )}
                    {rowMsg?.asset === a.name && <span className="muted row-err">{rowMsg.msg}</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {backfills.length > 0 && (
        <>
          <h2>backfills</h2>
          <table className="plain-rows">
            <thead>
              <tr>
                <th>asset</th>
                <th>range</th>
                <th className="num">progress</th>
                <th>status</th>
                <th>run</th>
                <th>started</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {backfills.map((b) => {
                const current = b.run_ids[b.run_ids.length - 1];
                return (
                  <tr key={b.id}>
                    <td>{b.asset}</td>
                    <td className="mono">
                      {b.from_key} → {b.to_key}
                    </td>
                    {/* launched, not materialized: what a chunk's run did is
                        the run's story, and the grid above tells the rest */}
                    <td className="num" title={`${b.launched} of ${b.total} partitions launched`}>
                      {b.launched}/{b.total}
                    </td>
                    <td>
                      <span className="status">
                        <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
                          <GlyphShape status={BACKFILL_GLYPH[b.status]} />
                        </svg>
                        {b.status}
                      </span>
                    </td>
                    <td>
                      {current ? (
                        <Link className="mono" to={`/runs/${current}`}>
                          {current.slice(0, 8)}
                        </Link>
                      ) : (
                        <span className="muted">—</span>
                      )}
                    </td>
                    <td className="muted" title={b.created_at}>
                      {relTime(b.created_at)}
                    </td>
                    <td className="row-action">
                      {b.status === "running" && (
                        <button className="text-btn" onClick={() => cancelBackfill(b.id)}>
                          cancel
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {backfillErr && <p className="muted">cancel failed: {backfillErr}</p>}
        </>
      )}

      {sensors.length > 0 && (
        <>
          <h2>sensors</h2>
          <table className="plain-rows">
            <thead>
              <tr>
                <th>name</th>
                <th>every</th>
                <th>cursor</th>
                <th>last tick</th>
                <th className="num">launched</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {sensors.map((s) => {
                const cursor = s.cursor === null ? null : JSON.stringify(s.cursor);
                return (
                  <tr key={s.name}>
                    <td>
                      {s.name}
                      {s.paused && <span className="tag">paused</span>}
                    </td>
                    <td className="mono">{fmtEvery(s.every_secs)}</td>
                    <td className="mono" title={cursor && cursor.length > 24 ? cursor : undefined}>
                      {cursor === null ? "—" : cursor.length > 24 ? `${cursor.slice(0, 24)}…` : cursor}
                    </td>
                    <td>
                      {s.last_tick ? (
                        <span className="status-cell" title={s.last_tick.error ?? undefined}>
                          <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
                            <GlyphShape status={SENSOR_GLYPH[s.last_tick.outcome]} />
                          </svg>
                          <span className="muted">{relTime(s.last_tick.evaluated_at)}</span>
                        </span>
                      ) : (
                        <span className="muted">no ticks</span>
                      )}
                    </td>
                    <td className="num">{s.last_tick ? s.last_tick.launched : "—"}</td>
                    <td className="row-action">
                      <button className="text-btn" onClick={() => setPaused(s.name, !s.paused)}>
                        {s.paused ? "resume" : "pause"}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {sensorErr && <p className="muted">sensor update failed: {sensorErr}</p>}
        </>
      )}

      {selected && <AssetPanel key={selected.name} asset={selected} onClose={() => setSel(null)} />}
    </>
  );
}
