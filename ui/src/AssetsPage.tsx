import { useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { get, post, usePoll } from "./api";
import { useMay } from "./role";
import { StateGlyph } from "./AssetDetail";
import AssetPanel from "./AssetPanel";
import DagView from "./DagView";
import type { NodeStatus } from "./DagView";
import { GlyphShape } from "./StatusGlyph";
import type { Status } from "./StatusGlyph";
import {
  SEPARATOR,
  STATE_FILTERS,
  filterAssets,
  groupAssets,
  groupOf,
  sortAssets,
} from "./catalog";
import type { Dir, SortKey, StateFilter } from "./catalog";
import { FOCUS_MAX, WHOLE_GRAPH_MAX, collapseGroups, groupNode, neighbourhood } from "./dag";
import type {
  AssetSummary,
  Backfill,
  BackfillStatus,
  CheckSummary,
  SensorOutcome,
  SensorSummary,
} from "./types";
import { assetPath, fmtDuration, fmtEvery, relTime, shortId, untilTime } from "./util";

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

// inside a group the prefix is the heading, so repeating it on every row is
// noise; an ungrouped row keeps its whole name
const leafName = (name: string, prefix: string) =>
  prefix === "" ? name : name.slice(prefix.length + 1);

const coverTitle = (a: AssetSummary) =>
  a.partitions === null
    ? undefined
    : `${a.partitions.materialized} fresh · ${a.partitions.stale} stale · ${a.partitions.missing} never built`;

// a sortable heading: clicking it sorts, clicking it again turns it around.
// the arrow is the only thing that says which, since the ui has no colour to
// say it with
function Column({
  label,
  sort,
  active,
  dir,
  onSort,
}: {
  label: string;
  sort: SortKey;
  active: SortKey;
  dir: Dir;
  onSort: (key: SortKey) => void;
}) {
  return (
    <th className="sortable" onClick={() => onSort(sort)}>
      {label}
      <span className="sort-mark" aria-hidden="true">
        {active === sort ? (dir === "asc" ? "↑" : "↓") : ""}
      </span>
    </th>
  );
}

// a node's staleness, whether it is one asset or a folded group of them
function staleOf(assets: AssetSummary[], node: string): boolean {
  const prefix = node.endsWith(SEPARATOR) ? node.slice(0, -1) : null;
  if (prefix === null) return assets.some((a) => a.name === node && a.stale);
  return assets.some((a) => groupOf(a.name) === prefix && a.stale);
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

export default function AssetsPage() {
  // building an asset and cancelling a backfill are work; pausing a sensor is
  // a decision about what the deployment does next
  const mayBuild = useMay("operator");
  const mayPause = useMay("admin");
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
  // every control on this page lives in the url, so a filtered, grouped,
  // sorted view is a link somebody else can open — including the selection,
  // which is where a Meta::AssetRef used to point
  const [params, setParams] = useSearchParams();
  const sel = params.get("asset");
  const query = params.get("q") ?? "";
  const stateFilter = (params.get("state") ?? "all") as StateFilter;
  const sortKey = (params.get("sort") ?? "name") as SortKey;
  const dir = (params.get("dir") ?? "asc") as Dir;
  // one hop is what feeds it and what it feeds, which is the question that
  // brought you to a focused graph; two is already most of a wide graph
  const depth = Number(params.get("depth") ?? 1);
  const closed = new Set((params.get("closed") ?? "").split(",").filter(Boolean));

  const set = (edits: Record<string, string | null>) =>
    setParams(
      (prev) => {
        for (const [key, value] of Object.entries(edits)) {
          if (value === null || value === "") prev.delete(key);
          else prev.set(key, value);
        }
        return prev;
      },
      { replace: true },
    );
  const select = (name: string | null) =>
    set({ asset: name === null || name === sel ? null : name });
  const toggleGroup = (prefix: string) => {
    const next = new Set(closed);
    if (!next.delete(prefix)) next.add(prefix);
    set({ closed: [...next].join(",") });
  };
  // a column already sorted turns around rather than re-sorting the same way
  const sortBy = (key: SortKey) =>
    set({ sort: key, dir: sortKey === key && dir === "asc" ? "desc" : "asc" });

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
  const shown = sortAssets(filterAssets(assets, query, stateFilter), sortKey, dir);
  const groups = groupAssets(shown);
  // the fold chips are about the registry, not about what survived a filter:
  // a group filtered down to nothing still has a name and still folds
  const allGroups = groupAssets(assets).filter((g) => g.prefix !== "");
  // a column only earns its width where something fills it
  const anyFreshness = assets.some((a) => a.freshness !== null);
  const anyPartitioned = assets.some((a) => a.partitions !== null);
  const columns = 6 + Number(anyFreshness) + Number(anyPartitioned);

  // the graph draws the whole registry rather than the filtered rows: what
  // feeds a thing does not stop mattering because it was filtered out. the
  // search highlights in it instead
  const whole = assets.map((a) => ({
    name: a.name,
    deps: a.deps,
    note: a.kind === "source" ? "source" : undefined,
  }));
  const folded = collapseGroups(whole, closed);
  // past the threshold the whole graph is a picture of having a lot of assets
  // rather than of how they fit together, so it opens focused — on the
  // selection, or on the first thing that is stale, which is what anyone
  // opening a graph of three hundred assets came to look at
  const mode = params.get("graph") ?? (folded.length > WHOLE_GRAPH_MAX ? "focus" : "whole");
  const stale = folded.find((n) => staleOf(assets, n.name));
  const focus = mode === "whole" ? null : (sel ?? stale?.name ?? folded[0]?.name ?? null);
  const nodes = focus === null ? folded : neighbourhood(folded, focus, depth);
  const staleness: Record<string, NodeStatus> = Object.fromEntries([
    ...assets.map((a) => [a.name, a.stale ? "stale" : "fresh"] as const),
    // a folded group is stale if anything in it is: the one claim that is
    // true of the group rather than of one of its members
    ...groupAssets(assets)
      .filter((g) => g.prefix !== "" && closed.has(g.prefix))
      .map((g) => [groupNode(g.prefix), g.assets.some((a) => a.stale) ? "stale" : "fresh"] as const),
  ]);

  return (
    <>
      <div className="page-head">
        <h1>Assets</h1>
        {mayBuild && anyStale && (
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
          <h2>
            graph
            <span className="log-filter">
              {(["whole", "focus"] as const).map((m) => (
                <button
                  key={m}
                  className={mode === m ? "text-btn active" : "text-btn"}
                  onClick={() => set({ graph: m })}
                >
                  {m}
                </button>
              ))}
            </span>
            {mode === "focus" && (
              <span className="log-filter">
                {[1, 2, 3].map((d) => (
                  <button
                    key={d}
                    className={depth === d ? "text-btn active" : "text-btn"}
                    onClick={() => set({ depth: String(d) })}
                  >
                    {d}
                  </button>
                ))}
              </span>
            )}
          </h2>
          {allGroups.length > 0 && (
            <div className="group-chips">
              <span className="filter-label">fold</span>
              {allGroups.map((g) => (
                <button
                  key={g.prefix}
                  className={closed.has(g.prefix) ? "text-btn active" : "text-btn"}
                  onClick={() => toggleGroup(g.prefix)}
                >
                  {g.prefix}/
                </button>
              ))}
            </div>
          )}
          <DagView
            label="asset dependency graph"
            nodes={nodes}
            statuses={staleness}
            selected={sel}
            onSelect={select}
            highlight={query}
          />
          <p className="muted dag-action">
            {focus === null
              ? `the whole graph · ${nodes.length} nodes`
              : `focused on ${focus} · ${nodes.length} of ${folded.length} nodes, ${depth} hop${depth > 1 ? "s" : ""} out`}
            {closed.size > 0 && ` · ${closed.size} group${closed.size > 1 ? "s" : ""} folded`}
            {focus !== null && nodes.length >= FOCUS_MAX && " · fold a group to see more of it"}
          </p>

          <h2>
            assets
            {shown.length !== assets.length && (
              <span className="secondary">
                {" "}
                — {shown.length} of {assets.length}
              </span>
            )}
          </h2>
          <div className="filter-row">
            <span className="filter-group">
              <span className="filter-label">state</span>
              {STATE_FILTERS.map((f) => (
                <button
                  key={f}
                  className={stateFilter === f ? "text-btn active" : "text-btn"}
                  onClick={() => set({ state: f === "all" ? null : f })}
                >
                  {f === "never" ? "never built" : f === "failed" ? "failed check" : f}
                </button>
              ))}
            </span>
            <span className="filter-group">
              <span className="filter-label">find</span>
              <input
                className="filter-input"
                value={query}
                placeholder="name"
                onChange={(e) => set({ q: e.target.value })}
                onKeyDown={(e) => {
                  if (e.key === "Escape") set({ q: null });
                }}
              />
            </span>
          </div>
          {shown.length === 0 ? (
            <p className="muted">no asset matches the filter</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <Column label="name" sort="name" active={sortKey} dir={dir} onSort={sortBy} />
                  <Column label="state" sort="state" active={sortKey} dir={dir} onSort={sortBy} />
                  <Column label="built" sort="built" active={sortKey} dir={dir} onSort={sortBy} />
                  <th>run</th>
                  {anyFreshness && (
                    <Column
                      label="freshness"
                      sort="freshness"
                      active={sortKey}
                      dir={dir}
                      onSort={sortBy}
                    />
                  )}
                  {anyPartitioned && (
                    <Column
                      label="partitions"
                      sort="coverage"
                      active={sortKey}
                      dir={dir}
                      onSort={sortBy}
                    />
                  )}
                  <th>checks</th>
                  <th />
                </tr>
              </thead>
              {groups.map((g) => (
                <tbody key={g.prefix}>
                  {/* the unprefixed assets are a heading too, or the first of
                      them reads as the last row of the group above */}
                  {groups.length > 1 && g.prefix === "" && (
                    <tr className="group-row plain-row">
                      <td colSpan={columns}>
                        <span className="group-mark" aria-hidden="true" />
                        <span className="muted">no prefix · {g.assets.length}</span>
                      </td>
                    </tr>
                  )}
                  {g.prefix !== "" && (
                    <tr className="group-row" onClick={() => toggleGroup(g.prefix)}>
                      <td colSpan={columns}>
                        <span className="group-mark" aria-hidden="true">
                          {closed.has(g.prefix) ? "▸" : "▾"}
                        </span>
                        {g.prefix}
                        <span className="muted"> · {g.assets.length}</span>
                      </td>
                    </tr>
                  )}
                  {(g.prefix === "" || !closed.has(g.prefix)) &&
                    g.assets.map((a) => (
                      <tr key={a.name} onClick={() => select(a.name)}>
                        <td>
                          {/* the row opens the panel; the name is the permanent
                              address, which is the thing you send somebody */}
                          <Link to={assetPath(a.name)} onClick={(e) => e.stopPropagation()}>
                            {leafName(a.name, g.prefix)}
                          </Link>
                          {a.kind === "source" && <span className="tag">source</span>}
                          {a.auto && <span className="tag">auto</span>}
                          {a.freshness?.status === "late" && <span className="tag">late</span>}
                        </td>
                        <td>
                          <span className="status-cell">
                            <StateGlyph stale={a.stale} />
                            {a.stale && (
                              <span className="muted" title={staleTitle(a)}>
                                {staleSummary(a)}
                              </span>
                            )}
                          </span>
                        </td>
                        <td className="muted" title={a.built_at ?? undefined}>
                          {a.partitions ? "—" : relTime(a.built_at)}
                        </td>
                        <td>
                          {a.run_id ? (
                            <Link
                              className="mono"
                              to={`/runs/${a.run_id}`}
                              onClick={(e) => e.stopPropagation()}
                            >
                              {shortId(a.run_id)}
                            </Link>
                          ) : (
                            <span className="muted">{a.built_at ? "probe" : "—"}</span>
                          )}
                        </td>
                        {anyFreshness && (
                          <td className="muted">
                            {a.freshness === null
                              ? "—"
                              : a.freshness.late_by_secs !== null
                                ? `late by ${fmtDuration(a.freshness.late_by_secs * 1000)}`
                                : `within ${fmtEvery(a.freshness.within_secs)}`}
                          </td>
                        )}
                        {anyPartitioned && (
                          <td className="mono" title={a.partitions ? coverTitle(a) : undefined}>
                            {a.partitions
                              ? `${a.partitions.materialized}/${a.partitions.total}`
                              : <span className="muted">—</span>}
                          </td>
                        )}
                        <td>
                          <Checks checks={a.checks} />
                        </td>
                        <td className="row-action">
                          {/* sources are probed, never built — the endpoint 400s */}
                          {mayBuild && a.kind !== "source" && (
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
                          {rowMsg?.asset === a.name && (
                            <span className="muted row-err">{rowMsg.msg}</span>
                          )}
                        </td>
                      </tr>
                    ))}
                </tbody>
              ))}
            </table>
          )}
        </>
      )}

      {backfills.length > 0 && (
        <>
          <h2>backfills</h2>
          <table className="plain-rows">
            <thead>
              <tr>
                <th className="num">#</th>
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
                    <td className="num">
                      <Link className="mono" to={`/backfills/${b.id}`}>
                        {b.id}
                      </Link>
                    </td>
                    <td>
                      <Link to={assetPath(b.asset)}>{b.asset}</Link>
                    </td>
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
                      {mayBuild && b.status === "running" && (
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
                <th className="num">skipped</th>
                <th className="num">took</th>
                <th>next</th>
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
                    <td className="num">{s.last_tick ? s.last_tick.skipped : "—"}</td>
                    <td className="num mono">
                      {s.last_tick ? fmtDuration(s.last_tick.duration_ms) : "—"}
                    </td>
                    <td className="muted">
                      {untilTime(s.next_eval)}
                      {s.consecutive_failures > 0 && (
                        <span className="tag" title={`${s.consecutive_failures} failed evaluations in a row`}>
                          backing off
                        </span>
                      )}
                    </td>
                    <td className="row-action">
                      {mayPause && (
                        <button className="text-btn" onClick={() => setPaused(s.name, !s.paused)}>
                          {s.paused ? "resume" : "pause"}
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {sensorErr && <p className="muted">sensor update failed: {sensorErr}</p>}
        </>
      )}

      {selected && <AssetPanel key={selected.name} asset={selected} onClose={() => select(null)} />}
    </>
  );
}
