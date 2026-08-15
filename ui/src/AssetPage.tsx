import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { get, post, usePoll } from "./api";
import AssetDetail, { StateGlyph } from "./AssetDetail";
import BackfillLauncher from "./BackfillLauncher";
import type { KeyRange } from "./backfill";
import { CHAIN_DEPTH, downstreamOf, linkKind, movedInputs, whenChanged } from "./lineage";
import type { ChainLink } from "./lineage";
import { useMay } from "./role";
import { GlyphShape } from "./StatusGlyph";
import type {
  AssetSummary,
  Backfill,
  Freshness,
  InputFingerprint,
  MaterializationEntry,
  PartitionEntry,
  StaleReason,
} from "./types";
import { assetPath, fmtDuration, fmtEvery, relTime, shortId } from "./util";

// enough hex to tell fingerprints apart at a glance; the title carries the rest
const shortHash = (fp: string) => fp.slice(0, 12);

// how far back the chain reads for the build a fingerprint arrived in. a
// fingerprint older than this is reported as unreachable rather than guessed at
const CHAIN_HISTORY = 50;

async function historyOf(
  asset: string,
  partition: string | null,
  seen: Map<string, MaterializationEntry[]>,
): Promise<MaterializationEntry[]> {
  // one key's history, where the reason names one: a partitioned asset's
  // history interleaves every key, and the newest entry of the whole asset is
  // rarely the newest entry of the key that moved
  const at = partition === null ? "" : `&partition=${encodeURIComponent(partition)}`;
  const key = `${asset}[${partition ?? ""}]`;
  const held = seen.get(key);
  if (held) return held;
  const r = await get<{ materializations: MaterializationEntry[] }>(
    `/api/assets/${encodeURIComponent(asset)}/history?limit=${CHAIN_HISTORY}${at}`,
  ).catch(() => ({ materializations: [] }));
  seen.set(key, r.materializations);
  return r.materializations;
}

// what a build recorded for one dep, at the key that moved: the dep's own
// fingerprint where it was read whole, and one key's where it was mapped
function fingerprintAt(held: InputFingerprint | undefined, partition: string | null): string | null {
  if (held === undefined) return null;
  if (typeof held !== "object" || held === null) return held ?? null;
  return partition === null ? null : (held[partition] ?? null);
}

// walk the reasons outward. an upstream whose content moved is asked which of
// *its* inputs moved in the build that did it; an upstream that is only stale
// itself is asked its own reasons, which the assets list already carries
async function walk(
  reasons: StaleReason[],
  assets: AssetSummary[],
  depth: number,
  seen: Map<string, MaterializationEntry[]>,
): Promise<ChainLink[]> {
  const links: ChainLink[] = [];
  for (const r of reasons) {
    const kind = linkKind(r);
    const at =
      kind === "changed" ? whenChanged(await historyOf(r.dep, r.partition, seen), r.now) : null;
    let under: StaleReason[] = [];
    if (depth > 1 && at !== null && at.before !== null) {
      under = movedInputs(at.built, at.before).map(({ dep, partition }) => ({
        dep,
        partition,
        had: fingerprintAt(at.before!.inputs[dep], partition),
        now: fingerprintAt(at.built.inputs[dep], partition),
      }));
    } else if (depth > 1 && kind === "pending") {
      under = assets.find((a) => a.name === r.dep)?.reasons ?? [];
    }
    links.push({
      asset: r.dep,
      partition: r.partition,
      kind,
      had: r.had,
      now: r.now,
      built: at?.built ?? null,
      before: at?.before ?? null,
      causes: await walk(under, assets, depth - 1, seen),
    });
  }
  return links;
}

// the causal chain, one row per upstream that moved, nested under the build it
// moved in. the fingerprints are what makes this provable rather than inferred,
// so they are on the row and not on a hover
function Chain({ links }: { links: ChainLink[] }) {
  return (
    <>
      {links.map((link) => (
        <div key={`${link.asset}[${link.partition ?? ""}]`} className="chain-link">
          <div className="chain-row">
            <Link className="mono" to={assetPath(link.asset)}>
              {link.asset}
              {link.partition !== null && `[${link.partition}]`}
            </Link>
            <span className="muted">
              {link.kind === "changed"
                ? "changed"
                : link.kind === "pending"
                  ? "is stale itself"
                  : "has never been built"}
            </span>
            {link.kind === "changed" && (
              <span className="mono chain-fp" title={`${link.had ?? "none"} -> ${link.now ?? "none"}`}>
                {link.had ? shortHash(link.had) : "none"} → {link.now ? shortHash(link.now) : "none"}
              </span>
            )}
          </div>
          <div className="chain-when muted">
            {link.kind === "pending" ? (
              "nothing has moved here yet: rebuilding it is what would move it"
            ) : link.kind === "absent" ? (
              "there is no fingerprint to compare against"
            ) : link.built === null ? (
              "no recorded build of it holds that fingerprint"
            ) : (
              <>
                in{" "}
                {link.built.run_id ? (
                  <Link className="mono" to={`/runs/${link.built.run_id}`}>
                    {shortId(link.built.run_id)}
                  </Link>
                ) : (
                  <span className="mono">a probe</span>
                )}
                <span title={link.built.built_at}> · {relTime(link.built.built_at)}</span>
                {link.before === null && " · the first build recorded"}
              </>
            )}
          </div>
          {link.causes.length > 0 && (
            <div className="chain-causes">
              <Chain links={link.causes} />
            </div>
          )}
        </div>
      ))}
    </>
  );
}

// how many neighbours read as a list rather than as a column down the page.
// a source everything hangs off has sixty, and sixty names is not lineage
const NEIGHBOURS = 18;

// one side of the lineage: names that link, wrapped rather than stacked, with
// the rest behind a count: a hub asset's dependents are a fact about the
// graph, not a list to scroll past
function LineageList({
  label,
  names,
  empty,
}: {
  label: string;
  names: string[];
  empty: string;
}) {
  const [all, setAll] = useState(false);
  const shown = all ? names : names.slice(0, NEIGHBOURS);
  return (
    <div className="lineage-side">
      <div className="filter-label">{label}</div>
      {names.length === 0 ? (
        <p className="muted">{empty}</p>
      ) : (
        <div className="lineage-names">
          {shown.map((d) => (
            <Link key={d} className="mono" to={assetPath(d)}>
              {d}
            </Link>
          ))}
          {names.length > shown.length && (
            <button className="text-btn" onClick={() => setAll(true)}>
              {names.length - shown.length} more
            </button>
          )}
        </div>
      )}
    </div>
  );
}

// a declared policy and how far through it this asset is. the window is the
// point: "fresh" with four of six hours gone is a different fact from "fresh"
function FreshnessLine({ freshness }: { freshness: Freshness }) {
  const { within_secs, late_by_secs, last_success, status } = freshness;
  const elapsed =
    last_success === null ? null : (Date.now() - new Date(last_success).getTime()) / 1000;
  const through = elapsed === null ? 0 : Math.min(1, elapsed / Math.max(within_secs, 1));
  return (
    <div className="fresh-line">
      <span className="status">
        <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
          <GlyphShape
            status={status === "fresh" ? "success" : status === "late" ? "failed" : "pending"}
          />
        </svg>
        {status}
      </span>
      <span className="fresh-bar" aria-hidden="true">
        <span style={{ width: `${through * 100}%` }} />
      </span>
      <span className="muted">
        {status === "never"
          ? `nothing has succeeded yet · window ${fmtEvery(within_secs)}`
          : late_by_secs !== null
            ? `late by ${fmtDuration(late_by_secs * 1000)} · window ${fmtEvery(within_secs)}`
            : `${fmtDuration((elapsed ?? 0) * 1000)} of ${fmtEvery(within_secs)} used`}
      </span>
    </div>
  );
}

export default function AssetPage() {
  const params = useParams();
  const name = params["*"] ?? "";
  if (!name) return null;
  // keyed so an asset switch remounts: no chain leaks across it
  return <AssetView key={name} name={name} />;
}

function AssetView({ name }: { name: string }) {
  const mayBuild = useMay("operator");
  const nav = useNavigate();
  const [assets, setAssets] = useState<AssetSummary[] | null>(null);
  const [chain, setChain] = useState<ChainLink[] | null>(null);
  const [building, setBuilding] = useState(false);
  const [buildMsg, setBuildMsg] = useState<string | null>(null);
  const [range, setRange] = useState<KeyRange | null>(null);
  const [shown, setShown] = useState<PartitionEntry[]>([]);
  const [backfills, setBackfills] = useState<Backfill[]>([]);
  // the walk reads every asset's reasons, but a new list every 5s is not a new
  // chain: the reasons are what changed it, so the list is held in a ref and
  // the effect turns on those
  const listRef = useRef<AssetSummary[]>([]);

  usePoll(
    () => {
      get<{ assets: AssetSummary[] }>("/api/assets")
        .then((r) => {
          listRef.current = r.assets;
          setAssets(r.assets);
        })
        .catch(() => {});
      get<{ backfills: Backfill[] }>("/api/backfills?limit=20")
        .then((r) => setBackfills(r.backfills))
        .catch(() => {});
    },
    5000,
    [],
  );

  const asset = assets?.find((a) => a.name === name) ?? null;
  // the reasons are the whole of what the chain is a walk of, so serializing
  // them is both the trigger and the input
  const reasonKey = JSON.stringify(asset?.reasons ?? []);
  useEffect(() => {
    const reasons: StaleReason[] = JSON.parse(reasonKey);
    if (reasons.length === 0) {
      setChain([]);
      return;
    }
    let live = true;
    walk(reasons, listRef.current, CHAIN_DEPTH, new Map()).then((links) => {
      if (live) setChain(links);
    });
    return () => {
      live = false;
    };
  }, [name, reasonKey]);

  const build = async () => {
    setBuilding(true);
    setBuildMsg(null);
    try {
      const r = await post<{ run_id?: string; up_to_date?: boolean }>(
        `/api/assets/${encodeURIComponent(name)}/build`,
      );
      if (r.run_id) nav(`/runs/${r.run_id}`);
      else setBuildMsg("up to date");
    } catch (e) {
      setBuildMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setBuilding(false);
    }
  };

  if (!assets) return <p className="muted">loading…</p>;
  if (!asset) return <p className="muted">no asset named {name}</p>;

  const upstream = asset.deps;
  const downstream = downstreamOf(assets, name);
  // one backfill per asset at a time, which the api enforces and the launcher
  // says before the click rather than after it
  const running = backfills.find((b) => b.asset === name && b.status === "running") ?? null;
  const mine = backfills.filter((b) => b.asset === name);
  // "built" would count a key built against inputs that have since moved,
  // which is exactly the thing the grid draws differently
  const built = asset.partitions
    ? `${asset.partitions.materialized} of ${asset.partitions.total} partitions fresh`
    : asset.built_at === null
      ? "never built"
      : `built ${relTime(asset.built_at)}`;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>
            {asset.name}
            <Link className="head-link" to="/assets">
              all assets
            </Link>
          </h1>
          <p className="muted">
            {asset.kind}
            {" · "}
            <span title={asset.built_at ?? undefined}>{built}</span>
            {asset.run_id && (
              <>
                {" · "}
                <Link className="mono" to={`/runs/${asset.run_id}`}>
                  {shortId(asset.run_id)}
                </Link>
              </>
            )}
            {asset.auto && " · auto"}
          </p>
        </div>
        <div className="run-actions">
          <div className="run-side">
            <span className="pill">
              <StateGlyph stale={asset.stale} />
            </span>
            {/* sources are probed, never built: the endpoint 400s */}
            {mayBuild && asset.kind !== "source" && (
              <button onClick={build} disabled={building}>
                build
              </button>
            )}
          </div>
          {buildMsg && <p className="muted">{buildMsg}</p>}
        </div>
      </div>

      {asset.stale && (
        <>
          <h2>why it is stale</h2>
          {asset.partitions ? (
            <p className="muted">
              {asset.partitions.stale} of {asset.partitions.total} keys were built against inputs
              that have since moved, and {asset.partitions.missing} have never been built. the grid
              below says which.
            </p>
          ) : asset.reasons.length === 0 ? (
            <p className="muted">
              nothing has ever materialized it
              {asset.kind === "source"
                ? ", and its probe has not run yet."
                : ". build it, and there will be something to compare against."}
            </p>
          ) : chain === null ? (
            <p className="muted">reading upstream history…</p>
          ) : (
            <>
              <div className="chain">
                <Chain links={chain} />
              </div>
              {chain.some((l) => l.kind === "changed") && (
                <p className="muted chain-note">
                  the fingerprint this asset consumed → the one that upstream holds now. it has not
                  been rebuilt since it read the first.
                </p>
              )}
            </>
          )}
        </>
      )}

      {asset.freshness && (
        <>
          <h2>freshness</h2>
          <FreshnessLine freshness={asset.freshness} />
        </>
      )}

      <h2>lineage</h2>
      <div className="lineage">
        <LineageList label="upstream" names={upstream} empty="nothing: it reads the outside world" />
        <LineageList label="downstream" names={downstream} empty="nothing reads it" />
      </div>

      <h2>detail</h2>
      <AssetDetail
        asset={asset}
        // a partitioned asset's grid is where a backfill range is picked, so
        // on the page it selects rather than building the key under the cursor
        range={asset.partitions ? { value: range, onChange: setRange } : undefined}
        onShown={setShown}
        partitionAction={
          mayBuild &&
          asset.partitions && (
            <BackfillLauncher
              asset={asset}
              shown={shown}
              range={range}
              onRange={setRange}
              running={running}
            />
          )
        }
      />

      {mine.length > 0 && (
        <>
          <h2>backfills</h2>
          {mine.map((b) => (
            <div key={b.id} className="tick-row">
              <Link className="mono" to={`/backfills/${b.id}`}>
                backfill {b.id}
              </Link>
              <span className="mono muted">
                {b.from_key} → {b.to_key}
              </span>
              <span className="muted">
                {b.launched}/{b.total} launched · {b.status}
              </span>
              <span className="muted" title={b.created_at}>
                {relTime(b.created_at)}
              </span>
            </div>
          ))}
        </>
      )}
    </>
  );
}
