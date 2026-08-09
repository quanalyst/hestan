import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { get, usePoll } from "./api";
import MetaList from "./MetaList";
import PartitionGrid from "./PartitionGrid";
import { GlyphShape } from "./StatusGlyph";
import type {
  AssetCheckResult,
  AssetSummary,
  MaterializationEntry,
  MetaPoint,
  Trends,
} from "./types";
import { numericMetaKeys, relTime, shortId } from "./util";

// enough hex to tell fingerprints apart at a glance; the title carries the rest
const shortHash = (fp: string) => fp.slice(0, 12);

export default function AssetPanel({
  asset,
  onClose,
}: {
  asset: AssetSummary;
  onClose: () => void;
}) {
  const [history, setHistory] = useState<MaterializationEntry[] | null>(null);
  const [checks, setChecks] = useState<AssetCheckResult[]>([]);
  const [trends, setTrends] = useState<Trends>({});

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  // the panel is keyed on the asset upstream, so a new selection remounts and
  // this starts over rather than showing the previous asset's history
  usePoll(
    () => {
      const name = encodeURIComponent(asset.name);
      get<{ materializations: MaterializationEntry[] }>(`/api/assets/${name}/history`)
        .then((r) => setHistory(r.materializations))
        .catch(() => setHistory([]));
      get<{ checks: AssetCheckResult[] }>(`/api/assets/${name}/checks`)
        .then((r) => setChecks(r.checks))
        .catch(() => {});
    },
    5000,
    [asset.name],
  );

  // one series per numeric key of the newest build, for the sparkline under
  // its value: refetched when the keys change or a build lands, rather than
  // on every poll of the history
  const latest = history?.[0] ?? null;
  const keys = numericMetaKeys(latest?.metadata ?? null).join(",");
  const newest = latest?.id ?? null;
  useEffect(() => {
    if (keys === "") {
      setTrends({});
      return;
    }
    let live = true;
    const name = encodeURIComponent(asset.name);
    Promise.all(
      keys.split(",").map((key) =>
        get<{ points: MetaPoint[] }>(`/api/assets/${name}/metadata/${encodeURIComponent(key)}`)
          .then((r) => [key, r.points] as const)
          .catch(() => [key, []] as const),
      ),
    ).then((series) => {
      if (live) setTrends(Object.fromEntries(series));
    });
    return () => {
      live = false;
    };
  }, [asset.name, keys, newest]);

  // the api hands back every check's results newest first, mixed together, so
  // the first row for a name is that check's latest
  const latestChecks = checks.filter(
    (c, i) => checks.findIndex((d) => d.check === c.check) === i,
  );

  return (
    <aside className="op-panel">
      <div className="op-panel-head">
        <span className="mono op-title">{asset.name}</span>
        <button className="text-btn" onClick={onClose} aria-label="close">
          ×
        </button>
      </div>

      <div className="op-lines">
        <div className="op-line">
          <span className="op-line-label">kind</span>
          <span className="mono">{asset.kind}</span>
        </div>
        {/* only worth a line when it is not simply the asset's own name */}
        {asset.op && asset.op !== asset.name && (
          <div className="op-line">
            <span className="op-line-label">op</span>
            <span className="mono">{asset.op}</span>
          </div>
        )}
        {asset.deps.length > 0 && (
          <div className="op-line">
            <span className="op-line-label">deps</span>
            <span className="mono">{asset.deps.join(", ")}</span>
          </div>
        )}
        <div className="op-line">
          <span className="op-line-label">state</span>
          <span className="mono">{asset.stale ? "stale" : "fresh"}</span>
        </div>
        {asset.partitions && (
          <div className="op-line">
            <span className="op-line-label">partitions</span>
            <span className="mono">
              {asset.partitions.materialized}/{asset.partitions.total} built
            </span>
          </div>
        )}
      </div>

      {asset.partitions && (
        <>
          <div className="sub-label">partitions</div>
          <PartitionGrid asset={asset.name} />
        </>
      )}

      {latestChecks.length > 0 && (
        <>
          <div className="sub-label">checks</div>
          {latestChecks.map((c) => (
            <div key={c.check} className="mat-entry">
              <div className="mat-row">
                <span className="mat-mark" aria-hidden="true">
                  <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12">
                    <GlyphShape status={c.status === "passed" ? "success" : "failed"} />
                  </svg>
                </span>
                <span className="mono mat-fp">{c.check}</span>
                {/* severity only earns a word when it changes what a failure did */}
                {c.severity === "warn" && <span className="muted">warn</span>}
                <span className="muted mat-when" title={c.checked_at}>
                  {relTime(c.checked_at)}
                </span>
              </div>
              {c.message && <div className="muted check-msg">{c.message}</div>}
              {c.metadata && <MetaList metadata={c.metadata} />}
            </div>
          ))}
        </>
      )}

      <div className="sub-label">materializations</div>
      {history === null ? (
        <p className="muted">loading…</p>
      ) : history.length === 0 ? (
        <p className="muted">never built</p>
      ) : (
        history.map((m, i) => (
          <div key={m.id} className="mat-entry">
            <div className="mat-row">
              {/* the marker is the point: a rebuild and a change are different facts */}
              <span className="mat-mark" aria-hidden="true">
                {m.changed ? "•" : ""}
              </span>
              <span className="mono mat-fp" title={m.fingerprint}>
                {m.partition ? `${m.partition} ` : ""}
                {shortHash(m.fingerprint)}
              </span>
              <span className="muted mat-when" title={m.built_at}>
                {relTime(m.built_at)}
              </span>
              {m.run_id ? (
                <Link className="mono mat-run" to={`/runs/${m.run_id}`}>
                  {shortId(m.run_id)}
                </Link>
              ) : (
                <span className="muted mat-run">probe</span>
              )}
            </div>
            {/* what that build reported, in the same gutter as the marker */}
            {/* the trend belongs to the current value, so it is drawn under
                the newest entry rather than repeated down the history */}
            {m.metadata && (
              <MetaList metadata={m.metadata} deltas={m.deltas} trends={i === 0 ? trends : {}} />
            )}
          </div>
        ))
      )}
      {history !== null && history.length > 0 && (
        <p className="muted op-stat-line">• marks a fingerprint that moved</p>
      )}
    </aside>
  );
}
