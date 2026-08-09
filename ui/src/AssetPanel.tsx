import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { get, usePoll } from "./api";
import MetaList from "./MetaList";
import type { AssetSummary, MaterializationEntry } from "./types";
import { relTime, shortId } from "./util";

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
      get<{ materializations: MaterializationEntry[] }>(
        `/api/assets/${encodeURIComponent(asset.name)}/history`,
      )
        .then((r) => setHistory(r.materializations))
        .catch(() => setHistory([]));
    },
    5000,
    [asset.name],
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
      </div>

      <div className="sub-label">materializations</div>
      {history === null ? (
        <p className="muted">loading…</p>
      ) : history.length === 0 ? (
        <p className="muted">never built</p>
      ) : (
        history.map((m) => (
          <div key={m.id} className="mat-entry">
            <div className="mat-row">
              {/* the marker is the point: a rebuild and a change are different facts */}
              <span className="mat-mark" aria-hidden="true">
                {m.changed ? "•" : ""}
              </span>
              <span className="mono mat-fp" title={m.fingerprint}>
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
            {m.metadata && <MetaList metadata={m.metadata} />}
          </div>
        ))
      )}
      {history !== null && history.length > 0 && (
        <p className="muted op-stat-line">• marks a fingerprint that moved</p>
      )}
    </aside>
  );
}
