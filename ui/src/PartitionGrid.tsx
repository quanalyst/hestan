import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { get, post, usePoll } from "./api";
import type { PartitionEntry } from "./types";
import { relTime } from "./util";

// enough hex to tell fingerprints apart at a glance
const shortHash = (fp: string) => fp.slice(0, 12);

// as many cells as read as a grid rather than as a wall; the rest are counted
const SHOWN = 120;

const LEGEND = [
  ["materialized", "built and fresh"],
  ["stale", "built, inputs moved"],
  ["missing", "never built"],
] as const;

function cellTitle(p: PartitionEntry): string {
  const lines = [p.key, p.state];
  if (p.fingerprint) lines.push(shortHash(p.fingerprint));
  if (p.built_at) lines.push(`built ${relTime(p.built_at)}`);
  return lines.join("\n");
}

// one cell per key, newest first, in the established shape vocabulary: solid
// materialized, hatched stale, hollow missing. clicking one builds that key.
export default function PartitionGrid({ asset }: { asset: string }) {
  const nav = useNavigate();
  const [parts, setParts] = useState<PartitionEntry[] | null>(null);
  const [total, setTotal] = useState(0);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  usePoll(
    () => {
      get<{ total: number; partitions: PartitionEntry[] }>(
        `/api/assets/${encodeURIComponent(asset)}/partitions?limit=${SHOWN}`,
      )
        .then((r) => {
          setParts(r.partitions);
          setTotal(r.total);
        })
        .catch(() => setParts([]));
    },
    5000,
    [asset],
  );

  const build = async (key: string) => {
    setBusy(key);
    setMsg(null);
    try {
      const r = await post<{ run_id?: string }>(
        `/api/assets/${encodeURIComponent(asset)}/build`,
        { partitions: [key] },
      );
      if (r.run_id) nav(`/runs/${r.run_id}`);
    } catch (e) {
      setMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  if (parts === null) return <p className="muted">loading…</p>;
  if (parts.length === 0) return <p className="muted">no partitions</p>;

  const older = total - parts.length;
  return (
    <>
      <div className="part-grid">
        {parts.map((p) => (
          <button
            key={p.key}
            className={`part-cell ${p.state}`}
            title={cellTitle(p)}
            disabled={busy !== null}
            aria-label={`${p.key}: ${p.state}`}
            onClick={() => build(p.key)}
          />
        ))}
      </div>
      <div className="part-legend muted">
        {LEGEND.map(([state, what]) => (
          <span key={state} className="part-legend-item" title={what}>
            <span className={`part-cell ${state}`} aria-hidden="true" />
            {state}
          </span>
        ))}
      </div>
      {older > 0 && <p className="muted part-more">{older} older not shown</p>}
      {msg && <p className="muted">build failed: {msg}</p>}
    </>
  );
}
