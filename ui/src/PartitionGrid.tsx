import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { get, post, usePoll } from "./api";
import { keysInRange, rangeOf } from "./backfill";
import type { KeyRange } from "./backfill";
import { useMay } from "./role";
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
  // what this key reads, and which upstream key left it stale: on a mapped
  // asset neither is the key itself
  for (const r of p.reads) {
    const span = r.first === r.last ? r.first : `${r.first} … ${r.last}`;
    lines.push(`reads ${r.dep}[${span ?? "nothing"}]${r.count > 1 ? ` (${r.count})` : ""}`);
    if (r.missing > 0) lines.push(`${r.missing} it covers are not keys of ${r.dep}`);
  }
  for (const r of p.reasons.filter((r) => r.partition !== null)) {
    lines.push(`${r.dep}[${r.partition}] moved`);
  }
  // what its policy wants and cannot have yet, which is why a stale key can
  // sit here with nothing building it
  if (p.waiting) lines.push(`waiting for ${p.waiting}`);
  return lines.join("\n");
}

// one cell per key, newest first, in the established shape vocabulary: solid
// materialized, hatched stale, hollow missing. clicking one builds that key,
// unless the grid was handed a range to fill, in which case dragging across it
// picks the span a backfill covers and a click builds nothing.
export default function PartitionGrid({
  asset,
  range,
  onShown,
}: {
  asset: string;
  range?: { value: KeyRange | null; onChange: (r: KeyRange | null) => void };
  // the keys this grid is drawing, for a caller that has to reason about them
  onShown?: (shown: PartitionEntry[]) => void;
}) {
  // the grid is a picture of what is built, and it stays one for everybody;
  // what a viewer does not get is the click that builds a key
  const mayBuild = useMay("operator");
  const nav = useNavigate();
  const [parts, setParts] = useState<PartitionEntry[] | null>(null);
  const [total, setTotal] = useState(0);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  // the cell the drag started on; the other end is wherever it is now
  const [anchor, setAnchor] = useState<string | null>(null);

  usePoll(
    () => {
      get<{ total: number; partitions: PartitionEntry[] }>(
        `/api/assets/${encodeURIComponent(asset)}/partitions?limit=${SHOWN}`,
      )
        .then((r) => {
          setParts(r.partitions);
          setTotal(r.total);
          onShown?.(r.partitions);
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

  // a drag in progress shows what it covers as it goes, so the count under the
  // grid is the count you would get by letting go now
  const selected = new Set(
    range === undefined ? [] : keysInRange(parts, range.value).map((p) => p.key),
  );
  const extend = (key: string) => {
    if (anchor === null || range === undefined) return;
    range.onChange(rangeOf(parts, anchor, key));
  };

  const older = total - parts.length;
  return (
    <>
      <div
        className="part-grid"
        onMouseLeave={() => setAnchor(null)}
        onMouseUp={() => setAnchor(null)}
      >
        {parts.map((p) => (
          <button
            key={p.key}
            className={`part-cell ${p.state}${selected.has(p.key) ? " picked" : ""}`}
            title={cellTitle(p)}
            disabled={busy !== null}
            aria-label={`${p.key}: ${p.state}`}
            onMouseDown={() => {
              if (range === undefined) return;
              setAnchor(p.key);
              range.onChange({ from: p.key, to: p.key });
            }}
            onMouseEnter={() => extend(p.key)}
            onClick={() => range === undefined && mayBuild && build(p.key)}
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
