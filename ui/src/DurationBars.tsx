import { useId, useState } from "react";
import { useNavigate } from "react-router-dom";
import { DIM, HatchPattern } from "./chart";
import StatusDot from "./StatusDot";
import { GlyphShape } from "./StatusGlyph";
import type { Run } from "./types";
import { durationMs, fmtDuration, relTime, shortId } from "./util";

const BAR_W = 14;
const GAP = 8;
const PLOT_H = 80;
const TOP = 16;

export default function DurationBars({ runs }: { runs: Run[] }) {
  const nav = useNavigate();
  const hatch = useId();
  const [hover, setHover] = useState<number | null>(null);

  // runs arrive newest first; the chart reads oldest on the left
  const finished = runs.filter((r) => durationMs(r) !== null).slice(0, 15).reverse();
  if (finished.length === 0) return <p className="muted">no finished runs yet</p>;

  const durs = finished.map((r) => durationMs(r)!);
  const max = Math.max(...durs);
  const tip = hover !== null ? finished[hover] : undefined;
  const w = finished.length * (BAR_W + GAP) - GAP;
  const baseY = TOP + PLOT_H;
  const anyFailed = finished.some((r) => r.status === "failed");
  const anyCanceled = finished.some((r) => r.status === "canceled");

  const bar = (d: number, i: number) => {
    const h = Math.max(3, (d / Math.max(max, 1)) * PLOT_H);
    const r = Math.min(4, h, BAR_W / 2);
    const x = i * (BAR_W + GAP);
    const top = baseY - h;
    return `M ${x} ${baseY} V ${top + r} Q ${x} ${top} ${x + r} ${top} H ${x + BAR_W - r} Q ${x + BAR_W} ${top} ${x + BAR_W} ${top + r} V ${baseY} Z`;
  };

  return (
    <>
      <div className="bars">
        <svg width={w} height={baseY + 1}>
          <HatchPattern id={hatch} />
          <text className="bars-max" x={0} y={10}>
            max {fmtDuration(max)}
          </text>
          {finished.map((run, i) => (
            <g key={run.id}>
              <path
                className={hover === i ? "bar bar-hot" : "bar"}
                d={bar(durs[i], i)}
                fill={
                  run.status === "failed"
                    ? `url(#${hatch})`
                    : run.status === "canceled"
                      ? "var(--mark-muted)"
                      : "var(--mark)"
                }
                fillOpacity={run.status === "canceled" ? DIM : undefined}
              />
              <rect
                x={i * (BAR_W + GAP) - GAP / 2}
                y={0}
                width={BAR_W + GAP}
                height={baseY}
                fill="transparent"
                style={{ cursor: "pointer" }}
                onMouseEnter={() => setHover(i)}
                onMouseLeave={() => setHover(null)}
                onClick={() => nav(`/runs/${run.id}`)}
              />
            </g>
          ))}
          <line x1={0} y1={baseY + 0.5} x2={w} y2={baseY + 0.5} stroke="var(--rule)" />
        </svg>
        {hover !== null && tip && (
          <div className="bars-tip" style={{ left: hover * (BAR_W + GAP) + BAR_W / 2 }}>
            <span className="mono">{shortId(tip.id)}</span> · {fmtDuration(durationMs(tip))} ·{" "}
            {tip.status === "canceled" ? <StatusDot status="canceled" /> : tip.status} ·{" "}
            {relTime(tip.finished_at)}
          </div>
        )}
      </div>
      {(anyFailed || anyCanceled) && (
        <div className="bars-legend">
          <span>
            <svg width="10" height="10" aria-hidden="true">
              <rect width="10" height="10" rx="2" fill="var(--mark)" />
            </svg>
            success
          </span>
          {anyFailed && (
            <span>
              <svg width="10" height="10" aria-hidden="true">
                <g stroke="var(--mark)" strokeWidth="1.3">
                  <line x1="-2" y1="7" x2="7" y2="-2" />
                  <line x1="0" y1="12" x2="12" y2="0" />
                  <line x1="5" y1="14" x2="14" y2="5" />
                </g>
              </svg>
              failed
            </span>
          )}
          {anyCanceled && (
            <span>
              <svg width="10" height="10" aria-hidden="true">
                <g transform="translate(5 5)">
                  <GlyphShape status="canceled" />
                </g>
              </svg>
              canceled
            </span>
          )}
        </div>
      )}
    </>
  );
}
