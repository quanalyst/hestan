import { useEffect, useId, useRef, useState } from "react";
import type { ReactNode } from "react";
import { clampTipX, DIM, HatchPattern } from "./chart";
import { GlyphShape } from "./StatusGlyph";
import type { OpRun, OpSummary } from "./types";
import { clockTime, fmtDuration } from "./util";

const GUTTER = 120;
const ROW_H = 22;
const BAR_H = 10;
const TOP = 6;
const AXIS_H = 22;
const MIN_BAR_W = 2;
const LABEL_W = 64; // reserved right of the plot so duration labels never clip

interface Span {
  start: number;
  end: number;
}

function niceStep(raw: number): number {
  const pow = 10 ** Math.floor(Math.log10(Math.max(1, raw)));
  for (const m of [1, 2, 5]) if (m * pow >= raw) return m * pow;
  return 10 * pow;
}

export default function GanttChart({ ops, opRuns }: { ops: OpSummary[]; opRuns: OpRun[] }) {
  const hatch = useId();
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [tip, setTip] = useState<{ key: string; x: number; y: number; node: ReactNode } | null>(null);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => setWidth(entries[0].contentRect.width));
    ro.observe(el);
    setWidth(el.getBoundingClientRect().width);
    return () => ro.disconnect();
  }, []);

  if (ops.length === 0) return null;

  const header = <h2>gantt</h2>;
  if (width === 0)
    return (
      <>
        {header}
        <div className="tl" ref={wrapRef} />
      </>
    );

  const now = Date.now();
  const byName = new Map(ops.map((o) => [o.name, o]));
  const runByOp = new Map(opRuns.map((o) => [o.op, o]));

  const layer = new Map<string, number>();
  const depth = (name: string): number => {
    const seen = layer.get(name);
    if (seen !== undefined) return seen;
    // marked visited before recursing so a cyclic input flattens, as in DagView
    layer.set(name, 0);
    const op = byName.get(name);
    const d = op && op.deps.length ? 1 + Math.max(...op.deps.map(depth)) : 0;
    layer.set(name, d);
    return d;
  };
  ops.forEach((o) => depth(o.name));
  // stable sort, so ops within a layer keep registry order
  const ordered = [...ops].sort((a, b) => layer.get(a.name)! - layer.get(b.name)!);

  const spans = new Map<string, Span>();
  for (const o of opRuns) {
    if (!o.started_at) continue;
    const start = new Date(o.started_at).getTime();
    const end = o.finished_at ? new Date(o.finished_at).getTime() : now;
    spans.set(o.op, { start, end: Math.max(start, end) });
  }

  // critical path: from the last op to finish, follow the dep that gated it
  let tail: string | null = null;
  for (const [name, s] of spans) if (tail === null || s.end > spans.get(tail)!.end) tail = name;
  const critical = new Set<string>();
  for (let n: string | null = tail; n && !critical.has(n); ) {
    critical.add(n);
    let gate: string | null = null;
    for (const d of byName.get(n)?.deps ?? []) {
      const s = spans.get(d);
      if (s && (gate === null || s.end > spans.get(gate)!.end)) gate = d;
    }
    n = gate;
  }

  const hasSpans = spans.size > 0;
  const plotBottom = TOP + ordered.length * ROW_H;
  const height = plotBottom + (hasSpans ? AXIS_H : 8);
  const plotW = Math.max(50, width - GUTTER - LABEL_W);

  const all = [...spans.values()];
  const t0 = hasSpans ? Math.min(...all.map((s) => s.start)) : 0;
  const span = hasSpans ? Math.max(1, Math.max(...all.map((s) => s.end)) - t0) : 1;
  const x = (t: number) => GUTTER + ((t - t0) / span) * plotW;
  const px = (v: number) => Math.round(v) + 0.5;

  const step = niceStep(span / 4);
  const ticks: number[] = [];
  if (hasSpans) for (let t = 0; t <= span; t += step) ticks.push(t);

  const anyTimedFailed = opRuns.some((o) => o.status === "failed" && o.started_at);
  const anyTimedCanceled = opRuns.some((o) => o.status === "canceled" && o.started_at);
  const truncate = (s: string) => (s.length > 16 ? s.slice(0, 15) + "…" : s);

  return (
    <>
      {header}
      <div className="tl" ref={wrapRef}>
        <svg width={width} height={height}>
          <HatchPattern id={`${hatch}i`} />
          <HatchPattern id={`${hatch}m`} stroke="var(--mark-muted)" />

          {ticks.map((off) => (
            <g key={off}>
              <line className="tl-grid" x1={px(x(t0 + off))} y1={TOP} x2={px(x(t0 + off))} y2={plotBottom} />
              <text className="tl-tick" x={x(t0 + off)} y={plotBottom + 15} textAnchor="middle">
                {off === 0 ? "0" : `+${fmtDuration(off)}`}
              </text>
            </g>
          ))}

          {ordered.map((op, i) => {
            const y = TOP + i * ROW_H;
            const st = runByOp.get(op.name)?.status ?? "pending";
            const s = spans.get(op.name);
            const label = (
              <text className="tl-label" x={GUTTER - 10} y={y + ROW_H / 2} textAnchor="end" dominantBaseline="central">
                {truncate(op.name)}
              </text>
            );
            if (!s)
              return (
                <g key={op.name}>
                  {i > 0 && <line className="tl-row" x1={GUTTER} y1={px(y)} x2={width} y2={px(y)} />}
                  {label}
                  <g transform={`translate(${GUTTER + 12}, ${y + ROW_H / 2})`}>
                    <GlyphShape status={st} />
                  </g>
                </g>
              );
            const bx = x(s.start);
            const bw = Math.max(MIN_BAR_W, x(s.end) - bx);
            const by = y + (ROW_H - BAR_H) / 2;
            const onPath = critical.has(op.name);
            const fill =
              st === "failed"
                ? `url(#${hatch}${onPath ? "i" : "m"})`
                : st === "canceled"
                  ? "var(--mark-muted)"
                  : onPath
                    ? "var(--mark)"
                    : "var(--mark-muted)";
            return (
              <g key={op.name}>
                {i > 0 && <line className="tl-row" x1={GUTTER} y1={px(y)} x2={width} y2={px(y)} />}
                {label}
                <rect
                  className={`${tip?.key === op.name ? "bar bar-hot" : "bar"}${st === "running" ? " tl-running" : ""}`}
                  x={bx}
                  y={by}
                  width={bw}
                  height={BAR_H}
                  rx={1}
                  fill={fill}
                  fillOpacity={st === "canceled" ? DIM : undefined}
                />
                <text className="tl-tick" x={bx + bw + 6} y={y + ROW_H / 2} dominantBaseline="central">
                  {fmtDuration(s.end - s.start)}
                </text>
              </g>
            );
          })}
          {hasSpans && <line className="tl-row" x1={GUTTER} y1={px(plotBottom)} x2={width} y2={px(plotBottom)} />}

          {ordered.map((op, i) => {
            const s = spans.get(op.name);
            const o = runByOp.get(op.name);
            if (!s || !o) return null;
            const y = TOP + i * ROW_H;
            const bx = x(s.start);
            const bw = Math.max(MIN_BAR_W, x(s.end) - bx);
            const hw = Math.max(bw, 12);
            return (
              <rect
                key={op.name}
                x={bx + bw / 2 - hw / 2}
                y={y + 1}
                width={hw}
                height={ROW_H - 2}
                fill="transparent"
                onMouseEnter={() =>
                  setTip({
                    key: op.name,
                    x: bx + bw / 2,
                    y: y + (ROW_H - BAR_H) / 2,
                    node: (
                      <>
                        <span className="mono">{op.name}</span> · {o.status} · {clockTime(o.started_at!)} ·{" "}
                        {fmtDuration(s.end - s.start)}
                      </>
                    ),
                  })
                }
                onMouseLeave={() => setTip(null)}
              />
            );
          })}
        </svg>

        {tip && (
          <div className="tl-tip" style={{ left: clampTipX(tip.x, width), top: tip.y - 6 }}>
            {tip.node}
          </div>
        )}
      </div>

      {hasSpans && spans.size >= 2 && (
        <div className="bars-legend">
          <span>
            <svg width="10" height="10" aria-hidden="true">
              <rect width="10" height="10" rx="2" fill="var(--mark)" />
            </svg>
            critical path
          </span>
          <span>
            <svg width="10" height="10" aria-hidden="true">
              <rect width="10" height="10" rx="2" fill="var(--mark-muted)" />
            </svg>
            other
          </span>
          {anyTimedFailed && (
            <span>
              <svg width="10" height="10" aria-hidden="true">
                <rect width="10" height="10" rx="2" fill={`url(#${hatch}i)`} />
              </svg>
              failed
            </span>
          )}
          {anyTimedCanceled && (
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
