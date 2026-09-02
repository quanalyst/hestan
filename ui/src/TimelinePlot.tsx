import { useEffect, useId, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent, ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { get } from "./api";
import { clampTipX, DIM, HatchPattern } from "./chart";
import { GlyphShape } from "./StatusGlyph";
import StatusDot from "./StatusDot";
import TimelineGutter from "./TimelineGutter";
import { BAR_H, GUTTER, NONE, TOP, failuresIn, lanesOf, place, rowsOf } from "./timeline";
import type { Bar, Row, TimelineJob } from "./timeline";
import type { OpRun, Run, RunStatus, UpcomingSchedule } from "./types";
import { clockTime, durationMs, fmtDuration, shortId } from "./util";

const AXIS_H = 24;
const MIN_BAR_W = 4;
const FUTURE = 0.15; // fraction of the axis right of the now line
const STRIP_H = 18;
const DRAG_MIN = 6; // px of movement before a press counts as a brush

const WINDOWS = [
  { label: "1h", secs: 3600 },
  { label: "6h", secs: 21600 },
  { label: "24h", secs: 86400 },
];

// how far ahead a caller must fetch schedules to fill the plot's future side
export function futureWindowSecs(windowSecs: number): number {
  return Math.max(60, Math.round((windowSecs * FUTURE) / (1 - FUTURE)));
}

function tickStep(windowSecs: number): number {
  if (windowSecs <= 3600) return 900_000;
  if (windowSecs <= 21600) return 3_600_000;
  return 14_400_000;
}

function fmtTick(t: number): string {
  return new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
}

function legendSwatch(status: RunStatus, hatch: string) {
  switch (status) {
    case "success":
      return <rect width="10" height="10" rx="2" fill="var(--mark)" />;
    case "failed":
      return <rect width="10" height="10" rx="2" fill={`url(#${hatch})`} />;
    case "running":
      return <rect className="tl-running" width="10" height="10" rx="2" fill="var(--mark)" />;
    case "queued":
      return <rect x="0.5" y="0.5" width="9" height="9" rx="2" fill="none" stroke="var(--mark-muted)" strokeWidth="1.25" />;
    case "canceled":
      return (
        <g transform="translate(5 5)">
          <GlyphShape status="canceled" />
        </g>
      );
  }
}

export default function TimelinePlot({
  jobs,
  runs,
  upcoming,
  windowSecs,
  onWindow,
  open = NONE,
  onOpen,
}: {
  jobs: TimelineJob[];
  runs: Run[];
  upcoming: UpcomingSchedule[];
  windowSecs: number;
  onWindow: (secs: number) => void;
  // which groups have their job rows showing under them, and how to turn one
  // of them around. both absent is a plot with no outline to work, which is
  // what a page showing one job wants
  open?: ReadonlySet<string>;
  onOpen?: (group: string) => void;
}) {
  const nav = useNavigate();
  const hatch = useId();
  const wrapRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [tip, setTip] = useState<{ key: string; x: number; y: number; node: ReactNode } | null>(null);
  const [drag, setDrag] = useState<{ x0: number; x1: number } | null>(null);
  const [brush, setBrush] = useState<{ a: number; b: number } | null>(null);
  const dragStop = useRef<(() => void) | null>(null);
  const viewRef = useRef({ t0: 0, t1: 1, plotW: 1, width: 0 }); // scale on screen, read by stale drag closures
  const failCache = useRef(new Map<string, string | null>());

  // a run of a job that has since left the code still gets a row, in no group:
  // there is nothing left to declare one on
  const all: TimelineJob[] = [...jobs];
  for (const r of runs) {
    if (!all.some((j) => j.name === r.job)) all.push({ name: r.job, group: null, group_hue: null });
  }
  const lanes = lanesOf(all, open);
  const hasLanes = lanes.length > 0;

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => setWidth(entries[0].contentRect.width));
    ro.observe(el);
    setWidth(el.getBoundingClientRect().width);
    return () => ro.disconnect();
  }, [hasLanes]);

  useEffect(() => setBrush(null), [windowSecs]);

  // unmounting mid-drag would leak the window listeners startBrush installs
  useEffect(() => () => dragStop.current?.(), []);

  const hasBrush = brush !== null;
  useEffect(() => {
    if (!hasBrush) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setBrush(null);
    };
    const onDown = (e: globalThis.MouseEvent) => {
      const t = e.target as Node;
      if (wrapRef.current?.contains(t) || listRef.current?.contains(t)) return;
      setBrush(null);
    };
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onDown);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onDown);
    };
  }, [hasBrush]);

  const header = (
    <h2>
      timeline
      <span className="log-filter">
        {WINDOWS.map((w) => (
          <button
            key={w.label}
            className={windowSecs === w.secs ? "text-btn active" : "text-btn"}
            onClick={() => onWindow(w.secs)}
          >
            {w.label}
          </button>
        ))}
      </span>
    </h2>
  );

  if (!hasLanes)
    return (
      <>
        {header}
        <p className="muted">no jobs registered</p>
      </>
    );

  if (width === 0)
    return (
      <>
        {header}
        <div className="tl" ref={wrapRef} />
      </>
    );

  const now = Date.now();
  const t0 = now - windowSecs * 1000;
  const t1 = now + (windowSecs * 1000 * FUTURE) / (1 - FUTURE);

  const byJob = new Map<string, Bar[]>();
  for (const run of runs) {
    const start = new Date(run.started_at ?? run.created_at).getTime();
    const end = run.finished_at ? new Date(run.finished_at).getTime() : now;
    const bar: Bar = { run, start, end: Math.max(start, end), lane: 0 };
    if (bar.end < t0 || bar.start > t1) continue; // stale fetch right after a window change
    const list = byJob.get(run.job);
    if (list) list.push(bar);
    else byJob.set(run.job, [bar]);
  }

  // a group row's bars are every member's, packed by the same rule one job's
  // row has always used. opening a group adds rows and never takes a bar off
  // the group's own
  const rows = rowsOf(lanes, byJob);
  const plotBottom = rows.reduce((y, row) => y + row.h, TOP);

  const plotW = width - GUTTER;
  const x = (t: number) => GUTTER + ((t - t0) / (t1 - t0)) * plotW;
  const nowX = x(now);
  const px = (v: number) => Math.round(v) + 0.5;
  viewRef.current = { t0, t1, plotW, width };

  const step = tickStep(windowSecs);
  const midnight = new Date(t0);
  midnight.setHours(0, 0, 0, 0);
  const ticks: number[] = [];
  for (let t = midnight.getTime(); t <= t1; t += step) if (t >= t0) ticks.push(t);

  // one entry per run per row it is on, and one rect below per entry: nothing
  // between here and the svg drops a bar
  const placed = place(rows, { x, nowX, gutter: GUTTER, minBarW: MIN_BAR_W });

  // a ghost lands on every row its job's runs land on, which inside an open
  // group is the group's row and the job's own
  const rowsByJob = new Map<string, Row[]>();
  for (const row of rows)
    for (const job of row.lane.jobs) {
      const held = rowsByJob.get(job);
      if (held) held.push(row);
      else rowsByJob.set(job, [row]);
    }
  const ghosts = upcoming.flatMap((u) =>
    (rowsByJob.get(u.job) ?? []).flatMap((row) =>
      u.times
        .map((iso) => ({ iso, t: new Date(iso).getTime() }))
        .filter(({ t }) => t > now && t <= t1)
        // a fire at exactly t1 maps to the plot edge; keep the 3px ghost inside
        .map(({ iso, t }) => ({
          // the row, and the schedule on it: two jobs of one group can be due
          // at the same moment, and both land on the group's row
          key: `${row.lane.key}|${u.job}|${u.expr}|${iso}`,
          job: u.job,
          expr: u.expr,
          iso,
          gx: Math.min(x(t), width - 1.5),
          gy: row.y + (row.h - BAR_H) / 2,
        })),
    ),
  );

  const present = (["success", "failed", "running", "queued", "canceled"] as RunStatus[]).filter((s) =>
    placed.some((b) => b.run.status === s),
  );
  const showLegend = present.length + (ghosts.length ? 1 : 0) >= 2;

  // below this the strip's x-clamp bounds invert and glyphs land left of the gutter
  const stripFits = width >= GUTTER + 8;
  const failures = stripFits ? failuresIn(placed) : [];
  const stripH = failures.length ? STRIP_H : 0;
  const height = plotBottom + stripH + AXIS_H;

  const clampX = (v: number) => Math.min(Math.max(v, GUTTER), width);

  const startBrush = (e: ReactMouseEvent) => {
    if (e.button !== 0 || !wrapRef.current) return;
    // presses that start on a bar/ghost/glyph belong to that element's click
    if ((e.target as Element).closest("[data-hit]")) return;
    const rect = wrapRef.current.getBoundingClientRect();
    const startClientX = e.clientX;
    const sx = startClientX - rect.left;
    const sy = e.clientY - rect.top;
    if (sx < GUTTER || sy < TOP || sy > plotBottom) return;
    e.preventDefault();
    let lastClientX = startClientX;
    let lastX = clampX(sx);
    let moved = false;
    let raf = 0;
    const move = (ev: globalThis.MouseEvent) => {
      lastClientX = ev.clientX;
      lastX = clampX(lastClientX - rect.left);
      if (!moved && Math.abs(lastX - sx) < DRAG_MIN) return;
      moved = true;
      if (!raf)
        raf = requestAnimationFrame(() => {
          raf = 0;
          setDrag({ x0: clampX(sx), x1: lastX });
        });
    };
    const stop = () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", up);
      if (raf) cancelAnimationFrame(raf);
      dragStop.current = null;
    };
    const up = () => {
      stop();
      setDrag(null);
      if (!moved) return;
      // a poll mid-drag rescales the axis, so commit against the scale at
      // release, not the one this closure captured at press
      const r = wrapRef.current?.getBoundingClientRect();
      if (!r) return;
      const v = viewRef.current;
      const cx = (n: number) => Math.min(Math.max(n, GUTTER), v.width);
      const at = (n: number) => v.t0 + ((n - GUTTER) / v.plotW) * (v.t1 - v.t0);
      const xa = cx(startClientX - r.left);
      const xb = cx(lastClientX - r.left);
      setBrush({ a: at(Math.min(xa, xb)), b: at(Math.max(xa, xb)) });
    };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    dragStop.current = stop;
  };

  const sel = drag
    ? { xa: Math.min(drag.x0, drag.x1), xb: Math.max(drag.x0, drag.x1) }
    : brush && x(brush.b) > GUTTER + 1
      ? { xa: clampX(x(brush.a)), xb: clampX(x(brush.b)) }
      : null;

  const brushRuns = brush
    ? runs.filter((r) => {
        const s = new Date(r.started_at ?? r.created_at).getTime();
        const e = r.finished_at ? new Date(r.finished_at).getTime() : now;
        return s <= brush.b && Math.max(s, e) >= brush.a;
      })
    : [];

  const hoverFail = (run: Run, fx: number, fy: number) => {
    const key = `f:${run.id}`;
    const node = (op: string | null | undefined) => (
      <>
        <span className="mono">{run.job}</span> · failed
        {op && (
          <>
            {" "}
            at <span className="mono">{op}</span>
          </>
        )}
      </>
    );
    const cached = failCache.current.get(run.id);
    setTip({ key, x: fx, y: fy - 4, node: node(cached) });
    if (cached !== undefined) return;
    get<{ run: Run; ops: OpRun[] }>(`/api/runs/${run.id}`)
      .then((r) => {
        const failedOps = r.ops
          .filter((o) => o.status === "failed")
          .sort((a, b) => (a.started_at ?? "~").localeCompare(b.started_at ?? "~")); // "~" sorts nulls last
        const op = failedOps[0]?.op ?? null;
        failCache.current.set(run.id, op);
        setTip((prev) => (prev && prev.key === key ? { ...prev, node: node(op) } : prev));
      })
      .catch(() => {});
  };

  return (
    <>
      {header}
      <div className="tl" ref={wrapRef}>
        <svg width={width} height={height} onMouseDown={startBrush}>
          <HatchPattern id={hatch} />

          <rect x={nowX} y={TOP} width={Math.max(0, width - nowX)} height={plotBottom - TOP} fill="var(--wash)" />
          <rect
            x={GUTTER}
            y={TOP}
            width={Math.max(0, width - GUTTER)}
            height={plotBottom - TOP}
            fill="transparent"
            style={{ cursor: "crosshair" }}
          />

          {ticks.map((t) => (
            <g key={t}>
              <line className="tl-grid" x1={px(x(t))} y1={TOP} x2={px(x(t))} y2={plotBottom} />
              <text className="tl-tick" x={x(t)} y={plotBottom + stripH + 15} textAnchor="middle">
                {fmtTick(t)}
              </text>
            </g>
          ))}

          {rows.map((row, i) =>
            i === 0 ? null : (
              <line
                key={row.lane.key}
                className="tl-row"
                x1={GUTTER}
                y1={px(row.y)}
                x2={width}
                y2={px(row.y)}
              />
            ),
          )}
          <TimelineGutter rows={rows} onToggle={onOpen} />
          <line className="tl-row" x1={GUTTER} y1={px(plotBottom)} x2={width} y2={px(plotBottom)} />

          {failures.length > 0 && (
            <>
              <text
                className="tl-strip-label"
                x={GUTTER - 10}
                y={plotBottom + stripH / 2}
                textAnchor="end"
                dominantBaseline="central"
              >
                failed
              </text>
              {failures.map((b) => (
                <g
                  key={`f${b.run.id}`}
                  transform={`translate(${Math.min(Math.max(b.bx + b.bw / 2, GUTTER + 4), width - 4)}, ${
                    plotBottom + stripH / 2
                  }) scale(0.8)`}
                >
                  <GlyphShape status="failed" />
                </g>
              ))}
              <line
                className="tl-row"
                x1={GUTTER}
                y1={px(plotBottom + stripH)}
                x2={width}
                y2={px(plotBottom + stripH)}
              />
            </>
          )}

          {placed.map((b) => {
            // the same run on an open group's row and on its job's row lights
            // up on both when either is under the cursor: the tip is keyed by
            // the run, and saying "these two marks are one run" is worth more
            // than lighting only the one being pointed at
            const hot = tip?.key === b.run.id;
            if (b.run.status === "queued")
              return (
                <rect
                  key={b.key}
                  className={hot ? "bar bar-hot" : "bar"}
                  x={b.bx + 0.5}
                  y={b.by + 0.5}
                  width={Math.max(1.5, b.bw - 1)}
                  height={BAR_H - 1}
                  rx={1}
                  fill="none"
                  stroke="var(--mark-muted)"
                  strokeWidth={1.25}
                />
              );
            if (b.run.status === "canceled")
              return (
                <rect
                  key={b.key}
                  className={hot ? "bar bar-hot" : "bar"}
                  x={b.bx}
                  y={b.by}
                  width={b.bw}
                  height={BAR_H}
                  rx={1}
                  fill="var(--mark-muted)"
                  fillOpacity={DIM}
                />
              );
            // a bar takes no hue. what a run is doing is carried by its shape
            // and its fill, and that is what leaves hue free to mean where the
            // work came from; the gutter block beside the row is where that
            // lives, and it is the only place on this page colour appears
            return (
              <rect
                key={b.key}
                className={`${hot ? "bar bar-hot" : "bar"}${b.run.status === "running" ? " tl-running" : ""}`}
                x={b.bx}
                y={b.by}
                width={b.bw}
                height={BAR_H}
                rx={1}
                fill={b.run.status === "failed" ? `url(#${hatch})` : "var(--mark)"}
              />
            );
          })}

          {ghosts.map((g) => (
            <rect
              key={g.key}
              className="tl-ghost"
              x={g.gx - 1.5}
              y={g.gy}
              width={3}
              height={BAR_H}
              fill="none"
              stroke="var(--mark-muted)"
              strokeWidth={1}
            />
          ))}

          <line className="tl-now" x1={px(nowX)} y1={TOP - 2} x2={px(nowX)} y2={plotBottom + 2} />

          {sel && (
            <g>
              <rect className="tl-brush" x={sel.xa} y={TOP} width={sel.xb - sel.xa} height={plotBottom - TOP} />
              <line className="tl-brush-edge" x1={px(sel.xa)} y1={TOP} x2={px(sel.xa)} y2={plotBottom} />
              <line className="tl-brush-edge" x1={px(sel.xb)} y1={TOP} x2={px(sel.xb)} y2={plotBottom} />
            </g>
          )}

          {placed.length === 0 && (
            <text
              className="tl-empty"
              x={GUTTER + plotW / 2}
              y={TOP + (plotBottom - TOP) / 2}
              textAnchor="middle"
              dominantBaseline="central"
            >
              no runs in this window
            </text>
          )}

          {/* hit targets on top, wider than the marks themselves */}
          {placed.map((b) => {
            const hw = Math.max(b.bw, 12);
            return (
              <rect
                key={b.key}
                data-hit="1"
                x={b.bx + b.bw / 2 - hw / 2}
                y={b.by - 3}
                width={hw}
                height={BAR_H + 6}
                fill="transparent"
                style={{ cursor: "pointer" }}
                onMouseEnter={() =>
                  setTip({
                    key: b.run.id,
                    x: b.bx + b.bw / 2,
                    y: b.by,
                    node: (
                      <>
                        <span className="mono">{b.run.job}</span> ·{" "}
                        {b.run.status === "canceled" ? <StatusDot status="canceled" /> : b.run.status} ·{" "}
                        {clockTime(b.run.started_at ?? b.run.created_at)} · {fmtDuration(b.end - b.start)}
                      </>
                    ),
                  })
                }
                onMouseLeave={() => setTip(null)}
                onClick={() => nav(`/runs/${b.run.id}`)}
              />
            );
          })}
          {ghosts.map((g) => (
            <rect
              key={g.key}
              data-hit="1"
              x={g.gx - 6}
              y={g.gy - 3}
              width={12}
              height={BAR_H + 6}
              fill="transparent"
              style={{ cursor: "pointer" }}
              onMouseEnter={() =>
                setTip({
                  key: g.key,
                  x: g.gx,
                  y: g.gy,
                  node: (
                    <>
                      <span className="mono">{g.job}</span> · scheduled {clockTime(g.iso)} ({g.expr})
                    </>
                  ),
                })
              }
              onMouseLeave={() => setTip(null)}
              onClick={() => nav(`/jobs/${encodeURIComponent(g.job)}`)}
            />
          ))}
          {failures.map((b) => {
            const fx = Math.min(Math.max(b.bx + b.bw / 2, GUTTER + 4), width - 4);
            return (
              <rect
                key={`fh${b.run.id}`}
                data-hit="1"
                x={fx - 7}
                y={plotBottom + 1}
                width={14}
                height={stripH - 1}
                fill="transparent"
                style={{ cursor: "pointer" }}
                onMouseEnter={() => hoverFail(b.run, fx, plotBottom + stripH / 2)}
                onMouseLeave={() => setTip(null)}
                onClick={() => nav(`/runs/${b.run.id}`)}
              />
            );
          })}

          {/* while a drag is live, catch the mouseup's click so bars underneath don't navigate */}
          {drag && <rect x={0} y={0} width={width} height={height} fill="transparent" style={{ cursor: "crosshair" }} />}
        </svg>

        {tip && (
          <div className="tl-tip" style={{ left: clampTipX(tip.x, width), top: tip.y - 6 }}>
            {tip.node}
          </div>
        )}
      </div>

      {showLegend && (
        <div className="bars-legend">
          {present.map((s) => (
            <span key={s}>
              <svg width="10" height="10" aria-hidden="true">
                {legendSwatch(s, hatch)}
              </svg>
              {s}
            </span>
          ))}
          {ghosts.length > 0 && (
            <span>
              <svg width="10" height="10" aria-hidden="true">
                <rect className="tl-ghost" x="3.5" y="0.5" width="3" height="9" fill="none" stroke="var(--mark-muted)" strokeWidth="1" />
              </svg>
              scheduled
            </span>
          )}
        </div>
      )}

      {brush && (
        <div className="brush-list" ref={listRef}>
          <div className="brush-head">
            <span className="sub-label">selection</span>
            <span className="muted mono">
              {clockTime(new Date(brush.a).toISOString())} – {clockTime(new Date(brush.b).toISOString())}
            </span>
            <span className="muted">{brushRuns.length === 1 ? "1 run" : `${brushRuns.length} runs`}</span>
            <button className="text-btn" onClick={() => setBrush(null)}>
              clear
            </button>
          </div>
          {brushRuns.length === 0 ? (
            <p className="muted">no runs in selection</p>
          ) : (
            <table>
              <tbody>
                {brushRuns.map((run) => (
                  <tr key={run.id} onClick={() => nav(`/runs/${run.id}`)}>
                    <td>
                      <StatusDot status={run.status} />
                    </td>
                    <td className="mono">{shortId(run.id)}</td>
                    <td>{run.job}</td>
                    <td className="muted">{clockTime(run.started_at ?? run.created_at)}</td>
                    <td className="num">{fmtDuration(durationMs(run))}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </>
  );
}
