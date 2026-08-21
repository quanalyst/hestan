import type { MetaSeries } from "./types";
import { stamp } from "./util";

// the plot box. wider than the metadata column beside it and narrower than
// the page: a series here is a shape read at a glance, not a page of its own
const PLOT_W = 460;
const PLOT_H = 72;
// room for the value labels down the left and the range under the axis
const PAD_L = 52;
const PAD_R = 8;
const PAD_T = 8;
const PAD_B = 16;
const W = PAD_L + PLOT_W + PAD_R;
const H = PAD_T + PLOT_H + PAD_B;

// past this many points the marks touch each other and the line is the whole
// story, so only the line is drawn
const DOTS_MAX = 60;

const num = (n: number) => n.toLocaleString(undefined, { maximumFractionDigits: 2 });

// a series is drawn in the ink everything else is drawn in. hue means group
// or origin in this ui and nothing else, and a chart of one series has
// neither to say
export default function SeriesChart({ series }: { series: MetaSeries }) {
  const points = series.points;
  const sampled = points.length < series.of;
  const note = sampled
    ? `${points.length.toLocaleString()} of ${series.of.toLocaleString()} points`
    : `${points.length.toLocaleString()} point${points.length === 1 ? "" : "s"}`;

  if (points.length === 0) {
    // the op looked and there was nothing there, which is a different fact
    // from having saved no series at all
    return <div className="muted meta-series-note">no points</div>;
  }

  const values = points.map(([, v]) => v);
  const times = points.map(([at]) => new Date(at).getTime());
  const [lo, hi] = [Math.min(...values), Math.max(...values)];
  const [t0, t1] = [times[0], times[times.length - 1]];
  // one point, or every point at one instant: no range to spread across, so
  // the marks sit in the middle of the axis they have none of
  const x = (t: number) => PAD_L + (t1 === t0 ? PLOT_W / 2 : ((t - t0) / (t1 - t0)) * PLOT_W);
  // a flat series has no range either, and drawing it along the bottom would
  // read as zero rather than as unchanged
  const y = (v: number) =>
    PAD_T + (hi === lo ? PLOT_H / 2 : PLOT_H - ((v - lo) / (hi - lo)) * PLOT_H);
  const line = points.map(([, v], i) => `${i ? "L" : "M"}${x(times[i])},${y(v)}`).join(" ");

  return (
    <div className="meta-series-wrap">
      <svg className="meta-series" viewBox={`0 0 ${W} ${H}`} width={W} height={H} role="img">
        <title>
          {`${num(lo)} to ${num(hi)}, over ${stamp(points[0][0])} to ${stamp(points[points.length - 1][0])}`}
        </title>
        {/* the value range, on the axis rather than on a hover */}
        <text className="meta-axis" x={PAD_L - 6} y={PAD_T} textAnchor="end" dominantBaseline="hanging">
          {num(hi)}
        </text>
        <text className="meta-axis" x={PAD_L - 6} y={PAD_T + PLOT_H} textAnchor="end">
          {num(lo)}
        </text>
        <line
          x1={PAD_L}
          y1={PAD_T + PLOT_H}
          x2={PAD_L + PLOT_W}
          y2={PAD_T + PLOT_H}
          stroke="var(--hairline)"
        />
        <path d={line} fill="none" stroke="var(--mark)" strokeWidth="1.2" />
        {points.length <= DOTS_MAX &&
          points.map(([, v], i) => <circle key={i} cx={x(times[i])} cy={y(v)} r={1.8} fill="var(--mark)" />)}
        {/* the time range, at the ends of the axis it belongs to */}
        <text className="meta-axis" x={PAD_L} y={H - 4}>
          {stamp(points[0][0])}
        </text>
        {t1 !== t0 && (
          <text className="meta-axis" x={PAD_L + PLOT_W} y={H - 4} textAnchor="end">
            {stamp(points[points.length - 1][0])}
          </text>
        )}
      </svg>
      {/* what the sample stands for, and the numbers behind the shape. the
          same voice a truncated table uses, and the same place */}
      <details className="meta-series-points">
        <summary className="muted meta-series-note">{note}</summary>
        <div className="meta-table-wrap">
          <table className="meta-table">
            <thead>
              <tr>
                <th>at</th>
                <th>value</th>
              </tr>
            </thead>
            <tbody>
              {points.map(([at, v], i) => (
                <tr key={i}>
                  <td className="mono">{stamp(at, true)}</td>
                  <td className="num">{num(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </div>
  );
}
