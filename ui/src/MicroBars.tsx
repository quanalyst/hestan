import { useId } from "react";
import { DIM, HatchPattern } from "./chart";
import type { Status } from "./StatusGlyph";

const BAR_W = 6;
const BAR_GAP = 2;
const BAR_H = 16;

export interface MicroBar {
  id: string;
  value: number;
  status: Status;
}

export default function MicroBars({ bars }: { bars: MicroBar[] }) {
  const hatch = useId();
  if (bars.length === 0) return null;
  const max = Math.max(...bars.map((b) => b.value));
  const w = bars.length * (BAR_W + BAR_GAP) - BAR_GAP;
  return (
    <svg className="spark" width={w} height={BAR_H} aria-hidden="true">
      <HatchPattern id={hatch} />
      {bars.map((b, i) => {
        const h = Math.max(2, (b.value / Math.max(max, 1)) * BAR_H);
        return (
          <rect
            key={b.id}
            x={i * (BAR_W + BAR_GAP)}
            y={BAR_H - h}
            width={BAR_W}
            height={h}
            rx={1}
            fill={b.status === "failed" ? `url(#${hatch})` : b.status === "canceled" ? "var(--mark-muted)" : "var(--mark)"}
            fillOpacity={b.status === "canceled" ? DIM : undefined}
          />
        );
      })}
    </svg>
  );
}
