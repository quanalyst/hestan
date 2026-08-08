// mirrors --dim, for the fill-opacity attributes CSS variables can't reach
export const DIM = 0.55;

// half the widest tip .tl-tip draws; the tip is centered on x, so clamping to
// this keeps both its edges inside the plot
const TIP_HALF_W = 130;

export function clampTipX(x: number, width: number): number {
  return Math.min(Math.max(x, TIP_HALF_W), Math.max(TIP_HALF_W, width - TIP_HALF_W));
}

// the fill every chart gives a failed mark
export function HatchPattern({ id, stroke = "var(--mark)" }: { id: string; stroke?: string }) {
  return (
    <defs>
      <pattern id={id} width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
        <line x1="0" y1="0" x2="0" y2="4" stroke={stroke} strokeWidth="1.3" />
      </pattern>
    </defs>
  );
}
