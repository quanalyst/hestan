import type { OpStatus, RunStatus } from "./types";

export type Status = RunStatus | OpStatus;

// shape carries state; the palette is monochrome, so grey level cannot
export function GlyphShape({ status }: { status: Status }) {
  switch (status) {
    case "success":
      return <circle r={4} className="g-fill" />;
    case "failed":
      return (
        <g className="g-stroke" strokeWidth={1.8} strokeLinecap="round">
          <line x1={-3.2} y1={-3.2} x2={3.2} y2={3.2} />
          <line x1={3.2} y1={-3.2} x2={-3.2} y2={3.2} />
        </g>
      );
    case "running":
      return (
        <g>
          <circle r={1.6} className="g-fill" />
          <circle
            r={4}
            className="g-stroke g-arc"
            strokeWidth={1.5}
            strokeLinecap="round"
            strokeDasharray="17.6 7.5"
          />
        </g>
      );
    case "skipped":
      return <circle r={3.5} className="g-stroke g-dim" strokeWidth={1.4} strokeDasharray="2.1 2.4" />;
    case "canceled":
      return (
        <path
          className="g-stroke g-dim"
          strokeWidth={1.5}
          strokeLinejoin="round"
          d="M 0 -4.1 L 4.1 0 L 0 4.1 L -4.1 0 Z"
        />
      );
    default: // queued / pending
      return <circle r={3.5} className="g-stroke g-dim" strokeWidth={1.5} />;
  }
}
