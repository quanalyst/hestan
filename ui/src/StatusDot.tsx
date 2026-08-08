import { GlyphShape } from "./StatusGlyph";
import type { OpStatus, RunStatus } from "./types";

export default function StatusDot({ status }: { status: RunStatus | OpStatus }) {
  return (
    <span className="status">
      <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
        <GlyphShape status={status} />
      </svg>
      {status}
    </span>
  );
}
