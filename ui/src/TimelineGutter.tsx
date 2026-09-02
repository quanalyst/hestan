// the outline down the left of the run timeline: a row per group, its jobs
// indented underneath it while it is open, and a block of the group's hue
// behind every one of those names.
//
// its own module because it is the half of the plot that is markup rather than
// arithmetic, and because what it draws is worth asserting directly: a
// deployment that declares no group has to come out of here with no block, no
// disclosure and nothing but the names it always had.
import { at } from "./Swatch";
import { GUTTER, laneLabel } from "./timeline";
import type { Row } from "./timeline";

// where a name ends. the gutter is aligned on its right edge, against the bars
// the name belongs to
const LABEL_X = GUTTER - 10;
// how far a job row steps back inside its group. the alignment edge is the
// right one, so nesting steps away from it
const INDENT = 12;
// the band down the left edge. narrow on purpose: it says which group a row
// belongs to and hands the rest of the gutter back to the name, which has to
// stay readable on the page's own ground rather than on a wash of colour
const BLOCK_W = 5;
// the column the disclosure sits in, kept clear of a group's name
const DISC_W = 22;
// what an ungrouped name is kept clear of the left edge by
const EDGE = 6;
// 11px monospace, close enough to size a truncation by
const CH = 6.6;

// a name too long for the room it has, cut to fit with the cut marked
export function truncate(name: string, max: number): string {
  return name.length > max ? name.slice(0, max - 1) + "…" : name;
}

// how many characters of a name fit on a row: what is left of the gutter once
// the indent and whatever sits left of the name are taken out of it
export function roomFor(row: Row): number {
  const indent = row.lane.kind === "job" && row.lane.group !== null ? INDENT : 0;
  const left = row.lane.kind === "group" ? DISC_W : EDGE;
  return Math.floor((LABEL_X - indent - left) / CH);
}

function Caret({ open }: { open: boolean }) {
  return (
    <svg
      className={open ? "tl-caret tl-caret-open" : "tl-caret"}
      width="8"
      height="8"
      viewBox="-4 -4 8 8"
      aria-hidden="true"
    >
      <path d="M -1.9 -3.3 L 2.6 0 L -1.9 3.3 Z" />
    </svg>
  );
}

export default function TimelineGutter({
  rows,
  onToggle,
}: {
  rows: Row[];
  // how to open or shut one group. absent is a plot with no outline to work,
  // which is what a page showing a single job passes
  onToggle?: (group: string) => void;
}) {
  return (
    <>
      {rows.map((row) => {
        const lane = row.lane;
        // hoisted so the disclosure's handler is holding a group rather than a
        // `string | null` narrowed somewhere it cannot see
        const group = lane.kind === "group" ? lane.group : null;
        const indent = lane.kind === "job" && lane.group !== null ? INDENT : 0;
        return (
          <g key={lane.key}>
            {/* the group's own hue, the full height of the row and the same on
                the group's row and its jobs' rows, so the colour reads as one
                band down the gutter. `at` is the only thing in this ui that
                says what a hue is; this end only says what to do with it */}
            {lane.hue !== null && (
              <rect className="tl-block" style={at(lane.hue)} x={0} y={row.y} width={BLOCK_W} height={row.h} />
            )}
            <text
              className="tl-label"
              x={LABEL_X - indent}
              y={row.y + row.h / 2}
              textAnchor="end"
              dominantBaseline="central"
            >
              {truncate(laneLabel(lane), roomFor(row))}
              {/* a group row stands for jobs whose names it has no room for */}
              {lane.kind === "group" && <title>{lane.jobs.join(", ")}</title>}
            </text>
            {group !== null && onToggle && (
              // the whole of the gutter row is the target, so the name is what
              // you click, and the row it opens is the row you are on
              <foreignObject x={0} y={row.y} width={LABEL_X} height={row.h}>
                <button
                  type="button"
                  className="tl-disc"
                  aria-expanded={lane.open}
                  aria-label={group}
                  // the disclosure covers the name, so the members are named
                  // here rather than on the text underneath it
                  title={lane.jobs.join(", ")}
                  onClick={() => onToggle(group)}
                >
                  <Caret open={lane.open} />
                </button>
              </foreignObject>
            )}
          </g>
        );
      })}
    </>
  );
}
