import { GlyphShape } from "./StatusGlyph";
import type { Status } from "./StatusGlyph";
import type { OpStatus } from "./types";

const PAD_X = 14;
const VGAP = 16;
const HGAP = 56;
const MARGIN = 10;
const NODE_H = 36;

// a node carrying a status stacks two text rows, centered on these offsets
const STATUS_NODE_H = 48;
const LABEL_ROW_Y = 17;
const STATUS_ROW_Y = 34;

// the minimum a node needs to be drawn; OpSummary satisfies it structurally
export interface DagNode {
  name: string;
  deps: string[];
  output_type?: string | null;
  note?: string;
  // a short suffix on the label, for a mapped op's instance count ("×3")
  badge?: string;
  // extra text a search should find this node by, for a node that stands for
  // several things: a folded group is findable by what is inside it
  find?: string;
}

// "absent" is a node a subset run never contained: no status to claim, no glyph
export type NodeStatus = OpStatus | "fresh" | "stale" | "absent";

const glyphFor = (st: Exclude<NodeStatus, "absent">): Status =>
  st === "fresh" ? "success" : st === "stale" ? "pending" : st;

// ops flattened out of a graph instance are named "{instance}.{inner}", so
// everything up to the last dot is the group this node belongs to
const cut = (name: string) => name.lastIndexOf(".");
const prefixOf = (name: string) => (cut(name) < 0 ? "" : name.slice(0, cut(name) + 1));
const leafOf = (name: string) => (cut(name) < 0 ? name : name.slice(cut(name) + 1));

interface Placed {
  node: DagNode;
  x: number;
  y: number;
  w: number;
}

export default function DagView({
  nodes,
  statuses,
  selected,
  onSelect,
  highlight,
  label = "op dependency graph",
}: {
  nodes: DagNode[];
  statuses?: Record<string, NodeStatus>;
  selected?: string | null;
  onSelect?: (name: string) => void;
  // a substring to find in the graph: everything that matches is marked and
  // everything else recedes, which is what makes a name findable in a graph
  // too big to read node by node
  highlight?: string;
  label?: string;
}) {
  if (nodes.length === 0) return null;
  const nodeH = statuses ? STATUS_NODE_H : NODE_H;
  const glyphW = statuses ? 16 : 0;
  const hay = (n: DagNode) => `${n.name} ${n.find ?? ""}`.toLowerCase();
  const wanted = (highlight ?? "").trim().toLowerCase();
  // a search nothing matches dims the whole graph, which looks like a fault
  // rather than an answer, so it does not count as a search
  const needle = wanted !== "" && nodes.some((n) => hay(n).includes(wanted)) ? wanted : "";

  const statusOf = (n: DagNode) => (statuses ? (statuses[n.name] ?? "pending") : undefined);
  const subOf = (n: DagNode) => {
    const st = statusOf(n);
    if (st === undefined) return null;
    const word = st === "absent" ? "not in run" : st;
    return n.note ? `${word} · ${n.note}` : word;
  };

  // only rendered deps count toward the column: deps can name things that
  // aren't (the assets job's ops depend on sources, which lower to no op)
  const byName = new Map(nodes.map((n) => [n.name, n]));
  const layer = new Map<string, number>();
  const depth = (name: string): number => {
    const seen = layer.get(name);
    if (seen !== undefined) return seen;
    // marked visited before recursing so a cyclic input flattens, not hangs
    layer.set(name, 0);
    const n = byName.get(name);
    const present = n ? n.deps.filter((d) => byName.has(d)) : [];
    const d = present.length ? 1 + Math.max(...present.map(depth)) : 0;
    layer.set(name, d);
    return d;
  };
  nodes.forEach((n) => depth(n.name));

  const cols: DagNode[][] = [];
  for (const n of nodes) {
    const l = layer.get(n.name)!;
    (cols[l] ??= []).push(n);
  }

  // no text metrics pre-render: ~7.5px/char at the 13px label, ~5.6 at the 10px
  const width = (n: DagNode) => {
    const sub = subOf(n);
    const badge = n.badge ? Math.round(n.badge.length * 6.6) + 5 : 0;
    const text = Math.max(
      Math.round(n.name.length * 7.5) + glyphW + badge,
      sub ? Math.round(sub.length * 5.6) : 0,
    );
    return Math.max(72, text + PAD_X * 2);
  };
  const colW = cols.map((c) => Math.max(...c.map(width)));
  const colX: number[] = [];
  let x = MARGIN;
  for (let i = 0; i < cols.length; i++) {
    colX[i] = x;
    x += colW[i] + HGAP;
  }
  const svgW = x - HGAP + MARGIN;
  const colH = (c: DagNode[]) => c.length * nodeH + (c.length - 1) * VGAP;
  const maxColH = Math.max(...cols.map(colH));
  const svgH = maxColH + MARGIN * 2;

  const placed = new Map<string, Placed>();
  cols.forEach((c, l) => {
    let y = MARGIN + (maxColH - colH(c)) / 2;
    for (const node of c) {
      placed.set(node.name, { node, x: colX[l], y, w: width(node) });
      y += nodeH + VGAP;
    }
  });

  return (
    <div className="dag-scroll">
      <svg width={svgW} height={svgH} role="img" aria-label={label}>
        {nodes.flatMap((n) =>
          n.deps.map((dep) => {
            const a = placed.get(dep);
            const b = placed.get(n.name);
            if (!a || !b) return null;
            const sx = a.x + a.w;
            const sy = a.y + nodeH / 2;
            const tx = b.x;
            const ty = b.y + nodeH / 2;
            const bend = (tx - sx) / 2;
            return (
              <path
                key={`${dep}->${n.name}`}
                className="dag-edge"
                d={`M ${sx} ${sy} C ${sx + bend} ${sy}, ${tx - bend} ${ty}, ${tx} ${ty}`}
              />
            );
          }),
        )}
        {[...placed.values()].map(({ node, x: nx, y, w }) => {
          const st = statusOf(node);
          const labelCy = statuses ? y + LABEL_ROW_Y : y + nodeH / 2;
          const hit = needle !== "" && hay(node).includes(needle);
          const cls =
            [
              st === "skipped" ? "dag-skipped" : null,
              st === "canceled" ? "dag-canceled" : null,
              st === "absent" ? "dag-absent" : null,
              needle === "" ? null : hit ? "dag-hit" : "dag-miss",
              onSelect ? "dag-click" : null,
              selected === node.name ? "dag-selected" : null,
            ]
              .filter(Boolean)
              .join(" ") || undefined;
          return (
            <g key={node.name} className={cls} onClick={onSelect ? () => onSelect(node.name) : undefined}>
              {node.output_type && <title>{`${node.name} -> ${node.output_type}`}</title>}
              <rect className="dag-node" x={nx} y={y} width={w} height={nodeH} rx={4} />
              {st && st !== "absent" && (
                <g transform={`translate(${nx + PAD_X + 5}, ${labelCy})`}>
                  <GlyphShape status={glyphFor(st)} />
                </g>
              )}
              <text className="dag-label" x={nx + PAD_X + glyphW} y={labelCy} dominantBaseline="central">
                {/* a graph instance's ops share a prefix; muting it groups them by eye */}
                {prefixOf(node.name) && <tspan className="dag-prefix">{prefixOf(node.name)}</tspan>}
                {leafOf(node.name)}
                {node.badge && <tspan className="dag-badge"> {node.badge}</tspan>}
              </text>
              {st && (
                <text className="dag-status" x={nx + PAD_X + glyphW} y={y + STATUS_ROW_Y} dominantBaseline="central">
                  {subOf(node)}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
