// what a graph draws when it has too many nodes to draw.
//
// `DagView` lays out every node it is handed, which stops working somewhere
// past a hundred: the columns get taller than the screen and the edges become
// a texture. these are the two ways of handing it fewer (a neighbourhood
// around one node, and a prefix group folded into a single node) as
// transforms over the node list, so the drawing code stays one thing and
// these stay testable.
import type { DagNode } from "./DagView";
import { SEPARATOR } from "./catalog";

// past this many nodes the whole graph is a picture of having a lot of assets
// rather than of how they fit together, so the view opens focused instead.
// it is about the tallest column rather than the count, but the count is what
// the page knows before laying anything out, and 60 is where the tallest
// column in a realistic graph stops fitting on a screen
export const WHOLE_GRAPH_MAX = 60;

// as many neighbours as are worth drawing at once. a source with sixty
// dependents has a neighbourhood the size of the graph at one hop, and
// drawing it would be the wall the focus was supposed to avoid
export const FOCUS_MAX = 40;

// the nodes within `depth` hops of `focus`, following deps in both directions:
// what feeds it and what it feeds, which are the two questions anybody has
// about one node. nearest first up to `max`, since a hub's neighbourhood can
// be the whole graph, and the count of what was left out is the caller's to
// print. input order is kept, so the layout is what it would have been
export function neighbourhood(
  nodes: DagNode[],
  focus: string,
  depth: number,
  max = FOCUS_MAX,
): DagNode[] {
  const out = new Map<string, string[]>();
  for (const n of nodes) {
    for (const d of n.deps) {
      if (!out.has(d)) out.set(d, []);
      out.get(d)!.push(n.name);
    }
  }
  const deps = new Map(nodes.map((n) => [n.name, n.deps]));
  if (!deps.has(focus)) return [];
  const reached = new Set([focus]);
  let edge = [focus];
  for (let step = 0; step < depth && reached.size < max; step++) {
    const next: string[] = [];
    for (const name of edge) {
      for (const other of [...(deps.get(name) ?? []), ...(out.get(name) ?? [])]) {
        if (reached.size >= max) break;
        if (!reached.has(other) && deps.has(other)) {
          reached.add(other);
          next.push(other);
        }
      }
    }
    edge = next;
  }
  return nodes.filter((n) => reached.has(n.name));
}

// the name a folded group draws under. the trailing separator is the point:
// `sales/` is visibly a group and could not collide with an asset name, which
// has to have something after the separator to have a prefix at all
export const groupNode = (prefix: string) => `${prefix}${SEPARATOR}`;

// fold every named prefix into one node, with the edges that crossed into or
// out of the group rewired to it. an edge that was inside the group is gone,
// which is the whole point: what you wanted to see was the group's own place
// in the graph
export function collapseGroups(nodes: DagNode[], collapsed: Set<string>): DagNode[] {
  if (collapsed.size === 0) return nodes;
  const fold = (name: string) => {
    const cut = name.indexOf(SEPARATOR);
    const prefix = cut < 0 ? null : name.slice(0, cut);
    return prefix !== null && collapsed.has(prefix) ? groupNode(prefix) : name;
  };
  const held = new Map<string, DagNode>();
  const counts = new Map<string, number>();
  const out: DagNode[] = [];
  for (const n of nodes) {
    const name = fold(n.name);
    const deps = [...new Set(n.deps.map(fold))].filter((d) => d !== name);
    counts.set(name, (counts.get(name) ?? 0) + 1);
    const group = held.get(name);
    if (group === undefined) {
      // a folded node is several assets: its note and badge belong to none of
      // them, so it carries the count instead, and the names it swallowed, so
      // a search for one of them still finds where it went
      const node =
        name === n.name ? { ...n, deps } : { name, deps, badge: "×1", find: n.name };
      held.set(name, node);
      out.push(node);
    } else {
      group.deps = [...new Set([...group.deps, ...deps])];
      group.badge = `×${counts.get(name)}`;
      group.find = `${group.find ?? ""} ${n.name}`;
    }
  }
  return out;
}
