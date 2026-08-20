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
import type { Stripe } from "./colour";

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
// `sales/` is visibly a group rather than an asset. a group name may not
// contain the separator (the build refuses one that does), so the fold never
// invents a name with two of them in it, and an asset that resolved into the
// group by its name prefix has something after the separator and so is not
// `sales/` either
export const groupNode = (group: string) => `${group}${SEPARATOR}`;

// fold every named group into one node, with the edges that crossed into or
// out of the group rewired to it. an edge that was inside the group is gone,
// which is the whole point: what you wanted to see was the group's own place
// in the graph.
//
// the group is the one a node declares, which is the answer the api resolved,
// rather than a prefix sliced off the name here. a node with no group of its
// own (an op, which is what the other graphs in this ui draw) folds into
// nothing
// what a folded node is coloured by: every label its members carry, once each,
// in name order. under `group` that is one label, since they share a group;
// under `origin` it is everything the group descends from, which is the claim
// the folded node can honestly make
function mergeHues(into: Stripe[] | undefined, from: Stripe[] | undefined): Stripe[] {
  const held = new Map((into ?? []).map((s) => [s.label, s]));
  for (const s of from ?? []) held.set(s.label, s);
  return [...held.values()].sort((a, b) => a.label.localeCompare(b.label));
}

export function collapseGroups(nodes: DagNode[], collapsed: Set<string>): DagNode[] {
  if (collapsed.size === 0) return nodes;
  const groups = new Map(nodes.map((n) => [n.name, n.group ?? null]));
  const fold = (name: string) => {
    const group = groups.get(name) ?? null;
    return group !== null && collapsed.has(group) ? groupNode(group) : name;
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
        name === n.name
          ? { ...n, deps }
          : // the folded node stands for the group, so it carries what its
            // members are coloured by rather than one member's share of it
            { name, deps, group: n.group, hues: n.hues, badge: "×1", find: n.name };
      held.set(name, node);
      out.push(node);
    } else {
      group.deps = [...new Set([...group.deps, ...deps])];
      group.badge = `×${counts.get(name)}`;
      group.find = `${group.find ?? ""} ${n.name}`;
      group.hues = mergeHues(group.hues, n.hues);
    }
  }
  return out;
}
