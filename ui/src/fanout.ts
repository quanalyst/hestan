import type { JobSummary, OpRun, OpStatus, OpSummary, RunEvent } from "./types";

// an instance's name is its op with one `[label]` per level of fan-out it sits
// inside, so a fan-out inside a fan-out reads `probe[2026-01-05][3]`. a label
// is the element's index, or the element itself on an op that names its
// instances by them, and it carries no bracket, which is what makes the name
// reversible. what makes it an instance rather than an op with brackets in its
// name is that the part before the first `[` is a mapped op of this job
const INSTANCE = /^([^[]+)((?:\[[^[\]]*\])+)$/;

// every instance row of the run, by the mapped op it belongs to.
export type FanOut = Map<string, OpRun[]>;

// the labels of one instance name, outermost first; `null` for a name no
// expansion could have written.
export function labelsOf(op: string): string[] | null {
  const m = INSTANCE.exec(op);
  return m ? m[2].slice(1, -1).split("][") : null;
}

// a mapped op writes no op_runs row of its own: its instances are the record,
// so the ui rebuilds the group from their bracketed names
export function fanOut(job: JobSummary | null, ops: OpRun[]): FanOut {
  const mapped = new Set((job?.ops ?? []).filter((o) => o.mapped_over).map((o) => o.name));
  const out: FanOut = new Map();
  for (const o of ops) {
    const m = INSTANCE.exec(o.op);
    if (!m || !mapped.has(m[1])) continue;
    const group = out.get(m[1]);
    if (group) group.push(o);
    else out.set(m[1], [o]);
  }
  // element order, which is what the collected output is in, level by level; a
  // key-labelled instance has no index to order by, so its label is the order
  for (const group of out.values()) group.sort(byLabels);
  return out;
}

function byLabels(a: OpRun, b: OpRun): number {
  const [x, y] = [labelsOf(a.op) ?? [], labelsOf(b.op) ?? []];
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    const order = compareLabel(x[i] ?? "", y[i] ?? "");
    if (order !== 0) return order;
  }
  return 0;
}

function compareLabel(x: string, y: string): number {
  const [i, j] = [Number(x), Number(y)];
  return Number.isNaN(i) || Number.isNaN(j) ? x.localeCompare(y) : i - j;
}

// one instance of a fan-out, or a whole fan-out nested inside one.
export type InstanceNode =
  | { kind: "instance"; row: OpRun }
  | { kind: "group"; label: string; children: InstanceNode[] };

// a mapped op's instances as the tree their names describe: one level of
// groups per level of nesting, so which outer element an instance belongs to
// is on the page rather than only in its name. a fan-out that never nested is
// a flat list of instances, exactly as it was.
export function instanceTree(rows: OpRun[]): InstanceNode[] {
  return level(rows, 0);
}

function level(rows: OpRun[], depth: number): InstanceNode[] {
  const out: InstanceNode[] = [];
  // insertion order is element order, since the rows arrive sorted
  const groups = new Map<string, OpRun[]>();
  for (const row of rows) {
    const labels = labelsOf(row.op);
    if (!labels || depth >= labels.length - 1) {
      out.push({ kind: "instance", row });
      continue;
    }
    const label = labels[depth];
    const group = groups.get(label);
    if (group) group.push(row);
    else groups.set(label, [row]);
  }
  for (const [label, group] of groups)
    out.push({ kind: "group", label, children: level(group, depth + 1) });
  return out;
}

// the worst thing any instance is doing is what the mapped op is doing:
// it succeeds only if all of them did
export function rollup(rows: OpRun[]): OpStatus {
  for (const st of ["failed", "canceled", "running", "pending", "skipped"] as const)
    if (rows.some((r) => r.status === st)) return st;
  return "success";
}

// how many instances a mapped op made, from the expansion event: the only
// place the count lives when it expanded over an empty array
export function expansions(events: RunEvent[]): Map<string, number> {
  const out = new Map<string, number>();
  for (const e of events) {
    if (e.kind !== "op_expanded" || !e.op) continue;
    const n = (e.data as { instances?: number } | null)?.instances;
    if (typeof n === "number") out.set(e.op, n);
  }
  return out;
}

// what one instance actually waited for, for the gantt to lay out: its own
// outer instance where its op fans out over a mapped one (`probe[1][0]`
// followed `sites[1]`, not every site there was) and whatever its other deps
// expanded into, the same way the mapped op's own row would have read them
export function instanceDeps(op: OpSummary, name: string, fan: FanOut): string[] {
  const labels = labelsOf(name) ?? [];
  return op.deps.flatMap((d) => {
    if (d === op.mapped_over && labels.length > 1) {
      return [`${d}[${labels.slice(0, -1).join("][")}]`];
    }
    return fan.get(d)?.map((r) => r.op) ?? [d];
  });
}
