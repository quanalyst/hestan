// Presentation metadata never replaces the persistent name used by routes or edges.
export interface Presentation {
  name: string;
  display_name?: string | null;
  subgroup?: string | null;
  labels?: Record<string, string>;
  group: string | null;
}
export const displayName = (item: { name: string; display_name?: string | null }) => item.display_name ?? item.name;
export const matchesName = (item: { name: string; display_name?: string | null }, query: string) =>
  [item.name, displayName(item)].some((s) => s.toLowerCase().includes(query.trim().toLowerCase()));

// JSON tuples keep equal subgroup names under different parents distinct.
export const subgroupKey = (parent: string, subgroup: string | null) => `@subgroup/${JSON.stringify([parent, subgroup])}`;
export interface Section<T> {
  key: string;
  name: string;
  level: number;
  members: T[];
  children: Section<T>[];
}
export function hierarchy<T extends Presentation>(items: T[], view: GroupingView = DEFAULT_VIEW): Section<T>[] {
  if (!isDefaultView(view)) return projectedHierarchy(items, view);
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const key = item.group ?? "";
    const held = groups.get(key);
    if (held) held.push(item); else groups.set(key, [item]);
  }
  return [...groups].map(([name, members]) => {
    const children: Section<T>[] = [];
    if (members.some((m) => m.subgroup != null)) {
      const subgroups = new Map<string | null, T[]>();
      for (const m of members) {
        const sub = m.subgroup ?? null;
        const held = subgroups.get(sub);
        if (held) held.push(m); else subgroups.set(sub, [m]);
      }
      for (const [sub, held] of subgroups) children.push({ key: subgroupKey(name, sub), name: sub ?? "no subgroup", level: 1, members: held, children: [] });
    }
    return { key: name, name: name || "no group", level: 0, members, children };
  });
}
export function visibleSections<T>(sections: Section<T>[], closed: ReadonlySet<string>, reveal = false): Section<T>[] {
  return sections.flatMap((s) => [s, ...((reveal || !closed.has(s.key)) ? s.children : [])]);
}
export function readSet(raw: string | null): Set<string> {
  if (raw?.startsWith("[")) {
    try { const v: unknown = JSON.parse(raw); if (Array.isArray(v) && v.every((s) => typeof s === "string")) return new Set(v); } catch { /* legacy comma list below */ }
  }
  return new Set((raw ?? "").split(",").filter(Boolean));
}
export const writeSet = (set: ReadonlySet<string>) => set.size === 0 ? "" : JSON.stringify([...set]);

export interface Health {
  last_run?: { status: string } | null;
  execution?: { failed: number; running: number };
  checks?: { failed: number };
  overdue?: boolean;
  stale?: boolean;
  freshness?: { status: string } | null;
}
export function summary(items: Health[]): string {
  const count = (f: (i: Health) => boolean) => items.filter(f).length;
  const parts: [number, string][] = [
    [count((i) => i.last_run?.status === "failed" || (i.execution?.failed ?? 0) > 0), "failed"],
    [count((i) => i.last_run?.status === "running" || (i.execution?.running ?? 0) > 0), "running"],
    [count((i) => i.freshness?.status === "late"), "late"],
    [count((i) => !!i.overdue), "overdue"],
    [count((i) => (i.checks?.failed ?? 0) > 0), "failed checks"],
    [count((i) => !!i.stale), "stale"],
  ];
  return [`${items.length} member${items.length === 1 ? "" : "s"}`, ...parts.filter(([n]) => n > 0).map(([n, label]) => `${n} ${label}`)].join(" · ");
}

export type Dimension = "group" | "subgroup" | `label:${string}`;
export type GroupingView = readonly [Dimension, Dimension];
export const DEFAULT_VIEW: GroupingView = ["group", "subgroup"];
export const isDefaultView = (view: GroupingView) => view[0] === "group" && view[1] === "subgroup";
const dimension = (s: string | null): s is Dimension => s === "group" || s === "subgroup" || (!!s?.startsWith("label:") && s.length > 6);
export function viewFrom(params: URLSearchParams): GroupingView {
  const first = params.get("by") ?? "group";
  const second = params.get("then") ?? "subgroup";
  return dimension(first) && dimension(second) && first !== second ? [first, second] : DEFAULT_VIEW;
}
export function viewParams(params: URLSearchParams, view: GroupingView): URLSearchParams {
  const next = new URLSearchParams(params);
  if (isDefaultView(view)) { next.delete("by"); next.delete("then"); }
  else { next.set("by", view[0]); next.set("then", view[1]); }
  // Keys from a different projection should not collapse unrelated buckets.
  next.delete("open"); next.delete("closed");
  return next;
}
export const dimensionName = (d: Dimension) => d === "group" ? "group" : d === "subgroup" ? "subgroup" : `label: ${d.slice(6)}`;
export function dimensionsOf(items: Presentation[]): Dimension[] {
  return ["group", "subgroup", ...[...new Set(items.flatMap((i) => Object.keys(i.labels ?? {})))].sort().map((key): Dimension => `label:${key}`)];
}
function valueOf(item: Presentation, d: Dimension): { key: string; name: string } {
  if (d === "group") return { key: JSON.stringify(item.group), name: item.group ?? "no group" };
  if (d === "subgroup") return { key: JSON.stringify([item.group, item.subgroup ?? null]), name: item.subgroup == null ? `${item.group ? item.group + " / " : ""}no subgroup` : `${item.group} / ${item.subgroup}` };
  const value = labelValue(item, d.slice(6));
  return { key: JSON.stringify(value ?? null), name: value ?? `no ${d.slice(6)} label` };
}
function projectedHierarchy<T extends Presentation>(items: T[], view: GroupingView): Section<T>[] {
  const buckets = (members: T[], level: number, parent: string | null): Section<T>[] => {
    const groups = new Map<string, Section<T>>();
    for (const member of members) {
      const value = valueOf(member, view[level]);
      const key = JSON.stringify(["view", view, parent, value.key]);
      const section = groups.get(key) ?? { key, name: value.name, level, members: [], children: [] };
      section.members.push(member); groups.set(key, section);
    }
    return [...groups.values()];
  };
  const roots = buckets(items, 0, null);
  for (const root of roots) root.children = buckets(root.members, 1, root.key);
  return roots;
}
export function matchesLabel(item: Presentation, params: URLSearchParams): boolean {
  const key = params.get("label");
  if (!key) return true;
  const value = labelValue(item, key);
  return params.get("missing") === "1" ? value === undefined : !params.has("value") || value === params.get("value");
}

export const labelValue = (item: Presentation, key: string): string | undefined => item.labels && Object.prototype.hasOwnProperty.call(item.labels, key) ? item.labels[key] : undefined;

// Filter a registered hierarchy without changing its shape: a matching loose
// member of a mixed parent still belongs to the explicit "no subgroup" row.
export function filterHierarchy<T>(sections: Section<T>[], matches: (item: T) => boolean): Section<T>[] {
  return sections.flatMap((section) => {
    const members = section.members.filter(matches);
    return members.length ? [{ ...section, members, children: filterHierarchy(section.children, matches) }] : [];
  });
}
