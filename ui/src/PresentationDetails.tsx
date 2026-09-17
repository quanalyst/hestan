import type { Presentation } from "./presentation";

// Shared detail rows keep the declaration visible independently of a view.
export default function PresentationDetails({ item }: { item: Presentation }) {
  const rows: [string, string][] = [];
  if (item.display_name) rows.push(["identifier", item.name]);
  if (item.group !== null) rows.push(["group", item.group]);
  if (item.subgroup) rows.push(["subgroup", item.subgroup]);
  for (const [key, value] of Object.entries(item.labels ?? {}).sort(([a], [b]) => a.localeCompare(b))) {
    rows.push([`label: ${key}`, value]);
  }
  return <>{rows.map(([label, value]) => (
    <div className="op-line" key={label}>
      <span className="op-line-label">{label}</span>
      <span className="mono">{value}</span>
    </div>
  ))}</>;
}
