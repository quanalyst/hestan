import { dimensionsOf, dimensionName, viewFrom, viewParams, labelValue } from "./presentation";
import type { Dimension, Presentation } from "./presentation";

export default function GroupingControls({ items, params, onChange }: {
  items: Presentation[];
  params: URLSearchParams;
  onChange: (params: URLSearchParams) => void;
}) {
  const view = viewFrom(params);
  const dimensions = [...new Set([...dimensionsOf(items), ...view])];
  const label = params.get("label") ?? "";
  const keys = [...new Set([...items.flatMap((i) => Object.keys(i.labels ?? {})), ...(label ? [label] : [])])].sort();
  const values = [...new Set(items.flatMap((i) => labelValue(i, label) === undefined ? [] : [labelValue(i, label)!]))].sort();
  const edit = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(params);
    for (const [key, value] of Object.entries(changes)) { if (value === null) next.delete(key); else next.set(key, value); }
    onChange(next);
  };
  return <div className="filter-row">
    {view.map((current, index) => <label key={index} className="filter-group">
      {index === 0 ? "group by" : "then"}
      <select aria-label={index === 0 ? "group by" : "then group by"} value={current} onChange={(e) => {
        const selected = e.target.value as Dimension;
        onChange(viewParams(params, index === 0 ? [selected, view[1]] : [view[0], selected]));
      }}>
        {dimensions.filter((d) => d !== view[1 - index]).map((d) => <option key={d} value={d}>{dimensionName(d)}</option>)}
      </select>
    </label>)}
    <button className="text-btn" onClick={() => onChange(viewParams(params, [view[1], view[0]]))}>swap levels</button>
    <button className="text-btn" onClick={() => onChange(viewParams(params, ["group", "subgroup"]))}>declared hierarchy</button>
    {keys.length > 0 && <>
      <label className="filter-group">label filter <select aria-label="label filter" value={label} onChange={(e) => edit({ label: e.target.value || null, value: null, missing: null })}>
        <option value="">all labels</option>
        {keys.map((k) => <option key={k}>{k}</option>)}
      </select></label>
      {label && <label className="filter-group">value <select aria-label="label value" value={params.get("missing") === "1" ? "missing" : params.has("value") ? JSON.stringify(params.get("value")) : "all"} onChange={(e) => edit({ missing: e.target.value === "missing" ? "1" : null, value: e.target.value === "missing" || e.target.value === "all" ? null : JSON.parse(e.target.value) as string })}>
        <option value="all">all values</option><option value="missing">missing value</option>
        {[...new Set([...values, ...(params.has("value") ? [params.get("value")!] : [])])].map((v) => <option key={v} value={JSON.stringify(v)}>{v}</option>)}
      </select></label>}
    </>}
  </div>;
}
