import type { MetaValue, Metadata } from "./types";

// metadata is typed at the source, so it renders by type rather than as json:
// numbers as numbers, a url as a link, markdown and json as source. hestan
// ships no markdown parser — the source is the honest thing to show.
function MetaValueView({ value }: { value: MetaValue }) {
  if ("int" in value || "float" in value) {
    const n = "int" in value ? value.int : value.float;
    return <span className="mono num meta-num">{n.toLocaleString()}</span>;
  }
  if ("url" in value) {
    return (
      <a className="mono meta-val" href={value.url} target="_blank" rel="noreferrer">
        {value.url}
      </a>
    );
  }
  if ("text" in value) {
    return <span className="meta-val">{value.text}</span>;
  }
  const source = "markdown" in value ? value.markdown : JSON.stringify(value.json, null, 2);
  return <pre className="mono muted meta-block">{source}</pre>;
}

export default function MetaList({ metadata }: { metadata: Metadata }) {
  const entries = Object.entries(metadata);
  if (entries.length === 0) return null;
  return (
    <div className="meta-list">
      {entries.map(([name, value]) => (
        <div key={name} className="meta-row">
          <span className="meta-name">{name}</span>
          <MetaValueView value={value} />
        </div>
      ))}
    </div>
  );
}
