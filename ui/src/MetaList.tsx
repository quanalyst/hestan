import { Link } from "react-router-dom";
import type { MetaTable, MetaValue, Metadata } from "./types";
import { fmtDataSize, fmtDuration, shortId } from "./util";

// a cell prints as itself when it is a string and as its json otherwise; a
// null cell is the gap the source padded a short row with
function cellText(v: unknown): string {
  if (v === null || v === undefined) return "—";
  return typeof v === "string" ? v : JSON.stringify(v);
}

function TableView({ table }: { table: MetaTable }) {
  return (
    <div className="meta-table-wrap">
      <table className="meta-table">
        <thead>
          <tr>
            {table.columns.map((c, i) => (
              <th key={i}>
                {c.name}
                {c.type && <span className="muted meta-col-type"> {c.type}</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.map((row, i) => (
            <tr key={i}>
              {row.map((v, j) => (
                <td key={j} className={typeof v === "number" ? "num" : undefined}>
                  {cellText(v)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {/* a hundred rows that were all of them and the head of a million are
          different facts, so the second one says so */}
      {table.truncated && (
        <div className="muted meta-table-note">first {table.rows.length} rows</div>
      )}
    </div>
  );
}

// the basename is what you are looking for; the directory is context
function PathView({ path }: { path: string }) {
  const cut = path.lastIndexOf("/");
  return (
    <span className="mono meta-val meta-path" title={path}>
      {cut >= 0 && <span className="muted">{path.slice(0, cut + 1)}</span>}
      {path.slice(cut + 1)}
    </span>
  );
}

// metadata is typed at the source, so it renders by type rather than as json:
// numbers as numbers in their unit, a url as a link, a run or asset reference
// as a link into this ui, markdown and json as source
function MetaValueView({ value }: { value: MetaValue }) {
  if ("int" in value || "float" in value || "count" in value) {
    const n = "int" in value ? value.int : "float" in value ? value.float : value.count;
    return <span className="mono num meta-num">{n.toLocaleString()}</span>;
  }
  if ("bytes" in value) {
    return (
      <span className="mono num meta-num" title={`${value.bytes.toLocaleString()} bytes`}>
        {fmtDataSize(value.bytes)}
      </span>
    );
  }
  if ("duration_secs" in value) {
    return <span className="mono num meta-num">{fmtDuration(value.duration_secs * 1000)}</span>;
  }
  if ("url" in value) {
    return (
      <a className="mono meta-val" href={value.url} target="_blank" rel="noreferrer">
        {value.url}
      </a>
    );
  }
  if ("path" in value) return <PathView path={value.path} />;
  if ("run" in value) {
    return (
      <Link className="mono meta-val" to={`/runs/${value.run}`} title={value.run}>
        {shortId(value.run)}
      </Link>
    );
  }
  if ("asset" in value) {
    return (
      <Link className="mono meta-val" to={`/assets?asset=${encodeURIComponent(value.asset)}`}>
        {value.asset}
      </Link>
    );
  }
  if ("table" in value) return <TableView table={value.table} />;
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
