import { Link } from "react-router-dom";
import Markdown from "./Markdown";
import type { Deltas, MetaDelta, MetaTable, MetaValue, Metadata } from "./types";
import { fmtDataSize, fmtDuration, shortId } from "./util";

// a real minus sign, and a sign on every delta: the monochrome ui has no
// colour to carry direction, and colour alone would not carry it anyway
const signed = (n: number, body: string) => (n === 0 ? "±" : n < 0 ? "−" : "+") + body;

const absolute = (value: MetaValue, delta: number): string => {
  if ("bytes" in value) return signed(delta, fmtDataSize(Math.abs(delta)));
  if ("duration_secs" in value) return signed(delta, fmtDuration(Math.abs(delta) * 1000));
  return signed(delta, Math.abs(delta).toLocaleString());
};

const percent = (pct: number): string => signed(pct, `${Math.abs(pct)}%`);

// a size or a duration reads as a percentage — nobody wants "+48,000,000
// bytes" — and a count reads as itself, since "+37" is the fact and "+3%" is
// a derivation of it. whichever is not shown is on the hover
function DeltaView({ value, delta }: { value: MetaValue; delta: MetaDelta }) {
  const wantsPercent = "bytes" in value || "duration_secs" in value;
  const usePercent = wantsPercent && delta.delta_pct !== null;
  const shown = usePercent ? percent(delta.delta_pct!) : absolute(value, delta.delta);
  const other = usePercent
    ? absolute(value, delta.delta)
    : delta.delta_pct === null
      ? undefined
      : percent(delta.delta_pct);
  return (
    <span className="mono muted meta-delta" title={other}>
      {shown}
    </span>
  );
}

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
// as a link into this ui, markdown rendered, json as source
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
  if ("markdown" in value) {
    return (
      <div className="meta-md">
        <Markdown source={value.markdown} />
      </div>
    );
  }
  return <pre className="mono muted meta-block">{JSON.stringify(value.json, null, 2)}</pre>;
}

export default function MetaList({
  metadata,
  deltas = {},
}: {
  metadata: Metadata;
  deltas?: Deltas;
}) {
  const entries = Object.entries(metadata);
  if (entries.length === 0) return null;
  return (
    <div className="meta-list">
      {entries.map(([name, value]) => (
        <div key={name} className="meta-row">
          <span className="meta-name">{name}</span>
          <MetaValueView value={value} />
          {/* a key with nothing to compare against shows nothing at all,
              which is a different claim from having not moved */}
          {deltas[name] && <DeltaView value={value} delta={deltas[name]} />}
        </div>
      ))}
    </div>
  );
}
