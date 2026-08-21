import { MetaValueView } from "./MetaList";
import type { MetaValue, OpRun } from "./types";
import { relTime } from "./util";

interface SavedEntry {
  op: string;
  name: string;
  taken_at: string;
  value: MetaValue;
}

// every sample every op of this run marked, in the order the ops come back
// and, within an op, the order the api stored them: the section is the run's,
// so each entry keeps the name of the op that took it
function savedEntries(ops: OpRun[]): SavedEntry[] {
  const out: SavedEntry[] = [];
  for (const op of ops) {
    for (const [name, value] of Object.entries(op.metadata ?? {})) {
      if ("saved" in value) {
        out.push({ op: op.op, name, taken_at: value.saved.taken_at, value: value.saved.value });
      }
    }
  }
  return out;
}

export default function SavedList({ ops }: { ops: OpRun[] }) {
  const entries = savedEntries(ops);
  // a run that saved nothing gets no section, rather than an empty one
  if (entries.length === 0) return null;
  return (
    <>
      <h2>saved</h2>
      {/* the line that stops this being read as live data */}
      <p className="muted saved-note">
        what each op sampled of what it wrote, as it stood when the op wrote it. a snapshot of
        that moment; nothing here goes back and looks again.
      </p>
      {entries.map((e) => (
        <div key={`${e.op} ${e.name}`} className="saved-entry">
          <div className="saved-head">
            <span className="mono saved-op">{e.op}</span>
            <span className="saved-name">{e.name}</span>
            <span className="muted saved-taken" title={e.taken_at}>
              taken {relTime(e.taken_at)}
            </span>
          </div>
          <MetaValueView value={e.value} />
        </div>
      ))}
    </>
  );
}
