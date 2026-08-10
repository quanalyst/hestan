import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { get, HttpError, post, usePoll } from "./api";
import { chunksOf, keyStates } from "./backfill";
import type { ChunkStatus } from "./backfill";
import StatusDot from "./StatusDot";
import { GlyphShape } from "./StatusGlyph";
import type { Status } from "./StatusGlyph";
import type { Backfill, BackfillStatus, Run } from "./types";
import { assetPath, isTerminal, relTime, shortId } from "./util";

// a backfill's states map onto the run states they are made of, exactly as on
// the assets page
const BACKFILL_GLYPH = {
  running: "running",
  complete: "success",
  failed: "failed",
  canceled: "canceled",
} as const satisfies Record<BackfillStatus, Status>;

// a key's cell says what the run launched for it did. nothing launched yet is
// the hollow cell the partition grid already uses for a key never built
const CELL: Record<ChunkStatus, string> = {
  success: "success",
  failed: "failed",
  running: "running",
  queued: "queued",
  canceled: "canceled",
  "not launched": "missing",
};

const LEGEND = [
  ["success", "built"],
  ["running", "building now"],
  ["failed", "its run failed"],
  ["missing", "not launched yet"],
] as const;

export default function BackfillPage() {
  const { id } = useParams();
  if (!id) return null;
  return <BackfillView key={id} id={id} />;
}

function BackfillView({ id }: { id: string }) {
  const [backfill, setBackfill] = useState<Backfill | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [missing, setMissing] = useState(false);
  const [canceling, setCanceling] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const done = backfill !== null && backfill.status !== "running";
  usePoll(
    () => {
      get<{ backfill: Backfill; runs: Run[] }>(`/api/backfills/${id}`)
        .then((r) => {
          setBackfill(r.backfill);
          setRuns(r.runs);
        })
        .catch((e) => {
          if (e instanceof HttpError && e.status === 404) setMissing(true);
        });
    },
    done || missing ? null : 2000,
    [id, done, missing],
  );

  const cancel = async () => {
    setCanceling(true);
    setError(null);
    try {
      await post<{ canceled: boolean }>(`/api/backfills/${id}/cancel`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setCanceling(false);
    }
  };

  if (missing) return <p className="muted">no backfill numbered {id}</p>;
  if (!backfill) return <p className="muted">loading…</p>;

  const chunks = chunksOf(backfill, runs);
  const states = keyStates(chunks);
  const count = (want: ChunkStatus) =>
    backfill.partitions.filter((key) => states.get(key) === want).length;
  const built = count("success");
  const failed = count("failed");
  const left = backfill.total - built - failed;
  const drawn = new Set(backfill.partitions.map((key) => CELL[states.get(key) ?? "not launched"]));
  const present = LEGEND.filter(([state]) => drawn.has(state));

  return (
    <>
      <div className="page-head">
        <div>
          <h1>
            <Link to={assetPath(backfill.asset)}>{backfill.asset}</Link>{" "}
            <span className="mono secondary">backfill {backfill.id}</span>
          </h1>
          <p className="muted">
            <span className="mono">
              {backfill.from_key} → {backfill.to_key}
            </span>{" "}
            · {backfill.total} partitions · started {relTime(backfill.created_at)}
          </p>
        </div>
        <div className="run-actions">
          <div className="run-side">
            <span className="pill">
              <span className="status">
                <svg className="glyph" width={12} height={12} viewBox="-6 -6 12 12" aria-hidden="true">
                  <GlyphShape status={BACKFILL_GLYPH[backfill.status]} />
                </svg>
                {backfill.status}
              </span>
            </span>
            {backfill.status === "running" && (
              <button className="text-btn" onClick={cancel} disabled={canceling}>
                cancel
              </button>
            )}
          </div>
          {error && <p className="muted">cancel failed: {error}</p>}
        </div>
      </div>

      {backfill.total === 0 ? (
        <p className="muted">
          nothing to do: every partition in that range was already fresh when it was asked for.
        </p>
      ) : (
        <>
          <h2>partitions</h2>
          <div className="part-grid">
            {backfill.partitions.map((key) => {
              const state = states.get(key) ?? "not launched";
              return (
                <span
                  key={key}
                  className={`part-cell ${CELL[state]}`}
                  title={`${key}\n${state}`}
                  aria-label={`${key}: ${state}`}
                />
              );
            })}
          </div>
          {/* the legend earns its place once there is more than one thing in
              the grid to tell apart, as everywhere else in the ui */}
          {present.length > 1 && (
            <div className="part-legend muted">
              {present.map(([state, what]) => (
                <span key={state} className="part-legend-item">
                  <span className={`part-cell ${state}`} aria-hidden="true" />
                  {what}
                </span>
              ))}
            </div>
          )}
          <p className="muted part-more">
            {built} built · {failed} failed · {left} left
          </p>

          <h2>chunks</h2>
          <table className="plain-rows">
            <thead>
              <tr>
                <th>run</th>
                <th>status</th>
                <th>keys</th>
                <th className="num">count</th>
                <th>started</th>
              </tr>
            </thead>
            <tbody>
              {chunks.map((chunk, i) => {
                const run = runs.find((r) => r.id === chunk.run_id);
                return (
                  <tr key={chunk.run_id ?? `left${i}`}>
                    <td>
                      {chunk.run_id ? (
                        <Link className="mono" to={`/runs/${chunk.run_id}`}>
                          {shortId(chunk.run_id)}
                        </Link>
                      ) : (
                        <span className="muted">—</span>
                      )}
                    </td>
                    <td>
                      {run ? (
                        <StatusDot status={run.status} />
                      ) : (
                        <span className="muted">not launched</span>
                      )}
                    </td>
                    <td className="mono">
                      {chunk.keys[0]}
                      {chunk.keys.length > 1 && ` → ${chunk.keys[chunk.keys.length - 1]}`}
                    </td>
                    <td className="num">{chunk.keys.length}</td>
                    <td className="muted">
                      {run ? relTime(run.started_at ?? run.created_at) : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {/* the record is closed by whichever chunk ended it, so a failed one
              leaves the rest unlaunched rather than carrying on past it */}
          {backfill.status === "failed" && (
            <p className="muted">
              a chunk failed, so no further chunk went out. the run above says which op broke;
              starting the range again picks up what is missing.
            </p>
          )}
          {runs.some((r) => !isTerminal(r.status)) && (
            <p className="muted">a chunk is running now — the grid fills in as it lands.</p>
          )}
        </>
      )}
    </>
  );
}
