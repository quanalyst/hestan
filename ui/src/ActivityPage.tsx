import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { get } from "./api";
import { token } from "./identity";
import type { Dropped, FeedRow, Filters } from "./activity";
import { decidingLine, kindLabel, linkFor, matches, merge, subjectOf } from "./activity";
import type { EventLevel, Health, RunEvent } from "./types";
import { clockTime, relTime, shortId } from "./util";

const PAGE = 100;

// how often an authenticated tab asks what has happened since, in place of the
// stream it cannot authenticate. the command line follows the log on the same
// interval and for the same reason: this is the whole system's log, and a
// second of lag on it costs nothing.
const FOLLOW_POLL = 1000;

const SUBJECTS = ["all", "run", "job", "asset", "schedule", "sensor", "backfill", "system"] as const;
const LEVELS = ["all", "info", "warn", "error"] as const;

function FilterGroup<T extends string>({
  label,
  value,
  options,
  onPick,
}: {
  label: string;
  value: T;
  options: readonly T[];
  onPick: (v: T) => void;
}) {
  return (
    <span className="filter-group">
      <span className="filter-label">{label}</span>
      {options.map((o) => (
        <button
          key={o}
          className={value === o ? "text-btn active" : "text-btn"}
          onClick={() => onPick(o)}
        >
          {o}
        </button>
      ))}
    </span>
  );
}

// the level in the margin, as a word. the palette is monochrome and an `error`
// that is only a shade darker than an `info` is not a signal anybody can read
function LevelTag({ level }: { level: EventLevel }) {
  if (level === "info") return <span className="act-level muted">·</span>;
  return <span className="act-level">{level}</span>;
}

function EventRow({ event }: { event: RunEvent }) {
  const subject = subjectOf(event);
  const to = linkFor(event);
  // a run id is a uuid and reads as eight characters everywhere else in the ui;
  // in full it is two lines of a column that is one word wide for every other
  // subject there is
  const shown = subject === null ? null : event.subject_kind === "run" ? shortId(subject) : subject;
  return (
    <tr className={event.level === "info" ? undefined : `ev-${event.level}`}>
      <td className="muted" title={event.ts}>
        {relTime(event.ts)}
      </td>
      <td>
        <LevelTag level={event.level} />
      </td>
      <td className="muted act-subject-kind">{event.subject_kind}</td>
      <td className="mono" title={subject ?? undefined}>
        {shown === null ? (
          <span className="muted">none</span>
        ) : to === null ? (
          shown
        ) : (
          <Link to={to}>{shown}</Link>
        )}
      </td>
      <td className="act-kind muted">{kindLabel(event.kind)}</td>
      <td>
        {event.message}
        {event.op !== null && <span className="muted act-op"> {event.op}</span>}
        {/* who asked for it, where the deployment knew: the other half of an
            audit trail is being able to read it without opening the run */}
        {event.actor !== null && <span className="muted act-op"> by {event.actor}</span>}
      </td>
    </tr>
  );
}

// the stream dropped events because this tab was not reading them fast enough.
// shown where they would have been, with what to do about it: a gap nobody can
// see is worse than no gap at all
function GapRow({ gap }: { gap: Dropped }) {
  return (
    <tr className="act-gap">
      <td colSpan={6} className="muted">
        {gap.count} event{gap.count === 1 ? "" : "s"} dropped through seq {gap.through}. this
        tab fell behind the stream. they are still in the log; reload to fetch them.
      </td>
    </tr>
  );
}

// who is deciding, said once, on the page somebody asking "why has nothing
// fired" is already reading. a deployment may serve this ui from several
// processes and exactly one of them decides, so which one is not a pedantic
// distinction: it is the whole answer.
function Deciding() {
  const [says, setSays] = useState<string | null>(null);
  useEffect(() => {
    let stopped = false;
    get<Health>("/api/health")
      .then((h) => {
        if (!stopped) setSays(decidingLine(h));
      })
      // a health endpoint that cannot be read says nothing here: every page
      // below reports a deployment that is not there, one failed fetch at a
      // time, and a second copy of that on this line helps nobody
      .catch(() => {});
    return () => {
      stopped = true;
    };
  }, []);
  if (says === null) return null;
  return <p className="act-deciding muted">deciding · {says}</p>;
}

export default function ActivityPage() {
  const [feed, setFeed] = useState<FeedRow[] | null>(null);
  const [live, setLive] = useState(false);
  const [subjectKind, setSubjectKind] = useState<(typeof SUBJECTS)[number]>("all");
  const [level, setLevel] = useState<(typeof LEVELS)[number]>("all");
  const [find, setFind] = useState("");
  const [older, setOlder] = useState(false);
  const [exhausted, setExhausted] = useState(false);
  const cursor = useRef<number | null>(null);

  // one page of history, then whatever happens next. the two halves are
  // deliberate: the page is what happened, the follow is what happens, and
  // starting the follow at the page's top is what makes them one list
  useEffect(() => {
    let source: EventSource | null = null;
    let poll: ReturnType<typeof setInterval> | null = null;
    let stopped = false;
    let newest = 0;
    get<{ events: RunEvent[] }>(`/api/events?limit=${PAGE}`)
      .then((r) => {
        if (stopped) return;
        setFeed(r.events.map((event) => ({ kind: "event", event }) as FeedRow));
        setExhausted(r.events.length < PAGE);
        newest = r.events[0]?.seq ?? 0;
        cursor.current = r.events[r.events.length - 1]?.seq ?? null;
        // an EventSource cannot carry a header, and the only other way to hand
        // a stream a token is to put it in the url, where it lands in the
        // browser's history and in every access log between here and the
        // deployment. so an authenticated tab polls instead: a second of lag
        // on "what is happening" costs less than a credential in a log
        if (token() !== null) {
          const since = async () => {
            try {
              const r = await get<{ events: RunEvent[] }>(`/api/events?after=${newest}&limit=${PAGE}`);
              setLive(true);
              if (r.events.length === 0) return;
              newest = Math.max(newest, ...r.events.map((e) => e.seq));
              setFeed((f) => merge(f ?? [], r.events.map((event) => ({ kind: "event", event }) as FeedRow)));
            } catch {
              setLive(false);
            }
          };
          void since();
          poll = setInterval(() => void since(), FOLLOW_POLL);
          return;
        }
        source = new EventSource(`/api/events/stream?after=${newest}`);
        source.onopen = () => setLive(true);
        source.onerror = () => setLive(false);
        source.onmessage = (e) => {
          const event = JSON.parse(e.data) as RunEvent;
          setFeed((f) => merge(f ?? [], [{ kind: "event", event }]));
        };
        source.addEventListener("dropped", (e) => {
          const gap = JSON.parse((e as MessageEvent<string>).data) as Dropped;
          setFeed((f) => merge(f ?? [], [{ kind: "gap", gap }]));
        });
      })
      .catch(() => setFeed([]));
    return () => {
      stopped = true;
      source?.close();
      if (poll !== null) clearInterval(poll);
    };
  }, []);

  const loadOlder = async () => {
    if (cursor.current === null) return;
    setOlder(true);
    try {
      const r = await get<{ events: RunEvent[] }>(
        `/api/events?limit=${PAGE}&before=${cursor.current}`,
      );
      if (r.events.length < PAGE) setExhausted(true);
      cursor.current = r.events[r.events.length - 1]?.seq ?? cursor.current;
      setFeed((f) => merge(f ?? [], r.events.map((event) => ({ kind: "event", event }) as FeedRow)));
    } catch {
      // keep the button; the next click retries
    } finally {
      setOlder(false);
    }
  };

  if (feed === null) return <p className="muted">loading…</p>;

  const filters: Filters = { subjectKind, level, find };
  const shown = feed.filter((r) => r.kind === "gap" || matches(r.event, filters));
  const filtered = subjectKind !== "all" || level !== "all" || find.trim() !== "";

  return (
    <>
      <h1>
        Activity
        <span className="secondary"> · {live ? "live" : "not following"}</span>
      </h1>
      <Deciding />
      {feed.length === 0 ? (
        // a database nothing has happened in yet. no fake rows, and no bare
        // "no data": what to do about it is the useful half
        <div className="act-empty">
          <p>nothing has happened yet.</p>
          <p className="muted">
            this is every event in the deployment: a run queued, an asset materialized, a check
            that failed, a schedule that fired or was skipped, a sensor tick, a backfill's chunks,
            an alert nobody received, a lease taken back from a worker that stopped answering.
          </p>
          <p className="muted">
            launch something from a <Link to="/">job</Link>, or build an{" "}
            <Link to="/assets">asset</Link>, and it appears here as it happens.
          </p>
        </div>
      ) : (
        <>
          <div className="filter-row">
            <FilterGroup label="about" value={subjectKind} options={SUBJECTS} onPick={setSubjectKind} />
            <FilterGroup label="level" value={level} options={LEVELS} onPick={setLevel} />
            <span className="filter-group">
              <span className="filter-label">find</span>
              <input
                className="filter-input"
                value={find}
                placeholder="message, kind or subject"
                onChange={(e) => setFind(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape") setFind("");
                }}
              />
            </span>
          </div>
          {shown.length === 0 ? (
            <p className="muted">nothing matches the filter</p>
          ) : (
            <table className="act-table">
              <thead>
                <tr>
                  <th>when</th>
                  <th />
                  <th>about</th>
                  <th>subject</th>
                  <th>what</th>
                  <th>detail</th>
                </tr>
              </thead>
              <tbody>
                {shown.map((r) =>
                  r.kind === "gap" ? (
                    <GapRow key={`gap-${r.gap.through}`} gap={r.gap} />
                  ) : (
                    <EventRow key={r.event.seq} event={r.event} />
                  ),
                )}
              </tbody>
            </table>
          )}
          {/* the filters above are local to what has been fetched, so a filter
              that finds little is a reason to fetch more rather than a dead end */}
          {!exhausted && (
            <div className="load-more">
              <button className="text-btn" onClick={loadOlder} disabled={older}>
                {older ? "loading…" : "load older"}
              </button>
              {filtered && <span className="muted"> · filtering what has been loaded</span>}
            </div>
          )}
          <p className="muted act-foot">
            newest first, {feed.length} loaded. times are local;{" "}
            <span className="mono">{clockTime(new Date().toISOString())}</span> now.
          </p>
        </>
      )}
    </>
  );
}
