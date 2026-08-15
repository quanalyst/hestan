import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent, ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { get, post } from "./api";
import { useMay } from "./role";
import type { JobSummary, Run } from "./types";
import { relTime, shortId } from "./util";

interface Item {
  key: string;
  node: ReactNode;
  meta?: string;
  hay: string;
  perform: () => void;
}

export default function CommandPalette() {
  const mayPause = useMay("admin");
  const nav = useNavigate();
  const [open, setOpen] = useState(false);
  const [ready, setReady] = useState(false);
  const [q, setQ] = useState("");
  const [idx, setIdx] = useState(0);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (!open) return;
    setQ("");
    setIdx(0);
    setErr(null);
    setReady(false);
    Promise.allSettled([
      get<{ jobs: JobSummary[] }>("/api/jobs").then((r) => setJobs(r.jobs)),
      get<{ runs: Run[] }>("/api/runs?limit=50").then((r) => setRuns(r.runs)),
    ]).then(() => setReady(true));
  }, [open]);

  useEffect(() => setIdx(0), [q]);

  useEffect(() => {
    listRef.current?.querySelector('[data-active="true"]')?.scrollIntoView({ block: "nearest" });
  }, [idx, q]);

  if (!open) return null;

  const doSched = async (job: string, expr: string, paused: boolean) => {
    try {
      await post<{ ok: boolean }>("/api/schedules/state", { job, expr, paused });
      setOpen(false);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  };

  const jobItems: Item[] = jobs.map((j) => ({
    key: `j:${j.name}`,
    node: <span>{j.name}</span>,
    meta: j.description ?? undefined,
    hay: `${j.name} ${j.description ?? ""}`.toLowerCase(),
    perform: () => {
      setOpen(false);
      nav(`/jobs/${encodeURIComponent(j.name)}`);
    },
  }));

  const runItems: Item[] = runs.map((r) => ({
    key: `r:${r.id}`,
    node: (
      <span>
        <span className="mono">{shortId(r.id)}</span> {r.job}
      </span>
    ),
    meta: `${r.status} · ${relTime(r.created_at)}`,
    hay: `${r.id} ${r.job} ${r.status} ${r.trigger}`.toLowerCase(),
    perform: () => {
      setOpen(false);
      nav(`/runs/${r.id}`);
    },
  }));

  // pausing a schedule from the palette is the same decision it is on the job
  // page, so it is the same role, and an action a role may not take is not
  // offered rather than offered and refused
  const actionItems: Item[] = !mayPause
    ? []
    : jobs.flatMap((j) =>
    j.schedules.map((s) => {
      const verb = s.paused ? "resume" : "pause";
      return {
        key: `a:${j.name}:${s.expr}`,
        node: (
          <span>
            {verb} {j.name}
          </span>
        ),
        meta: s.expr,
        hay: `${verb} ${j.name} ${s.expr}`.toLowerCase(),
        perform: () => doSched(j.name, s.expr, !s.paused),
      };
      }),
    );

  const tokens = q.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const match = (it: Item) => tokens.every((t) => it.hay.includes(t));
  const groups = [
    { name: "jobs", items: jobItems.filter(match) },
    { name: "runs", items: runItems.filter(match) },
    { name: "actions", items: actionItems.filter(match) },
  ].filter((g) => g.items.length > 0);
  const flat = groups.flatMap((g) => g.items);
  const total = jobItems.length + runItems.length + actionItems.length;
  const sel = flat.length ? Math.min(idx, flat.length - 1) : 0;

  const onKeyDown = (e: ReactKeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      if (flat.length) setIdx((sel + 1) % flat.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (flat.length) setIdx((sel - 1 + flat.length) % flat.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      flat[sel]?.perform();
    } else if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      setOpen(false);
    } else if (e.key === "Tab") {
      // focus stays trapped on the input while the palette is open
      e.preventDefault();
    }
  };

  return (
    <div
      className="palette-backdrop"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) setOpen(false);
      }}
    >
      <div
        className="palette"
        role="dialog"
        aria-modal="true"
        onKeyDown={onKeyDown}
        onMouseDown={(e) => {
          // blurring the input would kill the palette's key handling
          if (!(e.target instanceof HTMLInputElement)) e.preventDefault();
        }}
      >
        <input
          className="palette-input"
          placeholder="jobs · runs · schedules"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          autoFocus
        />
        <div className="palette-list" ref={listRef}>
          {groups.map((g) => (
            <div key={g.name}>
              <div className="palette-group">{g.name}</div>
              {g.items.map((it) => {
                const active = flat.indexOf(it) === sel;
                return (
                  <div
                    key={it.key}
                    className={active ? "palette-item active" : "palette-item"}
                    data-active={active || undefined}
                    onMouseEnter={() => setIdx(flat.indexOf(it))}
                    onClick={it.perform}
                  >
                    {it.node}
                    {it.meta && <span className="palette-meta">{it.meta}</span>}
                  </div>
                );
              })}
            </div>
          ))}
          {flat.length === 0 && ready && (
            <div className="palette-empty muted">{total === 0 ? "no jobs or runs yet" : "no matches"}</div>
          )}
        </div>
        {err && <div className="palette-err muted">{err}</div>}
      </div>
    </div>
  );
}
