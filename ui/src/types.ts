export type RunStatus = "queued" | "running" | "success" | "failed" | "canceled";
export type OpStatus = "pending" | "running" | "success" | "failed" | "skipped" | "canceled";
export type Trigger = "manual" | "schedule" | "retry" | "resume" | "build" | "sensor";
export type EventLevel = "info" | "warn" | "error";

export type EventKind =
  | "run_queued"
  | "run_started"
  | "run_success"
  | "run_failed"
  | "run_canceled"
  | "op_started"
  | "op_expanded"
  | "op_retry"
  | "op_success"
  | "op_failed"
  | "op_skipped"
  | "op_canceled"
  | "type_check_failed"
  | "log";

export type TickOutcome = "fired" | "error" | "skipped" | "deferred";

// narrower than TickOutcome: skipped/deferred are schedule ideas, not sensor ones
export type SensorOutcome = "fired" | "error";

export interface OpSummary {
  name: string;
  deps: string[];
  retries: number;
  timeout_secs: number | null;
  pool: string | null;
  // the dep this op fans out over, one instance per array element; null for
  // every ordinary op
  mapped_over: string | null;
  input_type: string | null;
  output_type: string | null;
  params_type: string | null;
}

// a named concurrency limit shared process-wide, not per job
export interface JobPool {
  name: string;
  limit: number | null;
}

export interface JobSchedule {
  expr: string;
  tz: string;
  paused: boolean;
  // what every fire of this schedule launches with, {} unless declared
  params: unknown;
  next_fire: string | null;
}

export interface JobSummary {
  name: string;
  description: string | null;
  ops: OpSummary[];
  schedules: JobSchedule[];
  last_run: Run | null;
  max_parallel: number | null;
  pools: JobPool[];
  overlap: "allow" | "skip" | "queue";
  overdue: boolean;
}

export interface UpcomingSchedule {
  job: string;
  expr: string;
  times: string[];
}

export interface Run {
  id: string;
  job: string;
  status: RunStatus;
  trigger: Trigger;
  params: unknown;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  // the first op that terminally failed, named; null unless the run failed
  error: string | null;
  resumed_from: string | null;
}

// what a resume would do: ops it executes, ops it seeds from a recorded output
export interface ResumePreview {
  rerun: string[];
  reuse: string[];
}

export interface OpRun {
  run_id: string;
  op: string;
  status: OpStatus;
  attempts: number;
  started_at: string | null;
  finished_at: string | null;
  output: unknown;
  error: string | null;
}

export interface RunEvent {
  seq: number;
  run_id: string;
  op: string | null;
  level: EventLevel;
  kind: EventKind;
  message: string;
  data: unknown;
  ts: string;
}

export interface OpStatSample {
  run_id: string;
  status: OpStatus;
  ms: number | null;
}

export interface OpStat {
  op: string;
  runs: number;
  failures: number;
  avg_ms: number | null;
  p95_ms: number | null;
  last_error: string | null;
  recent: OpStatSample[];
}

export interface JobState {
  op: string;
  value: unknown;
  updated_at: string;
}

export interface Tick {
  id: number;
  job: string;
  expr: string;
  scheduled_for: string;
  fired_at: string;
  outcome: TickOutcome;
  run_id: string | null;
  error: string | null;
}

export interface StaleReason {
  dep: string;
  had: string | null; // fingerprint recorded when this asset last consumed dep
  now: string | null;
}

export interface AssetSummary {
  name: string;
  kind: "source" | "derived";
  deps: string[];
  auto: boolean;
  fingerprint: string | null;
  built_at: string | null;
  run_id: string | null;
  stale: boolean;
  reasons: StaleReason[];
}

export interface SensorTick {
  id: number;
  sensor: string;
  evaluated_at: string;
  outcome: SensorOutcome;
  launched: number;
  error: string | null;
}

export interface SensorSummary {
  name: string;
  every_secs: number;
  paused: boolean;
  cursor: unknown;
  last_tick: SensorTick | null;
}
