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

// an op's trigger rule: what its deps must have done for it to run at all
export type When = "all_succeeded" | "any_failed" | "always";

export interface OpSummary {
  name: string;
  deps: string[];
  when: When;
  // process-wide resources this op declared with Op::requires
  requires: string[];
  retries: number;
  timeout_secs: number | null;
  pool: string | null;
  // the named io manager this op's output is persisted through; null is the
  // process default
  io: string | null;
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

// a typed fact an op reported with ctx.meta, stored tagged by its type so
// nothing downstream has to guess how to show it
export type MetaValue =
  | { int: number }
  | { float: number }
  | { text: string }
  | { url: string }
  | { markdown: string }
  | { json: unknown };

export type Metadata = Record<string, MetaValue>;

export interface OpRun {
  run_id: string;
  op: string;
  status: OpStatus;
  attempts: number;
  started_at: string | null;
  finished_at: string | null;
  output: unknown;
  // what the attempt that succeeded reported; null when it reported nothing
  metadata: Metadata | null;
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

export type CheckStatus = "passed" | "failed";
// what a failing check costs: error fails its op and the run, warn records it
export type Severity = "warn" | "error";

// counted from the latest result per check name; both zero means no check has
// ever recorded anything for this asset
export interface CheckSummary {
  passed: number;
  failed: number;
  last_run_at: string | null;
}

export interface AssetCheckResult {
  id: number;
  asset: string;
  partition: string | null;
  check: string;
  run_id: string;
  status: CheckStatus;
  severity: Severity;
  message: string | null;
  metadata: Metadata | null;
  checked_at: string;
}

export interface AssetSummary {
  name: string;
  kind: "source" | "derived";
  deps: string[];
  auto: boolean;
  // the op that materializes it: the asset's own name, unless a multi-asset
  // produces it alongside others. null for a source, which has no op
  op: string | null;
  // the shape of the key set, for a partitioned asset; null for every other
  // one, which has a single fingerprint instead. the three states are disjoint
  // and sum to total
  partitions: PartitionCounts | null;
  fingerprint: string | null;
  built_at: string | null;
  run_id: string | null;
  stale: boolean;
  reasons: StaleReason[];
  checks: CheckSummary;
}

export interface PartitionCounts {
  total: number;
  materialized: number;
  stale: number;
  missing: number;
}

export type PartitionState = "materialized" | "stale" | "missing";

// one key of a partitioned asset, newest first from the api
export interface PartitionEntry {
  key: string;
  state: PartitionState;
  fingerprint: string | null;
  built_at: string | null;
  run_id: string | null;
}

// one entry of an asset's materialization history, newest first from the api
export interface MaterializationEntry {
  id: number;
  // the key this entry is for, on a partitioned asset
  partition: string | null;
  fingerprint: string;
  // this build's fingerprint differs from the one before it in time — the
  // difference between having been rebuilt and having actually changed
  changed: boolean;
  inputs: Record<string, string | null>;
  run_id: string | null;
  built_at: string;
  // what the op that built it reported, the same map as its op run's
  metadata: Metadata | null;
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
