export type RunStatus = "queued" | "running" | "success" | "failed" | "canceled";
export type OpStatus = "pending" | "running" | "success" | "failed" | "skipped" | "canceled";
export type Trigger = "manual" | "schedule" | "retry" | "resume" | "replay" | "build" | "sensor";
export type EventLevel = "info" | "warn" | "error";

// what happened. the string is open, not closed: a build newer than this ui
// writes kinds it has never heard of, and the api hands them through rather
// than refusing the row, so the union documents what is known and the trailing
// `(string & {})` is what keeps an unknown one from being a type error
export type EventKind =
  | "run_queued"
  | "run_started"
  | "run_success"
  | "run_failed"
  | "run_canceled"
  | "run_reclaimed"
  | "op_started"
  | "op_expanded"
  | "op_retry"
  | "op_success"
  | "op_failed"
  | "op_skipped"
  | "op_canceled"
  | "type_check_failed"
  | "asset_materialized"
  | "policy_launched"
  | "check_passed"
  | "check_failed"
  | "schedule_fired"
  | "schedule_caught_up"
  | "schedule_skipped"
  | "schedule_deferred"
  | "schedule_error"
  | "sensor_tick"
  | "backfill_started"
  | "backfill_chunk"
  | "backfill_finished"
  | "backfill_canceled"
  | "schedule_paused"
  | "sensor_paused"
  | "notification_delivered"
  | "notification_failed"
  | "retention_pruned"
  | "log"
  | (string & {});

// which of hestan's tables an event is about
export type SubjectKind =
  | "run"
  | "job"
  | "asset"
  | "schedule"
  | "sensor"
  | "backfill"
  | "system"
  | (string & {});

export type TickOutcome = "fired" | "error" | "skipped" | "deferred";

// narrower than TickOutcome: deferred is a schedule idea. skipped is a turn
// the loop did not take, because the previous evaluation was still running
export type SensorOutcome = "fired" | "error" | "skipped";

// an op's trigger rule: what its deps must have done for it to run at all
export type When = "all_succeeded" | "any_failed" | "always";

// one field of a params schema. json schema says far more than this; these are
// the keys the launchpad's legend reads, and everything else is passed through
export interface SchemaField {
  type?: string | string[];
  description?: string;
  $ref?: string;
  enum?: unknown[];
}

// a json schema for the params editor to read. a legend, never a validator:
// what a launch is actually judged against is the ops' declared params types
export interface ParamsSchema {
  properties?: Record<string, SchemaField>;
  required?: string[];
}

export interface OpSummary {
  name: string;
  deps: string[];
  when: When;
  // resources this op declared with Op::requires, of either scope
  requires: string[];
  retries: number;
  timeout_secs: number | null;
  pool: string | null;
  rate: string | null;
  // the named io manager this op's output is persisted through; null is the
  // process default
  io: string | null;
  // the dep this op fans out over, one instance per array element; null for
  // every ordinary op
  mapped_over: string | null;
  // whether this op's body runs in a child process of its own, and what that
  // process is allowed to spend; both limits are null unless declared
  isolated: boolean;
  memory_limit_bytes: number | null;
  cpu_limit_secs: number | null;
  input_type: string | null;
  output_type: string | null;
  params_type: string | null;
  // what this op declared with Op::params_schema, verbatim; null for most ops
  params_schema: ParamsSchema | null;
}

// a named concurrency limit shared process-wide, not per job
export interface JobPool {
  name: string;
  limit: number | null;
}

// a named rate shared process-wide: n calls per period, as it was declared
export interface JobRate {
  name: string;
  limit: number | null;
  per_secs: number | null;
}

// one rate from GET /api/rates: the declaration plus how many ops are queued
// for a token in the process that answered, which is the only place the bucket
// exists
export interface RateView {
  name: string;
  limit: number;
  per_secs: number;
  waiting: number;
}

// what a schedule does about occurrences that came due while nothing was
// running: "skip" (the default), "one", or "all:<limit>"
export type Catchup = string;

export interface JobSchedule {
  expr: string;
  tz: string;
  paused: boolean;
  // what every fire of this schedule launches with, {} unless declared
  params: unknown;
  catchup: Catchup;
  // the newest occurrence the scheduler has accounted for; null until it has
  // seen the schedule once
  cursor: string | null;
  next_fire: string | null;
}

// a declared freshness policy's verdict: fresh inside the window, late past
// it, never when nothing has ever succeeded. null when nothing was declared
export interface Freshness {
  status: "fresh" | "late" | "never";
  // the window that was declared. how far a fresh one is into it cannot be
  // derived from late_by_secs, which is null exactly while it is inside it
  within_secs: number;
  late_by_secs: number | null;
  last_success: string | null;
}

// one currently-late thing from GET /api/late, in the shape on_late hands its
// hooks
export interface LateEntry {
  kind: "job" | "asset";
  name: string;
  late_by_secs: number | null;
  last_success: string | null;
}

export interface JobSummary {
  name: string;
  description: string | null;
  ops: OpSummary[];
  // every op's schema merged into one; null when no op declared any
  params_schema: ParamsSchema | null;
  schedules: JobSchedule[];
  last_run: Run | null;
  max_parallel: number | null;
  pools: JobPool[];
  rates: JobRate[];
  overlap: "allow" | "skip" | "queue";
  // the cron-derived heuristic, and always false once a policy is declared:
  // freshness is the answer then
  overdue: boolean;
  freshness: Freshness | null;
}

// a named parameter set stored against a job: declared with Hestan::preset or
// saved from the launchpad, and the same thing either way
export interface Preset {
  job: string;
  name: string;
  params: unknown;
  created_at: string;
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
  // the run this one replayed: re-ran ops of, on the inputs that run gave
  // them. null on every run that is not a replay, and never set beside
  // resumed_from: a resume re-runs what did not succeed and a replay re-runs
  // what did
  replay_of: string | null;
  // the cron occurrence this run stands for, not the clock it launched at;
  // null on a manual launch, retry, resume, replay, build or sensor fire
  scheduled_for: string | null;
  // flat key:value marks on the run: set at launch, defaulted process-wide,
  // and set automatically on machine-made runs. {} on an untagged run
  tags: Record<string, string>;
  // where this run sits in the queue: higher goes first, ties by created_at
  priority: number;
  // the instance executing this run; null on one nobody has claimed, which is
  // what a queued run is
  claimed_by: string | null;
  claimed_at: string | null;
  lease_until: string | null;
  // who asked for this run, where a person did and something checked who they
  // were. null on everything a schedule, a sensor or a backfill launched, and
  // on every launch through a deployment with no authentication
  actor: string | null;
}

// why a queued run is not executing: which limit, and the sentence to show
export interface Blocker {
  scope: "global" | "job" | "tag" | "undefined";
  reason: string;
}

export interface QueueEntry {
  run: Run;
  // 1 for the head of the queue
  position: number;
  // null on one the next dispatch pass starts
  blocked_by: Blocker | null;
}

export interface QueueView {
  // every unclaimed queued run, not just the page below
  depth: number;
  queued: QueueEntry[];
  limits: {
    global: number | null;
    jobs: { job: string; limit: number }[];
    tags: { key: string; value: string; limit: number }[];
  };
}

// what a resume would do: ops it executes, ops it seeds from a recorded output
export interface ResumePreview {
  rerun: string[];
  reuse: string[];
}

// what a replay would do: the ops it executes (exactly those, nothing
// downstream) and the deps it seeds from what the original run recorded
export interface ReplayPreview {
  ops: string[];
  inputs: string[];
}

// one column of a metadata table: its name, and the type the op named when it
// knew one: a label to print, never anything the ui parses
export interface MetaColumn {
  name: string;
  type: string | null;
}

// a sample of rows an op reported. rectangular by construction: the source
// pads and trims every row to the column count
export interface MetaTable {
  columns: MetaColumn[];
  rows: unknown[][];
  // rows were dropped to fit the cap where it was built, so a full table and
  // the head of a much larger one read differently
  truncated: boolean;
}

// a typed fact an op reported with ctx.meta, stored tagged by its type so
// nothing downstream has to guess how to show it. bytes, duration_secs and
// count are display types over one number: the same integer an `int` carries,
// with the unit it is in
export type MetaValue =
  | { int: number }
  | { float: number }
  | { text: string }
  | { url: string }
  | { markdown: string }
  | { json: unknown }
  | { table: MetaTable }
  | { bytes: number }
  | { duration_secs: number }
  | { count: number }
  | { path: string }
  // a run id and an asset name of this deployment, which the ui links to
  | { run: string }
  | { asset: string };

export type Metadata = Record<string, MetaValue>;

// what a numeric metadata value did since the build before it: the absolute
// change, and the percentage when the previous value was big enough for one
// to mean anything (null under 100). computed server-side, so a row never
// costs a request to render
export interface MetaDelta {
  delta: number;
  delta_pct: number | null;
}

// keyed by metadata name. a key that is new, gone, or was something other
// than a number last time is simply absent: no delta rather than a fake zero
export type Deltas = Record<string, MetaDelta>;

// one point of a numeric metadata key's trend, oldest first from the api.
// run_id is null on a materialization a probe wrote outside any run
export interface MetaPoint {
  at: string;
  value: number;
  run_id: string | null;
}

// keyed by metadata name, for the keys a trend was fetched for
export type Trends = Record<string, MetaPoint[]>;

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
  // what moved since the previous run of this job's op
  deltas: Deltas;
  error: string | null;
  // the child process an isolated op is running in right now; null once it has
  // finished, and for every op that runs in the orchestrator itself
  pid: number | null;
}

export interface RunEvent {
  seq: number;
  // the run this is about; null on everything that is not about a run
  run_id: string | null;
  subject_kind: SubjectKind;
  // which one, by name or id. null on a run event, where the run is run_id
  subject: string | null;
  op: string | null;
  level: EventLevel;
  kind: EventKind;
  message: string;
  data: unknown;
  ts: string;
  // who caused this, on the events a person caused; null everywhere else
  actor: string | null;
}

export type LogStream = "stdout" | "stderr";

// one line an op produced, as opposed to one hestan wrote about it. a
// subprocess's pipe fills `stream`; a captured tracing event fills `level` and
// `target`; a line hestan wrote about the capture itself carries the target
// `hestan` and no stream.
export interface OpLog {
  id: number;
  run_id: string;
  op: string;
  attempt: number;
  at: string;
  stream: LogStream | null;
  level: EventLevel | null;
  target: string | null;
  message: string;
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
  // the newest facts this op reported in the window; null if it reported none
  metadata: Metadata | null;
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
  // which key of the dep, when it is read through a mapping that reads one
  // other than this asset's own: the hour under a daily rollup, not the day.
  // null under identity, where the key is the reader's own
  partition: string | null;
  had: string | null; // fingerprint recorded when this asset last consumed dep
  now: string | null;
}

// how one dep's keys are read, for the deps that are read at anything but the
// same key. `mapping` is what the api calls the shape: all, covering, offset -1
export interface DepMapping {
  dep: string;
  mapping: string;
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

// when hestan rebuilds an asset by itself, and what it is waiting for if it
// wants a build it cannot have yet
export interface AssetPolicy {
  rule: "stale" | "missing" | "cron";
  // the expression and the clock it is read on, both null on a rule that reads
  // no clock
  cron: string | null;
  tz: string | null;
  upstream_ready: boolean;
  // what the policy says, in one line
  says: string;
  waiting: PolicyWait | null;
}

export interface PolicyWait {
  // the newest key that is waiting; null on an unpartitioned asset
  key: string | null;
  // what it is waiting on, as `dep[key]`
  for: string;
  // how many of its keys are in the same position
  keys: number;
}

export interface AssetSummary {
  name: string;
  // where it belongs: what it declared, else the part of the name before the
  // first "/", else null for an asset in no group at all
  group: string | null;
  // and the angle that group is drawn at; null wherever the group is
  group_hue: number | null;
  // where it came from: the source groups it descends from, sorted by name.
  // a source's own origin is itself. empty is a real answer and means no
  // source is upstream of it
  provenance: Origin[];
  kind: "source" | "derived";
  deps: string[];
  // whether hestan rebuilds this one itself, which is what a policy says
  auto: boolean;
  policy: AssetPolicy | null;
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
  // the deps this asset reads at anything but the same key; empty when every
  // one of them is identity, which is every dep that declared nothing
  mappings: DepMapping[];
  checks: CheckSummary;
  // stale and late are different claims: stale means a dep moved, late means
  // time passed. null unless a policy was declared
  freshness: Freshness | null;
}

// one source group an asset descends from, and the hue the server picked for
// it. a hue rather than a colour: the lightness that is legible depends on the
// theme, which is this end's business
export interface Origin {
  name: string;
  hue: number;
}

export interface PartitionCounts {
  total: number;
  materialized: number;
  stale: number;
  missing: number;
}

export type PartitionState = "materialized" | "stale" | "missing";

export type BackfillStatus = "running" | "complete" | "failed" | "canceled";

// a recorded request to materialize a range of one asset's partitions,
// launched in chunks so a long range never fires every partition at once
export interface Backfill {
  id: number;
  asset: string;
  from_key: string;
  to_key: string;
  partitions: string[];
  // one per chunk launched, oldest first
  run_ids: string[];
  total: number;
  launched: number;
  created_at: string;
  finished_at: string | null;
  status: BackfillStatus;
}

// one key of a partitioned asset, newest first from the api
export interface PartitionEntry {
  key: string;
  state: PartitionState;
  fingerprint: string | null;
  built_at: string | null;
  run_id: string | null;
  // what this key reads of each dep it maps, and why it is stale, per key,
  // because a mapping resolves per key
  reads: KeyRead[];
  reasons: StaleReason[];
  // what this key's policy is waiting for, as `dep[key]`; null when it is
  // waiting for nothing, and on every asset that declared no policy
  waiting: string | null;
}

// the dep keys one partition reads under one mapping: how many, and the ends
// of the range they run between
export interface KeyRead {
  dep: string;
  mapping: string;
  count: number;
  first: string | null;
  last: string | null;
  // keys it promised that the dep does not hold, which is what makes a key
  // unbuildable rather than merely unbuilt
  missing: number;
}

// what a build recorded of one dep: its fingerprint, or one per key of it
export type InputFingerprint = string | null | Record<string, string | null>;

// one entry of an asset's materialization history, newest first from the api
export interface MaterializationEntry {
  id: number;
  // the key this entry is for, on a partitioned asset
  partition: string | null;
  fingerprint: string;
  // this build's fingerprint differs from the one before it in time: the
  // difference between having been rebuilt and having actually changed
  changed: boolean;
  // one fingerprint per dep, or (for a dep read through a mapping that names
  // a set of its keys) one per key it consumed
  inputs: Record<string, InputFingerprint>;
  run_id: string | null;
  built_at: string;
  // what the op that built it reported, the same map as its op run's
  metadata: Metadata | null;
  // what moved since the previous build of this asset (and this partition)
  deltas: Deltas;
}

export interface SensorTick {
  id: number;
  sensor: string;
  evaluated_at: string;
  outcome: SensorOutcome;
  launched: number;
  // requests whose run key was already claimed, so they were not launched again
  skipped: number;
  // how long the evaluation took; 0 on a skipped tick, which never ran
  duration_ms: number;
  error: string | null;
}

// what a run-status sensor watches; null for a user sensor or a probe, which
// watch whatever their closure looks at
export interface SensorFilter {
  job: string | null;
  statuses: RunStatus[];
}

export interface SensorSummary {
  name: string;
  every_secs: number;
  paused: boolean;
  cursor: unknown;
  filter: SensorFilter | null;
  // when the loop next evaluates it: further out than every_secs while it is
  // backing off, which is what consecutive_failures explains
  next_eval: string;
  consecutive_failures: number;
  last_tick: SensorTick | null;
}

// where one durable notification got to. pending is undelivered and due
// again; failed is given up on after its attempts ran out, and is the state
// somebody has to look at
export type DeliveryState = "pending" | "failed" | "delivered";

export interface Notification {
  id: number;
  // which event shape the payload holds; "run" today
  kind: string;
  payload: { run_id?: string; job?: string; status?: RunStatus };
  created_at: string;
  attempts: number;
  next_attempt_at: string | null;
  delivered_at: string | null;
  last_error: string | null;
  state: DeliveryState;
}

// who is doing the deciding in this deployment, off `GET /api/health`.
//
// schedules fire, sensors evaluate and policies build on exactly one process
// at a time, and every other process is doing nothing about them on purpose.
// so "nothing has fired" is a question about the deciding process rather than
// about whichever one this browser happens to be talking to.
export interface Deciding {
  // whether the process serving this page is the one deciding
  leader: boolean;
  // and which one is, as the store has it. null when nothing holds the lease
  holder: string | null;
  // the term the holder is on; it moves on every handover
  term: number;
  // how long the holder has before anybody may take it; null when nothing
  // holds it
  lease_secs: number | null;
  // whether this process would ever take it. a worker never does
  decides: boolean;
}

export interface Health {
  ok: boolean;
  instance: string;
  holding: string[];
  // null when the process could not read its own lease
  deciding: Deciding | null;
}
