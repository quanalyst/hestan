# Backup and recovery

hestan's whole state is one store: a sqlite file or a postgres database. so a
backup is a copy of that one thing, and there is very little to it.

what there *is* to it is the other half of this page: a restored store is a
statement about a moment that has passed. the rows in it were true when the
copy was taken, and every claim, lease and `running` status in them describes
processes that are somewhere else. that is what has to be dealt with before a
deployment comes up on one.

## Taking a copy

### sqlite

```
hestan backup /backups/hestan-2026-08-24.db
```

or, from a program, `Store::backup_to(path)`.

**not `cp`.** a hestan sqlite database runs in WAL mode, so `hestan.db` on its
own is missing whatever is still in `hestan.db-wal`, and a copy of the two
taken a moment apart is missing the difference. neither says so; you find out
when you open the copy and last night's runs are not in it.

`hestan backup` runs sqlite's `VACUUM INTO`, which takes a read transaction and
writes a whole database out of it. one instant's worth of rows, taken while
runs go on being recorded, and no lock a writer waits on for longer than a
statement. it writes to `<dest>.part` and renames onto `<dest>`, so a file that
appears under the name you asked for is a finished copy, and it refuses a
destination that already exists rather than writing over last night's.

it also **marks the copy** as one; see [coming up on a
copy](#coming-up-on-a-copy).

### postgres

hestan does not copy a postgres database and does not pretend to. `hestan
backup` against a `postgres://` store says so and names what to use instead:

```
pg_dump --format=custom hestan > /backups/hestan-2026-08-24.dump
```

`pg_dump` runs in one repeatable-read transaction, so what it writes is one
instant's worth of every table, which is exactly what a run log needs. a base
backup (`pg_basebackup`, or whatever your provider calls a snapshot) is equally
good and gives you point-in-time recovery besides.

what such a dump **must not leave out**:

- **every table hestan uses.** `pg_dump -t runs` is not a backup of a run log.
  a `runs` row without its `op_runs` rows is a run hestan reports as having no
  ops; a materialization without the run that wrote it points at a value
  nothing can resolve. the consistency here is across tables, so the dump has
  to be across tables.
- **`schema_version`.** it is one row and it is what tells hestan which
  migrations a database has had. restore without it and the next `Store::open`
  creates the schema from scratch beside the tables that are already there,
  and fails.
- **the schema, if hestan is not in `public`.** a url carrying
  `options=-c search_path=...` puts every table in that schema, and a dump of
  `public` will be empty.

nothing marks a `pg_dump`, so after restoring one, run [`hestan
resettle`](#resettle) yourself before anything else starts.

## What a copy does not contain

**an [io manager](io-managers.md)'s files.** this is the one worth reading
twice, because it is the ordinary case rather than a corner.

with `FileIo` or `ParquetIo` configured, an op's output is a file on disk and
what the run log holds is a *handle* to it:

```json
{"$io": "parquet", "path": "/var/lib/hestan/parquet/019.../orders.parquet",
 "rows": 41210, "bytes": 88213}
```

the store contains that object. it does not contain the parquet file, because
the file was never in the store. so a restored run log can hold a
materialization that says `sales/orders` is built, with a fingerprint and a row
count and a link to the run that built it, and the value it names is not on
this machine at all.

**nothing notices.** [staleness](assets.md) is decided on fingerprints, so the
asset reads as fresh and nothing rebuilds it. the api says it is materialized,
the catalog draws it green, and the gap surfaces only when something tries to
read the value:

- a [replay](replay.md) is refused before it launches, with
  `Error::ReplayInput` naming the dep, because a replay resolves the inputs up
  front.
- a [resume](concepts.md#resume) and a downstream asset build seed the
  **handle** rather than the value, so they launch and then fail at the op that
  reads it, with what the manager said as the op's error.

so **back the io directory up beside the store, from the same instant**, or
accept that a restore is followed by rebuilding the assets whose values lived
out there. `hestan doctor` counts the runs held back from retention because an
asset's current value is what they wrote, under `values`, and that is the list
to go through.

the other thing a copy does not contain is anything your ops wrote anywhere
else. restoring a run log does not un-write a row in your warehouse, delete an
object you put in s3 or recall an email. the run log is a record of what
happened; putting an older one back does not make less of it have happened.

## What a restored store is

every row was true at one instant. what has not aged well:

| in the copy | what it means now |
| --- | --- |
| a run with status `running` | the process executing it ended, however long ago |
| `runs.claimed_by`, `claimed_at`, `lease_until` | an instance id that is not executing anything against this database, and a lease with time left on it that nobody is renewing |
| the [deciding lease](scaling.md#the-deciding-lease) | a holder that cannot renew it, so nothing new decides until it expires |
| `decider.term` | a counter that has gone **backwards**: terms this database has already handed out will be handed out again |
| `schedule_ticks` | missing every occurrence fired after the copy was taken, so [catch-up](scheduling.md#missed-fire-catch-up) may fire them again |
| `sensor_run_keys` | missing every key claimed after the copy was taken, so a sensor may launch those again |

the last three are not things hestan can fix, and it is worth being plain about
why. the store is the only record there is. an occurrence that fired after the
copy was taken left its trace in the copy's source and nowhere else, so nothing
in the restored database knows it happened. a schedule with
`Catchup::All` will fire it again and the job will run twice; one with
`Catchup::Skip` (the default) will not.

the term going backwards is the reason the [hazard](#the-hazard) below is a
hazard rather than an inconvenience. a term fences a decision: a write that
names a term the store has moved past is refused. after a restore the store has
*not* moved past terms it already issued, so a process still holding term 12
from before the copy would be accepted by a restored store that is about to
issue term 12 again. nothing detects that. stopping every process first is what
prevents it.

## Coming up on a copy

`hestan backup` writes a mark into the copy, and a deployment **refuses to
start** on a database carrying an unsettled one:

```
error: this run log is a copy (taken at 2026-08-24 02:00:00 UTC from
/var/lib/hestan/hestan.db) and nothing has resettled it. the claims in it are
held by processes that are not executing against this database and its deciding
lease names a holder that cannot renew it. stop everything still writing to the
store this was copied from, then run `hestan resettle`. see docs/backup.md
```

reads still work, which is deliberate: pointing a report, an export or a `hestan
runs --db copy.db` at a copy is the ordinary reason to have one, and none of
that acts on a claim.

**only a copy hestan took carries the mark.** a `cp`, an lvm snapshot and a
`pg_restore` into an empty database all produce a database identical to the one
they came from, and nothing inside it can know it is the second copy of itself.
after one of those, hestan will come up and treat the copy as the live store.
so: after any restore that was not `hestan backup`, run `hestan resettle`
yourself. it works on any run log, marked or not.

### resettle

```
hestan resettle --db /var/lib/hestan/hestan.db
```

it hands back what the copy claims, because nothing in the copy can be
executing anything:

| in the copy | after |
| --- | --- |
| `running` | **failed**, with the reason on the run and on its ops |
| `queued` and claimed | back on the queue, claim cleared |
| `queued` and unclaimed | untouched: that is the queue, and it survives |
| the deciding lease | cleared, so the next process takes it now |
| `decider.term` | left where it is |

two of those are choices worth defending.

**the lease is not consulted, and that is the whole difference from boot
recovery.** hestan already fails runs whose claimer stopped renewing; that runs
at every start and it believes leases on purpose, because on a live database a
claim with thirty seconds left on it is a run some other process is executing
right now. on a restored database there is no such process, whatever the lease
says, so the lease is exactly the thing that must not be believed.

**nothing is requeued for a run that was `running`.** it may well have finished
in the original after the copy was taken. re-running it would be hestan
deciding on its own to do somebody's work twice, which for a deploy job or a
payment run is not a small thing. so those are failed and left for a person:
`hestan resume <id>` re-runs what did not succeed, `hestan replay <id>` re-runs
what did.

the term is left alone for the reason in the table above: there is no honest
number to move it to.

### The hazard

**restoring an old copy while workers are still running against the live store
is how a recovery makes things worse.** two processes writing runs into two
different databases that each think they are the run log, or, on sqlite, two
processes writing into two different inodes that used to be the same file.

**nothing in hestan prevents it, and nothing in hestan can.** by the time any
hestan code runs against the restored store, the restore has already happened:
the file is in place, or the dump is loaded. there is no point before that at
which hestan is consulted.

what the operator has to do, in this order:

1. **stop every hestan process on this store.** every scheduler, every worker,
   every api process, on every host. not "the ones that look busy": a worker
   with an open connection and nothing to do is still going to claim the next
   run it sees.
2. on postgres, stop them *connecting*: `REVOKE CONNECT ON DATABASE hestan FROM
   hestan;` then `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE
   datname = 'hestan';`. a supervisor that restarts what you killed is not a
   theoretical problem.
3. on sqlite, be aware of what "replace the file" means to a process that has
   it open. `mv copy.db hestan.db` leaves any running worker writing happily
   into the old inode, which now has no name: those writes are lost and nothing
   reports it. `cp copy.db hestan.db` over an open database is worse, because
   it writes into the file the running processes are reading, and their WAL no
   longer matches it. stop them first, then move both `hestan.db-wal` and
   `hestan.db-shm` out of the way with the file.
4. restore.
5. `hestan resettle`.
6. start one process, check `hestan doctor`, then start the rest.

`hestan resettle` is a second pair of eyes on step 1, and it is worth knowing
exactly how much it can see. before it writes anything it reads every lease in
the database, waits twenty seconds, and reads them again. a process executing a
run renews its run leases every fifteen seconds and a process running an
election renews the deciding lease every two, so anything that is *doing*
something moves one of those. if something moved, it refuses:

```
error: a lease moved while this was watching, so something is running against
this database right now. resettling it now would take runs away from a process
that is executing them. stop every hestan process on this store and run this
again
```

**what that does not see**, plainly: a process that holds no run and runs no
election renews nothing and writes nothing on a timer. an idle worker is
invisible to it, and so is an api process that is only serving reads. the watch
catches the busy case, which is the one where resettling would do immediate
damage; it is not a lock and it is not a guarantee. `--watch 0` skips it, which
is for a machine you have just booted into single user mode and not for a
Tuesday afternoon.

## After a restore

`hestan doctor` reports the copy for as long as the database lives:

```
note  restored   this run log is a copy (taken at 2026-08-24 02:00:00 UTC
                 from /var/lib/hestan/hestan.db), resettled at 2026-08-24 09:14:02 UTC
```

and before it is resettled the same line is `wrong`, which makes `doctor` exit
7. a check in a deploy pipeline catches a restored database before it serves
anything.

then, in rough order of how much they hurt:

- **the assets whose values lived in an io manager.** `hestan doctor` counts
  them under `values`. rebuild them, or restore the io directory.
- **the window between the copy and the restore.** whatever ran in it is not in
  the run log and hestan cannot know it happened. a schedule with
  [catch-up](scheduling.md#missed-fire-catch-up) will re-fire the occurrences it
  cannot see; a keyed [sensor](sensors.md#run-keys) will re-launch the keys it
  cannot see. if either of those is expensive or not idempotent, pause them
  before starting the deployment and let somebody decide.
- **the failed runs.** everything that was `running` is failed with a reason
  naming the restore. `hestan runs --limit 100` and resume the ones that should
  finish.

## Testing a backup

the only backup worth having is one that has been restored. a copy is a
complete run log, so:

```
hestan resettle --db copy.db --watch 0
hestan doctor --db copy.db
hestan runs --db copy.db --limit 20
```

three commands, on a copy, on any machine. if `doctor` is happy and the runs
you expect are there, the copy is good.
