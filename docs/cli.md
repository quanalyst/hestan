# The command line

your jobs are compiled into your binary. so is the command line over them:

```rust
#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    let app = Hestan::new().job(orders).schedule("orders", "0 2 * * *");
    hestan::cli::run(app, ([127, 0, 0, 1], 4000)).await
}
```

that is the whole mount. **with no arguments it serves**, on exactly the
address it was handed (the same call `app.serve(addr)` was, with the same
behaviour and the same error if the socket will not bind), so a deployment that
swaps one line for the other and changes nothing else behaves as it did.

with arguments it is a command line that already knows every job, asset,
schedule and sensor by name. there is no workspace file to point it at, no
module to import, no server to be running: the registry is in the process, so
starting is opening a database and nothing else. everything below follows from
that one fact, including the two things that are usually out of reach:
`explain`, which resolves a real plan, and shell completion of your own job and
asset names, asked for at the moment you press tab.

```
$ orders run orders_etl --wait
14:22:01 run started
14:22:01 fetch_orders starting
14:22:02 fetch_orders fetched 1,204 rows
14:22:02 validate starting
14:22:02 validate dropping bad row: {"id":3}
14:22:03 publish finished
14:22:03 run succeeded
019ff1b7-8df6-7732-8f54-70fa61013409  orders_etl success in 1.5s
$ echo $?
0
```

## The exit codes

`run --wait` is the reason this phase exists, and the exit code is the reason
that is worth anything. a cron line is only as good as its exit status, so
these are fixed and each one means one thing:

| code | meaning |
| ---- | ------- |
| 0 | the command did what was asked; a `--wait` run succeeded |
| 1 | the run failed, or the command could not do what was asked |
| 2 | the command line was wrong: a bad flag, an unknown job, params the schema rejects, an ask this run cannot honour |
| 3 | the run was canceled |
| 4 | `--timeout` ran out; the run is still going |
| 5 | the store or the server could not be reached |
| 6 | this mode cannot serve this command, and the message says why |
| 7 | `doctor` found something actionable |
| 8 | the server refused this identity: no token, one it does not accept, or a role that may not |

5, 8 and 1 are deliberately different answers: "nothing was reachable" is worth
a retry, "it would not have me" is worth a person with the secret, and "the
work went wrong" is worth a person with the pipeline. 3 is not a failure (a run
somebody stopped is not a run that broke), so a cron line that pages on 1 and
2 will not page when you cancel something by hand.

each code has a case of its own in `tests/cli.rs`, which runs the real binary
and reads what it exited with.

`Exit` is deliberately **not** `#[non_exhaustive]`, unlike most of the enums
hestan exposes: the table above is what a cron line matches on, so the type
stays matchable with no `_` arm and a tenth code would owe you a compile
error rather than appearing quietly. `tests/stability.rs` reads these nine
numbers back out of this file and asserts them against the variants, so the
table and the type cannot drift apart. see [stability](stability.md).

## The output contract

**stdout belongs to the answer.** under `--json` or `--quiet` nothing else may
reach it, because something is parsing it.

- **default**: human-readable tables, padded to the widest cell.
- **`--json`**: one json object, shaped exactly as the [http api](http-api.md)
  shapes it. the same object whichever of the three modes below produced it, so
  a script does not have to care which one it was pointed at.
- **`--quiet`**: the id alone, which is what `$(...)` in a script wants, as in
  `id=$(orders --quiet run orders_etl)`.
- **anything that streams is NDJSON** under `--json`: one object per line, from
  `logs --follow` and `events --follow`.

`run --wait` streams the run's events and captured output to **stderr** as they
land. that is company while you wait, not the answer, so `--json` on stdout
stays parseable while it happens.

colour is a property of the terminal and not of the answer: `NO_COLOR`, a
pipe, `--json` and `--quiet` each mean plain text, and nothing emits an escape
code into a redirect.

> a host that installs a `tracing` subscriber gets its own copy of the events
> `--wait` streams, since those are the same events. write the subscriber to
> stderr and give it a filter: the demo uses `RUST_LOG`, so `RUST_LOG=warn`
> turns the second copy off.

## Three ways to reach a deployment

the same commands mean the same things whether the jobs are in this binary, in
a database on disk, or in a server across the network.

| | embedded | `--db <path\|url>` | `--server <url>` |
| --- | --- | --- | --- |
| where it looks | the registry compiled in | a run log, opened directly | a running instance's api |
| reads | everything | runs, logs, events, assets, schedules, queue | everything |
| launching | yes | no, in a binary without your jobs | yes |
| `explain`, `doctor` | yes | `doctor` yes, `explain` no | no |

**embedded** is the default and needs no flag. **`--db`** points the same
binary at a different run log (your registry, another database), and in the
standalone `hestan` binary it is a run log and nothing else. **`--server`**
drives a running instance over the http api it already serves; there are no new
endpoints, so it works against an instance that predates this.

where a mode genuinely cannot serve a command it says so in one line and exits
6:

```
$ hestan --db /var/lib/hestan.db run orders_etl
error: --db /var/lib/hestan.db opens a run log, which records what ran but
holds no job definitions. launching needs the binary they are compiled into,
or --server pointed at one that is running
```

and where a mode knows less, the answer **omits** what it does not know rather
than filling it in: `--db assets` has no staleness column because staleness is
a claim about a registry, and `--db queue` has no "waiting for" column because
the blame belongs to whoever owns the limits. an absent key is a mode saying it
does not know; there are no invented nulls.

the same rule tells `--db assets`' group and origin columns apart from a real
answer: both print `-`, because a run log has no registry to resolve either
from, and that is not the same as `no source`, which is a registry saying an
asset descends from nothing. `--db assets --group finance` is empty for the
same reason: the filter could not be applied, and showing every asset instead
would be worse than showing none.

### Reaching an authenticated one

a deployment that [checks who is asking](auth.md) wants a token, and there are
two ways to hand it one:

```
$ hestan --server https://hestan.internal --token "$(cat /run/secrets/hestan)" runs
$ HESTAN_TOKEN=… hestan --server https://hestan.internal runs
```

**prefer the variable.** an argument is visible in `ps` to every account on the
machine for as long as the process runs; a variable is not. the flag wins where
both are set, so a shell with one exported can still be pointed somewhere else.
hestan reads the variable itself rather than letting the argument parser do it,
because a parser that knows about an environment variable prints its **value**
in `--help`.

nothing to present, or a token it does not accept, is exit 8 and a line saying
what to do:

```
$ hestan --server https://hestan.internal runs
error: authentication required: present your credentials; https://hestan.internal
is authenticated: pass --token, or set HESTAN_TOKEN, which keeps it out of ps
```

a role that may not is exit 8 as well, with the server's own sentence about
what it would have taken. `--db` and the embedded mode take no token: neither
of them is talking to a server.

### The standalone binary

```
cargo install hestan --features cli
```

gives an operator `hestan`, which has the store and server modes and no
registry of its own, for inspecting a database or driving an instance from a
machine that does not have your code. it is behind the same feature, so a
default build of the library compiles no binary and no argument parser at all.

## Commands

### Running things

```
run <job> [--params JSON | --preset NAME] [--tag K=V]... [--priority N]
          [--key KEY] [--wait [--timeout SECS]] [--dry-run]
```

launches a run. **without `--wait` this enqueues and returns**: the run goes on
the queue for whatever is serving that database, and this process does not
start executing something it is about to exit out from under. **with `--wait`
this process executes it**, streams it, and exits with what it did.

`--key` makes the launch [idempotent](launching.md#launching-once): the same
key again is answered with the run the first call made, and nothing new is
created. that is what a ci step that may run twice wants, and what a cron line
wrapped in a retry wants:

```
$ hestan run deploy --key ci-build-4182 --quiet
019ff1b7-8df6-7c3e-9b21-2f5a0c8e41d7
$ hestan run deploy --key ci-build-4182 --quiet     # the retry
019ff1b7-8df6-7c3e-9b21-2f5a0c8e41d7
```

without `--quiet` the second one says so (`... already launched under this key;
nothing new was created`), and under `--json` it is
`{"run_id": ..., "job": ..., "repeat": true}`. there is no `status` key on that
object, unlike a launch: the run it names may have finished an hour ago, and
saying `queued` about it would be a guess. `--key` with `--wait` waits for
whichever run the key names.

the same key for a different request is exit 2, naming the run it already has.

```
retry <run>                     the same job again, with the same params
resume <run> [--from OP]...     re-run what did not succeed, seeding what did
replay <run> [--op OP]...       re-run what did, on the inputs it had
cancel <run>                    take a queued run off the queue
build <asset> [--partition KEY]... [--wait]
backfill <asset> --from KEY --to KEY [--all]
```

`replay` is the other direction from `resume`, and they are easy to confuse:
a resume re-runs the ops that did **not** succeed and everything downstream of
them, and a replay re-runs the ops that **did** (exactly the ones named, on
the inputs the original run gave them), which is how you find out whether a
fix works on the input that broke it. without `--op` it replays the ops that
run recorded as failed. what a replay does not reproduce is
[its own page](replay.md), and a run whose inputs
[retention](storage.md#retention) has taken is refused rather than run on a
hole, naming the op:

```
$ orders replay 019ff1b7-8df6 --op load
error: cannot replay load of run 019ff1b7-8df6: its input extract cannot be
read back: No such file or directory (os error 2)
```

that is exit 2, with `nothing to replay` and an op the run never ran.

`cancel` is worth a sentence. a cancel is a signal to whichever process is
*executing* the run, and there is no signal in the database, so a command line
that started a moment ago can take a run off the queue before anyone claims it,
and cannot stop one that is already running elsewhere. it says which, with the
instance that holds it, and exits 6:

```
$ orders cancel 019ff1b7-8df6
error: run 019ff1b7-8df6 is being executed by instance 0755badf, and only that
process can stop it. reach it with --server, or use the ui it is serving
```

### Looking at things

```
runs [--job NAME] [--tag K=V] [--build SHA] [--since 2h] [--limit N]
show <run>                      the run and every op of it
logs <run> [--op NAME] [--follow]
events [--kind K] [--subject S] [--level L] [--since 2h] [--follow [--after SEQ]]
jobs                            every job this deployment defines
assets [--group NAME] [--namespace NAME]
                                every asset: its group, what it descends from, whether it is stale
schedules                       every schedule, when it fires next, paused or not
queue                           what is waiting, in the order it will be taken
owner <job|asset>               who to wake about it, and how to reach them
```

`--group` and `--namespace` are different filters answering different
questions, and one asset can answer them differently: `--group` is what it is
labeled with on the graph, `--namespace` is whose slice of the deployment it is
in. [namespaces and owners](namespaces.md) is the whole of that.

`owner` answers the question a failed run's log does not:

```
$ orders owner margin
WHAT   NAME    OWNER           CONTACT      ESCALATES TO
asset  margin  ada of finance  #fin-alerts  ops@example.com
```

it answers for a job and for an asset, and for both where one name is used for
each, since picking one would be a guess about which was meant. a thing that
exists and that **nobody claimed** is a row with `-` in it, which is a
different answer from `no job or asset called margin` (exit 2) and reads like
one. `escalates_to` is a second contact hestan carries and nothing else: no
timer, no acknowledgement, no second page. `--db` opens a run log and holds no
definitions, so this needs the binary the jobs are compiled into or a
`--server` pointed at one.

`--build` is "show me the runs from the build before last": it matches the
build the run recorded when it was **launched**, so it answers about the code
that ran, not about the code running now. `show` prints a `build` line for a
run that has one and nothing for a run that does not, because a blank line
would read as a build with no name. see [deployment and build
identity](deployment.md).

`--since` takes `30m`, `2h`, `7d` or an rfc3339 instant, because one of those
is what you type and the other is what a program has.

`events --follow` resumes from `--after SEQ`, which is the last seq you saw, so
a follower that drops off picks up exactly where it was. over `--server` it
reads the [sse stream](events.md); locally it reads the log through the same
rule that stream does, so a terminal and a browser cannot disagree about what
has settled.

### Changing things

```
pause schedule <job> [--expr CRON]      every schedule on the job, or one
pause sensor <name>
unpause schedule <job> [--expr CRON]
unpause sensor <name>
priority <run> <n>                      move a queued run up or down the queue
```

### Diagnosing things

```
doctor                          why is nothing running
explain <job> [--params JSON]   the plan, without running it
completions <bash|zsh|fish>
serve [--addr HOST:PORT]
```

### Backing up and coming back

```
backup <dest>                   a consistent copy of a sqlite run log
resettle [--watch SECS]         hand back what a restored copy claims
```

`backup` runs sqlite's online backup, which is not a `cp`: a hestan database
is in WAL mode, so the file on its own is missing whatever is still in the
`-wal` beside it. against a postgres run log it says so and names `pg_dump`
rather than doing something that looks like a backup and is not one. it refuses
a destination that already exists.

`resettle` is what makes a **restored** run log startable. every claim in a
copy is held by a process that is not executing against this database and its
deciding lease names a holder that cannot renew it, so a deployment refuses to
come up on one until this has handed them back. it watches the leases for
twenty seconds first and refuses if any of them moves, because a lease that
moves is a process that is still running against this database; `--watch 0`
skips that. both are [backup and recovery](backup.md), which is also where the
hazard is written down.

both need the database, so they are `--db` or embedded-mode commands: over
`--server` they exit 6 and say which mode would serve them.

## doctor

one command for the question that matters at three in the morning. every check
looks at something real, and a check this mode cannot make is **not run** and
is listed as not run: a check that always passes because it cannot see the
thing it is about is worse than no check at all.

```
$ orders doctor
ok    deployment prod-eu, running build 9f2c1ab
ok    hestan     0.1.0 in this deployment's binary, linux/aarch64,
                 features: bundled cli postgres
ok    store      sqlite at /var/lib/hestan.db, schema v24
ok    writes     the store took a write lock and gave it back
ok    schedules  2 of 2 parse
note  schedules  paused, so they will not fire: warehouse_healthcheck
                 unpause schedule warehouse_healthcheck
ok    leases     every claim is current
ok    deciding   3f2a91cc holds the deciding lease on term 4, 8s left
wrong queue      1 run(s) are queued with nothing holding them back, so no
                 process is taking them off the queue
                 start a process whose role executes (`serve` with the default
                 role, or `work`) against this database
wrong policies   1 of 3 will never fire, however long they wait: totals waits
                 for raw_orders
                 totals waits for raw_orders, which nothing produces. give the
                 source a probe, or the window a dep that holds the keys it
                 covers, or drop the policy
ok    rates      orders_api 5 per 1s, counted in this process and no other
ok    retention  no policy: nothing is deleted
ok    disk       24.6gb free of 25.1gb where /var/lib/hestan.db lives (98%)
$ echo $?
7
```

what it checks, and what each one can actually see:

| check | it finds | needs |
| ----- | -------- | ----- |
| deployment | which installation this is and which build of your application it runs, or that it declares neither, which is a `note`: a run log that records no build cannot say which code produced anything in it. see [deployment and build identity](deployment.md) | the declaration, so the deployment's own binary |
| hestan | the hestan version, platform, features and debug assertions of **whichever binary is running this command**, and says which binary that is: `--db` pointed at somebody else's database reports the operator binary's, not theirs | nothing |
| store | that the database opened, which backend it is, and what schema version | a store |
| writes | that the database would take a write: a file whose permissions changed, a disk mounted read-only, another writer holding the lock. nothing is recorded about any run while this is wrong | a store |
| schedules | a cron expression or a timezone that no longer resolves, so the schedule silently never fires again | a store |
| schedules, sensors | anything paused, which is the answer to "why is nothing running" often enough to be worth a line | a store |
| leases | runs claimed by a process that stopped renewing, which nothing is reclaiming if nothing is running a lease loop | a store |
| deciding | who holds the [deciding lease](scaling.md#the-deciding-lease), and whether anybody does. a deployment where nothing decides fires no schedule and evaluates no sensor, and has no other symptom. over `--server` it answers the sharper question instead: whether the process you are pointed at is that one | a store, or a running deployment |
| queue | runs waiting on a limit, and (separately) runs waiting on **nothing**, which is a deployment where no process executes | the limits, so the deployment's own binary |
| policies | an [automation policy](assets.md#automation-policies) that can never fire, because a source it reads has no probe to observe it or a window promises keys its dep will never hold. a policy that will wait forever looks exactly like one with nothing to do: both are quiet | the asset graph, so the deployment's own binary |
| groups | a declared [group](assets.md#group) that disagrees with the name it is on, which is a rename somebody started and did not finish: the catalog says one thing and the name says another | the asset graph, so the deployment's own binary |
| colours | two group or origin labels whose [hues](assets.md#colour) land within eight degrees of each other. the hue is a hash of one name, so nothing reading one name at a time can stop two of them colliding; this names both and `Asset::hue(n)` moves one | the asset graph, so the deployment's own binary |
| rates | what this registry declares, and that a [rate](concepts.md#rates) is per process: a deployment that scaled by adding a worker doubled every one of them without changing a line. over `--server`, the live half instead: how many ops are queued for a token there | the registry, or a running deployment |
| retention | a policy in a process whose [role](scaling.md#roles) never sweeps, so the database grows and nothing says why | the role, so the deployment's own binary |
| disk | free space where the run log lives | a local file |

three levels, and the middle one earns its place. `wrong` is actionable and
sets exit 7. `note` is worth knowing and does not: a paused schedule is
usually something somebody chose, and a check nobody can satisfy is a check
everybody learns to ignore. `ok` means something was read and was as it should
be, which is what makes the other two believable.

`--json` gives `{"ok": bool, "findings": [...], "unchecked": [...]}` with a
`fix` on everything actionable.

six of these checks are the same facts `GET /metrics` now publishes, off the
same rows: `schedules`/`sensors` paused, `leases`, `deciding`, half of `queue`,
and `writes`. neither reads the other; both read the store. the difference is
that doctor is a person asking once and getting a sentence and a fix, and a
metric is a scrape asking every fifteen seconds and getting a number you can
put a threshold on. what stays here and cannot be a metric is everything that
needs the registry rather than the store (`policies`, `rates`, `groups`,
`colours`, `retention`), because each of those is per asset or per declaration
and a metric carrying one would need a label per asset; see
[metrics](metrics.md#what-may-be-a-label).

over `--server` doctor answers the five questions http can, and says plainly
that it saw nothing else. whether the deployment [checks who is
asking](auth.md), and (with a token) who it makes you; with that same token,
which deployment and build it says it is, whether its store is taking the
writes it makes, what is queued behind each of its
[rates](concepts.md#rates), and whether the process on the other end is
the one doing the [deciding](scaling.md#running-more-than-one-scheduler), the
things only the running process knows. the deployment line there is about the
**other** binary, which is the one useful thing a remote doctor can say that a
local one cannot. that last one is the answer to
"this schedule has not fired and this process looks fine", which is a question
about a different process. that second one is `wrong` when run outcomes are going unrecorded (the
process has stopped claiming and is leaving what it holds for a reclaimer), and
a `note` when it lost something and recovered. the schedules, the sensors, the
leases, the queue, the retention policy and the disk are listed as not checked,
because an api exposes none of them. point `--db` at the database, or run it in
the deployment's own binary, for the rest.

## explain

the plan a run would follow, resolved and not executed. this is the command the
mount pays for: a plan is a property of the job definitions, and the job
definitions are right here.

```
$ orders explain orders_etl
orders_etl: pull orders, clean them, publish aggregates
5 ops in 4 stages
pools    warehouse (limit 1)

stage 1
  fetch_orders             timeout 5s
stage 2  (2 in parallel)
  validate
  enrich
stage 3
  aggregate
stage 4
  publish                  pool warehouse, retries 2
```

a **stage** is what runs together: every op in it has its dependencies behind
it and none on each other, which is exactly the set the executor is free to run
at once, subject to `max_parallel` and to whatever pools they take from, both
of which are printed. an op with a non-default [trigger rule](concepts.md#trigger-rules)
says which, since that is what could skip it; an
[isolated](isolation.md) op says so; a [mapped](concepts.md#dynamic-fan-out) op
says what it fans out over.

`run --dry-run` is `explain` with the params checked:

```
$ orders run orders_etl --params '{"limit":"lots"}' --dry-run
error: invalid params for op fetch_orders: invalid type: string "lots", expected usize
$ echo $?
2
```

it validates through the same call a launch validates with, so a dry run that
passes and a launch that then fails on its params cannot happen. it creates no
run, opens nothing for writing, and executes nothing.

## Completions

```
eval "$(orders completions bash)"           # ~/.bashrc
orders completions zsh  > ~/.zfunc/_orders
orders completions fish > ~/.config/fish/completions/orders.fish
```

**the names are not baked in.** the script calls the binary back at the moment
you press tab:

```
$ orders run <TAB>
orders_etl  warehouse_healthcheck
```

that is a process start and a walk over a registry that is already in memory
(a few milliseconds), and it is only possible because the command line *is* the
deployment. a job you added this morning completes this afternoon with nothing
regenerated and nothing running. job and asset names come from the registry;
schedule, sensor and run names come from the database. `owner` completes jobs
and assets together, because it takes either and half the list would read as
the other half not existing.

## In a cron line

```cron
# nightly, and mail me the log if it breaks
0 2 * * *  /usr/local/bin/orders run nightly_rollup --wait --timeout 3600
```

cron mails you the output of a command that exits non-zero, and `--wait`
streams the run to stderr, so the mail you get is the run that failed, with
its own log in it.

something more deliberate:

```bash
#!/usr/bin/env bash
set -uo pipefail    # not -e: the exit code is the answer, not a reason to stop

# --quiet --wait: the run id on stdout, the run's own log on stderr
run=$(orders --quiet run nightly_rollup --wait --timeout 3600)
code=$?

case $code in
  0) exit 0 ;;
  3) exit 0 ;;                            # somebody canceled it on purpose
  4) page "nightly_rollup is still going an hour in: $run" ;;
  5) exit 75 ;;                           # EX_TEMPFAIL: unreachable, try later
  8) page "nightly_rollup: the deployment refused this token" ;;
  *) page "nightly_rollup: $(orders --json show "$run" | jq -r .run.error)" ;;
esac
exit $code
```

## In a ci step

```yaml
- name: rebuild the warehouse
  run: |
    ./target/release/orders build warehouse --wait --timeout 1800
- name: check the deployment before we start
  run: ./target/release/orders doctor
```

`doctor` exiting 7 fails the step, which is what you want in a pipeline that is
about to add work to a deployment that is already stuck.

and against a deployment that is already running somewhere:

```yaml
- name: kick off the load and wait for it
  run: |
    hestan --server https://hestan.internal run warehouse_load --wait --timeout 3600
```

a ci step is the place `--key` earns itself, because a re-run of a job is one
click away and a re-run of a **deploy** is not what anybody meant by it:

```yaml
- name: deploy, at most once per build
  run: |
    hestan --server https://hestan.internal \
      run deploy --key "ci-$GITHUB_RUN_ID" --wait --timeout 900
```

re-running that step waits on the run the first attempt made rather than
launching a second one, and exits with what it did.

## What it does not do

- **`--server` cannot `explain`, and can only half `doctor`.** both read things
  an api does not expose: the registry, the role, the disk. `explain` says so
  and points at the two modes that can; `doctor` answers what it can reach and
  lists the rest as not checked.
- **`--db` cannot launch, `explain`, or list jobs** in a binary your jobs are
  not compiled into. a run log records what ran; it holds no definitions.
- **`cancel` cannot stop a run executing in another process.** see above: there
  is no cancel signal in the database, and the process holding the run is the
  only one that can stop it.
- **`runs` has no status filter.** the store's query does not take one, and
  filtering a page after it was fetched would silently show you fewer rows than
  `--limit` asked for. `--json` and `jq` is the honest workaround for now.
- **`backup` copies sqlite only, and `--server` serves neither it nor
  `resettle`.** a copy has to land on the filesystem the database is on, and a
  resettle has to write to a database nothing else is writing to, which a
  running server is the opposite of. both exit 6 with the mode that would serve
  them.
