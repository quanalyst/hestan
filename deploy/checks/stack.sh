#!/usr/bin/env bash
# part 2: the role split as five containers rather than as a paragraph.
#
# one scheduler decides, three workers execute, and everything they share is
# one postgres. the spare from `docker-compose.spare.yml` is up as well
# because the fault checks need it and it costs this one nothing: a second
# scheduler that evaluates nothing is exactly what it is supposed to look
# like.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

trap stack_down EXIT
stack_up

# a run takes a couple of seconds, so this is a handful of occurrences and the
# runs they queued, finished
say "watching for 40 seconds"
sleep 40

say ""
say "== one deployment, five processes"
ids=()
for c in "$scheduler" "$spare" "${workers[@]}"; do
    ids+=("$(api_in "$(ip_of "$c")" /api/health | json instance)")
done
uniq_ids="$(printf '%s\n' "${ids[@]}" | sort -u | wc -l)"
if [ "$uniq_ids" = "5" ]; then
    pass "five processes with five instance ids: ${ids[*]}"
else
    bad "expected five distinct instance ids, got ${ids[*]}"
fi

say ""
say "== a schedule fires once, across all of them"
# the tick log is the record, and the unique index over the occurrence is what
# makes it one row. the logs are the second half of the same claim: whatever
# the row says, only one process ever believed it had fired
occurrences="$(q "SELECT count(DISTINCT scheduled_for) FROM schedule_ticks
                  WHERE job = 'orders_etl' AND outcome = 'fired'")"
fired="$(q "SELECT count(*) FROM schedule_ticks
            WHERE job = 'orders_etl' AND outcome = 'fired'")"
if [ "$occurrences" -ge 3 ] && [ "$fired" = "$occurrences" ]; then
    pass "$fired fired ticks over $occurrences occurrences"
else
    bad "$fired fired ticks over $occurrences occurrences"
fi

deciders=0
for c in "$scheduler" "$spare" "${workers[@]}"; do
    n="$(docker logs "$c" 2>&1 | grep -c 'schedule fired' || true)"
    [ "$n" -gt 0 ] && deciders=$((deciders + 1))
    note "$c: $n 'schedule fired' lines"
done
if [ "$deciders" = "1" ]; then
    pass "exactly one of the five processes fired anything"
else
    bad "$deciders processes fired schedules; exactly one may"
fi

say ""
say "== a queued run is claimed and executed by exactly one worker"
worker_ids=()
for c in "${workers[@]}"; do worker_ids+=("$(api_in "$(ip_of "$c")" /api/health | json instance)"); done
scheduler_id="$(api 4000 /api/health | json instance)"
spare_id="$(api 4001 /api/health | json instance)"

claimers="$(q "SELECT DISTINCT claimed_by FROM runs
               WHERE \"trigger\" = 'schedule' AND claimed_by IS NOT NULL")"
stray=0
for who in $claimers; do
    case " ${worker_ids[*]} " in
        *" $who "*) ;;
        *) bad "a run was claimed by $who, which is not one of the workers"; stray=1 ;;
    esac
done
[ "$stray" = "0" ] && pass "every scheduled run was claimed by a worker: $(echo "$claimers" | tr '\n' ' ')"

if [ "$(q "SELECT count(*) FROM runs WHERE claimed_by IN ('$scheduler_id', '$spare_id')")" = "0" ]; then
    pass "neither scheduler claimed anything ($scheduler_id, $spare_id)"
else
    bad "a scheduler claimed a run, and Role::Scheduler executes nothing"
fi

# and the log side of it: the run id appears in exactly one worker's output
run="$(q "SELECT id FROM runs WHERE status = 'success' ORDER BY finished_at DESC LIMIT 1")"
saw=0
for c in "${workers[@]}"; do
    docker logs "$c" 2>&1 | grep -q "$run" && saw=$((saw + 1))
done
if [ "$saw" = "1" ]; then
    pass "run $run was executed in exactly one of the three worker containers"
else
    bad "run $run appears in $saw worker containers"
fi

say ""
say "== the ui on any of them shows the same runs"
want="$(api 4000 "/api/runs/$run" | python3 -c '
import json, sys
r = json.load(sys.stdin)["run"]
print(r["id"], r["job"], r["status"], r["scheduled_for"])')"
same=1
for c in "$spare" "${workers[@]}"; do
    got="$(api_in "$(ip_of "$c")" "/api/runs/$run" | python3 -c '
import json, sys
r = json.load(sys.stdin)["run"]
print(r["id"], r["job"], r["status"], r["scheduled_for"])')"
    if [ "$got" != "$want" ]; then
        bad "$c says '$got', the scheduler says '$want'"
        same=0
    fi
done
[ "$same" = "1" ] && pass "all five agree about run $run: $want"

say ""
one_fire_per_occurrence
done_checking
