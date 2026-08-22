#!/usr/bin/env bash
# what a clean stop is worth, next to what a kill costs.
#
# the same stack, the same machine, the same measurement, twice. the container
# holding the deciding lease is **stopped**, and the one holding it afterwards
# is **killed**. the pair is the point: the kill number is the lease running
# out and does not move, and the stop number is what handing the lease back
# buys.
#
#     bash deploy/checks/stop.sh
#
# **as pid 1, and without `--init`.** the kernel drops an unhandled signal sent
# to pid 1 of a container rather than applying its default action, which is
# what made a stopped hestan container a killed one. a check run behind an init
# shim would pass without going anywhere near that, so this asserts the shape
# it is testing before it tests anything.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

trap stack_down EXIT
stack_up

# how long the store takes to say somebody else holds the lease, polled at the
# same interval `handover.sh` polls it at, so the numbers are comparable: a
# `sleep 0.1` plus a psql round trip is about 120ms a turn, and both answers
# are that much coarse
handover_ms=""
handover_holder=""
watch_handover() {
    local was="$1" from="$2" holder
    handover_ms=""
    handover_holder=""
    for _ in $(seq 1 600); do
        holder="$(q "SELECT coalesce(claimed_by, '') FROM decider" 2>/dev/null || true)"
        if [ -n "$holder" ] && [ "$holder" != "$was" ]; then
            handover_ms=$(( $(now_ms) - from ))
            handover_holder="$holder"
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# milliseconds between two rfc3339 timestamps, since docker records when a
# container's process exited and that beats polling for it: a poll that also
# had to ask docker would make its own answer coarse. python's job for the same
# reason the json reader in lib.sh is: `date -d` parses these on gnu coreutils
# and not on every uutils build, and docker's stamps carry nanoseconds
ms_between() {
    python3 -c '
import sys
from datetime import datetime

def parse(s):
    s = s.strip().replace("Z", "+00:00")
    if "." in s:
        head, rest = s.split(".", 1)
        digits = ""
        while rest and rest[0].isdigit():
            digits += rest[0]
            rest = rest[1:]
        s = head + "." + (digits + "000000")[:6] + rest
    return datetime.fromisoformat(s)

print(int((parse(sys.argv[2]) - parse(sys.argv[1])).total_seconds() * 1000))
' "$1" "$2"
}

# how long the container's own process took to exit, from the moment the signal
# was asked for, as docker recorded it
exit_took() { ms_between "$1" "$(docker inspect -f '{{.State.FinishedAt}}' "$2")"; }

# which container an instance id belongs to, for the worker half below
worker_ids=()
for c in "${workers[@]}"; do
    worker_ids+=("$(api_in "$(ip_of "$c")" /api/health | json instance)")
done
container_of() {
    local want="$1" i
    for i in "${!workers[@]}"; do
        if [ "${worker_ids[$i]}" = "$want" ]; then
            echo "${workers[$i]}"
            return 0
        fi
    done
    return 1
}

leader="$(leader_container)"

say ""
say "== the shape this is testing"

# unset renders as `<nil>` on this docker and `<no value>` on some others,
# and neither is `true`, which is the only value that would matter
init="$(docker inspect -f '{{.HostConfig.Init}}' "$leader")"
case "$init" in
    ""|"false"|"<nil>"|"<no value>"|"null")
        pass "$leader runs with no init shim (HostConfig.Init=${init:-unset})" ;;
    *)
        bad "$leader has an init shim (HostConfig.Init=$init); a signal behind one proves nothing" ;;
esac

pid1="$(docker exec "$leader" cat /proc/1/comm 2>/dev/null || true)"
if [ "$pid1" = "hestan-demo" ]; then
    pass "the hestan process is pid 1 in its container, which is the case that was broken"
else
    bad "pid 1 in $leader is '$pid1', not the hestan binary"
fi

say ""
say "== docker stop on the deciding container"

leader_id="$(q 'SELECT claimed_by FROM decider')"
term_before="$(q 'SELECT term FROM decider')"
say "$leader ($leader_id) holds term $term_before"

started_iso="$(now_iso)"
started="$(now_ms)"
# in the background, because the lease is handed back before the process has
# finished leaving and the poll has to be running to see it. the poll asks the
# store and nothing else: a `docker inspect` in the same loop would make this
# answer as coarse as the daemon is busy, and when the container's process
# exited is something docker wrote down anyway
docker stop "$leader" >/dev/null &
stopper=$!
stop_handover=""
stop_holder=""
if watch_handover "$leader_id" "$started"; then
    stop_handover="$handover_ms"
    stop_holder="$handover_holder"
else
    bad "nobody took the deciding lease in the minute after the stop"
fi
wait "$stopper" || bad "docker stop on $leader failed"
stop_exit="$(exit_took "$started_iso" "$leader")"
code="$(docker inspect -f '{{.State.ExitCode}}' "$leader")"
term_after="$(q 'SELECT term FROM decider')"

# 137 is 128+9: the grace period ran out and docker sent SIGKILL, which is
# what this image did before it listened for anything
if [ "$code" = "0" ]; then
    pass "the container exited 0 after ${stop_exit}ms, on the signal rather than on the kill"
elif [ "$code" = "137" ]; then
    bad "the container exited 137 after ${stop_exit}ms: it waited out the grace period and was killed"
else
    bad "the container exited $code after ${stop_exit}ms"
fi

# docker's default grace is ten seconds. a process that exits well inside it is
# one that acted on the signal rather than one that happened to be finishing
if [ "$stop_exit" -lt 8000 ]; then
    pass "it took ${stop_exit}ms of the 10s grace period docker gives it"
else
    bad "it took ${stop_exit}ms of a 10s grace period"
fi

if [ -n "$stop_handover" ]; then
    pass "the lease moved to $stop_holder on term $term_after after ${stop_handover}ms"
fi

# the process's own account of it, beside the two numbers measured from
# outside: what it decided to do about the signal, and in what order
note "what $leader said on its way out:"
docker logs "$leader" 2>&1 | grep -iE 'stopping|deciding: lease' | tail -4 \
    | sed 's/^/       /' || true

say ""
say "== docker kill on whichever container is deciding now"

docker start "$leader" >/dev/null
for _ in $(seq 1 60); do
    if api "$(port_of "$leader")" /api/health >/dev/null 2>&1; then break; fi
    sleep 1
done
if api "$(port_of "$leader")" /api/health >/dev/null 2>&1; then
    note "$leader is back up as the spare"
else
    bad "$leader did not come back up"
fi

leader="$(leader_container)"
leader_id="$(q 'SELECT claimed_by FROM decider')"
say "$leader ($leader_id) is deciding"

started="$(now_ms)"
docker kill "$leader" >/dev/null
if watch_handover "$leader_id" "$started"; then
    kill_handover="$handover_ms"
    pass "the lease moved to $handover_holder after ${kill_handover}ms"
else
    kill_handover=""
    bad "nobody took the deciding lease in the minute after the kill"
fi

say ""
say "== stop, and kill, measured the same way"
if [ -n "$stop_handover" ] && [ -n "$kill_handover" ]; then
    note "stop: the lease moved after ${stop_handover}ms"
    note "kill: the lease moved after ${kill_handover}ms"
    if [ "$stop_handover" -lt "$kill_handover" ]; then
        pass "a stop handed over $(( kill_handover - stop_handover ))ms sooner than a kill"
    else
        bad "a stop handed over no sooner than a kill"
    fi
    # the bound worth failing on, and it is loose on purpose, the same way
    # handover.sh fails at 30000 for a design number of 10000. a handback makes
    # the lease free at once, so what is left is how long the next process
    # takes to look, which is one two second renewal interval; a busy machine
    # is allowed to be slower than that without it being a bug, and the exact
    # number is reported rather than asserted
    if [ "$stop_handover" -lt 6000 ]; then
        pass "the stop handover is inside the 6000ms this check fails at"
    else
        bad "the stop handover took ${stop_handover}ms"
    fi
fi

say ""
say "== a worker stopped while it is running something"

# catch one in flight. the demo fires every ten seconds here and a run takes
# somewhere over a second, so polling this fast finds one within a couple of
# occurrences
in_flight=""
claimer=""
for _ in $(seq 1 900); do
    row="$(q "SELECT id || ' ' || claimed_by FROM runs
              WHERE status = 'running' AND claimed_by IS NOT NULL LIMIT 1")"
    if [ -n "$row" ]; then
        in_flight="${row%% *}"
        claimer="${row##* }"
        break
    fi
    sleep 0.1
done

if [ -z "$in_flight" ]; then
    bad "no run was in flight to catch in 90 seconds"
else
    holder="$(container_of "$claimer" || true)"
    if [ -z "$holder" ]; then
        bad "run $in_flight is claimed by $claimer, which is not one of the workers"
    else
        say "stopping $holder while it runs $in_flight"
        started_iso="$(now_iso)"
        docker stop "$holder" >/dev/null
        took="$(exit_took "$started_iso" "$holder")"
        code="$(docker inspect -f '{{.State.ExitCode}}' "$holder")"
        status="$(q "SELECT status FROM runs WHERE id = '$in_flight'")"
        still="$(q "SELECT coalesce(claimed_by, '') FROM runs WHERE id = '$in_flight'")"
        if [ "$code" = "0" ]; then
            pass "$holder exited 0 after ${took}ms with a run in flight"
        else
            bad "$holder exited $code after ${took}ms"
        fi
        # the run was short enough to finish inside the deadline, which is the
        # case worth showing: a stopping worker is not a worker that drops
        # what it is holding
        if [ "$status" = "success" ] && [ "$still" = "$claimer" ]; then
            pass "run $in_flight finished as $status on the worker that was stopped"
        elif [ "$status" = "queued" ]; then
            pass "run $in_flight went back on the queue rather than failing"
        else
            bad "run $in_flight is '$status' claimed by '${still:-nobody}' after its worker stopped"
        fi
    fi
fi

say ""
one_fire_per_occurrence

done_checking
