#!/usr/bin/env bash
# part 3, the first two claims: a partitioned leader stops deciding, and
# reconnecting it does not resurrect it.
#
# `docker network disconnect` on a running container is a real partition. the
# process keeps running, its clock keeps going, and it cannot reach the
# database. that is the condition the term fence exists for, and it is the one
# several processes on one box cannot produce: there, a leader that has "lost"
# the store is a leader somebody stopped.
#
#     bash deploy/checks/partition.sh
#     PARTITION_SECS=120 PARTITION_ATTEMPTS=5 bash deploy/checks/partition.sh
#
# what every cycle asserts:
#
#   - the cut off process is still running, and its clock is still running
#   - it writes nothing at all while it is cut off
#   - another process takes the term and goes on firing
#   - nothing it does on reconnect fires an occurrence from before the reconnect
#   - it does not take the lease back
#
# and what the script is looking for across cycles: a reconnect where it goes
# on to **attempt a fire** believing it still leads, so that the store gets to
# refuse one. that is not certain on any single cycle. a pass that finds a run
# of the job already active skips the occurrence instead, and a skipped tick
# creates no run, so it names no term and is not fenced. so this cycles until
# it gets a fire, and fails if it never does, because a run where nothing was
# fenced proved nothing about the fence.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

partition_secs="${PARTITION_SECS:-60}"
attempts="${PARTITION_ATTEMPTS:-3}"

# what a stale decider says when it goes on with the pass it was in the middle
# of. the first two are decisions that would create a run and are fenced; the
# third is the overlap policy declining to make one, which is not
woke='schedule fired|fire skipped|the lease is no longer this process|waiting for the deciding lease'

cycle() {
    local leader leader_id term_before fired_before
    leader="$(leader_container)"
    leader_id="$(q 'SELECT claimed_by FROM decider')"
    term_before="$(q 'SELECT term FROM decider')"
    say "the deciding lease is held by $leader ($leader_id) on term $term_before"
    fired_before="$(q "SELECT count(*) FROM schedule_ticks WHERE outcome = 'fired'")"

    say ""
    say "== cutting $leader off the network for ${partition_secs}s"
    local cut_iso cut_ms
    cut_iso="$(now_iso)"
    cut_ms="$(now_ms)"
    docker network disconnect "$network" "$leader"

    # it is alive, and its clock is running. `docker exec` goes through the
    # daemon rather than the network, so both are askable while the container
    # has no network at all
    sleep 5
    local running restarts
    running="$(docker inspect -f '{{.State.Running}}' "$leader" 2>&1)"
    restarts="$(docker inspect -f '{{.RestartCount}}' "$leader" 2>&1)"
    if [ "$running" = "true" ] && [ "$restarts" = "0" ]; then
        pass "$leader is still running, and was not restarted"
    else
        bad "$leader: running=$running restarts=$restarts; a partition is not a stop"
    fi
    local inside outside drift
    inside="$(docker exec "$leader" date -u +%s)"
    outside="$(date -u +%s)"
    drift=$((inside - outside))
    if [ "${drift#-}" -le 2 ]; then
        pass "its clock reads the same second as ours (drift ${drift}s), with no route to the database"
    else
        bad "its clock is ${drift}s away from ours"
    fi

    sleep "$partition_secs"

    say ""
    say "== what happened while it was cut off"
    local wrote
    wrote="$(docker logs --since "$cut_iso" "$leader" 2>&1 | grep -cE 'schedule fired|fire skipped' || true)"
    if [ "$wrote" = "0" ]; then
        pass "the partitioned process decided nothing while it was cut off"
    else
        bad "the partitioned process made $wrote decisions while it had no network"
    fi

    local holder_now term_now
    holder_now="$(q 'SELECT claimed_by FROM decider')"
    term_now="$(q 'SELECT term FROM decider')"
    if [ "$holder_now" != "$leader_id" ] && [ "$term_now" -gt "$term_before" ]; then
        pass "the term moved on without it: $leader_id term $term_before to $holder_now term $term_now"
    else
        bad "the term did not move: holder $holder_now term $term_now"
    fi

    local fired_during
    fired_during="$(q "SELECT count(*) FROM schedule_ticks WHERE outcome = 'fired'")"
    if [ "$fired_during" -gt "$fired_before" ]; then
        pass "$((fired_during - fired_before)) occurrences fired while the old leader was cut off"
    else
        bad "nothing fired while the old leader was cut off; the deployment stopped deciding entirely"
    fi

    # every occurrence the store has a fire for at the moment of the reconnect.
    # this is the list the paragraph below is about: a leader that woke up and
    # flushed its backlog would add to it, for occurrences already in the past
    local before_rejoin
    before_rejoin="$(q "SELECT job || ' ' || expr || ' ' || scheduled_for
                        FROM schedule_ticks WHERE outcome = 'fired' ORDER BY 1")"

    say ""
    say "== letting it back in"
    local rejoin_iso rejoin_ms woke_ms
    rejoin_iso="$(now_iso)"
    rejoin_ms="$(now_ms)"
    docker network connect "$network" "$leader"
    say "cut for $(( (rejoin_ms - cut_ms) / 1000 ))s"

    # it does not come back the instant the interface does. its store calls are
    # blocked in a tcp write that has been backing off exponentially for the
    # whole partition, so when it notices the network again is a retransmit
    # timer rather than anything hestan chose. wait for it to say something
    woke_ms=""
    local _
    for _ in $(seq 1 180); do
        if docker logs --since "$rejoin_iso" "$leader" 2>&1 | grep -qE "$woke"; then
            woke_ms="$(now_ms)"
            break
        fi
        sleep 1
    done
    if [ -n "$woke_ms" ]; then
        note "its blocked store calls came back $(( woke_ms - rejoin_ms ))ms after the interface did"
    else
        bad "it said nothing at all for 180s after being let back in"
    fi
    sleep 5

    local tried refused skipped first
    tried="$(docker logs --since "$rejoin_iso" "$leader" 2>&1 | grep -c 'schedule fired' || true)"
    refused="$(docker logs --since "$rejoin_iso" "$leader" 2>&1 \
        | grep -c 'fire refused: the deciding lease moved on before the fire landed' || true)"
    skipped="$(docker logs --since "$rejoin_iso" "$leader" 2>&1 | grep -c 'fire skipped' || true)"
    first="$(docker logs --since "$rejoin_iso" "$leader" 2>&1 \
        | grep -oE 'schedule fired|fire skipped|the lease is no longer this process' | head -1)"
    note "the first thing it said on waking: '$first'"

    # nothing it did put a fire against an occurrence that was already in the
    # past when it was let back in. this is the claim whichever way the cycle
    # went, so it is asserted every time
    local after_rejoin
    after_rejoin="$(q "SELECT job || ' ' || expr || ' ' || scheduled_for
                       FROM schedule_ticks WHERE outcome = 'fired'
                       AND scheduled_for::timestamptz <= '$rejoin_iso'::timestamptz
                       ORDER BY 1")"
    if [ "$after_rejoin" = "$before_rejoin" ]; then
        pass "not one occurrence from before the reconnect gained a fire afterwards"
    else
        bad "an occurrence from before the reconnect was fired after it:"
        diff <(printf '%s\n' "$before_rejoin") <(printf '%s\n' "$after_rejoin") >&2 || true
    fi

    local holder_after term_after
    holder_after="$(q 'SELECT claimed_by FROM decider')"
    term_after="$(q 'SELECT term FROM decider')"
    if [ "$holder_after" = "$holder_now" ] && [ "$term_after" = "$term_now" ]; then
        pass "it did not take the lease back: $holder_after still holds term $term_after"
    else
        bad "the lease moved again: $holder_after term $term_after"
    fi

    local health
    health="$(api "$(port_of "$leader")" /api/health)"
    if [ "$(printf '%s' "$health" | json deciding.leader)" = "false" ] \
        && [ "$(printf '%s' "$health" | json deciding.holder)" = "$holder_after" ]; then
        pass "it reports the store's answer about who decides, not its own"
    else
        bad "/api/health on the reconnected process says $health"
    fi

    if [ "$tried" -ge 1 ]; then
        pass "on reconnect it decided to fire $tried occurrence(s): it still believed it led"
        if [ "$refused" = "$tried" ]; then
            pass "the store refused all $refused of them on the term they named"
        else
            bad "it attempted $tried fires and the store refused $refused"
        fi
        say ""
        say "what it said when it came back:"
        docker logs --since "$rejoin_iso" "$leader" 2>&1 \
            | grep -E 'decider|schedule' | head -6 | sed 's/^/     /'
        return 0
    fi

    if [ "$skipped" -ge 1 ]; then
        note "this cycle ended in $skipped skipped occurrence(s) rather than a fire: a run of"
        note "the job was still active, so the overlap policy declined to make a decision"
        note "there was a term to put on. nothing was fenced, so this cycle proves nothing"
        note "about the fence"
    else
        note "it attempted nothing at all on reconnect"
    fi
    return 1
}

trap stack_down EXIT
stack_up

fenced=0
for attempt in $(seq 1 "$attempts"); do
    say ""
    say "#################### partition $attempt of $attempts"
    say ""
    cycle && { fenced=1; break; }
done

say ""
if [ "$fenced" = "1" ]; then
    pass "a stale decider attempted a fire and the store refused it"
else
    bad "$attempts partitions and not one ended in an attempted fire, so nothing was fenced"
fi

say ""
one_fire_per_occurrence
done_checking
