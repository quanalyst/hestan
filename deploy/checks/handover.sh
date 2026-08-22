#!/usr/bin/env bash
# part 3, the third claim: with the leader killed, another process takes over,
# and here is how long it took.
#
# the number is the point. "within the lease" is what the design says and is
# not a measurement, so this kills the process holding the deciding lease and
# watches the `decider` row until somebody else's instance id is in it.
#
#     bash deploy/checks/handover.sh
#
# `docker kill` is SIGKILL with no chance to hand the lease back, which is the
# case the expiry is for. a clean stop is the other case and is not this one.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

trap stack_down EXIT
stack_up

leader="$(leader_container)"
other="$(other_container "$leader")"
leader_id="$(q 'SELECT claimed_by FROM decider')"
term_before="$(q 'SELECT term FROM decider')"
lease_secs="$(api "$(port_of "$leader")" /api/health | json deciding.lease_secs)"
say "$leader ($leader_id) holds term $term_before, ${lease_secs}s left on its lease"

fired_before="$(q "SELECT count(*) FROM schedule_ticks WHERE outcome = 'fired'")"

say ""
say "== killing $leader"
kill_ms="$(now_ms)"
docker kill "$leader" >/dev/null

# polled rather than watched, so the number carries the poll: a `sleep 0.1`
# plus a psql round trip is about 120ms a turn, and the answer is that much
# coarse. it is well inside what is being measured, which is seconds
took_ms=""
for _ in $(seq 1 600); do
    holder="$(q "SELECT coalesce(claimed_by, '') FROM decider" 2>/dev/null || true)"
    if [ -n "$holder" ] && [ "$holder" != "$leader_id" ]; then
        took_ms="$(now_ms)"
        break
    fi
    sleep 0.1
done

if [ -z "$took_ms" ]; then
    bad "nobody took the deciding lease in the minute after the leader was killed"
    done_checking
fi

handover=$((took_ms - kill_ms))
term_after="$(q 'SELECT term FROM decider')"
pass "the lease moved to $holder on term $term_after after ${handover}ms"

# and the number that matters to a deployment: how long until something is
# decided again, which is the handover plus the wait for the next occurrence
decided_ms=""
for _ in $(seq 1 600); do
    n="$(q "SELECT count(*) FROM schedule_ticks WHERE outcome = 'fired'")"
    if [ "$n" -gt "$fired_before" ]; then
        decided_ms="$(now_ms)"
        break
    fi
    sleep 0.1
done
if [ -n "$decided_ms" ]; then
    pass "the first occurrence fired by the new decider landed $((decided_ms - kill_ms))ms after the kill"
else
    bad "nothing fired after the handover"
fi

# the design says ten seconds, because that is DECIDE_LEASE. this asserts
# something looser on purpose: the bound worth failing on is "it happened at
# all, and soon", and the exact number is reported rather than asserted, since
# a busy machine is allowed to be slow without that being a bug
if [ "$handover" -lt 30000 ]; then
    pass "handover took ${handover}ms, inside the 30000ms this check fails at"
else
    bad "handover took ${handover}ms"
fi

health="$(api "$(port_of "$other")" /api/health)"
if [ "$(printf '%s' "$health" | json deciding.leader)" = "true" ]; then
    pass "$other says it is the one deciding now"
else
    bad "$other does not think it leads: $health"
fi

say ""
one_fire_per_occurrence

say ""
say "the gap, as the tick log has it:"
q "SELECT '     ' || job || '  ' || scheduled_for || '  ' || outcome
   FROM schedule_ticks ORDER BY id DESC LIMIT 8" | tac

done_checking
