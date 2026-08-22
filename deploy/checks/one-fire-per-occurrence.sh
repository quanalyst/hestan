#!/usr/bin/env bash
# part 3, the fourth claim: no occurrence fires twice, across a partition, a
# handover and a reconnect, one after the other.
#
#     bash deploy/checks/one-fire-per-occurrence.sh
#
# this is the unique index over `(job, expr, scheduled_for)` earning its keep,
# and it is the claim that should hold even if the election misbehaves, which
# is why it went in before the election did. the sequence is deliberately the
# nastiest one this stack can be put through:
#
#   1. cut the leader off. it is alive, and it cannot renew.
#   2. the spare takes the term and goes on firing.
#   3. kill the spare while the first one is still cut off. now nobody is
#      deciding, and one of the two processes that could be still thinks it is.
#   4. let the first one back in. it has a term in its memory that two
#      acquisitions have gone past.
#
# then count. one fired tick per occurrence, one run per occurrence, or this
# fails.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

trap stack_down EXIT
stack_up

leader="$(leader_container)"
other="$(other_container "$leader")"
leader_id="$(q 'SELECT claimed_by FROM decider')"
term_0="$(q 'SELECT term FROM decider')"
say "$leader ($leader_id) holds term $term_0"

say ""
say "1. cutting $leader off"
docker network disconnect "$network" "$leader"
sleep 30

term_1="$(q 'SELECT term FROM decider')"
holder_1="$(q 'SELECT claimed_by FROM decider')"
if [ "$holder_1" != "$leader_id" ]; then
    pass "2. $other took over on term $term_1"
else
    bad "2. nobody took over from the partitioned leader"
fi

say ""
say "3. killing $other, with $leader still cut off"
docker kill "$other" >/dev/null
sleep 20
say "     nobody has decided anything for 20s; the store says the lease is held by"
say "     $(q "SELECT coalesce(claimed_by, 'nobody') || ' on term ' || term FROM decider"), expired at $(q 'SELECT lease_until FROM decider')"

say ""
say "4. letting $leader back in, holding term $term_0 in its memory"
rejoin_iso="$(now_iso)"
rejoin_ms="$(now_ms)"
docker network connect "$network" "$leader"

# its store calls have been blocked in a backed off tcp write for the whole
# partition, so it comes back when a retransmit timer says so and not when the
# interface does. wait for it to say something
woke_ms=""
for _ in $(seq 1 180); do
    if docker logs --since "$rejoin_iso" "$leader" 2>&1 | grep -qE \
        'schedule fired|fire skipped|the lease is no longer this process|holds the lease'; then
        woke_ms="$(now_ms)"
        break
    fi
    sleep 1
done
if [ -n "$woke_ms" ]; then
    note "it woke $(( woke_ms - rejoin_ms ))ms after the interface came back"
else
    bad "it said nothing at all for 180s after being let back in"
fi
# and then let it decide for a while, so the count at the end is over a
# reasonable number of occurrences rather than a handful
sleep 45

refused="$(docker logs --since "$rejoin_iso" "$leader" 2>&1 \
    | grep -c 'fire refused: the deciding lease moved on before the fire landed' || true)"
retook="$(docker logs --since "$rejoin_iso" "$leader" 2>&1 \
    | grep -c 'this process holds the lease' || true)"
note "on reconnect it had $refused decision(s) refused on the term it named"
note "and then took the lease again $retook time(s), under a new term"

term_2="$(q 'SELECT term FROM decider')"
if [ "$term_2" -gt "$term_1" ]; then
    pass "the term is now $term_2: it is back in, as a new decider rather than the old one"
else
    bad "the term is still $term_2 and nobody is deciding"
fi

say ""
say "== the count"
one_fire_per_occurrence

# and the same question asked of the runs rather than the ticks, because a
# tick log with one row per occurrence and two runs behind it would be the
# same failure wearing a hat
runs="$(q "SELECT count(*) FROM runs WHERE \"trigger\" = 'schedule'")"
occ="$(q "SELECT count(DISTINCT job || scheduled_for) FROM runs WHERE \"trigger\" = 'schedule'")"
if [ "$runs" = "$occ" ]; then
    pass "$runs scheduled runs over $occ occurrences: one each"
else
    bad "$runs scheduled runs over $occ occurrences"
fi

done_checking
