# what the checks in this directory share: the stack, the store, and the
# questions both get asked.
#
# these are not `cargo test` cases and cannot be. each one wants a docker
# daemon, a six container stack and a minute or two of wall clock, and none of
# that belongs in a test binary that has to run on a laptop in five seconds.
# they are here so that ci can run them as one command when there is a daemon
# to run them on:
#
#     bash deploy/checks/run.sh          # all of them, in order
#     bash deploy/checks/partition.sh    # one of them
#
# nothing here needs a registry. the image is built from the checkout the
# script is in, and `busybox:1.36` is pulled once to have something on the
# compose network that can make an http request, because the runtime layer of
# the hestan image deliberately has no curl in it.

set -euo pipefail

checks_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$checks_dir/../.." && pwd)"

# a compose project of its own, so a stack somebody is looking at in the ui is
# not the stack a check is about to cut in half. it still wants host ports
# 4000, 4001 and 55432 free, because that is what the compose file publishes.
project="${HESTAN_CHECK_PROJECT:-hestancheck}"
network="${project}_default"
scheduler="${project}-scheduler-1"
spare="${project}-spare-1"
workers=("${project}-worker-1" "${project}-worker-2" "${project}-worker-3")
token="demo-token-change-me"
pg="postgres://hestan:hestan@127.0.0.1:55432/hestan"

# an occurrence every ten seconds. six fields, so the first one is seconds.
# the demo's own schedule is every two minutes, which is right for a demo and
# wrong for a check somebody has to sit and watch
export HESTAN_SCHEDULE="${HESTAN_SCHEDULE:-*/10 * * * * *}"

failures=0

say() { printf '%s\n' "$*"; }
note() { printf '     %s\n' "$*"; }
pass() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*" >&2; failures=$((failures + 1)); }

# every check ends here, and the exit code is the number of things that were
# not true
done_checking() {
    if [ "$failures" -ne 0 ]; then
        say ""
        say "$failures check(s) failed"
        exit 1
    fi
    say ""
    say "every check passed"
    exit 0
}

compose() {
    docker compose -p "$project" \
        -f "$repo/docker-compose.yml" -f "$repo/docker-compose.spare.yml" "$@"
}

# ask the store. it is the only participant in any of this with no opinion of
# its own about who the decider is
q() { psql "$pg" -tAqc "$1"; }

# ask a process, on a port published to the host
api() { curl -fsS -H "Authorization: Bearer $token" "http://127.0.0.1:$1$2"; }

# ask a process that publishes no port, from a throwaway container on the
# compose network
api_in() {
    local ip="$1" path="$2"
    docker run --rm --network "$network" busybox:1.36 \
        wget -qO- --header="Authorization: Bearer $token" "http://$ip:4000$path"
}

ip_of() {
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

# one field out of a json body, by dotted path, without a jq to do it with
json() {
    python3 -c '
import json, sys
value = json.load(sys.stdin)
for key in sys.argv[1].split("."):
    value = value[key]
if isinstance(value, bool):
    value = "true" if value else "false"
print(value)
' "$1"
}

now_iso() { date -u +%Y-%m-%dT%H:%M:%S.%NZ; }
# epoch milliseconds. `%3N` truncates nanoseconds on gnu coreutils and does
# not on uutils, which is what some distributions ship now, so this truncates
# the string instead and is right on both
now_ms() { local n; n="$(date -u +%s%N)"; echo "${n:0:13}"; }

# the image, built by docker rather than by compose. `docker compose up
# --build` goes through buildx, which is a separate plugin and not always
# installed; `docker build` works on either builder. the compose file still
# declares `build: .` because that is what somebody running the stack by hand
# wants.
build_image() {
    docker build -t hestan-demo "$repo" >/tmp/hestan-image-build.log 2>&1 \
        || { bad "the image did not build; see /tmp/hestan-image-build.log"; exit 1; }
}

# bring the stack up from nothing: a database with no rows in it, five hestan
# processes, and one of them holding the deciding lease
stack_up() {
    say "bringing up $project (postgres, one scheduler, one spare, three workers)"
    build_image
    compose down -v --remove-orphans >/dev/null 2>&1 || true
    compose up -d --wait >/tmp/hestan-compose-up.log 2>&1 \
        || { bad "the stack did not come up; see /tmp/hestan-compose-up.log"; exit 1; }
    local i
    for i in $(seq 1 60); do
        if api 4000 /api/health >/dev/null 2>&1 && api 4001 /api/health >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    api 4000 /api/health >/dev/null || { bad "the scheduler never answered"; exit 1; }
    api 4001 /api/health >/dev/null || { bad "the spare never answered"; exit 1; }
    for i in $(seq 1 60); do
        [ -n "$(q 'SELECT claimed_by FROM decider' 2>/dev/null || true)" ] && break
        sleep 1
    done
    # and one occurrence in the log, so the deployment is demonstrably deciding
    # before anything is done to it
    for i in $(seq 1 60); do
        local ticks
        ticks="$(q "SELECT count(*) FROM schedule_ticks" 2>/dev/null || echo 0)"
        [ "${ticks:-0}" -ge 1 ] && break
        sleep 1
    done
}

stack_down() {
    if [ "${HESTAN_CHECK_KEEP:-0}" = "1" ]; then
        say "leaving $project up (HESTAN_CHECK_KEEP=1)"
        return
    fi
    compose down -v --remove-orphans >/dev/null 2>&1 || true
}

# which container is holding the deciding lease right now, asked of the store
# and matched against what each process says its own instance id is
leader_container() {
    local held scheduler_id
    held="$(q 'SELECT claimed_by FROM decider')"
    scheduler_id="$(api 4000 /api/health | json instance)"
    if [ "$held" = "$scheduler_id" ]; then echo "$scheduler"; else echo "$spare"; fi
}

other_container() {
    if [ "$1" = "$scheduler" ]; then echo "$spare"; else echo "$scheduler"; fi
}

port_of() {
    if [ "$1" = "$scheduler" ]; then echo 4000; else echo 4001; fi
}

# the assertion every check makes, whatever else it was about. two runs of one
# cron occurrence is the failure the unique index over (job, expr,
# scheduled_for) exists to make unrepresentable, and it should hold even when
# the election misbehaves, because that is why it was built first
one_fire_per_occurrence() {
    local dup_ticks dup_runs
    dup_ticks="$(q "SELECT count(*) FROM (
        SELECT job, expr, scheduled_for FROM schedule_ticks
        WHERE outcome = 'fired' GROUP BY 1, 2, 3 HAVING count(*) > 1) d")"
    dup_runs="$(q "SELECT count(*) FROM (
        SELECT job, scheduled_for FROM runs
        WHERE \"trigger\" = 'schedule' GROUP BY 1, 2 HAVING count(*) > 1) d")"
    local fired
    fired="$(q "SELECT count(*) FROM schedule_ticks WHERE outcome = 'fired'")"
    if [ "$dup_ticks" = "0" ] && [ "$dup_runs" = "0" ]; then
        pass "no occurrence fired twice ($fired fired ticks, $dup_ticks repeated, $dup_runs repeated runs)"
    else
        bad "an occurrence fired twice: $dup_ticks repeated ticks, $dup_runs repeated runs"
    fi
}
