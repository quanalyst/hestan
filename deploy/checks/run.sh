#!/usr/bin/env bash
# every check in this directory, in order, against a stack each one brings up
# and tears down for itself.
#
#     bash deploy/checks/run.sh
#
# it wants a docker daemon, the compose plugin, psql, python3, and host ports
# 4000, 4001 and 55432. it takes about ten minutes, most of it waiting for
# leases and cron occurrences, which is what these are about.
#
# ci would run this on a runner with a daemon. it is not part of `cargo test`
# and never will be: a partition takes a minute of wall clock to be a
# partition, and a test binary that takes ten minutes is a test binary nobody
# runs.

set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

checks=(image.sh stack.sh partition.sh handover.sh stop.sh one-fire-per-occurrence.sh)
failed=()

for check in "${checks[@]}"; do
    printf '\n========================================  %s\n\n' "$check"
    if bash "$here/$check"; then
        :
    else
        failed+=("$check")
    fi
done

printf '\n========================================  summary\n\n'
if [ "${#failed[@]}" -eq 0 ]; then
    printf 'all %d checks passed\n' "${#checks[@]}"
    exit 0
fi
printf 'failed: %s\n' "${failed[*]}"
exit 1
