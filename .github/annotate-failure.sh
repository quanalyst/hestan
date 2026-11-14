#!/usr/bin/env bash
# Preserve command status and expose bounded failure details in check annotations.
set -uo pipefail
output=$(mktemp)
trap 'rm -f "$output"' EXIT
"$@" 2>&1 | tee "$output"
status=${PIPESTATUS[0]}
if [ "$status" -ne 0 ]; then
    python3 - "$output" <<'PY'
import pathlib
import re
import sys

output = pathlib.Path(sys.argv[1]).read_text(errors="replace")
output = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", output)
detail = "\n".join(output.splitlines()[-160:])[-12000:]
detail = detail.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
print(f"::error title=Test command failed::{detail}")
PY
fi
exit "$status"
