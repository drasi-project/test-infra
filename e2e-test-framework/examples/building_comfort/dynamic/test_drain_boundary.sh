#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
directory="$(mktemp -d "${TMPDIR:-/tmp}/drain-boundary-test.XXXXXX")"
LOG_DIR="$directory"
TEST_RUN_ID=repo.test.run
SERVICE_PID=1
DRASI_PID=2
TIMEOUT_SECS=2
POLL_INTERVAL_SECS=1
clock=0
append_after_wait=""
log() { printf '%s\n' "$*" >> "$directory/runner.log"; }
reaction_progress() { :; }
kill() { return 0; }
date() { printf '%s\n' "$clock"; }
sleep() {
    clock=$((clock + 1))
    if [[ -n "$append_after_wait" ]]; then
        printf '%s\n' "$append_after_wait" >> "$LOG_DIR/test-service.log"
        append_after_wait=""
    fi
}
eval "$(sed -n '/^wait_for_source_finished() {/,/^}/p' "$here/run_dynamic.sh")"

check() {
    local expected="$1" text="$2" ret=0
    clock=0
    printf '%s\n' "$text" > "$LOG_DIR/test-service.log"
    wait_for_source_finished || ret=$?
    if [[ "$ret" != "$expected" ]]; then
        printf 'FAIL: expected %s, got %s for %s\n' "$expected" "$ret" "$text" >&2
        exit 1
    fi
}

check 1 'Script Finished for TestRunSource repo.test.run.facilities-db'
check 1 'Source dispatchers drained for TestRunSource other.test.run.facilities-db'
check 0 'Source dispatchers drained for TestRunSource repo.test.run.facilities-db'
check 1 'Source dispatcher drain failed for TestRunSource repo.test.run.facilities-db: send failed'
[[ "$clock" == 0 ]]
check 2 "TestRun 'repo.test.run' completed:"
append_after_wait='Source dispatchers drained for TestRunSource repo.test.run.facilities-db'
check 0 'Script Finished for TestRunSource repo.test.run.facilities-db'
[[ "$clock" == 1 ]]
check 1 $'Source dispatchers drained for TestRunSource repo.test.run.facilities-db\nSource dispatcher drain failed for TestRunSource repo.test.run.facilities-db: failed'
printf 'PASS: early/wrong-run markers rejected, delayed drain accepted, drain failure fails closed. Artifacts: %s\n' "$directory"