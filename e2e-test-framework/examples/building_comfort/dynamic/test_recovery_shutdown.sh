#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --child ]]; then
    case "$2" in
        clean) trap 'exit 0' TERM ;;
        abnormal) trap 'exit 7' TERM ;;
        unhandled) trap - TERM ;;
        timeout) trap '' TERM ;;
    esac
    printf 'ready\n' >&4
    while IFS= read -r line; do :; done
    exit 1
fi
here="$(cd "$(dirname "$0")" && pwd)"
directory="$(mktemp -d "${TMPDIR:-/tmp}/recovery-shutdown-test.XXXXXX")"
trap 'if [[ -n "${DRASI_PID:-}" ]]; then builtin kill -KILL "$DRASI_PID" 2>/dev/null || true; builtin wait "$DRASI_PID" 2>/dev/null || true; fi; rm -rf "$directory"' EXIT
LOG_DIR="$directory"
ARTIFACTS_DIR="$directory"
SHUTDOWN_TIMEOUT_SECS=2
clock=0
alive=yes
child_exit=0
behavior=clean
log() { printf '%s\n' "$*" >> "$directory/runner.log"; }
date() { printf '%s\n' "$clock"; }
sleep() { clock=$((clock + 1)); }
kill() {
    case "$1" in
        -0) [[ "$alive" == yes ]] ;;
        -TERM)
            printf '%s\n' TERM >> "$directory/signals"
            [[ "$behavior" != signal_failed ]] || return 1
            if [[ "$behavior" != timeout ]]; then alive=no; fi
            ;;
        -KILL)
            printf '%s\n' KILL >> "$directory/signals"
            alive=no
            child_exit=137
            ;;
        *) return 1 ;;
    esac
}
wait() { return "$child_exit"; }
eval "$(sed -n '/^stop_drasi_for_recovery() {/,/^}/p' "$here/run_dynamic.sh")"
eval "$(sed -n '/^inject_crash_and_restart() {/,/^}/p' "$here/run_dynamic.sh")"
wait_for_source_finished() { return 0; }
restart_drasi_server() { touch "$directory/restarted"; }
reaction_progress() { :; }
curl() { printf '{"data":[{}]}\n'; }
TEST_RUN_ID=repo.test.run
CRASH_DELAY_MS=0
CRASH_REAPPLY_COMPONENTS=no
DRASI_API=http://unused
touch "$LOG_DIR/test-service.log"

check() {
    local signal="$1" mode="$2" code="$3" expected="$4" expected_outcome="$5"
    local actual=0
    RECOVERY_SIGNAL="$signal"
    behavior="$mode"
    child_exit="$code"
    DRASI_PID=123
    clock=0
    alive=yes
    CRASH_INJECTED=no
    rm -f "$directory/restarted" "$directory/signals"
    inject_crash_and_restart || actual=$?
    [[ "$actual" == "$expected" ]]
    jq -e --arg signal "$signal" --arg outcome "$expected_outcome" \
        '.signal == $signal and .outcome == $outcome' "$ARTIFACTS_DIR/recovery-shutdown.json" >/dev/null
    if [[ "$expected" == 0 ]]; then
        [[ -f "$directory/restarted" && "$CRASH_INJECTED" == yes ]]
    else
        [[ ! -f "$directory/restarted" && "$CRASH_INJECTED" != yes ]]
    fi
}

check SIGTERM clean 0 0 stopped
[[ "$(cat "$directory/signals")" == TERM ]]
check SIGTERM clean 143 1 abnormal_exit
check SIGTERM clean 1 1 abnormal_exit
check SIGTERM timeout 0 1 timed_out
[[ "$(cat "$directory/signals")" == $'TERM\nKILL' ]]
check SIGTERM signal_failed 0 1 signal_failed
check SIGKILL clean 0 0 stopped
[[ "$(cat "$directory/signals")" == KILL ]]
printf 'PASS: clean TERM and KILL restart; timeout, abnormal exit, and signal failure never restart.\n'

unset -f kill wait sleep date
mkfifo "$directory/input" "$directory/ready"
exec 3<>"$directory/input"
exec 4<>"$directory/ready"
SHUTDOWN_TIMEOUT_SECS=1
check_child() {
    local mode="$1" signal="$2" expected="$3" outcome="$4" code="$5"
    local actual=0 ready
    bash "$0" --child "$mode" <&3 &
    DRASI_PID=$!
    read -r -t 5 ready <&4
    [[ "$ready" == ready ]]
    RECOVERY_SIGNAL="$signal"
    stop_drasi_for_recovery || actual=$?
    [[ "$actual" == "$expected" && -z "$DRASI_PID" ]]
    jq -e --arg outcome "$outcome" --argjson code "$code" \
        '.outcome == $outcome and .exit_code == $code' "$ARTIFACTS_DIR/recovery-shutdown.json" >/dev/null
}
check_child clean SIGTERM 0 stopped 0
check_child abnormal SIGTERM 1 abnormal_exit 7
check_child unhandled SIGTERM 1 abnormal_exit 143
check_child timeout SIGTERM 1 timed_out 137
check_child timeout SIGKILL 0 stopped 137
exec 3>&-
exec 4>&-
printf 'PASS: real children verify clean TERM, unhandled TERM, abnormal exit, timeout cleanup, and SIGKILL reaping.\n'