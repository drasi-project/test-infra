#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
directory="$(mktemp -d "${TMPDIR:-/tmp}/receiver-recovery-test.XXXXXX")"
trap 'rm -rf "$directory"' EXIT
source "$here/receiver_rejection.sh"
ARTIFACTS_DIR="$directory"
RECEIVER_REJECTION_DIR="$directory"
DRASI_PID=123
SERVICE_PID=124
TIMEOUT_SECS=2
POLL_INTERVAL_SECS=1
DRASI_API=http://unused
clock=0
mode=success
log() { :; }
date() { printf '%s\n' "$clock"; }
sleep() { clock=$((clock + 1)); }
kill() { [[ "$mode" != server_dead ]]; }
curl() {
    local endpoint="${*: -1}"
    printf '%s\n' "$endpoint" >> "$directory/calls"
    case "$endpoint" in
        */queries)
            if [[ "$mode" == query_failed ]]; then
                printf '{"success":true,"data":[{"status":"Error"}]}\n'
            else
                printf '{"success":true,"data":[{"status":"Running"}]}\n'
            fi
            ;;
        */stop) printf '{"success":true}\n' ;;
        */start)
            [[ "$mode" != start_failed ]] || { printf '{"success":false}\n'; return; }
            printf '{"success":true}\n'
            printf '{"phase":"restored","rejected_requests":1}\n' > "$directory/50052.json"
            ;;
        *)
            if [[ -f "$directory/50052.restore" ]]; then
                printf '{"success":true,"data":{"status":"Running"}}\n'
            elif [[ "$mode" == no_error ]]; then
                printf '{"success":true,"data":{"status":"Running"}}\n'
            elif [[ "$mode" == wrong_error ]]; then
                printf '{"success":true,"data":{"status":"Error","error_message":"checkpoint write failed"}}\n'
            else
                printf '{"success":true,"data":{"status":"Error","error_message":"gRPC delivery failed for query"}}\n'
            fi
            ;;
    esac
}
printf '[{"id":"building-comfort-out","port":50052}]\n' > "$directory/targets.json"
check() {
    mode="$1"
    local expected="$2" actual=0
    clock=0
    CRASH_INJECTED=no
    rm -f "$directory/50052.restore" "$directory/calls"
    printf '{"phase":"rejecting","accepted_items_before_outage":100,"rejected_requests":1}\n' > "$directory/50052.json"
    if [[ "$mode" == no_rejection ]]; then rm "$directory/50052.json"; fi
    recover_rejected_receivers || actual=$?
    [[ "$actual" == "$expected" ]]
    if [[ "$expected" == 0 ]]; then
        [[ "$CRASH_INJECTED" == yes ]]
        verify_receiver_restoration
        jq -e '.server_restarted == false and .reactions_restarted == 1' "$directory/receiver-recovery.json" >/dev/null
        [[ "$(grep -Ec '/(stop|start)$' "$directory/calls")" == 2 ]]
    else
        [[ "$CRASH_INJECTED" != yes ]]
    fi
}
check success 0
check no_rejection 1
check no_error 1
check wrong_error 1
check query_failed 1
check start_failed 1
check server_dead 1
printf 'PASS: reaction-only recovery requires rejection, delivery Error, successful restart, and live server.\n'

for variant in http_standard http_adaptive grpc_standard grpc_adaptive; do
    (
        VARIANT="$variant"
        BOOTSTRAP_ENABLED=false
        SERVER_PROFILE_PERSIST_INDEX=true
        SERVER_PROFILE_STATE_STORE=true
        COMPONENTS_DIR="$here/components/server"
        SELECTED_QUERIES_JSON='["building-comfort","building-comfort-floor-agg"]'
        case "$variant" in
            http_standard) SERVER_REACTIONS_FILE=reactions_http.json; ports='[9001,9002]' ;;
            http_adaptive) SERVER_REACTIONS_FILE=reactions_http_adaptive.json; ports='[9001,9002]' ;;
            *) SERVER_REACTIONS_FILE=reactions_grpc.json; ports='[50052,50053]' ;;
        esac
        ARTIFACTS_DIR="$directory/$variant"
        mkdir "$ARTIFACTS_DIR"
        unset RECEIVER_REJECTION_DIR
        prepare_receiver_rejection
        jq -e --argjson ports "$ports" '
            [.[].port] == $ports and [.[].id] == ["building-comfort-out","building-comfort-floor-agg-out"]' \
            "$RECEIVER_REJECTION_DIR/targets.json" >/dev/null
    )
done
printf 'PASS: all four variants resolve the selected reaction IDs and HTTP/gRPC receiver ports.\n'