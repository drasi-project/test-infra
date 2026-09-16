#!/usr/bin/env bash

prepare_receiver_rejection() {
    case "${VARIANT:-}" in
        http_standard|http_adaptive|grpc_standard|grpc_adaptive) ;;
        *) log "ERROR: receiver rejection requires an HTTP/gRPC variant"; return 1 ;;
    esac
    [[ "${BOOTSTRAP_ENABLED:-false}" == false ]] || {
        log "ERROR: receiver rejection requires bootstrap off"
        return 1
    }
    [[ "$SERVER_PROFILE_PERSIST_INDEX" == true && "$SERVER_PROFILE_STATE_STORE" == true ]] || return 1
    [[ -z "${RECEIVER_REJECTION_DIR:-}" ]] || {
        log "ERROR: receiver rejection requires a fresh runner-owned fault directory"
        return 1
    }
    export RECEIVER_REJECTION_DIR="$ARTIFACTS_DIR/receiver-rejection"
    mkdir "$RECEIVER_REJECTION_DIR" || return 1
    jq -e --argjson selected "$SELECTED_QUERIES_JSON" '
        [.[] | select(((.queries // []) - $selected | length) == 0)
          | {id, port: ((.endpoint // .baseUrl) | capture(":(?<port>[0-9]+)(/.*)?$").port | tonumber)}]
        | select(length > 0)' "$COMPONENTS_DIR/$SERVER_REACTIONS_FILE" \
        > "$RECEIVER_REJECTION_DIR/targets.json" || return 1
    jq -n '{outcome:"not_completed", scenario:"receiver_rejection"}' \
        > "$ARTIFACTS_DIR/receiver-recovery.json"
}

receiver_reaction_state() {
    local reaction_id="$1" destination="$2"
    curl --max-time 10 -fsS "$DRASI_API/reactions/$reaction_id" > "$destination" || return 1
    jq -e '.success == true and (.data.status | type == "string")' "$destination" >/dev/null
}

recover_rejected_receivers() {
    local original_pid="$DRASI_PID" deadline=$(( $(date +%s) + TIMEOUT_SECS ))
    local reaction_id port state observed count=0
    while IFS=$'\t' read -r reaction_id port; do
        observed=no
        while (( $(date +%s) < deadline )); do
            kill -0 "$original_pid" 2>/dev/null && kill -0 "$SERVICE_PID" 2>/dev/null || return 1
            [[ "$DRASI_PID" == "$original_pid" ]] || return 1
            if [[ -s "$RECEIVER_REJECTION_DIR/$port.json" ]] &&
                jq -e '.phase == "rejecting" and .rejected_requests > 0 and .accepted_items_before_outage >= 100' \
                    "$RECEIVER_REJECTION_DIR/$port.json" >/dev/null; then
                receiver_reaction_state "$reaction_id" "$RECEIVER_REJECTION_DIR/$port.before.json" || return 1
                state="$(jq -r '.data.status | ascii_downcase' "$RECEIVER_REJECTION_DIR/$port.before.json")"
                if [[ "$state" == error ]]; then
                    jq -e '.data.error_message | strings | contains("delivery failed")' \
                        "$RECEIVER_REJECTION_DIR/$port.before.json" >/dev/null || {
                        log "ERROR: reaction failed for a reason other than delivery rejection"
                        return 1
                    }
                    observed=yes
                    break
                fi
            fi
            sleep "$POLL_INTERVAL_SECS"
        done
        [[ "$observed" == yes ]] || {
            log "ERROR: receiver rejection and reaction Error were not both observed for $reaction_id"
            return 1
        }
        curl --max-time 10 -fsS "$DRASI_API/queries" \
            > "$RECEIVER_REJECTION_DIR/$port.queries.json" || return 1
        jq -e '.success == true and (.data | length > 0) and
            all(.data[]; (.status | ascii_downcase) == "running")' \
            "$RECEIVER_REJECTION_DIR/$port.queries.json" >/dev/null || {
            log "ERROR: queries must remain running during receiver rejection"
            return 1
        }
        cp "$RECEIVER_REJECTION_DIR/$port.json" "$RECEIVER_REJECTION_DIR/$port.rejected.json"
        log "Receiver rejection confirmed for $reaction_id; restoring endpoint and restarting only this reaction"
        curl --max-time 30 -fsS -X POST "$DRASI_API/reactions/$reaction_id/stop" \
            > "$RECEIVER_REJECTION_DIR/$port.stop.json" || return 1
        jq -e '.success == true' "$RECEIVER_REJECTION_DIR/$port.stop.json" >/dev/null || return 1
        printf 'restore\n' > "$RECEIVER_REJECTION_DIR/$port.restore"
        curl --max-time "$TIMEOUT_SECS" -fsS -X POST "$DRASI_API/reactions/$reaction_id/start" \
            > "$RECEIVER_REJECTION_DIR/$port.start.json" || return 1
        jq -e '.success == true' "$RECEIVER_REJECTION_DIR/$port.start.json" >/dev/null || return 1
        receiver_reaction_state "$reaction_id" "$RECEIVER_REJECTION_DIR/$port.after.json" || return 1
        jq -e '(.data.status | ascii_downcase) == "running"' "$RECEIVER_REJECTION_DIR/$port.after.json" >/dev/null || return 1
        count=$((count + 1))
    done < <(jq -r '.[] | [.id, .port] | @tsv' "$RECEIVER_REJECTION_DIR/targets.json")
    (( count > 0 )) || return 1
    kill -0 "$original_pid" 2>/dev/null && [[ "$DRASI_PID" == "$original_pid" ]] || return 1
    jq -n --argjson server_pid "$original_pid" --argjson reactions "$count" \
        '{scenario:"receiver_rejection", outcome:"reactions_restarted", server_pid:$server_pid,
          server_restarted:false, reactions_restarted:$reactions}' > "$ARTIFACTS_DIR/receiver-recovery.json"
    CRASH_INJECTED=yes
}

verify_receiver_restoration() {
    local port count=0 original_pid
    [[ "$CRASH_INJECTED" == yes ]] || return 1
    original_pid="$(jq -er '.server_pid' "$ARTIFACTS_DIR/receiver-recovery.json")" || return 1
    [[ "$DRASI_PID" == "$original_pid" ]] && kill -0 "$original_pid" 2>/dev/null || return 1
    while read -r port; do
        jq -e '.phase == "restored" and .rejected_requests > 0' \
            "$RECEIVER_REJECTION_DIR/$port.json" >/dev/null || return 1
        count=$((count + 1))
    done < <(jq -r '.[].port' "$RECEIVER_REJECTION_DIR/targets.json")
    (( count > 0 )) || return 1
    jq '.outcome = "receiver_restored"' "$ARTIFACTS_DIR/receiver-recovery.json" \
        > "$ARTIFACTS_DIR/receiver-recovery.tmp" || return 1
    mv "$ARTIFACTS_DIR/receiver-recovery.tmp" "$ARTIFACTS_DIR/receiver-recovery.json"
}