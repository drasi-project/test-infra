#!/usr/bin/env bash
set -euo pipefail

DRASI_SERVER_URL="${DRASI_SERVER_URL:-http://drasi-server:8080}"
DRASI_SOURCE_ENDPOINT="${DRASI_SOURCE_ENDPOINT:-tcp://drasi-server:50051}"
DRASI_SOURCE_ADDRESS="${DRASI_SOURCE_ENDPOINT#*://}"
DRASI_SOURCE_HOST="${DRASI_SOURCE_HOST:-${DRASI_SOURCE_ADDRESS%%:*}}"
DRASI_SOURCE_PORT="${DRASI_SOURCE_PORT:-${DRASI_SOURCE_ADDRESS##*:}}"
E2E_REACTION_HOST="${E2E_REACTION_HOST:-e2e-runner}"
TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-63123}"
TEST_RUN_ID="${TEST_RUN_ID:-drasi_server_dev_repo.building_comfort.test_run_001}"
TEST_REACTION_IDS="${TEST_REACTION_IDS:-building-comfort building-comfort-floor-agg}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-10}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-/artifacts}"
WORK_DIR="${WORK_DIR:-/work}"
REDIS_CONNECTION_STRING="${REDIS_CONNECTION_STRING:-${REDIS_URI:-}}"
KEEP_ALIVE_AFTER_COMPLETION="${KEEP_ALIVE_AFTER_COMPLETION:-false}"

SCENARIO_DIR="/app/scenario"
COMPONENTS_DIR="$SCENARIO_DIR/components/server"
LOG_DIR="$ARTIFACTS_DIR/logs"
DATA_CACHE="$WORK_DIR/test_data_cache"
TEST_CFG="$WORK_DIR/config.generated.json"
DRASI_API="${DRASI_SERVER_URL%/}/api/v1"
SERVICE_PID=""

mkdir -p "$ARTIFACTS_DIR" "$LOG_DIR" "$WORK_DIR" "$DATA_CACHE"

log() {
    printf '[aspire-e2e] %s\n' "$*"
}

cleanup() {
    if [[ -n "$SERVICE_PID" ]] && kill -0 "$SERVICE_PID" 2>/dev/null; then
        kill "$SERVICE_PID" 2>/dev/null || true
        wait "$SERVICE_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_http() {
    local url="$1" name="$2" timeout="$3"
    local deadline=$(( $(date +%s) + timeout ))
    until curl -fsS "$url" >/dev/null 2>&1; do
        if (( $(date +%s) >= deadline )); then
            log "ERROR: timed out waiting for $name at $url"
            return 1
        fi
        sleep 2
    done
}

wait_for_port() {
    local host="$1" port="$2" name="$3" timeout="$4"
    local deadline=$(( $(date +%s) + timeout ))
    until nc -z "$host" "$port" >/dev/null 2>&1; do
        if (( $(date +%s) >= deadline )); then
            log "ERROR: timed out waiting for $name at $host:$port"
            return 1
        fi
        sleep 2
    done
}

write_test_config() {
    log "Generating test-service config for Drasi source ${DRASI_SOURCE_HOST}:${DRASI_SOURCE_PORT}"
    jq \
        --arg cache "$DATA_CACHE" \
        --arg source_host "$DRASI_SOURCE_HOST" \
        --argjson source_port "$DRASI_SOURCE_PORT" \
        '.data_store.data_store_path = $cache
         | .data_store.delete_on_start = false
         | .data_store.delete_on_stop = false
         | (.data_store.test_repos[]?.local_tests[]?.sources[]?.source_change_dispatchers[]?
             | select(.kind == "Grpc")) |= (.host = $source_host | .port = $source_port)' \
        "$SCENARIO_DIR/config.json" > "$TEST_CFG"
}

drasi_apply() {
    local path="$1" body="$2"
    local response http_code json_body ok
    response="$(curl -sS -w $'\n%{http_code}' -X POST "${DRASI_API}${path}" \
        -H 'Content-Type: application/json' --data-binary "$body" 2>&1)" || {
        log "ERROR: POST $path failed: $response"
        return 1
    }

    http_code="${response##*$'\n'}"
    json_body="${response%$'\n'*}"
    if [[ "$http_code" != "2"* ]]; then
        log "ERROR: POST $path returned HTTP $http_code: $json_body"
        return 1
    fi

    ok="$(printf '%s' "$json_body" | jq -r 'if type=="object" then (.success // "true") else "true" end' 2>/dev/null || echo true)"
    if [[ "$ok" == "false" ]]; then
        log "ERROR: POST $path reported failure: $json_body"
        return 1
    fi
}

apply_server_components() {
    local source_body reactions_body

    log "Applying gRPC source"
    source_body="$(jq '.' "$COMPONENTS_DIR/source_grpc.json")"
    drasi_apply "/sources" "$source_body"

    log "Applying queries"
    while IFS= read -r query; do
        [[ -n "$query" ]] || continue
        log "  -> $(printf '%s' "$query" | jq -r '.id')"
        drasi_apply "/queries" "$query"
    done < <(jq -c '.[]' "$COMPONENTS_DIR/queries.json")

    log "Applying gRPC reactions with endpoint host $E2E_REACTION_HOST"
    reactions_body="$(jq --arg host "$E2E_REACTION_HOST" \
        'map(.endpoint |= sub("^grpc://localhost"; "grpc://" + $host))' \
        "$COMPONENTS_DIR/reactions_grpc.json")"
    while IFS= read -r reaction; do
        [[ -n "$reaction" ]] || continue
        log "  -> $(printf '%s' "$reaction" | jq -r '.id')"
        drasi_apply "/reactions" "$reaction"
    done < <(printf '%s' "$reactions_body" | jq -c '.[]')

    wait_for_port "$DRASI_SOURCE_HOST" "$DRASI_SOURCE_PORT" "Drasi gRPC source" 120
}

start_test_service() {
    log "Starting test-service on port $TEST_SERVICE_PORT"
    (
        RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
        DRASI_PORT="$TEST_SERVICE_PORT" \
        test-service --config "$TEST_CFG" > "$LOG_DIR/test-service.log" 2>&1
    ) &
    SERVICE_PID=$!
    wait_for_http "http://127.0.0.1:${TEST_SERVICE_PORT}/docs" "test-service API" 600
}

reaction_progress() {
    local id body count status output=""
    for id in $TEST_REACTION_IDS; do
        body="$(curl -sS "http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}/reactions/${id}" 2>/dev/null || true)"
        [[ -n "$body" ]] || continue
        count="$(printf '%s' "$body" | jq -r '.reaction_observer.result_summary.reaction_invocation_count // "?"' 2>/dev/null || echo '?')"
        status="$(printf '%s' "$body" | jq -r '.reaction_observer.status // "?"' 2>/dev/null || echo '?')"
        output+=" [$id: $count records, $status]"
    done
    printf '%s' "$output"
}

wait_for_completion() {
    local marker="TestRun '${TEST_RUN_ID}' completed:"
    local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
    local last_log=0

    log "Waiting up to ${TIMEOUT_SECS}s for completion marker"
    while (( $(date +%s) < deadline )); do
        if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
            log "ERROR: test-service exited unexpectedly"
            tail -n 200 "$LOG_DIR/test-service.log" || true
            return 1
        fi

        if grep -qF "$marker" "$LOG_DIR/test-service.log" 2>/dev/null; then
            grep -F "$marker" "$LOG_DIR/test-service.log" | tail -n 1 | tee "$ARTIFACTS_DIR/completion.txt"
            return 0
        fi

        local now
        now="$(date +%s)"
        if (( now - last_log >= 30 )); then
            log "waiting$(reaction_progress)"
            last_log="$now"
        fi
        sleep "$POLL_INTERVAL_SECS"
    done

    log "ERROR: completion marker was not observed"
    tail -n 200 "$LOG_DIR/test-service.log" || true
    return 1
}

snapshot_results() {
    local id body
    for id in $TEST_REACTION_IDS; do
        body="$(curl -sS "http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}/reactions/${id}" 2>/dev/null || true)"
        [[ -n "$body" ]] && printf '%s\n' "$body" > "$ARTIFACTS_DIR/final_reaction_state__${id}.json"
    done

    body="$(curl -sS "http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}" 2>/dev/null || true)"
    [[ -n "$body" ]] && printf '%s\n' "$body" > "$ARTIFACTS_DIR/final_test_run_status.json"

    local verdict="$DATA_CACHE/test_runs/$TEST_RUN_ID/determinism_verdict.json"
    if [[ -f "$verdict" ]]; then
        cp "$verdict" "$ARTIFACTS_DIR/determinism_verdict.json"
    fi

    local metrics_dir="$ARTIFACTS_DIR/performance_metrics"
    mkdir -p "$metrics_dir"
    find "$DATA_CACHE" -path '*output_log/performance_metrics/*.json' -type f -print0 2>/dev/null |
        while IFS= read -r -d '' metrics_file; do
            local rid
            rid="$(jq -r '.test_run_reaction_id // "unknown"' "$metrics_file" 2>/dev/null | awk -F'.' '{print $NF}')"
            cp "$metrics_file" "$metrics_dir/${rid}.json"
        done
}

write_run_summary() {
    local summary_md="$ARTIFACTS_DIR/summary.md"
    local summary_json="$ARTIFACTS_DIR/run-summary.json"
    local verdict_file="$ARTIFACTS_DIR/determinism_verdict.json"

    {
        echo "## E2E test summary — \`building_comfort.grpc_standard\`"
        echo
        echo "- test run: \`$TEST_RUN_ID\`"
        echo "- transport: Aspire-managed external Drasi Server (gRPC source/reactions)"
        echo "- drasi-server endpoint: \`${DRASI_SERVER_URL%/}\`"
        echo "- source ingress: \`${DRASI_SOURCE_HOST}:${DRASI_SOURCE_PORT}\`"
        echo "- redis: provisioned by Aspire"
        echo

        echo "### Reactions"
        echo
        echo "| Reaction | Status | Records | Runtime | SHA-256 | Determinism |"
        echo "| --- | --- | ---: | --- | --- | --- |"

        local id state_file status invocations runtime sha verdict_passed verdict_cell sha_short
        for id in $TEST_REACTION_IDS; do
            state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"
            status="n/a"; invocations="n/a"; runtime="n/a"; sha="n/a"
            if [[ -s "$state_file" ]]; then
                status="$(jq -r '.reaction_observer.status // "n/a"' "$state_file" 2>/dev/null)"
                invocations="$(jq -r '.reaction_observer.result_summary.reaction_invocation_count // "n/a"' "$state_file" 2>/dev/null)"
                runtime="$(jq -r '.reaction_observer.result_summary.observer_runtime_s // "n/a"' "$state_file" 2>/dev/null)"
                sha="$(jq -r '
                    (.reaction_observer.logger_results[]?
                        | select(.logger_name == "DeterminismHash")
                        | .summary.sha256) // "n/a"' "$state_file" 2>/dev/null)"
            fi

            verdict_cell="-"
            if [[ -s "$verdict_file" ]]; then
                verdict_passed="$(jq -r --arg id "$id" '.results[$id].passed // empty' "$verdict_file" 2>/dev/null)"
                case "$verdict_passed" in
                    true)  verdict_cell="✅ pass" ;;
                    false) verdict_cell="❌ fail" ;;
                esac
            fi

            sha_short="${sha:0:12}"
            [[ "$sha" == "n/a" ]] && sha_short="n/a"
            echo "| \`$id\` | $status | $invocations | $runtime | \`$sha_short\` | $verdict_cell |"
        done
        echo

        echo "### Throughput"
        echo
        echo "| Reaction | Records | Duration (s) | Records/sec |"
        echo "| --- | ---: | ---: | ---: |"
        local metrics_file rid records duration rps
        while IFS= read -r -d '' metrics_file; do
            rid="$(jq -r '.test_run_reaction_id // "unknown"' "$metrics_file" 2>/dev/null | awk -F'.' '{print $NF}')"
            records="$(jq -r '.record_count // "n/a"' "$metrics_file" 2>/dev/null)"
            duration="$(jq -r '(.duration_ns // 0) / 1e9 | . * 1000 | round / 1000' "$metrics_file" 2>/dev/null)"
            rps="$(jq -r '.records_per_second // "n/a" | if type == "number" then . * 100 | round / 100 else . end' "$metrics_file" 2>/dev/null)"
            echo "| \`$rid\` | $records | $duration | $rps |"
        done < <(find "$ARTIFACTS_DIR/performance_metrics" -name '*.json' -type f -print0 2>/dev/null | sort -z)
        echo

        if [[ -s "$verdict_file" ]]; then
            echo "### Determinism verdict"
            echo
            echo '```json'
            jq '.' "$verdict_file" 2>/dev/null || cat "$verdict_file"
            echo '```'
        fi
    } > "$summary_md"

    jq -n \
        --arg scenario "building_comfort" \
        --arg variant "grpc_standard" \
        --arg test_run_id "$TEST_RUN_ID" \
        --arg transport "aspire-external-drasi-server-grpc" \
        --slurpfile verdict "$verdict_file" \
        --arg artifacts_dir "$ARTIFACTS_DIR" '
        def maybe_verdict: if ($verdict | length) > 0 then $verdict[0] else null end;
        {
          scenario: $scenario,
          variant: $variant,
          test_run_id: $test_run_id,
          transport: $transport,
          artifacts_dir: $artifacts_dir,
          determinism: maybe_verdict
        }' > "$summary_json"
}

log "Redis is provisioned by Aspire"
wait_for_http "${DRASI_SERVER_URL%/}/health" "Drasi Server health endpoint" 180
write_test_config
apply_server_components
start_test_service
wait_for_completion
snapshot_results
write_run_summary
log "Completed building_comfort grpc_standard POC"

if [[ "$KEEP_ALIVE_AFTER_COMPLETION" == "true" ]]; then
    log "Keeping the runner available for Azure Container Apps result inspection"
    wait "$SERVICE_PID"
fi
