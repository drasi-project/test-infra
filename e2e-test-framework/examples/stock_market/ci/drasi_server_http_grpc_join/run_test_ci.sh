#!/usr/bin/env bash
# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Scheduled / CI runner for the stock_market / drasi_server_http_grpc_join
# E2E test. Two sources (HTTP + gRPC) feed a single Cypher query — a
# synthetic multi-source join — and emit results via an HTTP reaction.
#
# Responsibilities:
#   1. Obtain the drasi-server binary: download a release, build from a
#      branch/tag/SHA ($DRASI_SERVER_REF) of $DRASI_REPO, or use a pre-set
#      DRASI_SERVER_BIN.
#   2. Patch the example configs so the run is CI-safe (port collision, keep
#      artifacts on shutdown).
#   3. Start drasi-server and the test-service as background processes.
#   4. Verify both source drains and the final query snapshot, then stop the run.
#   5. Tear down both processes and copy artifacts to $ARTIFACTS_DIR.
#
# Required tools: bash, jq, curl, ruby, python3, cargo. Either `gh` (preferred) or `curl`
# is used to fetch the release.
#
# Environment variables (with defaults):
#   VARIANT               drasi_server_http_grpc_join (default) or
#                         drasi_server_http_grpc_join_adaptive (both source dispatchers).
#   BATCHING_SPEED        Adaptive max batch size: 5000, 10000, or 50000 (10000).
#   DRASI_REPO            GitHub repo (owner/name) for the release download or
#                         the source build. Default: drasi-project/drasi-server.
#                         Point at a fork (e.g. myuser/drasi-server) to build a
#                         fork branch.
#   DRASI_SERVER_VERSION  Release tag to download. Default: latest (release mode).
#   DRASI_SERVER_REF      Branch/tag/SHA to BUILD drasi-server from source with
#                         cargo. An explicit DRASI_REPO also selects a source
#                         build; an empty ref then uses that repo's default branch.
#   DRASI_SERVER_BIN      Pre-built binary; skips both download and source build.
#   TEST_SERVICE_BIN      Pre-built test-service binary; otherwise uses cargo run.
#   DRASI_ADMIN_PORT      Admin port to patch into drasi_server_config.yaml. Default: 8090
#   DRASI_HTTP_PORT       HTTP source port. Default: 9000
#   DRASI_GRPC_PORT       gRPC source port. Default: 50051
#   TEST_SERVICE_PORT     test-service REST API port. Default: 63123
#   TEST_RUN_ID           Full run id used by the API: test_repo_id.test_id.test_run_id
#                         Default: drasi_server_dev_repo.stock_market.test_run_001
#   TEST_REACTION_IDS     Space-separated list of test_reaction_id values to
#                         snapshot at completion.
#                         Default: "stock-market-join"
#   TIMEOUT_SECS          Max seconds to wait for source drain and snapshot equality.
#                         Default: 1800
#   WORKLOAD_SIZE         Stock-trade changes to generate. Must be at least 100000.
#                         Default: 100000
#   QUERY_TUNING          Query capacity: 1000, 10000, or 100000. Default: 10000
#   PERSIST_INDEX         Enable the built-in RocksDB index and source WALs.
#                         Default: false
#   STATE_STORE           Enable the redb plugin state store. Default: false
#   WAL_MAX_EVENTS        Source WAL retention when PERSIST_INDEX=true. Default: 500000
#   DRASI_PLUGIN_REGISTRY OCI registry for short plugin refs. Empty = server default
#   DRASI_PLUGIN_TAG      OCI tag appended to untagged plugin refs. Empty = untagged
#   RENDER_CONFIG_ONLY    Render and validate scratch configs, then exit. Default: false
#   ARTIFACTS_DIR         Where to copy outputs. Default: ./ci_artifacts
#   WORK_DIR              Scratch dir. Default: ./.ci_work

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives at examples/stock_market/ci/drasi_server_http_grpc_join/ —
# five levels below the repo root.
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"

VARIANT="${VARIANT:-drasi_server_http_grpc_join}"
BATCHING_SPEED="${BATCHING_SPEED:-10000}"
DRASI_REPO_EXPLICIT="${DRASI_REPO:-}"
DRASI_REPO="${DRASI_REPO:-drasi-project/drasi-server}"
DRASI_SERVER_VERSION="${DRASI_SERVER_VERSION:-}"
DRASI_SERVER_REF="${DRASI_SERVER_REF:-}"
DRASI_ADMIN_PORT="${DRASI_ADMIN_PORT:-8090}"
DRASI_HTTP_PORT="${DRASI_HTTP_PORT:-9000}"
DRASI_GRPC_PORT="${DRASI_GRPC_PORT:-50051}"
TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-63123}"
TEST_RUN_ID="${TEST_RUN_ID:-drasi_server_dev_repo.stock_market.test_run_001}"
TEST_REACTION_IDS="${TEST_REACTION_IDS:-watchlist-prices}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"
WORKLOAD_SIZE="${WORKLOAD_SIZE:-100000}"
QUERY_TUNING="${QUERY_TUNING:-10000}"
PERSIST_INDEX="${PERSIST_INDEX:-false}"
STATE_STORE="${STATE_STORE:-false}"
WAL_MAX_EVENTS="${WAL_MAX_EVENTS:-}"
DRASI_PLUGIN_REGISTRY="${DRASI_PLUGIN_REGISTRY:-}"
DRASI_PLUGIN_TAG="${DRASI_PLUGIN_TAG:-}"
RENDER_CONFIG_ONLY="${RENDER_CONFIG_ONLY:-false}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/ci_artifacts}"
WORK_DIR="${WORK_DIR:-$SCRIPT_DIR/.ci_work}"

LOG_DIR="$WORK_DIR/logs"
DOWNLOAD_DIR="$WORK_DIR/drasi-server-download"
SRC_BUILD_DIR="$WORK_DIR/drasi-server-src"
DATA_CACHE="$WORK_DIR/test_data_cache"
DRASI_CFG_SRC="$SCRIPT_DIR/drasi_server_config.yaml"
TEST_CFG_SRC="$SCRIPT_DIR/config.json"
DRASI_CFG_CI="$WORK_DIR/drasi_server_config.ci.yaml"
TEST_CFG_CI="$WORK_DIR/config.ci.json"

mkdir -p "$WORK_DIR" "$LOG_DIR" "$ARTIFACTS_DIR"

DRASI_PID=""
SERVICE_PID=""
# Human-readable description of where DRASI_SERVER_BIN came from (release tag,
# source build, or preset). Surfaced in the step summary for result labeling.
DRASI_BUILD_SOURCE=""

log() { echo "[ci] $*"; }

resolve_variant() {
    case "$VARIANT" in
        drasi_server_http_grpc_join) ADAPTIVE_ENABLED=false ;;
        drasi_server_http_grpc_join_adaptive) ADAPTIVE_ENABLED=true ;;
        *)
            log "ERROR: unsupported stock-market variant: $VARIANT"
            return 1
            ;;
    esac
    case "$BATCHING_SPEED" in
        5000|10000|50000) ;;
        *)
            log "ERROR: BATCHING_SPEED must be 5000, 10000, or 50000"
            return 1
            ;;
    esac
    log "Variant: $VARIANT (adaptive_dispatchers=$ADAPTIVE_ENABLED)"
}

resolve_workload() {
    if [[ ! "$WORKLOAD_SIZE" =~ ^[1-9][0-9]*$ ]] || (( WORKLOAD_SIZE < 100000 )); then
        log "ERROR: WORKLOAD_SIZE must be an integer of at least 100000"
        return 1
    fi

    REACTION_RECORD_COUNT=$(( WORKLOAD_SIZE * 3 / 4 ))
    local minimum_wal_events=$(( WORKLOAD_SIZE + 1000 ))
    if [[ -z "$WAL_MAX_EVENTS" ]]; then
        WAL_MAX_EVENTS=500000
    elif [[ ! "$WAL_MAX_EVENTS" =~ ^[1-9][0-9]*$ ]]; then
        log "ERROR: WAL_MAX_EVENTS must be a positive integer"
        return 1
    fi
    if (( WAL_MAX_EVENTS < minimum_wal_events )); then
        WAL_MAX_EVENTS=$minimum_wal_events
    fi

    log "Workload: changes=$WORKLOAD_SIZE measurement_records=$REACTION_RECORD_COUNT wal_max_events=$WAL_MAX_EVENTS"
}

cleanup() {
    local exit_code=$?
    set +e
    for pid_name in SERVICE_PID DRASI_PID; do
        pid="${!pid_name}"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            log "Stopping $pid_name (pid=$pid)"
            kill -TERM "$pid" 2>/dev/null
            for _ in $(seq 1 30); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -KILL "$pid" 2>/dev/null
        fi
    done

    # Best-effort artifact collection.
    [[ -d "$DATA_CACHE" ]] && { rm -rf "$ARTIFACTS_DIR/test_data_cache"; cp -R "$DATA_CACHE" "$ARTIFACTS_DIR/test_data_cache" 2>/dev/null; }
    [[ -d "$LOG_DIR"   ]] && { rm -rf "$ARTIFACTS_DIR/logs";            cp -R "$LOG_DIR"   "$ARTIFACTS_DIR/logs" 2>/dev/null; }
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

wait_for_port() {
    local host="$1" port="$2" name="$3" timeout="${4:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if (echo > "/dev/tcp/$host/$port") >/dev/null 2>&1; then
            log "$name is listening on $host:$port"
            return 0
        fi
        sleep 1
    done
    log "ERROR: $name did not start listening on $host:$port within ${timeout}s"
    return 1
}

download_drasi_server() {
    if [[ -n "${DRASI_SERVER_BIN:-}" ]]; then
        log "Using pre-set DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
        DRASI_BUILD_SOURCE="preset ${DRASI_SERVER_BIN}"
        return 0
    fi

    if [[ -n "$DRASI_REPO_EXPLICIT" || -n "$DRASI_SERVER_REF" ]]; then
        build_drasi_server_from_source
        return 0
    fi

    download_drasi_server_release
}

# Build drasi-server from a branch/tag/SHA of $DRASI_REPO using cargo, then set
# DRASI_SERVER_BIN to the freshly built binary. Point DRASI_REPO at a fork to
# build fork branches; $DRASI_SERVER_REF may be a branch, tag, or commit SHA.
build_drasi_server_from_source() {
    local ref="$DRASI_SERVER_REF"
    local repo_url="https://github.com/${DRASI_REPO}.git"
    local display_ref="${ref:-default branch}"
    log "Building drasi-server from source: repo=$DRASI_REPO ref=$display_ref"

    rm -rf "$SRC_BUILD_DIR"
    # Shallow branch/tag clone is fastest; fall back to a full clone + checkout
    # when $ref is a commit SHA (which --branch does not accept).
    if [[ -z "$ref" ]]; then
        git clone --depth 1 "$repo_url" "$SRC_BUILD_DIR"
    elif ! git clone --depth 1 --branch "$ref" "$repo_url" "$SRC_BUILD_DIR" 2>/dev/null; then
        log "Shallow clone of ref '$ref' failed; retrying with full clone + checkout"
        rm -rf "$SRC_BUILD_DIR"
        git clone "$repo_url" "$SRC_BUILD_DIR"
        git -C "$SRC_BUILD_DIR" checkout "$ref"
    fi

    local built_sha
    built_sha="$(git -C "$SRC_BUILD_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    log "Checked out $DRASI_REPO @ $display_ref ($built_sha); running cargo build --release"

    if ! ( cd "$SRC_BUILD_DIR" && cargo build --release --bin drasi-server ); then
        log "cargo build --bin drasi-server failed; retrying default release build"
        ( cd "$SRC_BUILD_DIR" && cargo build --release )
    fi

    local built_bin="$SRC_BUILD_DIR/target/release/drasi-server"
    if [[ ! -x "$built_bin" ]]; then
        # Bin name may differ from the crate default; locate it under target/release.
        built_bin="$(find "$SRC_BUILD_DIR/target/release" -maxdepth 1 -type f -name 'drasi-server*' -perm -u+x 2>/dev/null | head -n1)"
    fi
    [[ -n "$built_bin" && -x "$built_bin" ]] || { log "ERROR: cargo build did not produce a drasi-server binary"; return 1; }

    DRASI_SERVER_BIN="$built_bin"
    export DRASI_SERVER_BIN
    DRASI_BUILD_SOURCE="source ${DRASI_REPO}@${display_ref} (${built_sha})"
    log "DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
    "$DRASI_SERVER_BIN" --version || true
}

download_drasi_server_release() {
    mkdir -p "$DOWNLOAD_DIR"
    cd "$DOWNLOAD_DIR"

    local tag="$DRASI_SERVER_VERSION"
    if [[ -z "$tag" ]]; then
        if command -v gh >/dev/null 2>&1; then
            tag="$(gh release view --repo "$DRASI_REPO" --json tagName -q .tagName)"
        else
            tag="$(curl -fsSL "https://api.github.com/repos/${DRASI_REPO}/releases/latest" | jq -r '.tag_name')"
        fi
    fi
    log "drasi-server release tag: $tag"

    # drasi-server publishes raw, unarchived per-target binaries named
    # `drasi-server-<arch>-<os>-<libc>`. Pick by $DRASI_TARGET (default
    # x86_64-linux-gnu) so this script also works on ARM runners.
    local target="${DRASI_TARGET:-x86_64-linux-gnu}"
    local asset_name="drasi-server-${target}"
    log "Selected asset: $asset_name"

    if command -v gh >/dev/null 2>&1; then
        gh release download "$tag" --repo "$DRASI_REPO" --pattern "$asset_name"
    else
        curl -fsSL -O "https://github.com/${DRASI_REPO}/releases/download/${tag}/${asset_name}"
    fi

    [[ -f "$asset_name" ]] || { log "ERROR: download did not produce $asset_name"; ls -la; return 1; }
    chmod +x "$asset_name"
    mv "$asset_name" drasi-server

    DRASI_SERVER_BIN="$DOWNLOAD_DIR/drasi-server"
    export DRASI_SERVER_BIN
    DRASI_BUILD_SOURCE="release ${tag} (${DRASI_REPO})"
    log "DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
    "$DRASI_SERVER_BIN" --version || true
    cd - >/dev/null
}

patch_configs() {
    log "Rendering drasi_server_config.yaml (query=$QUERY_TUNING persist_index=$PERSIST_INDEX state_store=$STATE_STORE)"
    DRASI_ADMIN_PORT="$DRASI_ADMIN_PORT" \
    QUERY_TUNING="$QUERY_TUNING" \
    PERSIST_INDEX="$PERSIST_INDEX" \
    STATE_STORE="$STATE_STORE" \
    WAL_MAX_EVENTS="$WAL_MAX_EVENTS" \
    DRASI_PLUGIN_REGISTRY="$DRASI_PLUGIN_REGISTRY" \
    DRASI_PLUGIN_TAG="$DRASI_PLUGIN_TAG" \
        ruby "$SCRIPT_DIR/render_server_config.rb" "$DRASI_CFG_SRC" "$DRASI_CFG_CI"

    log "Patching config.json: workload=$WORKLOAD_SIZE, measurement_records=$REACTION_RECORD_COUNT, data_store_path=$DATA_CACHE"
    jq --arg cache "$DATA_CACHE" \
        --arg srcroot "$SCRIPT_DIR/dev_repo" \
        --argjson workload "$WORKLOAD_SIZE" \
        --argjson reaction_stop "$REACTION_RECORD_COUNT" \
        --argjson adaptive "$ADAPTIVE_ENABLED" \
        --argjson batch_size "$BATCHING_SPEED" \
        '.data_store.data_store_path = $cache
         | .data_store.delete_on_start = false
         | .data_store.delete_on_stop = false
            | (.data_store.test_repos[]? | select(.kind == "LocalStorage") | .source_path) |= $srcroot
            | (.data_store.test_repos[]?.local_tests[]?.sources[]?
             | select(.test_source_id == "stock-trades-db")
             | .model_data_generator.change_count) = $workload
         | (.data_store.test_repos[]?.local_tests[]?.reactions[]?
             | select(.test_reaction_id == "watchlist-prices")
             | .stop_triggers) = []
         | (.test_run_host.test_runs[]?.reactions[]?
             | select(.test_reaction_id == "watchlist-prices")
             | .output_loggers[]? | select(.kind == "PerformanceMetrics")
             | .measurement_record_count) = $reaction_stop
         | (.test_run_host.test_runs[]?.reactions[]?.start_immediately) = false
         | (.test_run_host.test_runs[]?.sources[]?.start_mode) = "manual"
         | if $adaptive then
             (.data_store.test_repos[]?.local_tests[]?.sources[]?) |= (
                 .test_source_id as $source_id
                 | (.source_change_dispatchers[]? | select(.kind == "Http" or .kind == "Grpc")) |= (
                     .adaptive_enabled = true
                     | .batch_events = true
                     | .source_id = $source_id
                     | .batch_size = $batch_size
                     | .batch_timeout_ms = 50
                 )
             )
           else . end' \
        "$TEST_CFG_SRC" > "$TEST_CFG_CI"

    # Enforce deterministic inputs by requiring explicit seed(s) for model sources.
    local seed_count
    seed_count="$(jq '[.data_store.test_repos[]?.local_tests[]?.sources[]? | select(.kind == "Model") | .model_data_generator.seed? | select(. != null)] | length' "$TEST_CFG_CI")"
    if [[ "$seed_count" -eq 0 ]]; then
        log "ERROR: No model_data_generator.seed configured in $TEST_CFG_CI"
        return 1
    fi
}

start_drasi_server() {
    log "Starting drasi-server"
    (
        cd "$WORK_DIR"
        mkdir -p data
        exec "$DRASI_SERVER_BIN" --config "$DRASI_CFG_CI" \
            > "$LOG_DIR/drasi-server.log" 2>&1
    ) &
    DRASI_PID=$!
    log "drasi-server pid=$DRASI_PID"
    if ! wait_for_port 127.0.0.1 "$DRASI_HTTP_PORT" "drasi-server HTTP source"; then
        log "--- drasi-server.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/drasi-server.log" || true
        log "--- end drasi-server.log ---"
        return 1
    fi
    if ! wait_for_port 127.0.0.1 "$DRASI_GRPC_PORT" "drasi-server gRPC source"; then
        log "--- drasi-server.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/drasi-server.log" || true
        log "--- end drasi-server.log ---"
        return 1
    fi
}

start_test_service() {
    if [[ -n "${TEST_SERVICE_BIN:-}" ]]; then
        log "Starting pre-built test-service: $TEST_SERVICE_BIN"
        RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
            "$TEST_SERVICE_BIN" --config "$TEST_CFG_CI" \
            > "$LOG_DIR/test-service.log" 2>&1 &
    else
        log "Building & starting test-service"
        (
            cd "$REPO_ROOT/e2e-test-framework"
            RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
            cargo run --release --manifest-path "test-service/Cargo.toml" -- --config "$TEST_CFG_CI" \
                > "$LOG_DIR/test-service.log" 2>&1
        ) &
    fi
    SERVICE_PID=$!
    log "test-service pid=$SERVICE_PID"
    if ! wait_for_port 127.0.0.1 "$TEST_SERVICE_PORT" "test-service API" 600; then
        log "--- test-service.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/test-service.log" || true
        log "--- end test-service.log ---"
        return 1
    fi
}

fetch_final_reaction_state() {
    # fetch_final_reaction_state <test_reaction_id>
    # Snapshots the reaction's current state to $ARTIFACTS_DIR/final_reaction_state__<id>.json.
    # Require the stopped observer and its finalized measurement window.
    local reaction_id="$1"
    local state_file="$ARTIFACTS_DIR/final_reaction_state__${reaction_id}.json"
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}/reactions/${reaction_id}"
    local body status
    body="$(curl -sS "$url" 2>/dev/null || true)"
    if [[ -z "$body" ]]; then
        log "WARNING: [$reaction_id] empty response from $url"
        return 1
    fi
    echo "$body" > "$state_file"
    status="$(echo "$body" | jq -r '.reaction_observer.status // "Unknown"')"
    case "$status" in
        Stopped)
            if ! jq -e --argjson count "$REACTION_RECORD_COUNT" '
                .reaction_observer.error_message == null and
                ([.reaction_observer.logger_results[]? | select(.logger_name == "PerformanceMetrics")] |
                    length == 1 and .[0].has_output == true and .[0].summary.record_count == $count)
            ' "$state_file" >/dev/null; then
                log "ERROR: [$reaction_id] missing or incomplete performance metrics"
                return 1
            fi
            log "[$reaction_id] final state: Stopped; measurement complete"
            return 0 ;;
        Error)   log "ERROR: [$reaction_id] final state: Error"; return 1 ;;
        *)       log "ERROR: [$reaction_id] final state: $status (expected Stopped)"; return 1 ;;
    esac
}

print_summary() {
    local id state_file
    for id in $TEST_REACTION_IDS; do
        state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"

        echo "::group::Final reaction state [$id]"
        if [[ -s "$state_file" ]]; then
            jq '{
                id: .id,
                status: .reaction_observer.status,
                handler_status: .reaction_observer.handler_status,
                error_message: .reaction_observer.error_message,
                result_summary: .reaction_observer.result_summary,
                logger_results: .reaction_observer.logger_results
            }' "$state_file" 2>/dev/null || cat "$state_file"

            local runtime invocations
            runtime="$(jq -r '.reaction_observer.result_summary.observer_runtime_s // "unknown"' "$state_file" 2>/dev/null || echo unknown)"
            invocations="$(jq -r '.reaction_observer.result_summary.reaction_invocation_count // "unknown"' "$state_file" 2>/dev/null || echo unknown)"
            log "[$id] Observer runtime: $runtime  Reaction invocations: $invocations"
        else
            log "[$id] No final_reaction_state file available"
        fi
        echo "::endgroup::"
    done

    echo "::group::Performance metrics output"
    local found=0
    while IFS= read -r -d '' metrics_file; do
        found=1
        log "--- $metrics_file ---"
        jq '.' "$metrics_file" 2>/dev/null || cat "$metrics_file"
    done < <(find "$DATA_CACHE" -path '*output_log/performance_metrics/*.json' -type f -print0 2>/dev/null || true)

    if (( found == 0 )); then
        log "No performance_metrics JSON files found under $DATA_CACHE"
    fi
    echo "::endgroup::"
}

verify_test_run_status() {
    # Best-effort snapshot of the overall test-run state.
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}"
    local body
    body="$(curl -sS --max-time 5 "$url" 2>/dev/null || true)"
    [[ -n "$body" ]] && echo "$body" > "$ARTIFACTS_DIR/final_test_run_status.json"
    return 0
}

verify_final_snapshot() {
    log "Verifying final query rows against the drained input streams"
    python3 "$SCRIPT_DIR/verify_snapshot.py" \
        --config "$TEST_CFG_CI" \
        --server-config "$DRASI_CFG_CI" \
        --service-log "$LOG_DIR/test-service.log" \
        --run-id "$TEST_RUN_ID" \
        --service-url "http://127.0.0.1:$TEST_SERVICE_PORT" \
        --admin-url "http://127.0.0.1:$DRASI_ADMIN_PORT" \
        --artifacts "$ARTIFACTS_DIR" \
        --server-pid "$DRASI_PID" \
        --service-pid "$SERVICE_PID" \
        --timeout "$TIMEOUT_SECS"
}

start_test_inputs() {
    local api="http://127.0.0.1:$TEST_SERVICE_PORT/api/test_runs/$TEST_RUN_ID"
    curl -fsS --max-time 60 -X POST "$api/reactions/watchlist-prices/start" >/dev/null || return 1
    curl -fsS --max-time 60 -X POST "$api/sources/stock-trades-db/start" >/dev/null || return 1
    curl -fsS --max-time 60 -X POST "$api/sources/watchlist-db/start" >/dev/null || return 1
}

finish_test_run() {
    curl -fsS --max-time "$TIMEOUT_SECS" -X POST \
        "http://127.0.0.1:$TEST_SERVICE_PORT/api/test_runs/$TEST_RUN_ID/stop" >/dev/null
}

write_step_summary() {
    local out="$ARTIFACTS_DIR/summary.md"
    local drasi_source="${DRASI_BUILD_SOURCE:-unknown}"
    local server_version
    server_version="$("$DRASI_SERVER_BIN" --version 2>/dev/null | head -n1 || echo unknown)"

    {
        echo "## E2E test summary — \`$TEST_RUN_ID\`"
        echo
        echo "- drasi-server source: \`$drasi_source\`"
        echo "- drasi-server binary: \`$server_version\`"
        echo "- variant: \`${VARIANT:-drasi_server_http_grpc_join}\`"
        if [[ "${ADAPTIVE_ENABLED:-false}" == "true" ]]; then
            echo "- adaptive HTTP + gRPC dispatch: max batch size=\`$BATCHING_SPEED\`, max wait=\`50 ms\`"
        fi
        echo "- workload: \`$WORKLOAD_SIZE\` stock-trade changes (measured reaction records=$REACTION_RECORD_COUNT)"
        echo "- query tuning: \`$QUERY_TUNING\`"
        echo "- server config: persistIndex=\`$PERSIST_INDEX\`, stateStore=\`$STATE_STORE\`"
        echo "- plugin registry: \`${DRASI_PLUGIN_REGISTRY:-server default}\`"
        echo "- plugin tag: \`${DRASI_PLUGIN_TAG:-latest-compatible}\`"
        echo

        echo "### Final query snapshot"
        if [[ -s "$ARTIFACTS_DIR/snapshot_verdict.json" ]]; then
            jq -r '"- Passed: \(.passed)",
                   "- Expected rows: \(.expected_rows // "unavailable"); actual rows: \(.actual_rows // "unavailable")",
                   (.error // empty)' "$ARTIFACTS_DIR/snapshot_verdict.json"
        else
            echo "Not verified: the test did not reach successful completion."
        fi
        echo

        echo "### Reactions"
        echo
        echo "| Reaction | Status | Records | Runtime |"
        echo "| --- | --- | ---: | --- |"

        local id state_file status invocations runtime
        for id in $TEST_REACTION_IDS; do
            state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"
            status="n/a"; invocations="n/a"; runtime="n/a"
            if [[ -s "$state_file" ]]; then
                status="$(jq -r '.reaction_observer.status // "n/a"' "$state_file" 2>/dev/null)"
                invocations="$(jq -r '.reaction_observer.result_summary.reaction_invocation_count // "n/a"' "$state_file" 2>/dev/null)"
                runtime="$(jq -r '.reaction_observer.result_summary.observer_runtime_s // "n/a"' "$state_file" 2>/dev/null)"
            fi

            echo "| \`$id\` | $status | $invocations | $runtime |"
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
        done < <(find "$DATA_CACHE" -path '*output_log/performance_metrics/*.json' -type f -print0 2>/dev/null || true)
        echo
    } > "$out"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" && "$GITHUB_STEP_SUMMARY" != "$out" ]]; then
        cat "$out" >> "$GITHUB_STEP_SUMMARY"
    fi
}

resolve_variant
resolve_workload
patch_configs
if [[ "$RENDER_CONFIG_ONLY" == "true" ]]; then
    log "Rendered configs under $WORK_DIR"
    exit 0
fi
download_drasi_server
start_drasi_server
start_test_service
start_test_inputs

poll_rc=0
verify_final_snapshot || poll_rc=1
finish_test_run || poll_rc=1
for id in $TEST_REACTION_IDS; do
    fetch_final_reaction_state "$id" || poll_rc=1
done
print_summary

verify_test_run_status || true
write_step_summary

exit "$poll_rc"
