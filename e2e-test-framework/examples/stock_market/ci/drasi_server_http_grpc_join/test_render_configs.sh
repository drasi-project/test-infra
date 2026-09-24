#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

WORK_DIR="$TEMP_DIR/work" \
ARTIFACTS_DIR="$TEMP_DIR/artifacts" \
VARIANT= \
BATCHING_SPEED= \
RENDER_CONFIG_ONLY=true \
WORKLOAD_SIZE=250000 \
QUERY_TUNING=100000 \
PERSIST_INDEX=true \
STATE_STORE=true \
WAL_MAX_EVENTS=100000 \
DRASI_PLUGIN_REGISTRY=ghcr.io/example \
DRASI_PLUGIN_TAG=candidate \
    bash "$SCRIPT_DIR/run_test_ci.sh"

ruby -ryaml -e '
  config = YAML.load_file(ARGV.fetch(0))
  abort unless config["persistIndex"] == true
  abort unless config.dig("stateStore", "kind") == "redb"
  abort unless config["sources"].all? { |source| source.dig("durability", "max_events") == 251_000 }
  abort unless config.dig("queries", 0, "priorityQueueCapacity") == 100_000
  abort unless config["plugins"].all? { |plugin| plugin.fetch("ref").end_with?(":candidate") }
' "$TEMP_DIR/work/drasi_server_config.ci.yaml"

jq -e '
  .data_store.test_repos[0].local_tests[0] as $test
  | ($test.sources[] | select(.test_source_id == "stock-trades-db")
      | .model_data_generator.change_count) == 250000
    and ($test.reactions[] | select(.test_reaction_id == "watchlist-prices") | .stop_triggers) == []
    and (.test_run_host.test_runs[0].reactions[0].output_loggers[]
      | select(.kind == "PerformanceMetrics") | .measurement_record_count) == 187500
' "$TEMP_DIR/work/config.ci.json" >/dev/null

jq -e --slurpfile original "$SCRIPT_DIR/config.json" '
  [.data_store.test_repos[0].local_tests[0].sources[].source_change_dispatchers]
  == [$original[0].data_store.test_repos[0].local_tests[0].sources[].source_change_dispatchers]
' "$TEMP_DIR/work/config.ci.json" >/dev/null

for batch_size in 5000 10000 50000; do
    WORK_DIR="$TEMP_DIR/adaptive-$batch_size" \
    ARTIFACTS_DIR="$TEMP_DIR/artifacts-adaptive-$batch_size" \
    RENDER_CONFIG_ONLY=true \
    VARIANT=drasi_server_http_grpc_join_adaptive \
    BATCHING_SPEED="$batch_size" \
        bash "$SCRIPT_DIR/run_test_ci.sh"

    jq -e --argjson batch_size "$batch_size" --slurpfile original "$SCRIPT_DIR/config.json" '
      .data_store.test_repos[0].local_tests[0] as $test
      | [$test.sources[].source_change_dispatchers[] | select(.kind == "Http" or .kind == "Grpc")] as $dispatchers
      | ($dispatchers | length == 2)
      and ($dispatchers | all(.adaptive_enabled == true and .batch_events == true
                             and .batch_size == $batch_size and .batch_timeout_ms == 50))
      and ($dispatchers | map(.source_id) == ["stock-trades-db", "watchlist-db"])
      and ([$test.sources[].source_change_dispatchers[] | select(.kind == "JsonlFile")]
           == [$original[0].data_store.test_repos[0].local_tests[0].sources[].source_change_dispatchers[] | select(.kind == "JsonlFile")])
       and ($test.reactions[0].output_handler == $original[0].data_store.test_repos[0].local_tests[0].reactions[0].output_handler)
       and ($test.reactions[0].stop_triggers == [])
       and (.test_run_host.test_runs[0].reactions[0].output_loggers[]
         | select(.kind == "PerformanceMetrics") | .measurement_record_count) == 75000
    ' "$TEMP_DIR/adaptive-$batch_size/config.ci.json" >/dev/null
done

if WORK_DIR="$TEMP_DIR/invalid" ARTIFACTS_DIR="$TEMP_DIR/invalid-artifacts" \
    RENDER_CONFIG_ONLY=true VARIANT=unknown \
    bash "$SCRIPT_DIR/run_test_ci.sh" > "$TEMP_DIR/invalid.log" 2>&1; then
    echo "Expected an unsupported variant to fail" >&2
    exit 1
fi
grep -q 'unsupported stock-market variant' "$TEMP_DIR/invalid.log"

if WORK_DIR="$TEMP_DIR/invalid" ARTIFACTS_DIR="$TEMP_DIR/invalid-artifacts" \
    RENDER_CONFIG_ONLY=true VARIANT=drasi_server_http_grpc_join_adaptive BATCHING_SPEED=zero \
    bash "$SCRIPT_DIR/run_test_ci.sh" > "$TEMP_DIR/invalid.log" 2>&1; then
    echo "Expected an invalid batch size to fail" >&2
    exit 1
fi
grep -q 'BATCHING_SPEED must be' "$TEMP_DIR/invalid.log"