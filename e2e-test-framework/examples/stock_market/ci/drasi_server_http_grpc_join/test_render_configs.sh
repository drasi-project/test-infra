#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

WORK_DIR="$TEMP_DIR/work" \
ARTIFACTS_DIR="$TEMP_DIR/artifacts" \
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
  and ($test.reactions[] | select(.test_reaction_id == "watchlist-prices")
      | .stop_triggers[] | select(.kind == "RecordCount")
      | .record_count) == 187500
' "$TEMP_DIR/work/config.ci.json" >/dev/null