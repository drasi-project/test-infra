#!/bin/bash
# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# building_comfort throughput sweep.
#
# Generates a patched test-service config from config.base.json for the
# requested knobs, then hands it to the shared local runner. Only the per-room
# query is exercised so the expected result count is exact (see EXPECTED below);
# RecordCount is the only stop trigger kind available, and an over-estimate
# hangs the run until TIMEOUT_SECS rather than failing fast.
#
# Knobs (env vars):
#   CHANGE_COUNT      300000    total source change events (incl. initial inserts)
#   BUILDINGS         1         buildings
#   FLOORS            3         floors per building
#   ROOMS             4         rooms per floor
#   ADAPTIVE          0         1 = AdaptiveHttpSourceChangeDispatcher
#   BATCH_EVENTS      0         1 = POST to /events/batch (adaptive only)
#   BATCH_SIZE        1000      adaptive max batch size
#   BATCH_TIMEOUT_MS  50        adaptive max wait
#   LOG_JSONL         0         1 = re-enable JsonlFile dispatcher + logger
#   LABEL             auto      row label in results.csv
#
# Anything not listed is inherited from the committed drasi_server_http
# scenario, so a run with all defaults is the standard-HTTP baseline at 300k.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

CHANGE_COUNT="${CHANGE_COUNT:-300000}"
BUILDINGS="${BUILDINGS:-1}"
FLOORS="${FLOORS:-3}"
ROOMS="${ROOMS:-4}"
ADAPTIVE="${ADAPTIVE:-0}"
BATCH_EVENTS="${BATCH_EVENTS:-0}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
BATCH_TIMEOUT_MS="${BATCH_TIMEOUT_MS:-50}"
LOG_JSONL="${LOG_JSONL:-0}"

# Every change event updates exactly one room (BuildingGraph::generate_update ->
# update_random_room), so the per-room query emits one result per update plus
# one "added" per room during the initial insert burst:
#
#   elements = B + B*F (floors) + B*F*R (rooms)
#                + B*F (BUILDING_FLOOR rels) + B*F*R (FLOOR_ROOM rels)
#   expected = B*F*R + (CHANGE_COUNT - elements)
#
# For the committed 1x3x4 @ 100000 this yields 99981, matching the value
# hard-coded in drasi_server_http/config.json.
ROOMS_TOTAL=$(( BUILDINGS * FLOORS * ROOMS ))
FLOORS_TOTAL=$(( BUILDINGS * FLOORS ))
ELEMENTS=$(( BUILDINGS + 2 * FLOORS_TOTAL + 2 * ROOMS_TOTAL ))
EXPECTED=$(( ROOMS_TOTAL + CHANGE_COUNT - ELEMENTS ))

if (( EXPECTED <= ROOMS_TOTAL )); then
    echo "CHANGE_COUNT=$CHANGE_COUNT is too small for a ${BUILDINGS}x${FLOORS}x${ROOMS} graph ($ELEMENTS elements)" >&2
    exit 1
fi

if (( ADAPTIVE == 1 )); then
    DEFAULT_LABEL="adaptive_b${BATCH_SIZE}_t${BATCH_TIMEOUT_MS}$( (( BATCH_EVENTS == 1 )) && echo _batched )"
else
    DEFAULT_LABEL="standard"
fi
LABEL="${LABEL:-$DEFAULT_LABEL}"

echo "=== perf sweep: $LABEL ==="
echo "  graph            ${BUILDINGS}x${FLOORS}x${ROOMS} = ${ROOMS_TOTAL} rooms"
echo "  change_count     ${CHANGE_COUNT}"
echo "  expected records ${EXPECTED}"
echo "  adaptive         ${ADAPTIVE} (batch_events=${BATCH_EVENTS} size=${BATCH_SIZE} timeout=${BATCH_TIMEOUT_MS}ms)"
echo "  jsonl logging    ${LOG_JSONL}"
echo

GEN="$SCRIPT_DIR/config.generated.json"

jq \
  --argjson cc "$CHANGE_COUNT" \
  --argjson b "$BUILDINGS" \
  --argjson f "$FLOORS" \
  --argjson r "$ROOMS" \
  --argjson expected "$EXPECTED" \
  --argjson rooms_total "$ROOMS_TOTAL" \
  --argjson adaptive "$ADAPTIVE" \
  --argjson batch_events "$BATCH_EVENTS" \
  --argjson batch_size "$BATCH_SIZE" \
  --argjson batch_timeout "$BATCH_TIMEOUT_MS" \
  --argjson log_jsonl "$LOG_JSONL" '
  .data_store.test_repos[0].local_tests[0] |= (
      .sources[0].model_data_generator.change_count = $cc
    | .sources[0].model_data_generator.building_count = [$b, 0]
    | .sources[0].model_data_generator.floor_count    = [$f, 0]
    | .sources[0].model_data_generator.room_count     = [$r, 0]
    | .reactions[0].stop_triggers[0].record_count     = $expected
    | .sources[0].source_change_dispatchers = (
        [
          if $adaptive == 1 then
            {
              kind: "Http",
              url: "http://localhost",
              port: 9000,
              timeout_seconds: 60,
              source_id: "facilities-db",
              adaptive_enabled: true,
              batch_events: ($batch_events == 1),
              batch_size: $batch_size,
              batch_timeout_ms: $batch_timeout
            }
          else
            {
              kind: "Http",
              url: "http://localhost",
              port: 9000,
              timeout_seconds: 60,
              batch_events: false
            }
          end
        ]
        + (if $log_jsonl == 1
           then [{ kind: "JsonlFile", max_lines_per_file: 15000 }]
           else [] end)
      )
  )
  | .test_run_host.test_runs[0].reactions[0].output_loggers = (
      [{ kind: "PerformanceMetrics", bootstrap_record_count: $rooms_total }]
      + (if $log_jsonl == 1
         then [{ kind: "JsonlFile", max_lines_per_file: 15000 }]
         else [] end)
    )
' config.base.json > "$GEN" || { echo "failed to generate config" >&2; exit 1; }

# The adaptive dispatcher POSTs to /sources/<source_id>/events[/batch]; the
# standard one uses the plugin's default route. Keep the server's source id
# aligned either way.
export AUTO_DRASI_SERVER="${AUTO_DRASI_SERVER:-1}"

source "$SCRIPT_DIR/../_local_runner.sh"
export RUST_LOG="${RUST_LOG:-warn}"

START=$(date +%s)
run_local_test "$GEN"
RC=$?
END=$(date +%s)

# Pull the authoritative numbers out of the metrics file the logger wrote.
METRICS="$(find "$SCRIPT_DIR/test_data_cache" -path '*building-comfort*' -name 'performance_metrics_*.json' 2>/dev/null | sort | tail -n1)"

RESULTS="$SCRIPT_DIR/results.csv"
if [[ ! -f "$RESULTS" ]]; then
    echo "label,change_count,rooms,adaptive,batch_events,batch_size,batch_timeout_ms,log_jsonl,expected,records,duration_s,rec_per_s,bootstrap_rec_per_s,steady_rec_per_s,wall_s" > "$RESULTS"
fi

if [[ -n "$METRICS" && -s "$METRICS" ]]; then
    read -r RECORDS DUR RPS BRPS SRPS <<<"$(jq -r '
        [ .record_count,
          (.duration_ns / 1e9 * 100 | round / 100),
          (.records_per_second | round),
          (.bootstrap.records_per_second // 0 | round),
          (.steady_state.records_per_second // 0 | round)
        ] | @tsv' "$METRICS")"

    echo
    echo "=========================================="
    printf "  %-22s %s\n" "label"            "$LABEL"
    printf "  %-22s %s / %s\n" "records"     "$RECORDS" "$EXPECTED"
    printf "  %-22s %s s\n" "duration"       "$DUR"
    printf "  %-22s %s\n" "records/sec"      "$RPS"
    printf "  %-22s %s\n" "bootstrap rec/s"  "$BRPS"
    printf "  %-22s %s\n" "steady rec/s"     "$SRPS"
    echo "=========================================="

    if [[ "$RECORDS" != "$EXPECTED" ]]; then
        echo "  WARNING: record count != expected — results not comparable (event loss?)" >&2
    fi

    echo "$LABEL,$CHANGE_COUNT,$ROOMS_TOTAL,$ADAPTIVE,$BATCH_EVENTS,$BATCH_SIZE,$BATCH_TIMEOUT_MS,$LOG_JSONL,$EXPECTED,$RECORDS,$DUR,$RPS,$BRPS,$SRPS,$((END-START))" >> "$RESULTS"
    echo "  appended to $RESULTS"
else
    echo "  no metrics file found (run failed or timed out)" >&2
    echo "$LABEL,$CHANGE_COUNT,$ROOMS_TOTAL,$ADAPTIVE,$BATCH_EVENTS,$BATCH_SIZE,$BATCH_TIMEOUT_MS,$LOG_JSONL,$EXPECTED,ERROR,,,,,$((END-START))" >> "$RESULTS"
fi

exit $RC
