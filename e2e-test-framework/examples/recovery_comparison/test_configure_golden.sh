#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
directory="$(mktemp -d "${TMPDIR:-/tmp}/golden-config-test.XXXXXX")"
golden="$here/goldens/local-20260914-LGboor"
config="$directory/config.json"
queries="$directory/queries.json"
jq '.queries' "$golden/workload.json" > "$queries"
jq '{data_store:{test_repos:[{local_tests:[{sources:.sources, completion_handlers:[{kind:"Sha256Determinism"},{kind:"Log"}]}]}]}}' "$golden/workload.json" > "$config"
bash "$here/configure_golden.sh" "$config" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$golden" http://localhost:8090/api/v1
jq -e '.data_store.test_repos[0].local_tests[0].completion_handlers |
    map(.kind) == ["Sha256Determinism","RecoveryComparison","Log"] and
    .[1].enforce == false and .[1].policy.delivery == "exactly_once" and
    .[1].policy.allow_reordering == false and (.[1].queries | length == 2)' "$config" >/dev/null
if bash "$here/configure_golden.sh" "$config" "$queries" '["building-comfort"]' "$golden" http://localhost:8090/api/v1; then
    printf 'FAIL: accepted incompatible query selection\n' >&2
    exit 1
fi
jq '.data_store.test_repos[0].local_tests[0].sources[0].model_data_generator.seed = 1' "$config" > "$directory/changed.json"
if bash "$here/configure_golden.sh" "$directory/changed.json" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$golden" http://localhost:8090/api/v1; then
    printf 'FAIL: accepted changed workload seed\n' >&2
    exit 1
fi
printf 'PASS: strict policy, advisory mode, handler ordering, and workload guards. Artifacts: %s\n' "$directory"