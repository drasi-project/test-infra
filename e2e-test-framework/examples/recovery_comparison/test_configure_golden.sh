#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
directory="$(mktemp -d "${TMPDIR:-/tmp}/golden-config-test.XXXXXX")"
golden="$here/goldens/local-20260915-I1FGSs"
config="$directory/config.json"
queries="$directory/queries.json"
jq '.queries' "$golden/workload.json" > "$queries"
jq '{data_store:{test_repos:[{local_tests:[{sources:.sources, reactions:[.queries[] | {test_reaction_id:.id,output_handler:{kind:"Grpc"}}], completion_handlers:[{kind:"Sha256Determinism"},{kind:"Log"}]}]}]}}' "$golden/workload.json" > "$config"
bash "$here/configure_golden.sh" "$config" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$golden" http://localhost:8090/api/v1
jq -e '.data_store.test_repos[0].local_tests[0].completion_handlers |
    map(.kind) == ["Sha256Determinism","RecoveryResultVerification","Log"] and
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
jq -e '.data_store.test_repos[0].local_tests[0].completion_handlers[1].queries |
    all(.[]; .identity_contract == "grpc-query-sequence-row-operation-v1" and .identity_pointer == "/payload/headers/x-drasi-producer-key")' "$config" >/dev/null
golden="$here/goldens/local-20260915-I1FGSs"
jq '.queries' "$golden/workload.json" > "$queries"
jq '{data_store:{test_repos:[{local_tests:[{sources:.sources, reactions:[.queries[] | {test_reaction_id:.id,output_handler:{kind:"Grpc"}}], completion_handlers:[{kind:"Sha256Determinism"},{kind:"Log"}]}]}]}}' "$golden/workload.json" > "$config"
bash "$here/configure_golden.sh" "$config" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$golden" http://localhost:8090/api/v1
jq -e '.data_store.test_repos[0].local_tests[0].completion_handlers[1] |
    .policy.delivery == "exactly_once" and .policy.allow_reordering == false and .enforce == false and
    (.queries | all(.[]; .identity_contract == "grpc-query-sequence-row-operation-v1" and
       .identity_pointer == "/payload/headers/x-drasi-producer-key"))' "$config" >/dev/null
mkdir "$directory/invalid-golden"
cp "$golden/workload.json" "$directory/invalid-golden/"
gzip -dc "$golden/golden-actual.json.gz" |
    jq '.queries[].identity_contract = "unsupported-contract"' |
    gzip > "$directory/invalid-golden/golden-actual.json.gz"
if bash "$here/configure_golden.sh" "$config" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$directory/invalid-golden" http://localhost:8090/api/v1; then
    printf 'FAIL: accepted unsupported identity contract\n' >&2
    exit 1
fi
for variant in config config.grpc_adaptive config.http config.http_adaptive; do
    mkdir "$directory/$variant"
    variant_config="$directory/$variant/config.json"
    cp "$here/../building_comfort/dynamic/$variant.json" "$variant_config"
    bash "$here/configure_golden.sh" "$variant_config" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$golden" http://localhost:8090/api/v1
    jq '[.data_store.test_repos[0].local_tests[0].completion_handlers[] | select(.kind == "RecoveryResultVerification")][0]' "$variant_config" > "$directory/handler.json"
    jq -e '.policy.delivery == "exactly_once" and .policy.allow_reordering == false and .enforce == false' "$directory/handler.json" >/dev/null
    case "$variant" in
        config.http*)
            jq -e 'all(.queries[]; .identity_contract == null and .identity_pointer == null and .payload_pointer == "/payload/request_body/request_body")' "$directory/handler.json" >/dev/null
            ;;
        *)
            jq -e 'all(.queries[]; .identity_contract == "grpc-query-sequence-row-operation-v1" and .identity_pointer == "/payload/headers/x-drasi-producer-key" and .payload_pointer == "/payload/request_body/result")' "$directory/handler.json" >/dev/null
            ;;
    esac
    used_baseline="$(jq -r .baseline_path "$directory/handler.json")"
    jq -e -s '([.[0].queries[]|{query_id,snapshot}] | sort_by(.query_id)) == ([.[1].queries[]|{query_id,snapshot}] | sort_by(.query_id))' \
        "$directory/recovery-golden.json" "$used_baseline" >/dev/null
    jq '.data_store.test_repos[0].local_tests[0].sources[0].model_data_generator.change_count = 1' "$variant_config" > "$directory/$variant/changed.json"
    if bash "$here/configure_golden.sh" "$directory/$variant/changed.json" "$queries" '["building-comfort","building-comfort-floor-agg"]' "$golden" http://localhost:8090/api/v1; then
        printf 'FAIL: accepted changed event count for %s\n' "$variant" >&2
        exit 1
    fi
done
printf 'PASS: current golden, all four transports, unchanged snapshots, strict policy, and workload guards. Artifacts: %s\n' "$directory"