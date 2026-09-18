#!/usr/bin/env bash
set -euo pipefail

config="$1"
queries="$2"
selected="$3"
golden="$4"
api="$5"
work="$(cd "$(dirname "$config")" && pwd)"
golden="$(cd "$golden" && pwd)"

jq -e --argjson selected "$selected" '([.[].id] | sort) == ($selected | sort)' "$queries" >/dev/null
jq -e -n --slurpfile config "$config" --slurpfile queries "$queries" --slurpfile workload "$golden/workload.json" '
    def logical_sources: map(del(.source_change_dispatchers)) | sort_by(.test_source_id);
    $workload[0].schema_version == 1 and
    $workload[0].normalization == "full-row-multiset-v1" and
    ($workload[0].sources | logical_sources) == ($config[0].data_store.test_repos[0].local_tests[0].sources | logical_sources) and
    $workload[0].queries == $queries[0]
' >/dev/null || { printf 'Golden workload does not match this run.\n' >&2; exit 1; }

fingerprint="$(jq -cS . "$golden/workload.json" | shasum -a 256 | awk '{print $1}')"
baseline="$work/recovery-golden.json"
gzip -dc "$golden/golden-actual.json.gz" > "$baseline"
jq -e --arg fingerprint "$fingerprint" --argjson selected "$selected" '
    .schema_version == 1 and .workload_fingerprint == $fingerprint and
    ([.queries[].query_id] | sort) == ($selected | sort)
' "$baseline" >/dev/null

handler_queries='[]'
http_queries='[]'
while IFS= read -r query; do
    id="$(printf '%s' "$query" | jq -r .id)"
    query_fingerprint="$(printf '%s\n' "$query" | jq -cS . | shasum -a 256 | awk '{print $1}')"
    jq -e --arg id "$id" --arg fingerprint "$query_fingerprint" '
        [.queries[] | select(.query_id == $id)] | length == 1 and
                .[0].config_fingerprint == $fingerprint and
                (.[0].identity_contract == null or .[0].identity_contract == "grpc-query-sequence-row-operation-v1")
    ' "$baseline" >/dev/null
    contract="$(jq -c --arg id "$id" '.queries[] | select(.query_id == $id) | .identity_contract' "$baseline")"
    transport="$(jq -er --arg id "$id" '
        [.data_store.test_repos[0].local_tests[0].reactions[] | select(.test_reaction_id == $id) | .output_handler.kind] |
        if length == 1 and (.[0] == "Grpc" or .[0] == "Http") then .[0] else error("Unsupported or missing reaction transport") end
    ' "$config")"
    payload_pointer='/payload/request_body/result'
    if [[ "$transport" == Http ]]; then
        contract=null
        payload_pointer='/payload/request_body/request_body'
        http_queries="$(printf '%s' "$http_queries" | jq --arg id "$id" '. + [$id]')"
        printf 'Query %s: shared golden snapshot; HTTP delivery identity unavailable, SHA-256 remains enforced.\n' "$id"
    fi
    handler_queries="$(printf '%s' "$handler_queries" | jq --arg id "$id" --arg fingerprint "$query_fingerprint" --arg api "$api" --argjson contract "$contract" --arg payload_pointer "$payload_pointer" '
        . + [{query_id:$id, test_reaction_id:$id, config_fingerprint:$fingerprint,
                    identity_contract:$contract,
                    identity_pointer:(if $contract == null then null else "/payload/headers/x-drasi-producer-key" end),
          query_id_pointer:"/payload/request_body/query_id", payload_pointer:$payload_pointer,
          snapshot_url:($api + "/queries/" + ($id | @uri) + "/results")}]
    ')"
done < <(jq -c '.[]' "$queries")

if [[ "$http_queries" != '[]' ]]; then
    jq --argjson ids "$http_queries" '
        .queries |= map(if (.query_id as $id | $ids | index($id)) != null
            then .identity_contract = null | .events |= map(.identity = null)
            else . end)
    ' "$baseline" > "$work/recovery-golden-http.json"
    baseline="$work/recovery-golden-http.json"
fi

jq --arg baseline "$baseline" --arg fingerprint "$fingerprint" --argjson queries "$handler_queries" '
    .data_store.test_repos[0].local_tests[0].completion_handlers |=
    (map(select(.kind != "Log" and .kind != "RecoveryResultVerification" and .kind != "RecoveryComparison")) +
       [{kind:"RecoveryResultVerification", baseline_path:$baseline, workload_fingerprint:$fingerprint,
         enforce:false, queries:$queries}] +
       map(select(.kind == "Log")))
' "$config" > "$work/recovery-config.tmp"
mv "$work/recovery-config.tmp" "$config"
printf 'Enabled advisory RecoveryResultVerification against %s (candidate golden).\n' "$golden"