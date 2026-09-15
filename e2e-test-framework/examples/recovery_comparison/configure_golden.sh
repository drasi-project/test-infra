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
    $workload[0].schema_version == 1 and
    $workload[0].normalization == "full-row-multiset-v1" and
    $workload[0].sources == $config[0].data_store.test_repos[0].local_tests[0].sources and
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
while IFS= read -r query; do
    id="$(printf '%s' "$query" | jq -r .id)"
    query_fingerprint="$(printf '%s\n' "$query" | jq -cS . | shasum -a 256 | awk '{print $1}')"
    jq -e --arg id "$id" --arg fingerprint "$query_fingerprint" '
        [.queries[] | select(.query_id == $id)] | length == 1 and
                .[0].config_fingerprint == $fingerprint and
                (.[0].identity_contract == null or .[0].identity_contract == "grpc-query-sequence-row-operation-v1")
    ' "$baseline" >/dev/null
        contract="$(jq -c --arg id "$id" '.queries[] | select(.query_id == $id) | .identity_contract' "$baseline")"
        handler_queries="$(printf '%s' "$handler_queries" | jq --arg id "$id" --arg fingerprint "$query_fingerprint" --arg api "$api" --argjson contract "$contract" '
        . + [{query_id:$id, test_reaction_id:$id, config_fingerprint:$fingerprint,
                    identity_contract:$contract,
                    identity_pointer:(if $contract == null then null else "/payload/headers/x-drasi-producer-key" end),
          query_id_pointer:"/payload/request_body/query_id", payload_pointer:"/payload/request_body/result",
          snapshot_url:($api + "/queries/" + ($id | @uri) + "/results")}]
    ')"
done < <(jq -c '.[]' "$queries")

jq --arg baseline "$baseline" --arg fingerprint "$fingerprint" --argjson queries "$handler_queries" '
    .data_store.test_repos[0].local_tests[0].completion_handlers |=
      (map(select(.kind != "Log" and .kind != "RecoveryComparison")) +
       [{kind:"RecoveryComparison", baseline_path:$baseline, workload_fingerprint:$fingerprint,
         policy:{delivery:"exactly_once",allow_reordering:false}, enforce:false, queries:$queries}] +
       map(select(.kind == "Log")))
' "$config" > "$work/recovery-config.tmp"
mv "$work/recovery-config.tmp" "$config"
printf 'Enabled advisory RecoveryComparison against %s (candidate golden).\n' "$golden"