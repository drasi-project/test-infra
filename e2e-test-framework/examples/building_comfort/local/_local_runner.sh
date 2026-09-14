# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Helper sourced by building_comfort/local/<variant>/run_test*.sh scripts.
#
# Usage from a variant script:
#
#     SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#     source "$SCRIPT_DIR/../_local_runner.sh"
#     RUST_LOG='info,drasi_core::query::continuous_query=error,...' \
#       run_local_test "$SCRIPT_DIR/config.json"
#
# Behavior:
#   - Optional config patch: SHA_CHECK=1 attaches a `Sha256Determinism`
#     (expected={}, missing_baseline=Warn) completion handler + a
#     `DeterminismHash` output logger to every reaction. Requires `jq`.
#   - Auto-exit (default): launches the test-service in the background,
#     polls each reaction in the test config every POLL_INTERVAL_SECS
#     until it reaches Stopped, prints a one-shot performance summary,
#     and shuts the service down. Set AUTO_EXIT=0 to keep the legacy
#     "run as a service, Ctrl-C to stop" behavior.
#
# Knobs (env vars):
#   AUTO_EXIT            1 (default) | 0    poll-and-exit vs. run-as-service
#   AUTO_DRASI_SERVER    1 (default) | 0    auto-launch drasi-server if a
#                                           drasi_server_config.yaml sits
#                                           alongside the test config.json
#   DRASI_SERVER_BIN     unset (default)    skip download, use this binary
#   DRASI_SERVER_VERSION unset (default)    tag to download (else: latest)
#   DRASI_REPO           drasi-project/drasi-server
#   DRASI_TARGET         auto-detected      e.g. aarch64-apple-darwin
#   SHA_CHECK            1 | 0 (default)    patch SHA-256 logger + handler in
#   TIMEOUT_SECS         1800               max seconds to wait for Stopped
#   POLL_INTERVAL_SECS   5                  seconds between status polls
#   TEST_SERVICE_PORT    63123              test-service REST API port

prepare_sha_config() {
    local original="$1"
    CONFIG_PATH="$original"
    if [[ "${SHA_CHECK:-0}" != "1" ]]; then
        return 0
    fi
    if ! command -v jq >/dev/null 2>&1; then
        echo "SHA_CHECK=1 requires jq on PATH; install jq or unset SHA_CHECK" >&2
        return 1
    fi
    local tmp
    tmp="$(mktemp -t bc-local-cfg.XXXXXX)"
    jq '
      .data_store.test_repos[0].local_tests[0].completion_handlers = (
        ((.data_store.test_repos[0].local_tests[0].completion_handlers // [])
          | map(select(.kind != "Sha256Determinism")))
        + [{ "kind": "Sha256Determinism", "expected": {}, "missing_baseline": "Warn" }]
      )
      | .test_run_host.test_runs[0].reactions |= map(
          .output_loggers = (
            ((.output_loggers // [])
              | map(select(.kind != "DeterminismHash")))
            + [{"kind": "DeterminismHash"}]
          )
        )
    ' "$original" > "$tmp"
    CONFIG_PATH="$tmp"
    _LOCAL_RUNNER_TMP_CONFIG="$tmp"
    echo "SHA_CHECK=1: enabled per-reaction SHA-256 logger + Warn-only determinism handler (config patched into $tmp)" >&2
}

# Pick the right drasi-server release asset name for this host. The user can
# override with DRASI_TARGET (e.g. when running under Rosetta).
_local_runner_detect_drasi_target() {
    if [[ -n "${DRASI_TARGET:-}" ]]; then
        echo "$DRASI_TARGET"
        return 0
    fi
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"
    case "$os" in
        Darwin)
            case "$arch" in
                arm64|aarch64) echo "aarch64-apple-darwin" ;;
                x86_64)        echo "x86_64-apple-darwin" ;;
                *) echo "" ;;
            esac
            ;;
        Linux)
            case "$arch" in
                x86_64) echo "x86_64-linux-gnu" ;;
                aarch64) echo "aarch64-linux-gnu" ;;
                *) echo "" ;;
            esac
            ;;
        *) echo "" ;;
    esac
}

# Download (or reuse) a drasi-server binary into the local-runner cache
# directory $script_dir/.local_run/drasi-server-bin. Sets DRASI_SERVER_BIN.
_local_runner_prepare_drasi_server_bin() {
    local script_dir="$1"

    if [[ -n "${DRASI_SERVER_BIN:-}" ]]; then
        echo "[local] using pre-set DRASI_SERVER_BIN=$DRASI_SERVER_BIN" >&2
        return 0
    fi

    local target
    target="$(_local_runner_detect_drasi_target)"
    if [[ -z "$target" ]]; then
        echo "[local] could not auto-detect drasi-server target for $(uname -s)/$(uname -m); set DRASI_TARGET or DRASI_SERVER_BIN" >&2
        return 1
    fi

    local repo="${DRASI_REPO:-drasi-project/drasi-server}"
    local cache_dir="$script_dir/.local_run/drasi-server-bin"
    mkdir -p "$cache_dir"

    # Resolve the release tag.
    local tag="${DRASI_SERVER_VERSION:-}"
    if [[ -z "$tag" ]]; then
        if command -v gh >/dev/null 2>&1; then
            tag="$(gh release view --repo "$repo" --json tagName -q .tagName 2>/dev/null || true)"
        fi
        if [[ -z "$tag" ]]; then
            tag="$(curl -fsSL "https://api.github.com/repos/${repo}/releases/latest" 2>/dev/null | jq -r '.tag_name' 2>/dev/null || true)"
        fi
    fi
    if [[ -z "$tag" || "$tag" == "null" ]]; then
        echo "[local] could not resolve a drasi-server release tag (set DRASI_SERVER_VERSION)" >&2
        return 1
    fi

    local asset="drasi-server-${target}"
    local cached="$cache_dir/drasi-server-${tag}-${target}"

    if [[ ! -x "$cached" ]]; then
        echo "[local] downloading drasi-server $tag ($target)..." >&2
        local tmp
        tmp="$cache_dir/$asset.download"
        if command -v gh >/dev/null 2>&1 && \
           gh release download "$tag" --repo "$repo" --pattern "$asset" --output "$tmp" >/dev/null 2>&1; then
            :
        elif curl -fsSL -o "$tmp" "https://github.com/${repo}/releases/download/${tag}/${asset}"; then
            :
        else
            echo "[local] failed to download $asset for $tag" >&2
            rm -f "$tmp"
            return 1
        fi
        chmod +x "$tmp"
        mv "$tmp" "$cached"
    fi

    DRASI_SERVER_BIN="$cached"
    export DRASI_SERVER_BIN
    echo "[local] DRASI_SERVER_BIN=$DRASI_SERVER_BIN" >&2
    return 0
}

# Parse the test-service config.json for the first port a source dispatcher
# is targeting. We use it as the "drasi-server is ready" signal.
_local_runner_detect_drasi_port() {
    local config="$1"
    jq -r '
        first(
            .data_store.test_repos[]?.local_tests[]?.sources[]?.source_change_dispatchers[]?
            | select(.kind == "Http" or .kind == "Grpc" or .kind == "AdaptiveHttp" or .kind == "AdaptiveGrpc")
            | .port
        ) // empty
    ' "$config" 2>/dev/null
}

# Terminate whatever this runner launched. Registered as an EXIT/INT/TERM trap
# before the first process is spawned, so an early failure (e.g. drasi-server
# never binding its port) still tears things down.
#
# Reads pids from globals rather than the caller's locals on purpose: the trap
# runs after run_local_test_autoexit has returned, at which point its `local`
# svc_pid/ds_pid no longer exist. Referencing them there silently skipped every
# kill (and errored outright under `set -u`), stranding test-service and
# drasi-server on their ports and breaking the next run.
_local_runner_cleanup() {
    local svc_pid="${_LOCAL_RUNNER_SVC_PID:-}"
    local ds_pid="${_LOCAL_RUNNER_DS_PID:-}"

    if [[ -n "$svc_pid" ]] && kill -0 "$svc_pid" 2>/dev/null; then
        kill -TERM "$svc_pid" 2>/dev/null
        local n=0
        while kill -0 "$svc_pid" 2>/dev/null && (( n < 30 )); do
            sleep 1
            n=$((n+1))
        done
        kill -KILL "$svc_pid" 2>/dev/null
    fi

    if [[ -n "$ds_pid" ]] && kill -0 "$ds_pid" 2>/dev/null; then
        echo "[local] stopping drasi-server (pid=$ds_pid)" >&2
        kill -TERM "$ds_pid" 2>/dev/null
        local n=0
        while kill -0 "$ds_pid" 2>/dev/null && (( n < 30 )); do
            sleep 1
            n=$((n+1))
        done
        kill -KILL "$ds_pid" 2>/dev/null
    fi

    [[ -n "${_LOCAL_RUNNER_TMP_CONFIG:-}" ]] && rm -f "$_LOCAL_RUNNER_TMP_CONFIG"
    return 0
}

# Run the test-service in auto-exit mode: spawn it as a background process,
# poll each reaction's status via the REST API, print a summary when all
# reactions reach Stopped, then shut the service down.
#
# Args:
#   $1   patched (or original) test-service config path
#   $2   absolute manifest path of the test-service crate
run_local_test_autoexit() {
    local config="$1"
    local manifest="$2"

    if ! command -v jq >/dev/null 2>&1; then
        echo "AUTO_EXIT requires jq on PATH; install jq or set AUTO_EXIT=0" >&2
        return 1
    fi
    if ! command -v curl >/dev/null 2>&1; then
        echo "AUTO_EXIT requires curl on PATH; install curl or set AUTO_EXIT=0" >&2
        return 1
    fi

    local port="${TEST_SERVICE_PORT:-63123}"
    local timeout_secs="${TIMEOUT_SECS:-1800}"
    local poll_secs="${POLL_INTERVAL_SECS:-5}"

    # Extract { test_run_id, [reaction_id, ...] } pairs from the config.
    local meta
    meta="$(jq -c '
        .test_run_host.test_runs[]
        | {
            run_id: ((.test_repo_id // "") + "." + (.test_id // "") + "." + (.test_run_id // "")),
            reactions: [.reactions[]?.test_reaction_id]
          }
    ' "$config")"
    if [[ -z "$meta" ]]; then
        echo "AUTO_EXIT: no test runs found in $config; falling back to foreground mode" >&2
        return 2
    fi

    local svc_log
    svc_log="$(mktemp -t bc-local-svc.XXXXXX)"

    # Pre-flight: kill any stale test-service holding the API port. Without
    # this, a leftover process from a previous interrupted run answers the
    # REST API with the wrong test_run_id and the new cargo run can't bind
    # the port either.
    if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
        echo "[local] :$port is already in use; killing stale test-service" >&2
        pkill -f 'target/release/test-service' >/dev/null 2>&1 || true
        local n=0
        while (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1 && (( n < 10 )); do
            sleep 1
            n=$((n+1))
        done
        if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
            echo "[local] could not free :$port; check 'lsof -nP -iTCP:$port -sTCP:LISTEN'" >&2
            return 1
        fi
        echo "[local] :$port is free" >&2
    fi

    # Optionally launch drasi-server first, when a drasi_server_config.yaml
    # sits next to the test-service config and AUTO_DRASI_SERVER!=0.
    local ds_pid=""
    local ds_log=""
    # Clear any pids left over from a previous run_local_test call in this
    # same shell, so the EXIT trap never signals an unrelated process.
    _LOCAL_RUNNER_SVC_PID=""
    _LOCAL_RUNNER_DS_PID=""
    trap _local_runner_cleanup EXIT INT TERM
    local script_dir
    script_dir="$(cd "$(dirname "$config")" && pwd)"
    # _LOCAL_RUNNER_SOURCE_DIR is set by run_local_test before any tmp-config
    # patch happens so we can find drasi_server_config.yaml in the original
    # variant folder, not in /tmp.
    local variant_dir="${_LOCAL_RUNNER_SOURCE_DIR:-$script_dir}"
    local ds_yaml="$variant_dir/drasi_server_config.yaml"
    if [[ "${AUTO_DRASI_SERVER:-1}" != "0" && -f "$ds_yaml" ]]; then
        echo "[local] found $ds_yaml; launching drasi-server" >&2

        # Pre-flight: kill any stale drasi-server holding a port we need.
        # Same reasoning as the test-service stale-kill above: a crashed
        # previous run can leave drasi-server bound to its admin port
        # (typically 8080) or a source port, blocking our launch.
        local ds_admin_port
        ds_admin_port="$(sed -nE 's/^port:[[:space:]]*([0-9]+)$/\1/p' "$ds_yaml" 2>/dev/null | head -n1)"
        local ds_source_port
        ds_source_port="$(_local_runner_detect_drasi_port "$config")"
        local p
        for p in "$ds_admin_port" "$ds_source_port"; do
            [[ -z "$p" ]] && continue
            if (echo > "/dev/tcp/127.0.0.1/$p") >/dev/null 2>&1; then
                echo "[local] drasi-server port :$p is already in use; killing stale drasi-server" >&2
                pkill -f 'drasi-server' >/dev/null 2>&1 || true
                local n=0
                while (echo > "/dev/tcp/127.0.0.1/$p") >/dev/null 2>&1 && (( n < 10 )); do
                    sleep 1
                    n=$((n+1))
                done
                if (echo > "/dev/tcp/127.0.0.1/$p") >/dev/null 2>&1; then
                    echo "[local] could not free :$p; check 'lsof -nP -iTCP:$p -sTCP:LISTEN'" >&2
                    return 1
                fi
            fi
        done

        if ! _local_runner_prepare_drasi_server_bin "$variant_dir"; then
            return 1
        fi
        ds_log="$(mktemp -t bc-local-drasi.XXXXXX)"
        (
            cd "$variant_dir"
            # DRASI_SERVER_ARGS lets a caller pass extra flags (e.g.
            # --plugins-dir for a statically linked build that must not pick up
            # the cdylibs sitting next to the binary). Unquoted on purpose so
            # multiple flags word-split.
            exec "$DRASI_SERVER_BIN" --config "$ds_yaml" ${DRASI_SERVER_ARGS:-} >"$ds_log" 2>&1
        ) &
        ds_pid=$!
        _LOCAL_RUNNER_DS_PID="$ds_pid"
        echo "[local] drasi-server pid=$ds_pid; log=$ds_log" >&2

        local ds_port
        ds_port="$(_local_runner_detect_drasi_port "$config")"
        if [[ -n "$ds_port" ]]; then
            echo "[local] waiting for drasi-server source port :$ds_port..." >&2
            local ds_deadline=$(( $(date +%s) + 120 ))
            while (( $(date +%s) < ds_deadline )); do
                if (echo > "/dev/tcp/127.0.0.1/$ds_port") >/dev/null 2>&1; then
                    echo "[local] drasi-server is listening on :$ds_port" >&2
                    break
                fi
                if ! kill -0 "$ds_pid" 2>/dev/null; then
                    echo "[local] drasi-server exited before binding :$ds_port" >&2
                    tail -n 50 "$ds_log" >&2
                    return 1
                fi
                sleep 1
            done
        else
            echo "[local] could not detect drasi-server source port from config; sleeping 5s" >&2
            sleep 5
        fi
    fi

    cargo run --release --manifest-path "$manifest" -- --config "$config" \
        > "$svc_log" 2>&1 &
    local svc_pid=$!
    _LOCAL_RUNNER_SVC_PID="$svc_pid"

    echo "[local] test-service pid=$svc_pid; log=$svc_log" >&2
    echo "[local] waiting for http://127.0.0.1:$port to come up..." >&2

    local deadline=$(( $(date +%s) + 600 ))
    local first_run_id
    first_run_id="$(jq -r '.run_id' <<<"$(head -n1 <<<"$meta")")"
    while (( $(date +%s) < deadline )); do
        # Port open AND we can read our run via the REST API. The second
        # check guards against the small window where the binary has bound
        # the port but not yet registered the configured test runs.
        if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
            local probe
            probe="$(curl -sS --max-time 2 \
                "http://127.0.0.1:$port/api/test_runs/$first_run_id" 2>/dev/null \
                | jq -r '.id // empty' 2>/dev/null)"
            if [[ "$probe" == "$first_run_id" ]]; then
                echo "[local] test-service is listening on :$port and knows our run" >&2
                break
            fi
        fi
        if ! kill -0 "$svc_pid" 2>/dev/null; then
            echo "[local] test-service exited before responding" >&2
            tail -n 80 "$svc_log" >&2
            return 1
        fi
        sleep 1
    done

    echo "[local] polling reactions (timeout=${timeout_secs}s interval=${poll_secs}s)" >&2

    local end_ts=$(( $(date +%s) + timeout_secs ))
    local last_heartbeat=0
    while (( $(date +%s) < end_ts )); do
        if ! kill -0 "$svc_pid" 2>/dev/null; then
            echo "[local] test-service exited unexpectedly" >&2
            tail -n 80 "$svc_log" >&2
            return 1
        fi

        local all_stopped=1
        while IFS= read -r line; do
            local run_id reactions
            run_id="$(jq -r '.run_id' <<<"$line")"
            reactions="$(jq -r '.reactions[]' <<<"$line")"
            for r in $reactions; do
                local status
                status="$(curl -sS --max-time 5 \
                    "http://127.0.0.1:$port/api/test_runs/$run_id/reactions/$r" 2>/dev/null \
                    | jq -r '.reaction_observer.status // "Unknown"' 2>/dev/null \
                    || echo Unknown)"
                if [[ "$status" != "Stopped" ]]; then
                    all_stopped=0
                fi
            done
        done <<<"$meta"

        if (( all_stopped == 1 )); then
            echo "[local] all reactions reached Stopped" >&2
            _local_runner_summary "$port" "$meta"
            return 0
        fi

        local now elapsed
        now=$(date +%s)
        if (( now - last_heartbeat >= 30 )); then
            elapsed=$(( now - (end_ts - timeout_secs) ))
            local hb=""
            while IFS= read -r line; do
                local run_id reactions
                run_id="$(jq -r '.run_id' <<<"$line")"
                reactions="$(jq -r '.reactions[]' <<<"$line")"
                for r in $reactions; do
                    local count
                    count="$(curl -sS --max-time 5 \
                        "http://127.0.0.1:$port/api/test_runs/$run_id/reactions/$r" 2>/dev/null \
                        | jq -r '.reaction_observer.result_summary.reaction_invocation_count // "?"' 2>/dev/null \
                        || echo '?')"
                    hb+="${r}=${count} "
                done
            done <<<"$meta"
            echo "[local] t=${elapsed}s ${hb}(waiting)" >&2
            last_heartbeat=$now
        fi

        sleep "$poll_secs"
    done

    echo "[local] timed out after ${timeout_secs}s waiting for reactions to Stop" >&2
    _local_runner_summary "$port" "$meta"
    return 1
}

# Pretty-print per-reaction status / records / runtime + records-per-second.
_local_runner_summary() {
    local port="$1"
    local meta="$2"

    echo
    echo "=========================================="
    echo "  Local test-service run summary"
    echo "=========================================="
    while IFS= read -r line; do
        local run_id reactions
        run_id="$(jq -r '.run_id' <<<"$line")"
        reactions="$(jq -r '.reactions[]' <<<"$line")"
        echo "[$run_id]"
        printf "  %-32s %-9s %-12s %-15s %-15s\n" \
            REACTION STATUS RECORDS RUNTIME REC/SEC
        for r in $reactions; do
            local body status records runtime rps sha
            body="$(curl -sS --max-time 5 \
                "http://127.0.0.1:$port/api/test_runs/$run_id/reactions/$r" 2>/dev/null || echo '{}')"
            status="$(jq -r '.reaction_observer.status // "n/a"' <<<"$body")"
            records="$(jq -r '.reaction_observer.result_summary.reaction_invocation_count // "n/a"' <<<"$body")"
            runtime="$(jq -r '.reaction_observer.result_summary.observer_runtime_s // "n/a"' <<<"$body")"
            rps="$(jq -r '
                (.reaction_observer.logger_results[]?
                    | select(.logger_name == "PerformanceMetrics")
                    | .summary.records_per_second) // "n/a"
                | if type == "number" then . | . * 100 | round / 100 else . end
            ' <<<"$body")"
            sha="$(jq -r '
                (.reaction_observer.logger_results[]?
                    | select(.logger_name == "DeterminismHash")
                    | .summary.sha256) // ""
            ' <<<"$body")"
            printf "  %-32s %-9s %-12s %-15s %-15s\n" \
                "$r" "$status" "$records" "$runtime" "$rps"
            if [[ -n "$sha" ]]; then
                printf "  %-32s sha256=%s\n" "" "$sha"
            fi
        done
    done <<<"$meta"
    echo "=========================================="
    echo
}

# High-level entrypoint. Calls prepare_sha_config + either runs the test
# service in the foreground (AUTO_EXIT=0) or in poll-and-exit mode.
#
# Args:
#   $1   original config.json path
#
# Resolves the test-service manifest by walking up from $1.
run_local_test() {
    local config="$1"
    local script_dir
    script_dir="$(cd "$(dirname "$config")" && pwd)"
    _LOCAL_RUNNER_SOURCE_DIR="$script_dir"
    # local/<variant>/config.json   -> ../../../../test-service/Cargo.toml
    local manifest="$script_dir/../../../../test-service/Cargo.toml"
    if [[ ! -f "$manifest" ]]; then
        echo "Could not find test-service Cargo.toml at $manifest" >&2
        return 1
    fi
    manifest="$(cd "$(dirname "$manifest")" && pwd)/Cargo.toml"

    prepare_sha_config "$config" || return 1

    if [[ "${AUTO_EXIT:-1}" == "0" ]]; then
        echo "[local] AUTO_EXIT=0: running test-service in foreground (Ctrl-C to stop)" >&2
        # Foreground: cargo run inherits stdout/stderr. RUST_LOG must be
        # set by the caller. We intentionally do NOT trap to clear the
        # tmp config so the user can re-inspect it after Ctrl-C.
        exec cargo run --release --manifest-path "$manifest" -- --config "$CONFIG_PATH"
    fi

    run_local_test_autoexit "$CONFIG_PATH" "$manifest"
}

