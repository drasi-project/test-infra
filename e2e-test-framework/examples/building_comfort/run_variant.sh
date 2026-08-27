#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${VARIANT:?VARIANT must name a building comfort variant}"

case "$VARIANT" in
    drasi_lib)
        exec bash "$SCRIPT_DIR/ci/drasi_lib/run_test_ci.sh"
        ;;
    http_standard)
        export SERVER_SOURCE_FILE="source_http.json"
        export SERVER_REACTIONS_FILE="reactions_http.json"
        export DRASI_SOURCE_PORT=9000
        export TEST_CFG_SRC="$SCRIPT_DIR/dynamic/config.http.json"
        ;;
    http_adaptive)
        export SERVER_SOURCE_FILE="source_http_adaptive.json"
        export SERVER_REACTIONS_FILE="reactions_http.json"
        export DRASI_SOURCE_PORT=9000
        export TEST_CFG_SRC="$SCRIPT_DIR/dynamic/config.http.json"
        ;;
    http_batched)
        # HTTP with batching on BOTH sides: the framework's adaptive dispatcher
        # (config.http_adaptive.json) coalesces ingress, and the reaction
        # (reactions_http_adaptive.json) coalesces egress into {"batch":[...]}
        # POSTs. A plain server source is used because the dispatcher, not the
        # server, does the ingress batching here.
        export SERVER_SOURCE_FILE="source_http.json"
        export SERVER_REACTIONS_FILE="reactions_http_adaptive.json"
        export DRASI_SOURCE_PORT=9000
        export TEST_CFG_SRC="$SCRIPT_DIR/dynamic/config.http_adaptive.json"
        ;;
    grpc_standard)
        export SERVER_SOURCE_FILE="source_grpc.json"
        export SERVER_REACTIONS_FILE="reactions_grpc.json"
        export DRASI_SOURCE_PORT=50051
        export TEST_CFG_SRC="$SCRIPT_DIR/dynamic/config.json"
        ;;
    grpc_adaptive)
        export SERVER_SOURCE_FILE="source_grpc.json"
        export SERVER_REACTIONS_FILE="reactions_grpc.json"
        export DRASI_SOURCE_PORT=50051
        export TEST_CFG_SRC="$SCRIPT_DIR/dynamic/config.grpc_adaptive.json"
        ;;
    *)
        echo "Unsupported building comfort variant: $VARIANT" >&2
        exit 1
        ;;
esac

export SERVER_QUERIES_FILE="queries.json"
exec bash "$SCRIPT_DIR/dynamic/run_dynamic.sh"
