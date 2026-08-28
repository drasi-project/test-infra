#!/usr/bin/env bash
set -euo pipefail

if (( $# != 3 )); then
    echo "Usage: $0 <workspace> <environment-file> <artifacts-directory>" >&2
    exit 1
fi

WORKSPACE="$(cd "$1" && pwd)"
ENV_FILE="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
ARTIFACTS_DIR="$3"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Test environment file not found: $ENV_FILE" >&2
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

DRASI_SERVER_VERSION="${DRASI_SERVER_VERSION:-}"
DRASI_SERVER_REPO="${DRASI_SERVER_REPO:-}"
DRASI_SERVER_REF="${DRASI_SERVER_REF:-}"
# Naming either a repo or a ref means "build and test this code"; an unset repo
# falls back to the canonical one but still builds from source when a ref is set.
DRASI_REPO_EXPLICIT="$DRASI_SERVER_REPO"
DRASI_REPO="${DRASI_SERVER_REPO:-drasi-project/drasi-server}"
VARIANTS="${VARIANTS:-drasi_lib http_standard http_adaptive grpc_standard grpc_adaptive}"
: "${SUITE_WORK_DIR:?SUITE_WORK_DIR is required}"
: "${PERF_PROFILE_ID:?PERF_PROFILE_ID is required}"

needs_drasi_server=false
read -r -a variant_list <<< "${VARIANTS//,/ }"
for variant in "${variant_list[@]}"; do
    if [[ -n "$variant" && "$variant" != "drasi_lib" ]]; then
        needs_drasi_server=true
        break
    fi
done

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    ca-certificates curl git jq build-essential pkg-config libssl-dev libjq-dev libonig-dev \
    libprotobuf-dev protobuf-compiler cmake clang libclang-dev lsof

# test-run-host and drasi-server enable drasi-core's `middleware-jq` feature,
# which pulls in jq-sys; jq-sys links against the system libjq. Export
# JQ_LIB_DIR now, before ANY cargo build (drasi-server source build and the
# test-service build both need it), mirroring e2e-building-comfort.yml.
libjq_so="$(find /usr/lib -name 'libjq.so' -print -quit)"
[[ -n "$libjq_so" ]] || {
    echo "libjq.so not found under /usr/lib; is libjq-dev installed?" >&2
    exit 1
}
export JQ_LIB_DIR
JQ_LIB_DIR="$(dirname "$libjq_so")"

if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 --fail --show-error --silent \
        https://sh.rustup.rs |
        sh -s -- -y --profile minimal --default-toolchain 1.88.0
fi

source "$HOME/.cargo/env"
rustup toolchain install 1.88.0 --profile minimal

mkdir -p "$ARTIFACTS_DIR" "$SUITE_WORK_DIR"
if [[ "$needs_drasi_server" == "true" ]]; then
    : "${DRASI_SERVER_BIN:?DRASI_SERVER_BIN is required for server-based variants}"
    mkdir -p "$(dirname "$DRASI_SERVER_BIN")"
fi

metadata_json="$ARTIFACTS_DIR/environment-metadata.json"
instance_metadata="$(curl -fsS \
    -H Metadata:true \
    'http://169.254.169.254/metadata/instance?api-version=2021-02-01' || echo '{}')"

jq -n \
    --arg profile_id "$PERF_PROFILE_ID" \
    --arg timestamp "$(date -u +%FT%TZ)" \
    --arg kernel "$(uname -srmo)" \
    --arg os_release "$(grep '^PRETTY_NAME=' /etc/os-release | cut -d= -f2- | tr -d '"')" \
    --arg cpu_model "$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)" \
    --argjson cpu_count "$(nproc)" \
    --argjson mem_total_kb "$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)" \
    --argjson instance "$instance_metadata" \
    '{
        profile_id: $profile_id,
        timestamp: $timestamp,
        os: {
            description: $os_release,
            kernel: $kernel
        },
        hardware: {
            cpu_model: $cpu_model,
            cpu_count: $cpu_count,
            mem_total_kb: $mem_total_kb
        },
        azure: $instance
    }' > "$metadata_json"

build_drasi_server_from_source() {
    local ref="$DRASI_SERVER_REF"
    local repo_url="https://github.com/${DRASI_REPO}.git"
    local src_dir="$SUITE_WORK_DIR/drasi-server-src"

    rm -rf "$src_dir"
    if [[ -n "$ref" ]]; then
        echo "Building drasi-server from source: repo=$DRASI_REPO ref=$ref"
        # Shallow branch/tag clone is fastest; fall back to a full clone +
        # checkout when $ref is a commit SHA (which --branch does not accept).
        if ! git clone --depth 1 --branch "$ref" "$repo_url" "$src_dir" 2>/dev/null; then
            echo "Shallow clone of ref '$ref' failed; retrying with full clone + checkout"
            rm -rf "$src_dir"
            git clone "$repo_url" "$src_dir"
            git -C "$src_dir" checkout "$ref"
        fi
    else
        echo "Building drasi-server from source: repo=$DRASI_REPO default branch"
        git clone --depth 1 "$repo_url" "$src_dir"
    fi

    local built_sha
    built_sha="$(git -C "$src_dir" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "Checked out $DRASI_REPO ($built_sha)"

    # The clone carries its own rust-toolchain.toml, so rustup auto-fetches the
    # toolchain drasi-server pins when cargo runs inside $src_dir.
    if ! ( cd "$src_dir" && cargo build --release --bin drasi-server ); then
        echo "cargo build --bin drasi-server failed; retrying default release build"
        ( cd "$src_dir" && cargo build --release )
    fi

    local built_bin="$src_dir/target/release/drasi-server"
    if [[ ! -x "$built_bin" ]]; then
        built_bin="$(find "$src_dir/target/release" -maxdepth 1 -type f -name 'drasi-server*' -perm -u+x 2>/dev/null | head -n1)"
    fi
    [[ -n "$built_bin" && -x "$built_bin" ]] || {
        echo "ERROR: cargo build did not produce a drasi-server binary" >&2
        exit 1
    }

    install -m 0755 "$built_bin" "$DRASI_SERVER_BIN"
    echo "Built drasi-server -> $DRASI_SERVER_BIN"
    "$DRASI_SERVER_BIN" --version || true
}

download_drasi_server_release() {
    local tag="$DRASI_SERVER_VERSION"
    if [[ -z "$tag" ]]; then
        tag="$(curl -fsSL "https://api.github.com/repos/${DRASI_REPO}/releases/latest" | jq -r '.tag_name')"
    fi
    [[ -n "$tag" && "$tag" != "null" ]] || {
        echo "Could not resolve the latest drasi-server release tag" >&2
        exit 1
    }

    local asset_name="drasi-server-x86_64-linux-gnu"
    curl --fail --show-error --silent --location \
        --retry 3 --retry-delay 5 --retry-all-errors \
        "https://github.com/${DRASI_REPO}/releases/download/${tag}/${asset_name}" \
        -o "$DRASI_SERVER_BIN"
    chmod +x "$DRASI_SERVER_BIN"
}

if [[ "$needs_drasi_server" == "true" ]]; then
    # Naming a repo and/or a ref selects the source build ("test this code");
    # otherwise download the release binary (default, comparable time series).
    if [[ -n "$DRASI_SERVER_REF" || -n "$DRASI_REPO_EXPLICIT" ]]; then
        build_drasi_server_from_source
    else
        download_drasi_server_release
    fi
fi

cd "$WORKSPACE/e2e-test-framework"
cargo build --release --locked --manifest-path test-service/Cargo.toml
export TEST_SERVICE_BIN="$WORKSPACE/e2e-test-framework/target/release/test-service"

cd "$WORKSPACE"
bash e2e-test-framework/examples/building_comfort/dynamic/run_test_suite.sh
