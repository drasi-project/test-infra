# Native versus adapter-backed Server transports

The Server performance workloads use these measured plugin pairs:

| Workload | Source | Reaction |
|---|---|---|
| Nightly `http_standard`, local HTTP and `perf_sweep` | `source/http` | `reaction/http` |
| Nightly `grpc_standard`, local gRPC | `source/grpc` | `reaction/grpc` |

The `reaction/log` dependency in some configurations is not on the measured
result-delivery path. The embedded `drasi_lib` scenario is a different deployment
model and is not substituted for a Server measurement.

`run_comparison.py` runs the existing Building Comfort generator and HTTP/gRPC
observers against the same local Server binary in two configurations:

- **adapters**: ordinary source/query/reaction configuration and the existing
  dynamic Source/Reaction plugins.
- **native**: native network source/sink factories surrounding the same
  continuous-query evaluator in a ComputationGraph.

The native plugin is `drasi-computation-network`, using native ABI 1.0. Existing
network plugins retain their separate ABI 0.15. Both families are loaded into the
same Server binary; neither comparison arm selects the removed ComponentGraph
execution engine.

## Build matching local artifacts

Use the existing sibling checkouts and the same release build profile:

```sh
# In drasi-core:
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=3
cargo build --release --lib -p drasi-computation-network --features dynamic-plugin
for package in drasi-source-http drasi-source-grpc drasi-reaction-http drasi-reaction-grpc; do
    cargo build --release --lib -p "$package" --features "$package/dynamic-plugin"
done

# In drasi-server:
cargo build --release --bin drasi-server --example native-perf-config

# In test-infra/e2e-test-framework:
cargo build --release -p test-service
```

Create a dedicated plugin directory containing the native network library and
the four existing network libraries. For macOS these are:

```text
libdrasi_computation_network.dylib
libdrasi_source_http.dylib
libdrasi_source_grpc.dylib
libdrasi_reaction_http.dylib
libdrasi_reaction_grpc.dylib
```

Use the corresponding `.so` or `.dll` files on other platforms. Do not mix
downloaded plugins with locally modified SDK builds.

## Run a comparison

The output directory must not already exist. Supply absolute paths or paths
relative to your current directory:

```sh
python3 run_comparison.py \
  --server /path/to/drasi-server/target/release/drasi-server \
  --test-service /path/to/test-infra/e2e-test-framework/target/release/test-service \
  --config-generator /path/to/drasi-server/target/release/examples/native-perf-config \
  --native-plugin /path/to/drasi-core/target/release/libdrasi_computation_network.dylib \
  --plugins /path/to/comparison-plugins \
  --output /path/to/new-comparison-results \
  --transport both --queries rooms --changes 100000 --warmups 1 --runs 3
```

The driver starts the existing test-service with manual source activation,
waits for the receiver endpoints and Server components, and then starts the
unchanged workload through the test-service API. Each run gets separate ports,
configuration, working directory and retained evidence. Processes are stopped
using their exact owned PIDs. The driver does not invoke the older local runner
with its broad process-killing preflight, download binaries, or change branches.

The default rooms-only profile uses the same room query as `perf_sweep` and
expects `change_count - 19` results for the fixed 1-building/3-floor/4-room
generator. `--queries both` enables the existing floor aggregate as well and
requires the calibrated 100,000-input workload: 99,981 room results and 49,860
aggregate results.

The initial comparison is standard batching with volatile source/query state.
HTTP requests are single-event and gRPC uses the existing streaming dispatcher;
gRPC reaction batches contain one result. Query bootstrap is disabled because
the generator sends the complete initial graph as ordinary ordered inserts.
The source, query input/output and reaction queue settings are explicit in the
recorded configurations. Adaptive batching, persistent indexes, WAL recovery and
state-store profiles are not silently converted to this profile.

## What the result means

Each adapter/native pair must produce exactly the same record count and
order-sensitive `DeterminismHash` for each query. The source must finish the
entire requested workload without skips or errors. The driver retains the
published fixture hashes as provenance, but compares the two actual local arms
instead of claiming a different release's hashes are a current baseline.

Timing uses the existing `PerformanceMetrics` logger, from the first observed
result through observer completion. Initial-load and steady-state metrics are
both retained. Warmups are excluded from medians; execution order alternates.
`summary.json` is written with `equivalent: true` only after every pair passes.
A negative `native_elapsed_change_percent` means lower elapsed time.

This is an **end-to-end native-versus-adapter path comparison**, not a claim that
all timing differences are serializer cost alone. The old plugin path includes
its existing subscription/result queues and forwarding tasks. The native path
uses direct graph pipes and native FFI operations. The rooms-only profile avoids
a further difference: ordinary two-query configuration owns separate query
tasks, whereas two queries in one native graph share a graph controller.
Two-query timings therefore include that scheduling difference.

The wire protocol, query text, generator seed/data, selected queries and output
meaning remain fixed. Native FFI still serializes envelopes and validates
schemas; removing adapters does not promise better throughput.

Evidence includes all generated configs, binary/plugin hashes, process IDs,
loaded-plugin metadata, source and observer states, complete logs, raw metrics,
ordered hashes and every run's results. `--jsonl` additionally records per-event
diagnostics in both arms, but changes the measured workload's I/O overhead.
The driver rejects changed binaries or a changed plugin directory before
publishing a successful comparison. Configuration-generator errors and failed
startup process IDs are retained alongside the logs.

## Initial local measurement

On 2026-09-24, matching local release builds on macOS 26.6.2 arm64 produced
the following rooms-only results. Each arm used 100,000 input events, 99,981
output records, queue capacity 10,000 and no per-event JSONL logging. There was
one warmup pair followed by three measured pairs, with alternating run order.

| Transport | Adapter median | Native median | Native elapsed-time change |
|---|---:|---:|---:|
| HTTP | 13.093 s | 21.575 s | +64.8% |
| gRPC | 11.972 s | 20.462 s | +70.9% |

Every pair matched exact output counts and ordered hashes. Separate 100,000-input
two-query runs also matched both 99,981 room results and 49,860 floor-aggregate
results on each transport.

**The initial native implementation is slower in this local end-to-end comparison.**
This establishes a comparable baseline, not an adapter-removal speedup or a
measurement of adapter cost in isolation. These numbers apply to this build and
machine; rerun after changes rather than treating them as a release benchmark.
The original evidence is retained in the session artifacts
`native-perf-paired-100k-rooms/` and `native-perf-paired-100k-both/`.

## Driver checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s . -p 'test_*.py'
```

These checks verify workload selection, exact result/source counts, hash gates
and invalid measurements. They do not replace the real Server comparison.
