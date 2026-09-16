# Receiver Rejection and Reaction Recovery

This scenario keeps Drasi Server, its source and queries, and the test-service
running. It rejects HTTP/gRPC result deliveries before capture, waits for the affected
Drasi reaction to report a delivery failure under `Strict`, then stops/starts
only that reaction. The endpoint is restored before the start request. Existing
checkpoints, configuration, query indexes, and recorded outputs are retained.

## Launch From GitHub Actions

Use [E2E - building_comfort recovery](../../../../.github/workflows/e2e-building-comfort-recovery.yml)
after publishing the updated workflow, runner, and test-service code:

| Input | Selection |
|-------|-----------|
| `recovery_scenario` | `receiver_rejection` |
| `http_standard`, `http_adaptive`, `grpc_standard`, `grpc_adaptive` | Select any combination; at least one is required |
| `bootstrap_size` | `off` |
| `persist_index`, `state_store` | Both `true` |
| `golden_snapshot` | `building-comfort-small-v1` for the shared advisory comparison |
| Query selections | Both enabled when using that golden |
| `outbox_capacity` | Start with `20000`; retention must cover the outage backlog |

Keep the same server/core/plugin versions used by the other recovery scenarios.
The workflow rebuilds test-service with fault-injection support; the new scenario
does not require changing Drasi core or plugins. The selected build must support
strict reaction delivery recovery and reaction start/stop. The `recovery_signal`
and shutdown timeout inputs do not control this scenario. Existing server restart
runs remain the default and retain their signal-specific artifacts.

## Fault Boundary and Checks

Each selected HTTP/gRPC receiver accepts at least 100 result items before arming its
one-time rejection. Whole requests are accepted or rejected at this boundary;
a request can cross the threshold without being split. Subsequent requests
receive HTTP 503 or gRPC `success=false`, `items_processed=0` before conversion,
invocation counting, producer metadata capture, or logger delivery. HTTP single
requests and adaptive batch envelopes share the guard; a rejected batch produces
no captured items. Both gRPC source variants use the same protected gRPC receiver.
The guard also protects the gRPC streaming processing path.

The runner requires all of the following for each selected receiver/reaction:

1. Positive evidence of successful pre-outage acceptance and rejected requests.
2. A Drasi reaction `Error` whose message identifies delivery failure.
3. Running queries, with the original server and test-service processes alive.
4. Successful reaction stop/start API responses and a running reaction afterward.
5. A subsequent delivery passing the restored receiver guard.
6. Existing completion-marker and count/hash checks; count-settle is not success.

It does not send SIGTERM/SIGKILL during injection, reapply components, clear
checkpoints, restart the receiver, or change expected results. An outbox gap,
unexpected error, server exit, missed injection, or deadline expiry fails the
scenario. `Strict` is selected explicitly; no automatic skip/reset fallback is
used. Catch-up behavior and checkpoint correctness are tested through observed
delivery and result checks, not direct inspection of the stored checkpoint.

The optional golden is the same as for SIGKILL/SIGTERM. Its comparison remains
advisory, with unchanged per-query ordered exactly-once policy and existing
capture-completeness limitations. A passing runner is not a general proof of
exactly-once delivery or absence of arbitrarily late extra output.
HTTP state comparison uses the same golden snapshots, while HTTP delivery-identity
diagnostics remain inconclusive without compatible producer metadata. This change
does not add HTTP producer identities or change comparison policy.

This is application-level receiver rejection, not a physical listener shutdown,
network partition, lost acknowledgment after acceptance, or storage fault.
Retention of 20,000 outputs is a starting configuration, not a guarantee that
every build/runner can recover within that window. No full Drasi end-to-end run
has been verified for this new scenario yet.

## Artifacts and Local Controls

The artifact is named `recovery-receiver-rejection-<variant>` for each selected variant.
Under its `receiver-rejection/artifacts` directory:

- `receiver-recovery.json`: orchestration outcome, original server PID, and
  reaction restart count. `receiver_restored` describes fault recovery, not the
  separate final correctness verdict.
- `receiver-rejection/targets.json`: selected reaction IDs and receiver ports.
- Per-port JSON files: rejected request counts, accepted items before outage,
  before/after reaction state, query status, and lifecycle API responses.
- Existing test/server logs, final result states, determinism verdicts, and
  optional golden comparison artifacts.

For local runs, set `VARIANT` to any of the four HTTP/gRPC variants, `CRASH_INJECT=receiver_rejection`,
and `OUTBOX_CAPACITY=20000`, then invoke the existing variant runner with normal
server/plugin settings. The runner creates a fresh `RECEIVER_REJECTION_DIR` and
exports it only to its child processes. Do not set it for ordinary runs. The
receiver writes atomic per-port evidence files; the runner creates a per-port
restore marker only after the expected failure. Do not edit executing scripts.

From the repository root:

```bash
bash e2e-test-framework/examples/building_comfort/dynamic/test_receiver_rejection.sh
ruby e2e-test-framework/examples/building_comfort/dynamic/test_recovery_workflow.rb
cd e2e-test-framework
cargo test --locked -p test-run-host receiver_rejection --lib
```