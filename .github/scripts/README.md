# Performance regression alerts

The scheduled results publisher compares new summaries with the history in
[drasi-project/test-results](https://github.com/drasi-project/test-results).
It opens issues in **test-infra**, labeled `performance-regression`. It does
not change test execution, determinism baselines, or SHA-256 failure handling.

## Initial policy

Configure the policy in [performance-regression.json](../performance-regression.json).
The initial settings are conservative starting values, not statistically
calibrated confidence bounds:

- Metric: each reaction's `records_per_sec`, for both drasi-lib and Drasi Server.
- Alert: throughput drops **more than 20%** below the baseline.
- Baseline: median of the latest **7** comparable successful scheduled runs,
  requiring at least **5** valid measurements within **30 days** of the current run.
- The current run, future runs, manual runs, failures, and timeouts are excluded
  from baselines. Only successful scheduled current runs are evaluated.
- Multiple attempts of one historical run count once; its latest attempt wins.
- SHA-256 values and determinism verdicts are not inspected by the detector.
  A hash mismatch alone never creates a performance alert. A run marked failed
  by existing test behavior is skipped, regardless of why it failed.
- Missing, negative, nonnumeric, and nonfinite metrics are skipped, never
  interpreted as zero. A measured zero is valid; a zero baseline cannot support
  a percentage comparison and is skipped.

Comparison profiles include the schema version, all `dimensions`, `params`
(when present), workflow name, full `run.runner`, reaction IDs, and per-reaction
record counts. Each reaction has its own baseline. Different workloads, targets,
transports, reaction sets, counts, and runner profiles do not share baselines.
Binary versions and commit SHAs deliberately do not partition the baseline:
changes in those are exactly what performance comparisons should detect.

`ubuntu-latest` stays separate from Azure. Future Azure runners must emit a
stable hardware-specific `run.runner` (including VM SKU and disk profile), not
just `self-hosted`. A new profile needs its own minimum history before alerting.
The existing Azure publisher is wired to the same checks, without pooling data.

The current emitter does not populate `params`. If workload settings become
variable on scheduled runs, emit them there or give the workload a new variant
before comparing it. The detector cannot distinguish unrecorded settings.
Changes to a workflow name, schema, or recorded profile also start a new baseline.

## Alert lifecycle

[check_performance.py](check_performance.py) emits a JSON comparison report and
Markdown step summary. [report_performance.cjs](report_performance.cjs) consumes
the report and handles GitHub issues, independent of metric extraction.

- Open one issue per comparison profile, reaction, and metric.
- Comment on that open issue for subsequent regressing runs. Hidden markers
  prevent duplicate issues/comments for the same run, including reruns.
- Include current and baseline values, percentage degradation, threshold,
  runner details, current versions, and links to the current and baseline runs.
- Leave issues open for human investigation; recovery does not auto-close them.
  A later breach after closure opens a new issue.
- Publish `performance-regression-report` as a workflow artifact and report
  skip reasons in the step summary, including insufficient baseline history.

Alerts are advisory: neither a breach nor a reporting failure fails the tests
or blocks result publishing. The GitHub token needs `issues: write` in the
calling publish job and reusable workflow. It creates issues in test-infra;
the existing GitHub App continues to write results to test-results and does not
need additional permissions. Enable GitHub notifications for the issues/repo
to receive notifications outside Actions. No email, chat, or on-call integration
is configured.

The median and sample minimum reduce sensitivity to noisy shared runners but
do not eliminate noise. The 20% threshold may need tuning after observing real
alerts. Rolling baselines can absorb sustained or gradual slowdowns, so these
alerts are not a substitute for long-term trend review or a pinned benchmark.

## Tune a workload threshold

Add an override to a metric's `overrides` array:

```json
{
  "match": {
    "scenario": "building_comfort",
    "variant": "drasi_lib",
    "runner": "ubuntu-latest",
    "reaction_id": "building-comfort"
  },
  "threshold_percent": 25
}
```

Matches are exact, all keys must match, and the last matching override wins.
Omit keys to cover multiple variants or runners. Selectors may use any dimension
plus `runner`, `workflow`, and `reaction_id` (`totals` for a totals-scoped metric).

## Add a metric later

First emit a numeric measurement in the result summaries. Then add a metric
entry; no changes to the comparator or GitHub notifier are needed for fields
under `reactions[]` or `totals`:

```json
{
  "name": "p95_latency",
  "scope": "reactions",
  "path": "latency.p95_ms",
  "unit": "ms",
  "direction": "lower_is_better",
  "threshold_percent": 20,
  "overrides": []
}
```

This is an example, not an enabled metric. `path` is relative to a reaction or
`totals` and accepts nested dot-separated fields. Directions are
`higher_is_better` and `lower_is_better`; scope is `reactions` or `totals`.
New metrics warm up independently because older records lack measurements.
Other notification channels can consume the same JSON report's `regressions`
array without implementing baseline selection again.

## Local checks

From the repository root:

```sh
python3 -m unittest discover -s .github/scripts/tests -v
node --test .github/scripts/tests/report_performance.test.cjs
python3 .github/scripts/check_performance.py \
  --current /path/to/current-summaries \
  --history /path/to/test-results/results \
  --config .github/performance-regression.json \
  --output /tmp/performance-report.json \
  --markdown /tmp/performance-report.md
```

Both readers recurse through date and runner directories. The current run may
already be in history: it is explicitly excluded by run ID. The comparator
does not call GitHub, and a regression does not change its exit code. API tests
use mocks and never create real issues. Both test suites run in the PR lint
workflow.