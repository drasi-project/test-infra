const assert = require('node:assert/strict');
const test = require('node:test');
const reportPerformance = require('../report_performance.cjs');

function regression(key = 'series-key', runId = '100') {
  return {
    key,
    series: { dimensions: { scenario: 'building_comfort', variant: 'drasi_lib' },
      runner: 'ubuntu-latest', workflow: 'e2e-building-comfort.yml', params: {} },
    run: { run_id: runId, started_at: '2026-09-22T00:00:00Z',
      url: `https://github.com/drasi-project/test-infra/actions/runs/${runId}` },
    versions: { test_infra_sha: 'abcdef' },
    metric: 'throughput', subject: 'comfort', unit: 'records/s', direction: 'higher_is_better',
    current: 70, baseline: 100, degradation_percent: 30, threshold_percent: 20,
    baseline_samples: [{ value: 100, run: { run_id: '99', started_at: '2026-09-21T00:00:00Z',
      url: 'https://github.com/drasi-project/test-infra/actions/runs/99' } }],
  };
}

function fixture(issues = [], comments = []) {
  const calls = [];
  const github = {
    rest: { issues: {
      listForRepo: 'listForRepo', listComments: 'listComments',
      createLabel: async args => { calls.push({ method: 'createLabel', args }); },
      create: async args => {
        calls.push({ method: 'create', args });
        return { data: { ...args, number: 75 + calls.filter(call => call.method === 'create').length } };
      },
      createComment: async args => { calls.push({ method: 'createComment', args }); },
    } },
    paginate: async (method, args) => {
      calls.push({ method, args });
      return method === 'listForRepo' ? [...issues] : [...comments];
    },
  };
  return {
    calls, github, context: { repo: { owner: 'drasi-project', repo: 'test-infra' } },
    core: { warning() {}, notice() {} },
  };
}

test('no regressions makes no API calls', async () => {
  const client = fixture();
  await reportPerformance({ ...client, report: { regressions: [] } });
  assert.equal(client.calls.length, 0);
});

test('creates an issue with current and historical evidence', async () => {
  const client = fixture();
  await reportPerformance({ ...client, report: { regressions: [regression()] } });
  const created = client.calls.find(call => call.method === 'create').args;
  assert.equal(created.repo, 'test-infra');
  assert.deepEqual(created.labels, ['performance-regression']);
  assert.match(created.body, /70\.00 \| 100\.00 \| 30\.0%/);
  assert.match(created.body, /actions\/runs\/99/);
  assert.match(created.body, /performance-observation:series-key:100/);
});

test('comments on the matching open issue, ignoring pull requests', async () => {
  const client = fixture([
    { number: 1, body: '<!-- performance-regression:other-key -->' },
    { number: 2, body: '<!-- performance-regression:series-key -->', pull_request: {} },
    { number: 3, body: '<!-- performance-regression:series-key -->' },
  ]);
  await reportPerformance({ ...client, report: { regressions: [regression()] } });
  assert.equal(client.calls.find(call => call.method === 'createComment').args.issue_number, 3);
  assert.equal(client.calls.filter(call => call.method === 'create').length, 0);
});

test('rerun is idempotent whether recorded in issue or comment', async () => {
  const marker = '<!-- performance-regression:series-key -->';
  const observation = '<!-- performance-observation:series-key:100 -->';
  for (const inBody of [true, false]) {
    const client = fixture([{ number: 3, body: marker + (inBody ? observation : '') }],
      inBody ? [] : [{ body: observation }]);
    await reportPerformance({ ...client, report: { regressions: [regression()] } });
    assert.equal(client.calls.filter(call => ['create', 'createComment'].includes(call.method)).length, 0);
  }
});

test('different series open independent issues and duplicate observations do not', async () => {
  const client = fixture();
  await reportPerformance({ ...client, report: { regressions: [regression(), regression(), regression('azure-key')] } });
  assert.equal(client.calls.filter(call => call.method === 'create').length, 2);
  assert.equal(client.calls.filter(call => call.method === 'createComment').length, 0);
});

test('an existing label is accepted but permission errors surface', async () => {
  for (const errorStatus of [422, 403]) {
    const client = fixture();
    client.github.rest.issues.createLabel = async () => {
      const error = new Error('API error');
      error.status = errorStatus;
      throw error;
    };
    const result = reportPerformance({ ...client, report: { regressions: [regression()] } });
    if (errorStatus === 403) await assert.rejects(result, /API error/);
    else await result;
  }
});