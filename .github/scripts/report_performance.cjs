const label = 'performance-regression';

function issueBody(check, marker, observation) {
  const dimensions = check.series.dimensions;
  const lines = [
    marker,
    observation,
    `## ${check.metric} regression`,
    '',
    `- Workload: ${dimensions.scenario} / ${dimensions.variant}`,
    `- Runner: ${check.series.runner}`,
    `- Workflow: ${check.series.workflow}`,
    `- Reaction: ${check.subject}`,
    `- Run: ${check.run.url}`,
    `- Started: ${check.run.started_at}`,
    '',
    '| Metric | Current | Baseline median | Degradation | Threshold |',
    '| --- | ---: | ---: | ---: | ---: |',
    `| ${check.metric} (${check.unit}) | ${check.current.toFixed(2)} | ${check.baseline.toFixed(2)} | ${check.degradation_percent.toFixed(1)}% | >${check.threshold_percent}% |`,
    '',
    `Direction: ${check.direction}. Baseline: ${check.baseline_samples.length} comparable successful scheduled runs.`,
    '',
    '### Baseline evidence',
    '',
    '| Run | Started | Value |',
    '| --- | --- | ---: |',
    ...check.baseline_samples.map(sample =>
      `| [${sample.run.run_id}](${sample.run.url}) | ${sample.run.started_at} | ${sample.value.toFixed(2)} |`),
    '',
    '### Versions',
    '',
    '```json',
    JSON.stringify(check.versions, null, 2),
    '```',
    '',
    '### Comparison profile',
    '',
    '```json',
    JSON.stringify(check.series, null, 2),
    '```',
    '',
    'Results: https://github.com/drasi-project/test-results',
    'Dashboard: https://drasi-project.github.io/test-results/',
    '',
    'This is an advisory performance alert. SHA-256 verdicts do not trigger these alerts.',
    'Investigate before changing the threshold or closing the issue. Recovery does not automatically close it.',
  ];
  return lines.join('\n');
}

module.exports = async function reportPerformance({ github, context, core, report }) {
  if (!report.regressions.length) return;
  const repo = context.repo;
  try {
    await github.rest.issues.createLabel({
      ...repo, name: label, color: 'b60205',
      description: 'Automated performance regression against recent comparable runs',
    });
  } catch (error) {
    if (error.status !== 422) throw error;
  }
  const issues = await github.paginate(github.rest.issues.listForRepo, {
    ...repo, state: 'open', labels: label, per_page: 100,
  });
  for (const check of report.regressions) {
    const marker = `<!-- performance-regression:${check.key} -->`;
    const observation = `<!-- performance-observation:${check.key}:${check.run.run_id} -->`;
    const existing = issues.find(issue => !issue.pull_request && (issue.body || '').includes(marker));
    const body = issueBody(check, marker, observation);
    core.warning(`${check.metric} degraded ${check.degradation_percent.toFixed(1)}%: ` +
      `${check.series.dimensions.variant} / ${check.series.runner} / ${check.subject}`);
    if (existing) {
      const args = { ...repo, issue_number: existing.number };
      if ((existing.body || '').includes(observation)) continue;
      const comments = await github.paginate(github.rest.issues.listComments, { ...args, per_page: 100 });
      if (comments.some(comment => (comment.body || '').includes(observation))) continue;
      await github.rest.issues.createComment({ ...args, body });
      core.notice(`Updated performance regression issue #${existing.number}`);
    } else {
      const dimensions = check.series.dimensions;
      const title = `[Performance regression] ${dimensions.scenario} / ${dimensions.variant} / ` +
        `${check.series.runner} / ${check.subject} / ${check.metric}`;
      const created = await github.rest.issues.create({
        ...repo, title: title.slice(0, 256), body, labels: [label],
      });
      issues.push(created.data);
      core.notice(`Opened performance regression issue #${created.data.number}`);
    }
  }
};