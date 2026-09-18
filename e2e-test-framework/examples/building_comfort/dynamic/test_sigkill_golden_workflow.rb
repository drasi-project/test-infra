require 'yaml'
require 'open3'
require 'tmpdir'
require 'fileutils'
require 'json'

workflow = YAML.load_file(File.expand_path('../../../../.github/workflows/e2e-building-comfort-recovery.yml', __dir__))
trigger = workflow['on'] || workflow[true]
abort 'Golden selector must not be exposed' if trigger.fetch('workflow_dispatch').fetch('inputs').key?('golden_snapshot')
abort 'Workflow still references golden input' if workflow.to_s.include?('inputs.golden_snapshot')
validation = workflow.fetch('jobs').fetch('prepare').fetch('steps').find { |step| step['id'] == 'settings' }.fetch('run')
base = {
  'MINUTES' => '30', 'OUTBOX_CAPACITY' => '20000',
  'V_HTTP_STD' => 'true', 'V_HTTP_ADAPTIVE' => 'true',
  'V_GRPC_STD' => 'true', 'V_GRPC_ADAPTIVE' => 'true',
  'V_Q_MAIN' => 'true', 'V_Q_AGG' => 'true', 'PERSIST_INDEX' => 'true',
  'STATE_STORE' => 'true',
  'BOOTSTRAP_SIZE' => 'off', 'GITHUB_OUTPUT' => '/dev/null'
}
[
  [{}, true],
  [{'V_Q_MAIN' => 'false'}, false],
  [{'V_Q_AGG' => 'false'}, false],
  [{'BOOTSTRAP_SIZE' => '10k'}, false]
].each do |overrides, expected|
  output, error, result = Open3.capture3(base.merge(overrides), 'bash', '-e', '-s', stdin_data: validation)
  abort "Unexpected validation: #{overrides}: #{output} #{error}" unless result.success? == expected
end

steps = workflow.fetch('jobs').fetch('recovery').fetch('steps')
injection = steps.find { |step| step.fetch('env', {}).key?('CRASH_INJECT') }.fetch('run')
abort 'Golden must be fixed and unconditional' unless injection.lines.first.strip ==
  'export RECOVERY_GOLDEN_DIR="$GITHUB_WORKSPACE/e2e-test-framework/examples/recovery_comparison/goldens/building-comfort-small-v1"'
summary = steps.find { |step| step['name'] == 'Recovery summary' }.fetch('run')
Dir.mktmpdir('required-golden-') do |directory|
  environment = {
    'RUNNER_TEMP' => directory, 'RECOVERY_CASE' => 'sigkill-drain',
    'VARIANT' => 'grpc_standard', 'JOB_STATUS' => 'success',
    'GITHUB_STEP_SUMMARY' => File.join(directory, 'summary.md')
  }
  _, _, result = Open3.capture3(environment, 'bash', '-e', '-s', stdin_data: summary)
  abort 'Missing report passed' if result.success?
  report = File.join(directory, 'results/sigkill-drain/work/test_data_cache/test_runs/drasi_server_dev_repo.building_comfort.test_run_001/recovery_verdict.json')
  FileUtils.mkdir_p(File.dirname(report))
  comparison = JSON.parse(File.read(File.expand_path('../../recovery_comparison/goldens/building-comfort-small-v1/self-comparison.json', __dir__)))
  valid = {
    'test_run_id' => 'drasi_server_dev_repo.building_comfort.test_run_001',
    'enforced' => false, 'verdict' => comparison.fetch('verdict'), 'comparison' => comparison
  }
  check_report = lambda do |label, content, expected|
    File.write(report, content)
    File.write(environment.fetch('GITHUB_STEP_SUMMARY'), '')
    _, error, result = Open3.capture3(environment, 'bash', '-e', '-s', stdin_data: summary)
    abort "Unexpected summary result for #{label}: #{result.exitstatus}: #{error}" unless result.success? == expected
    unless expected
      abort "Missing diagnostic for #{label}" unless File.read(environment.fetch('GITHUB_STEP_SUMMARY')).include?('ERROR: required')
    end
  end
  %w[passed failed inconclusive].each do |verdict|
    body = Marshal.load(Marshal.dump(valid))
    body['verdict'] = body['comparison']['verdict'] = verdict
    body['comparison']['queries'].each do |query|
      query['delivery']['verdict'] = verdict
    end
    check_report.call("advisory #{verdict} with matching snapshots", JSON.generate(body), true)
  end
  [0, 1].each do |index|
    %w[failed inconclusive].each do |state_verdict|
      %w[passed failed inconclusive].each do |overall|
        body = Marshal.load(Marshal.dump(valid))
        body['verdict'] = body['comparison']['verdict'] = overall
        body['comparison']['queries'][index]['state']['verdict'] = state_verdict
        check_report.call("query #{index} state #{state_verdict}, overall #{overall}", JSON.generate(body), false)
      end
    end
    %w[missing unexpected].each do |field|
      body = Marshal.load(Marshal.dump(valid))
      body['comparison']['queries'][index]['state'][field] = [{'row' => {'value' => 1}, 'count' => 1}]
      check_report.call("query #{index} passed state with #{field} rows", JSON.generate(body), false)
    end
  end
  %w[missing_queries unexpected_queries].each do |field|
    body = Marshal.load(Marshal.dump(valid))
    body['comparison'][field] = ['other-query']
    check_report.call("nonempty #{field}", JSON.generate(body), false)
  end
  {
    'invalid evaluation' => ->(body) { body['verdict'] = 'invalid'; body['error'] = 'snapshot fetch failed'; body.delete('comparison') },
    'evaluation error with comparison' => ->(body) { body['error'] = 'capture import failed' },
    'missing comparison' => ->(body) { body.delete('comparison') },
    'null comparison' => ->(body) { body['comparison'] = nil },
    'missing queries' => ->(body) { body['comparison'].delete('queries') },
    'empty queries' => ->(body) { body['comparison']['queries'] = [] },
    'missing expected query' => ->(body) { body['comparison']['queries'].pop },
    'duplicate query' => ->(body) { body['comparison']['queries'][1] = body['comparison']['queries'][0] },
    'unexpected query' => ->(body) { body['comparison']['queries'][1]['query_id'] = 'other' },
    'missing state' => ->(body) { body['comparison']['queries'][0].delete('state') },
    'missing delivery' => ->(body) { body['comparison']['queries'][0].delete('delivery') },
    'invalid query verdict' => ->(body) { body['comparison']['queries'][0]['state']['verdict'] = 'invalid' },
    'wrong run' => ->(body) { body['test_run_id'] = 'another.run' },
    'wrong schema' => ->(body) { body['comparison']['schema_version'] = 2 },
    'inconsistent verdict' => ->(body) { body['comparison']['verdict'] = 'passed' }
  }.each do |label, mutate|
    body = Marshal.load(Marshal.dump(valid))
    mutate.call(body)
    check_report.call(label, JSON.generate(body), false)
  end
  ['', '{broken', 'null', '{}', '[]', "#{JSON.generate(valid)}\n#{JSON.generate(valid)}"].each do |content|
    check_report.call('malformed or incomplete document', content, false)
  end

  schema_cases = {
    ['comparison', 'workload_fingerprint'] => [nil, '', 42, []],
    ['comparison', 'reasons'] => [nil, {}, 'reason', [false]],
    ['comparison', 'missing_queries'] => [nil, {}, [1]],
    ['comparison', 'unexpected_queries'] => [nil, 'items', [nil]]
  }
  [0, 1].each do |index|
    prefix = ['comparison', 'queries', index]
    %w[state delivery].each do |section|
      schema_cases[prefix + [section, 'reason']] = [false, 1, [], {}]
    end
    %w[expected_observations actual_observations].each do |field|
      schema_cases[prefix + ['delivery', field]] = [nil, '1', -1, 0.5, true, {}]
    end
    %w[missing unexpected conflicting].each do |field|
      schema_cases[prefix + ['delivery', field]] = [nil, {}, 'identity', [1]]
    end
    schema_cases[prefix + ['delivery', 'duplicates']] = [nil, [], 'duplicates', {'id' => nil}, {'id' => -1}, {'id' => 0.5}, {'id' => '1'}, {'id' => true}]
    schema_cases[prefix + ['delivery', 'reordered']] = ['false', 0, [], {}]
    %w[missing unexpected].each do |field|
      schema_cases[prefix + ['state', field]] = [nil, {}, 'rows', [nil], [{}], [{'row' => nil}], [{'count' => 1}]]
      [nil, -1, 0.5, '1', false].each do |count|
        schema_cases[prefix + ['state', field]] << [{'row' => {'value' => 1}, 'count' => count}]
      end
    end
  end
  schema_cases.each do |path, bad_values|
    body = Marshal.load(Marshal.dump(valid))
    body.dig(*path[0...-1]).delete(path.last)
    check_report.call("missing #{path.join('.')}", JSON.generate(body), false)
    bad_values.each do |value|
      body = Marshal.load(Marshal.dump(valid))
      body.dig(*path[0...-1])[path.last] = value
      check_report.call("invalid #{path.join('.')}: #{value.inspect}", JSON.generate(body), false)
    end
  end

  body = Marshal.load(Marshal.dump(valid))
  body['comparison']['queries'].each do |query|
    query['delivery']['verdict'] = 'inconclusive'
    query['delivery']['reason'] = 'Producer identities unavailable'
    query['delivery']['reordered'] = nil
  end
  check_report.call('valid nullable diagnostics', JSON.generate(body), true)

  body = Marshal.load(Marshal.dump(valid))
  body['verdict'] = body['comparison']['verdict'] = 'failed'
  body['comparison']['reasons'] = ['Observed differences']
  body['comparison']['queries'].each do |query|
    query['delivery'].merge!('verdict' => 'failed', 'reason' => 'Delivery differs',
      'missing' => ['missing-id'], 'unexpected' => ['extra-id'],
      'duplicates' => {'duplicate-id' => 2}, 'conflicting' => ['conflict-id'], 'reordered' => true)
    query['state'].merge!('verdict' => 'failed', 'reason' => 'Snapshot differs',
      'missing' => [{'row' => {'value' => 1}, 'count' => 2}],
      'unexpected' => [{'row' => nil, 'count' => 1}, {'row' => [1, 2], 'count' => 1}])
  end
  check_report.call('valid nonempty snapshot differences fail', JSON.generate(body), false)
  body['comparison']['queries'].each do |query|
    query['state'] = {'verdict' => 'passed', 'reason' => nil, 'missing' => [], 'unexpected' => []}
  end
  check_report.call('delivery differences remain advisory with matching snapshots', JSON.generate(body), true)

  minimal = {
    test_run_id: valid.fetch('test_run_id'), enforced: false, verdict: 'passed',
    comparison: {schema_version: 1, verdict: 'passed', queries: %w[building-comfort building-comfort-floor-agg].map do |query|
      {query_id: query, state: {verdict: 'passed'}, delivery: {verdict: 'passed'}}
    end}
  }
  check_report.call('verdict-only report from PR review', JSON.generate(minimal), false)
end
puts 'PASS: complete reports and matching snapshots required for both queries; delivery/overall verdicts remain advisory.'

abort 'Unexpected daily schedule' unless trigger.fetch('schedule') == [{'cron' => '0 22 * * *'}]
defaults = trigger.fetch('workflow_dispatch').fetch('inputs').transform_values { |input| input.fetch('default', '') }
resolve = lambda do |value, event, inputs|
  expression = value.to_s.match(/\A\$\{\{ github.event_name == 'schedule' && '([^']*)' \|\| (inputs\.([a-z_]+)|'') \}\}\z/)
  if expression
    event == 'schedule' ? expression[1] : inputs.fetch(expression[3], '').to_s
  elsif (direct = value.to_s.match(/\A\$\{\{ inputs\.([a-z_]+) \}\}\z/))
    inputs.fetch(direct[1], '').to_s
  else
    value.to_s
  end
end
prepare_steps = workflow.fetch('jobs').fetch('prepare').fetch('steps')
settings = prepare_steps.find { |step| step['id'] == 'settings' }
scheduled = settings.fetch('env').transform_values { |value| resolve.call(value, 'schedule', {}) }
expected_scheduled = base.reject { |key, _| key == 'GITHUB_OUTPUT' }.merge('MINUTES' => '45')
abort "Wrong scheduled settings: #{scheduled}" unless scheduled == expected_scheduled
Dir.mktmpdir('scheduled-recovery-') do |directory|
  output_file = File.join(directory, 'outputs')
  _, error, result = Open3.capture3(scheduled.merge('GITHUB_OUTPUT' => output_file), 'bash', '-e', '-s', stdin_data: validation)
  abort "Schedule failed validation: #{error}" unless result.success?
  outputs = File.readlines(output_file).to_h { |line| line.strip.split('=', 2) }
  abort 'Scheduled matrix incomplete' unless JSON.parse(outputs.fetch('variants')) == %w[http_standard http_adaptive grpc_standard grpc_adaptive]
  abort 'Wrong scheduled timeout' unless outputs['timeout_seconds'] == '2700'
end
manual = settings.fetch('env').transform_values { |value| resolve.call(value, 'workflow_dispatch', defaults.merge('http_standard' => false)) }
abort 'Manual false selection was overridden' unless manual['V_HTTP_STD'] == 'false' && manual['V_GRPC_ADAPTIVE'] == 'false'
build_env = prepare_steps.find { |step| step['name'] == 'Build or download Drasi Server' }.fetch('env')
{
  'DRASI_REPO' => 'drasi-project/drasi-server', 'DRASI_SERVER_REF' => 'main',
  'DRASI_CORE_REPO' => 'drasi-project/drasi-core', 'DRASI_CORE_REF' => 'main'
}.each do |key, expected|
  abort "Wrong scheduled build input: #{key}" unless resolve.call(build_env.fetch(key), 'schedule', {}) == expected
end
abort 'Manual builds unexpectedly override core' unless resolve.call(build_env.fetch('DRASI_CORE_REF'), 'workflow_dispatch', defaults) == ''
recovery_env = workflow.fetch('jobs').fetch('recovery').fetch('env')
{'DRASI_PLUGIN_REGISTRY' => 'ghcr.io/drasi-project', 'DRASI_PLUGIN_TAG' => 'drasi-nightly-test'}.each do |key, expected|
  abort "Wrong scheduled plugin input: #{key}" unless resolve.call(recovery_env.fetch(key), 'schedule', {}) == expected
end
injection_env = steps.find { |step| step.fetch('env', {}).key?('CRASH_INJECT') }.fetch('env')
{
  'CRASH_INJECT' => 'drain', 'OUTBOX_CAPACITY' => '20000', 'PERSIST_INDEX' => 'true',
  'STATE_STORE' => 'true', 'BATCHING_SPEED' => '10000', 'QUERY_TUNING' => '10000',
  'BOOTSTRAP_SIZE' => 'off', 'LOG_JSONL' => '1'
}.each do |key, expected|
  abort "Wrong scheduled runtime input: #{key}" unless resolve.call(injection_env.fetch(key), 'schedule', {}) == expected
end
puts 'PASS: schedule resolves all variants, upstream server/core main, nightly plugins, and persisted recovery defaults; manual selections remain independent.'