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
  File.write(report, JSON.generate({verdict: 'inconclusive', enforced: false, comparison: {queries: []}}))
  _, error, result = Open3.capture3(environment, 'bash', '-e', '-s', stdin_data: summary)
  abort "Advisory verdict policy changed: #{error}" unless result.success?
end
puts 'PASS: fixed golden used without a selector; incompatible workloads and missing reports fail; verdict remains advisory.'

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