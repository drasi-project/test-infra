require 'yaml'
require 'open3'
require 'tmpdir'
require 'fileutils'
require 'json'

workflow = YAML.load_file(File.expand_path('../../../../.github/workflows/e2e-building-comfort-recovery.yml', __dir__))
trigger = workflow['on'] || workflow[true]
golden = trigger.fetch('workflow_dispatch').fetch('inputs').fetch('golden_snapshot')
abort 'Golden selection is optional' unless golden['required'] == true &&
  golden['options'] == ['building-comfort-small-v1'] && golden['default'] == 'building-comfort-small-v1'
validation = workflow.fetch('jobs').fetch('prepare').fetch('steps').find { |step| step['id'] == 'settings' }.fetch('run')
base = {
  'MINUTES' => '30', 'OUTBOX_CAPACITY' => '20000',
  'V_HTTP_STD' => 'true', 'V_HTTP_ADAPTIVE' => 'true',
  'V_GRPC_STD' => 'true', 'V_GRPC_ADAPTIVE' => 'true',
  'V_Q_MAIN' => 'true', 'V_Q_AGG' => 'true', 'PERSIST_INDEX' => 'true',
  'STATE_STORE' => 'true', 'GOLDEN_SNAPSHOT' => 'building-comfort-small-v1',
  'BOOTSTRAP_SIZE' => 'off', 'GITHUB_OUTPUT' => '/dev/null'
}
[
  [{}, true],
  [{'GOLDEN_SNAPSHOT' => ''}, false],
  [{'GOLDEN_SNAPSHOT' => 'none'}, false],
  [{'GOLDEN_SNAPSHOT' => 'unknown'}, false],
  [{'V_Q_MAIN' => 'false'}, false],
  [{'V_Q_AGG' => 'false'}, false],
  [{'BOOTSTRAP_SIZE' => '10k'}, false]
].each do |overrides, expected|
  output, error, result = Open3.capture3(base.merge(overrides), 'bash', '-e', '-s', stdin_data: validation)
  abort "Unexpected validation: #{overrides}: #{output} #{error}" unless result.success? == expected
end

steps = workflow.fetch('jobs').fetch('recovery').fetch('steps')
injection = steps.find { |step| step.fetch('env', {}).key?('CRASH_INJECT') }.fetch('run')
abort 'Golden configuration remains conditional' unless injection.lines.first.start_with?('export RECOVERY_GOLDEN_DIR=')
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
puts 'PASS: golden required for all variants; invalid selections and missing reports fail; verdict remains advisory.'