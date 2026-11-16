require 'yaml'
require 'open3'
require 'tmpdir'
require 'json'
require 'fileutils'

workflow = YAML.load_file(File.expand_path('../../../../.github/workflows/e2e-stock-market-recovery.yml', __dir__))
trigger = workflow['on'] || workflow[true]
abort 'Missing daily recovery schedule' unless trigger.fetch('schedule') == [{'cron' => '0 22 * * *'}]
inputs = trigger.fetch('workflow_dispatch').fetch('inputs')
abort 'Single-choice variant input remains' if inputs.key?('variant')
%w[standard adaptive].each do |name|
  abort "Invalid checkbox default: #{name}" unless inputs.fetch(name).values_at('type', 'default') == ['boolean', true]
end
steps = workflow.fetch('jobs').fetch('prepare').fetch('steps')
settings = steps.find { |step| step['id'] == 'settings' }
abort 'Missing standard input wiring' unless settings.dig('env', 'V_STANDARD') == "${{ github.event_name == 'schedule' && 'true' || inputs.standard }}"
abort 'Missing adaptive input wiring' unless settings.dig('env', 'V_ADAPTIVE') == "${{ github.event_name == 'schedule' && 'true' || inputs.adaptive }}"
defaults = inputs.transform_values { |input| input.fetch('default', '') }
resolve = lambda do |value, event, values|
  expression = value.to_s.match(/\A\$\{\{ github.event_name == 'schedule' && '([^']*)' \|\| inputs\.([a-z_]+) \}\}\z/)
  if expression
    event == 'schedule' ? expression[1] : values.fetch(expression[2], '').to_s
  elsif (direct = value.to_s.match(/\A\$\{\{ inputs\.([a-z_]+) \}\}\z/))
    values.fetch(direct[1], '').to_s
  else
    value.to_s
  end
end
scheduled = settings.fetch('env').transform_values { |value| resolve.call(value, 'schedule', {}) }
expected_scheduled = {'V_STANDARD' => 'true', 'V_ADAPTIVE' => 'true', 'MINUTES' => '45',
                      'CORE_REF' => 'main', 'SERVER_REF' => 'main'}
abort "Wrong scheduled settings: #{scheduled}" unless scheduled == expected_scheduled
manual = settings.fetch('env').transform_values { |value| resolve.call(value, 'workflow_dispatch', defaults.merge('standard' => false)) }
abort 'Manual false selection was overridden' unless manual['V_STANDARD'] == 'false' && manual['V_ADAPTIVE'] == 'true'
build_env = steps.find { |step| step['name'] == 'Prepare Drasi Server and bundle' }.fetch('env')
{'DRASI_REPO' => 'drasi-project/drasi-server', 'DRASI_SERVER_REF' => 'main',
 'DRASI_CORE_REPO' => 'drasi-project/drasi-core', 'DRASI_CORE_REF' => 'main'}.each do |key, expected|
  abort "Wrong scheduled build input: #{key}" unless resolve.call(build_env.fetch(key), 'schedule', {}) == expected
end
abort 'Manual builds unexpectedly override core' unless resolve.call(build_env.fetch('DRASI_CORE_REF'), 'workflow_dispatch', defaults) == ''
runtime_env = workflow.fetch('jobs').fetch('recovery').fetch('env')
{'BATCH_SIZE' => '10000', 'DRASI_PLUGIN_REGISTRY' => 'ghcr.io/drasi-project',
 'DRASI_PLUGIN_TAG' => 'drasi-nightly-test'}.each do |key, expected|
  abort "Wrong scheduled runtime input: #{key}" unless resolve.call(runtime_env.fetch(key), 'schedule', {}) == expected
end
manual_overrides = defaults.merge('drasi_server_repo' => 'example/server', 'drasi_server_ref' => 'candidate',
                                  'drasi_core_ref' => 'test-core', 'plugin_registry' => 'ghcr.io/example',
                                  'plugin_tag' => 'candidate-plugins', 'batching_speed' => '5000')
{'DRASI_REPO' => 'example/server', 'DRASI_SERVER_REF' => 'candidate', 'DRASI_CORE_REF' => 'test-core'}.each do |key, expected|
  abort "Manual build override lost: #{key}" unless resolve.call(build_env.fetch(key), 'workflow_dispatch', manual_overrides) == expected
end
{'BATCH_SIZE' => '5000', 'DRASI_PLUGIN_REGISTRY' => 'ghcr.io/example',
 'DRASI_PLUGIN_TAG' => 'candidate-plugins'}.each do |key, expected|
  abort "Manual runtime override lost: #{key}" unless resolve.call(runtime_env.fetch(key), 'workflow_dispatch', manual_overrides) == expected
end
validation = settings.fetch('run')
Dir.mktmpdir('stock-recovery-workflow') do |directory|
  env = settings.fetch('env').transform_values { |value| resolve.call(value, 'workflow_dispatch', defaults) }
  env['GITHUB_OUTPUT'] = File.join(directory, 'output')
  [ ['true', 'false', %w[standard]], ['false', 'true', %w[adaptive]],
    ['true', 'true', %w[standard adaptive]], ['false', 'false', nil] ].each do |standard, adaptive, expected|
    File.write(env['GITHUB_OUTPUT'], '')
    output, errors, result = Open3.capture3(env.merge('V_STANDARD' => standard, 'V_ADAPTIVE' => adaptive), 'bash', stdin_data: validation)
    abort "Unexpected checkbox result: #{standard}/#{adaptive}: #{output} #{errors}" unless result.success? == !expected.nil?
    if expected
      outputs = File.readlines(env['GITHUB_OUTPUT']).to_h { |line| line.strip.split('=', 2) }
      abort 'Wrong variant matrix' unless JSON.parse(outputs.fetch('variants')) == expected
    else
      abort 'Missing empty selection diagnostic' unless errors.include?('no variants selected')
      abort 'Failed selection wrote outputs' unless File.zero?(env['GITHUB_OUTPUT'])
    end
  end
  [ [{}, true], [{'MINUTES' => '0'}, false],
    [{'MINUTES' => '61'}, false], [{'CORE_REF' => 'main'}, false],
    [{'CORE_REF' => 'main', 'SERVER_REF' => 'main'}, true] ].each do |override, expected|
    output, errors, result = Open3.capture3(env.merge(override), 'bash', stdin_data: validation)
    abort "Unexpected validation result: #{override}: #{output} #{errors}" unless result.success? == expected
  end
  outputs = File.readlines(env['GITHUB_OUTPUT']).to_h { |line| line.strip.split('=', 2) }
  abort 'Missing variant coverage' unless JSON.parse(outputs['variants']) == %w[standard adaptive]
  output, errors, result = Open3.capture3(scheduled.merge('GITHUB_OUTPUT' => env['GITHUB_OUTPUT']), 'bash', stdin_data: validation)
  abort "Scheduled validation failed: #{output} #{errors}" unless result.success?
  outputs = File.readlines(env['GITHUB_OUTPUT']).to_h { |line| line.strip.split('=', 2) }
  abort 'Scheduled matrix incomplete' unless JSON.parse(outputs.fetch('variants')) == %w[standard adaptive]
  abort 'Wrong scheduled timeout' unless outputs.fetch('timeout_seconds') == '2700'
  summary = workflow.fetch('jobs').fetch('recovery').fetch('steps').find { |step| step['name'] == 'Required recovery verdicts' }.fetch('run')
  %w[clean sigkill].each do |mode|
    FileUtils.mkdir_p(File.join(directory, 'results', mode))
    File.write(File.join(directory, 'results', mode, 'verdict.json'), JSON.dump({
      passed: true, mode: mode, variant: 'standard', scope: 'golden-query-snapshot', crash_injected: mode == 'sigkill'
    }))
  end
  env = {'RUNNER_TEMP' => directory, 'VARIANT' => 'standard', 'GITHUB_STEP_SUMMARY' => File.join(directory, 'summary')}
  _, error, result = Open3.capture3(env, 'bash', stdin_data: summary)
  abort error unless result.success?
  report = File.join(directory, 'results', 'sigkill', 'verdict.json')
  ['{}', '{invalid', JSON.dump({passed: true, mode: 'sigkill', variant: 'standard', scope: 'golden-query-snapshot', crash_injected: false})].each do |body|
    File.write(report, body)
    _, _, result = Open3.capture3(env, 'bash', stdin_data: summary)
    abort 'Invalid or skipped crash passed' if result.success?
  end
  File.delete(report)
  _, _, result = Open3.capture3(env, 'bash', stdin_data: summary)
  abort 'Missing report passed' if result.success?
end
puts 'PASS: scheduled core-main recovery, manual overrides, and required clean/SIGKILL verdicts.'