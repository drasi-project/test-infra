require 'yaml'
require 'open3'
require 'tmpdir'
require 'json'
require 'fileutils'

workflow = YAML.load_file(File.expand_path('../../../../.github/workflows/e2e-stock-market-recovery.yml', __dir__))
trigger = workflow['on'] || workflow[true]
inputs = trigger.fetch('workflow_dispatch').fetch('inputs')
abort 'Single-choice variant input remains' if inputs.key?('variant')
%w[standard adaptive].each do |name|
  abort "Invalid checkbox default: #{name}" unless inputs.fetch(name).values_at('type', 'default') == ['boolean', true]
end
steps = workflow.fetch('jobs').fetch('prepare').fetch('steps')
settings = steps.find { |step| step['id'] == 'settings' }
abort 'Missing standard input wiring' unless settings.dig('env', 'V_STANDARD') == '${{ inputs.standard }}'
abort 'Missing adaptive input wiring' unless settings.dig('env', 'V_ADAPTIVE') == '${{ inputs.adaptive }}'
validation = settings.fetch('run')
Dir.mktmpdir('stock-recovery-workflow') do |directory|
  env = {'V_STANDARD' => 'true', 'V_ADAPTIVE' => 'true', 'MINUTES' => '30', 'CORE_REF' => '', 'SERVER_REF' => '',
         'GITHUB_OUTPUT' => File.join(directory, 'output')}
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
puts 'PASS: recovery workflow validates inputs and fails on missing or skipped crash verdicts.'