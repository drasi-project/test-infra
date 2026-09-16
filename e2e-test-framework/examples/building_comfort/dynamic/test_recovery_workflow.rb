require 'yaml'
require 'open3'

path = File.expand_path('../../../../.github/workflows/e2e-building-comfort-recovery.yml', __dir__)
workflow = YAML.load_file(path)
script = workflow.fetch('jobs').fetch('prepare').fetch('steps').find { |step| step['id'] == 'settings' }.fetch('run')
base = {
  'RECOVERY_SCENARIO' => 'server_restart', 'RECOVERY_SIGNAL' => 'SIGKILL',
  'SHUTDOWN_TIMEOUT_SECS' => '120', 'MINUTES' => '30', 'OUTBOX_CAPACITY' => '20000',
  'V_HTTP_STD' => 'false', 'V_HTTP_ADAPTIVE' => 'false',
  'V_GRPC_STD' => 'true', 'V_GRPC_ADAPTIVE' => 'false',
  'V_Q_MAIN' => 'true', 'V_Q_AGG' => 'true', 'PERSIST_INDEX' => 'true',
  'STATE_STORE' => 'true', 'GOLDEN_SNAPSHOT' => 'building-comfort-small-v1',
  'BOOTSTRAP_SIZE' => 'off', 'GITHUB_OUTPUT' => '/dev/null'
}
cases = [
  [{}, true],
  [{'RECOVERY_SIGNAL' => 'SIGTERM'}, true],
  [{'RECOVERY_SCENARIO' => 'receiver_rejection'}, true],
  [{'RECOVERY_SCENARIO' => 'receiver_rejection', 'RECOVERY_SIGNAL' => 'SIGTERM'}, true],
  [{'RECOVERY_SCENARIO' => 'receiver_rejection', 'V_HTTP_STD' => 'true'}, false],
  [{'RECOVERY_SCENARIO' => 'receiver_rejection', 'V_GRPC_ADAPTIVE' => 'true'}, false],
  [{'RECOVERY_SCENARIO' => 'receiver_rejection', 'BOOTSTRAP_SIZE' => '10k'}, false],
  [{'RECOVERY_SCENARIO' => 'receiver_rejection', 'STATE_STORE' => 'false'}, false],
  [{'RECOVERY_SCENARIO' => 'invalid'}, false],
  [{'RECOVERY_SIGNAL' => 'SIGINT'}, false],
  [{'SHUTDOWN_TIMEOUT_SECS' => '0'}, false],
  [{'SHUTDOWN_TIMEOUT_SECS' => '601'}, false]
]
cases.each do |overrides, expected|
  output, error, result = Open3.capture3(base.merge(overrides), 'bash', '-s', stdin_data: script)
  abort "Validation mismatch #{overrides}: #{output} #{error}" unless result.success? == expected
end
puts 'PASS: workflow accepts server signals and receiver rejection with the shared golden; rejects unsupported settings.'