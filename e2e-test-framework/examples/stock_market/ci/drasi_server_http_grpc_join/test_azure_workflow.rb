#!/usr/bin/env ruby

require "fileutils"
require "json"
require "minitest/autorun"
require "open3"
require "tmpdir"
require "yaml"

class StockMarketAzureWorkflowTest < Minitest::Test
  ROOT = File.expand_path("../../../../..", __dir__)
  REMOTE_RUNNER = File.join(ROOT, "e2e-test-framework/examples/building_comfort/dynamic/run_remote_test.sh")

  def setup
    @directory = Dir.mktmpdir("stock-market-azure")
    @bin = File.join(@directory, "bin")
    FileUtils.mkdir_p(@bin)
    @workflow = YAML.load_file(File.join(ROOT, ".github/workflows/stock-market-azure.yml"))
    @shared = YAML.load_file(File.join(ROOT, ".github/workflows/building-comfort-azure.yml"))
    executable("az", <<~BASH)
      #!/usr/bin/env bash
      case "$*" in
        "vm image show"*) printf '%s\n' '24.04.202609160' ;;
        "vm list-skus"*) printf '%s\n' '{"restrictions":[],"family":"standardDSv6Family","capabilities":[{"name":"vCPUs","value":"4"}]}' ;;
        "vm list-usage"*) printf '%s\n' '{"currentValue":0,"limit":8}' ;;
        *) exit 99 ;;
      esac
    BASH
  end

  def teardown
    FileUtils.remove_entry(@directory)
  end

  def executable(name, content)
    file_name = File.join(@bin, name)
    File.write(file_name, content)
    File.chmod(0o755, file_name)
  end

  def triggers(workflow)
    workflow.fetch("on", workflow[true])
  end

  def step(name)
    @shared.fetch("jobs").fetch("test").fetch("steps").find { |entry| entry["name"] == name }.fetch("run")
  end

  def resolve(scenario, overrides = {})
    env = @shared.fetch("jobs").fetch("test").fetch("steps")
                 .find { |entry| entry["name"] == "Resolve test inputs" }.fetch("env")
                 .transform_values { "" }
    env.merge!(
      "PATH" => "#{@bin}:#{ENV.fetch('PATH')}",
      "SCENARIO" => scenario,
      "GITHUB_ENV" => File.join(@directory, "github.env"),
      "GITHUB_RUN_ID" => "1234",
      "GITHUB_RUN_ATTEMPT" => "1",
      "MATRIX_JOB_INDEX" => "0"
    )
    output, errors, result = Open3.capture3(env.merge(overrides), "bash", stdin_data: step("Resolve test inputs"))
    [output + errors, result]
  end

  def resolved_env
    File.readlines(File.join(@directory, "github.env")).to_h { |line| line.strip.split("=", 2) }
  end

  def test_manual_entry_forwards_all_settings_and_azure_secrets
    assert_equal "Stock market Azure", @workflow["name"]
    assert_equal ["workflow_dispatch"], triggers(@workflow).keys
    dispatch = triggers(@workflow).fetch("workflow_dispatch").fetch("inputs")
    assert_operator dispatch.length, :<=, 25
    job = @workflow.fetch("jobs").fetch("test")
    assert_equal "./.github/workflows/building-comfort-azure.yml", job["uses"]
    assert_equal "stock_market", job.dig("with", "scenario")
    dispatch.each_key do |input_name|
      assert_equal "${{ inputs.#{input_name} }}", job.fetch("with").fetch(input_name)
      assert triggers(@shared).fetch("workflow_call").fetch("inputs").key?(input_name)
    end
    assert_equal "write", @workflow.dig("permissions", "id-token")
    assert_equal %w[AZURE_CLIENT_ID AZURE_SUBSCRIPTION_ID AZURE_TENANT_ID], job.fetch("secrets").keys.sort
    refute @workflow.key?("concurrency")
    assert_equal "azure-resource-group-drasi-e2e-test-infra", @shared.dig("concurrency", "group")
    cleanup = @shared.fetch("jobs").fetch("test").fetch("steps").find { |entry| entry["name"] == "Delete per-run Azure resources" }
    assert_equal "always()", cleanup["if"]
  end

  def test_stock_market_inputs_are_packaged_for_the_vm
    output, result = resolve("stock_market", "IN_WORKLOAD_SIZE" => "250000", "IN_QUERY_TUNING" => "100000", "IN_STATE_STORE" => "true", "IN_BATCHING_SPEED" => "5000")
    assert result.success?, output
    values = resolved_env
    assert_equal "drasi_server_http_grpc_join", values["VARIANTS"]
    assert_equal "watchlist-prices", values["QUERIES"]
    assert_equal "250000", values["WORKLOAD_SIZE"]
    assert_equal "100000", values["QUERY_TUNING"]
    assert_equal "true", values["STATE_STORE"]
    executable("git", "#!/usr/bin/env bash\nexit 0\n")
    env = values.merge(
      "PATH" => "#{@bin}:#{ENV.fetch('PATH')}", "SCENARIO" => "stock_market",
      "RUNNER_TEMP" => @directory, "REMOTE_ROOT" => "/opt/drasi-test", "GITHUB_SHA" => "abc123"
    )
    output, errors, result = Open3.capture3(env, "bash", stdin_data: step("Package test workspace"))
    assert result.success?, output + errors
    output, errors, result = Open3.capture3("bash", "-s", "--", File.join(@directory, "test.env"), stdin_data: "set -a\nsource \"$1\"\nenv\n")
    assert result.success?, errors
    packaged = output.lines.to_h { |line| line.strip.split("=", 2) }
    assert_equal "stock_market", packaged["SCENARIO"]
    assert_equal "250000", packaged["WORKLOAD_SIZE"]
    assert_equal "true", packaged["STATE_STORE"]
    assert_equal "5000", packaged["BATCHING_SPEED"]
    assert_equal values["VARIANTS"], packaged["VARIANTS"]
    assert_equal "/opt/drasi-test/work", packaged["SUITE_WORK_DIR"]
  end

  def test_both_workflows_select_the_same_variants
    github = YAML.load_file(File.join(ROOT, ".github/workflows/e2e-stock-market-join.yml"))
    compute = github.fetch("jobs").fetch("select-variants").fetch("steps").first
    assert_equal "${{ inputs.variant }}", compute.dig("env", "IN_VARIANT")
    [github, @workflow].each do |workflow|
      assert_equal %w[standard adaptive both], triggers(workflow).dig("workflow_dispatch", "inputs", "variant", "options")
      assert_equal "standard", triggers(workflow).dig("workflow_dispatch", "inputs", "variant", "default")
    end
    {
      "" => %w[drasi_server_http_grpc_join],
      "standard" => %w[drasi_server_http_grpc_join],
      "adaptive" => %w[drasi_server_http_grpc_join_adaptive],
      "both" => %w[drasi_server_http_grpc_join drasi_server_http_grpc_join_adaptive]
    }.each do |selection, expected|
      output_file = File.join(@directory, "github-output")
      File.write(output_file, "")
      output, errors, result = Open3.capture3(
        { "IN_VARIANT" => selection, "GITHUB_OUTPUT" => output_file }, "bash", stdin_data: compute.fetch("run")
      )
      assert result.success?, output + errors
      assert_equal expected, JSON.parse(File.read(output_file).strip.split("=", 2).last)
      output, result = resolve("stock_market", "IN_VARIANT" => selection)
      assert result.success?, output
      assert_equal expected, resolved_env.fetch("VARIANTS").split
    end
    run_job = github.fetch("jobs").fetch("stock-market-drasi-server-http-grpc-join")
    assert_equal "${{ matrix.variant }}", run_job.dig("env", "VARIANT")
    assert_equal false, run_job.dig("strategy", "fail-fast")
    steps = run_job.fetch("steps")
    assert_equal "stock_market-${{ matrix.variant }}", steps.find { |entry| entry["name"] == "Upload artifacts" }.dig("with", "name")
    assert_includes steps.find { |entry| entry["name"] == "Build result summary" }.fetch("run"), '--variant "$VARIANT"'
    output, errors, result = Open3.capture3(
      { "IN_VARIANT" => "unknown", "GITHUB_OUTPUT" => File.join(@directory, "github-output") }, "bash", stdin_data: compute.fetch("run")
    )
    refute result.success?, output + errors
  end

  def test_building_comfort_defaults_are_preserved
    output, result = resolve("building_comfort")
    assert result.success?, output
    assert_equal "drasi_lib http_standard http_adaptive grpc_standard grpc_adaptive", resolved_env["VARIANTS"]
    assert_equal "building-comfort building-comfort-floor-agg", resolved_env["QUERIES"]
  end

  def test_invalid_scenario_and_workload_are_rejected
    output, result = resolve("unknown")
    refute result.success?
    assert_includes output, "Unsupported scenario"
    output, result = resolve("stock_market", "IN_WORKLOAD_SIZE" => "nonsense")
    refute result.success?
    assert_includes output, "Unsupported stock-market workload size"
    output, result = resolve("stock_market", "IN_VARIANT" => "unknown")
    refute result.success?
    assert_includes output, "Unsupported stock-market variant"
    output, result = resolve("stock_market", "IN_BATCHING_SPEED" => "zero")
    refute result.success?
    assert_includes output, "Unsupported stock-market batch size"
  end

  def test_remote_runner_selects_each_scenario_before_provisioning
    executable("sudo", "#!/usr/bin/env bash\nenv > \"$CAPTURE_ENV\"\nexit 73\n")
    [
      ["stock_market", "drasi_server_http_grpc_join", "stock_market/ci/drasi_server_http_grpc_join/run_test_ci.sh"],
      ["stock_market", "drasi_server_http_grpc_join_adaptive", "stock_market/ci/drasi_server_http_grpc_join/run_test_ci.sh"],
      ["stock_market", "drasi_server_http_grpc_join drasi_server_http_grpc_join_adaptive", "stock_market/ci/drasi_server_http_grpc_join/run_test_ci.sh"],
      ["building_comfort", "http_standard", "building_comfort/run_variant.sh"]
    ].each do |scenario, variants, runner|
      env_file = File.join(@directory, "remote.env")
      File.write(env_file, "SCENARIO=#{scenario}\nVARIANTS='#{variants}'\nWORKLOAD_SIZE=250000\nBATCHING_SPEED=5000\n")
      capture = File.join(@directory, "remote-capture.env")
      env = {
        "PATH" => "#{@bin}:#{ENV.fetch('PATH')}", "CAPTURE_ENV" => capture,
        "SUITE_WORK_DIR" => File.join(@directory, "work"), "PERF_PROFILE_ID" => "test-profile"
      }
      output, errors, result = Open3.capture3(env, "bash", REMOTE_RUNNER, ROOT, env_file, File.join(@directory, "artifacts"))
      assert_equal 73, result.exitstatus, output + errors
      captured = File.readlines(capture).to_h { |line| line.strip.split("=", 2) }
      assert_equal variants, captured["VARIANTS"]
      assert_equal File.join(ROOT, "e2e-test-framework/examples", runner), captured["RUN_SCRIPT"]
      assert_equal "250000", captured["WORKLOAD_SIZE"]
      assert_equal "5000", captured["BATCHING_SPEED"]
    end
  end

  def test_stock_summary_is_written_without_github_environment
    artifacts = File.join(@directory, "artifacts")
    metrics = File.join(@directory, "data/output_log/performance_metrics")
    FileUtils.mkdir_p([artifacts, metrics])
    File.write(File.join(metrics, "metrics.json"), JSON.dump(
      test_run_reaction_id: "repo.stock_market.run.watchlist-prices", record_count: 75000,
      duration_ns: 10_000_000_000, records_per_second: 7500
    ))
    executable("drasi-server", "#!/usr/bin/env bash\necho drasi-server-test\n")
    env = {
      "GITHUB_STEP_SUMMARY" => nil, "ARTIFACTS_DIR" => artifacts,
      "DATA_CACHE" => File.join(@directory, "data"), "DRASI_SERVER_BIN" => File.join(@bin, "drasi-server"),
      "TEST_RUN_ID" => "repo.stock_market.run", "TEST_REACTION_IDS" => "watchlist-prices",
      "WORKLOAD_SIZE" => "100000", "REACTION_RECORD_COUNT" => "75000",
      "QUERY_TUNING" => "10000", "PERSIST_INDEX" => "false", "STATE_STORE" => "false"
    }
    function = File.read(File.join(__dir__, "run_test_ci.sh"))[/^write_step_summary\(\) \{\n.*?^\}\n/m]
    refute_nil function
    output, errors, result = Open3.capture3(env, "bash", stdin_data: "set -euo pipefail\n#{function}\nwrite_step_summary\n")
    assert result.success?, output + errors
    summary = File.read(File.join(artifacts, "summary.md"))
    assert_includes summary, "Throughput"
    assert_includes summary, "75000 | 10 | 7500"
  end

  def test_remote_runner_rejects_unknown_or_empty_stock_variants_before_setup
    executable("sudo", "#!/usr/bin/env bash\nexit 73\n")
    ["drasi_server_http_grpc_join unknown", " , "].each do |variants|
      env_file = File.join(@directory, "remote.env")
      File.write(env_file, "SCENARIO=stock_market\nVARIANTS='#{variants}'\n")
      output, errors, result = Open3.capture3(
        {
          "PATH" => "#{@bin}:#{ENV.fetch('PATH')}",
          "SUITE_WORK_DIR" => File.join(@directory, "work"), "PERF_PROFILE_ID" => "test-profile"
        }, "bash", REMOTE_RUNNER, ROOT, env_file, File.join(@directory, "artifacts")
      )
      assert_equal 1, result.exitstatus, output + errors
      assert_match(/Unsupported stock-market variant|At least one test variant/, output + errors)
    end
  end
end