require "fileutils"
require "json"
require "minitest/autorun"
require "open3"
require "tmpdir"
require "yaml"

class StockMarketNightlyWorkflowTest < Minitest::Test
  ROOT = File.expand_path("../../../../..", __dir__)
  STOCK_VARIANTS = %w[drasi_server_http_grpc_join drasi_server_http_grpc_join_adaptive].freeze

  def setup
    @directory = Dir.mktmpdir("stock-market-nightly")
    @workflow = YAML.load_file(File.join(ROOT, ".github/workflows/nightly.yml"))
    @artifacts = File.join(@directory, "artifacts")
    @calls = File.join(@directory, "calls")
    @run_env = File.join(@directory, "run-env")
    @server = File.join(@directory, "drasi-server")
    @service = File.join(@directory, "test-service")
    FileUtils.mkdir_p(@artifacts)
  end

  def teardown
    FileUtils.remove_entry(@directory)
  end

  def triggers
    @workflow.fetch("on", @workflow[true])
  end

  def step(name, job = "nightly-e2e")
    @workflow.fetch("jobs").fetch(job).fetch("steps").find { |entry| entry["name"] == name }
  end

  def execute(script, env)
    Open3.capture3(@workflow.fetch("env").merge(env), "/bin/bash", "-e", "-s",
                   chdir: @directory, stdin_data: script)
  end

  def settings(overrides = {})
    env_file = File.join(@directory, "github.env")
    File.write(env_file, "")
    inputs = step("Resolve nightly inputs").fetch("env").transform_values { "" }
    output, errors, result = execute(step("Resolve nightly inputs").fetch("run"),
                                    inputs.merge(overrides).merge("GITHUB_ENV" => env_file))
    assert result.success?, output + errors
    File.readlines(env_file).to_h { |line| line.chomp.split("=", 2) }
  end

  def mock_runners
    scripts = {
      "e2e-test-framework/examples/building_comfort/dynamic/run_dynamic.sh" => <<~BASH,
        set -euo pipefail
        [[ "$1" == "--prepare-server" ]]
        printf 'prepare\\n' >> "$CALLS_FILE"
        [[ "$PREPARE_RC" == "0" ]] || exit "$PREPARE_RC"
        printf '%s\\n' "$SERVER_BUILD_JSON" > "$ARTIFACTS_DIR/server-build.json"
      BASH
      "e2e-test-framework/examples/stock_market/ci/drasi_server_http_grpc_join/run_test_ci.sh" => <<~BASH,
        set -euo pipefail
        printf 'stock\\n' >> "$CALLS_FILE"
        printf '%s\\n' "$DRASI_SERVER_REF" "$DRASI_CORE_REF" "$DRASI_PLUGIN_TAG" \
          "$TEST_SERVICE_BIN" "$DRASI_SERVER_BIN" "$VARIANT" > "$RUN_ENV"
        exit "$TEST_RC"
      BASH
      "e2e-test-framework/examples/building_comfort/run_variant.sh" => <<~BASH
        set -euo pipefail
        printf 'comfort\\n' >> "$CALLS_FILE"
        exit "$TEST_RC"
      BASH
    }
    scripts.each do |path, content|
      filename = File.join(@directory, path)
      FileUtils.mkdir_p(File.dirname(filename))
      File.write(filename, content)
    end
  end

  def run_test(variant, overrides = {})
    File.write(@calls, "")
    env = settings.merge(
      "VARIANT" => variant, "ARTIFACTS_DIR" => @artifacts,
      "WORK_DIR" => File.join(@directory, "work"), "CALLS_FILE" => @calls, "RUN_ENV" => @run_env,
      "TEST_SERVICE_BIN" => @service, "PREPARE_RC" => "0", "TEST_RC" => "0",
      "SERVER_BUILD_JSON" => JSON.dump("binary" => @server)
    )
    execute(step("Run test").fetch("run"), env.merge(overrides))
  end

  def test_default_matrix_keeps_building_comfort_and_adds_both_stock_variants
    expected = %w[http_standard http_adaptive grpc_standard grpc_adaptive] + STOCK_VARIANTS
    assert_equal expected, @workflow.fetch("env").fetch("DEFAULT_VARIANTS").split
    assert_equal expected, triggers.dig("workflow_dispatch", "inputs", "variants", "default").split
    assert_equal [{ "cron" => "0 22 * * *" }], triggers.fetch("schedule")
    selector = step("Compute variant matrix", "select-variants")
    output_file = File.join(@directory, "selection")
    ["", STOCK_VARIANTS.join(", ")].each do |raw|
      File.write(output_file, "")
      output, errors, result = execute(selector.fetch("run"),
                                      "IN_VARIANTS" => raw, "GITHUB_OUTPUT" => output_file)
      assert result.success?, output + errors
      selected = File.readlines(output_file).to_h { |line| line.chomp.split("=", 2) }
      assert_equal(raw.empty? ? expected : STOCK_VARIANTS, JSON.parse(selected.fetch("variants")))
    end
    assert_includes step("Install dependencies").fetch("run"), "jq curl ruby"
    assert_includes step("Build test-service").fetch("run"), "TEST_SERVICE_BIN=$GITHUB_WORKSPACE/"
    assert_equal "nightly-e2e", @workflow.dig("jobs", "report-failure", "needs")
    assert_equal "nightly-e2e-${{ matrix.variant }}", step("Upload artifacts").dig("with", "name")
  end

  def test_nightly_defaults_and_manual_upstream_overrides
    expected = {
      "DRASI_REPO" => "drasi-project/drasi-server", "DRASI_SERVER_REF" => "main",
      "DRASI_CORE_REPO" => "drasi-project/drasi-core", "DRASI_CORE_REF" => "main",
      "DRASI_PLUGIN_TAG" => "drasi-nightly-test", "BOOTSTRAP_SIZE" => "", "TIMEOUT_SECS" => "2700"
    }
    assert_equal expected, settings
    assert_equal expected.merge(
      "DRASI_REPO" => "example/server", "DRASI_SERVER_REF" => "candidate",
      "DRASI_CORE_REPO" => "example/core", "DRASI_CORE_REF" => "test-core",
      "DRASI_PLUGIN_TAG" => "candidate-plugins", "TIMEOUT_SECS" => "1800"
    ), settings("IN_SERVER_REPO" => "example/server", "IN_SERVER_REF" => "candidate",
                "IN_CORE_REPO" => "example/core", "IN_CORE_REF" => "test-core",
                "IN_PLUGIN_TAG" => "candidate-plugins", "IN_TIMEOUT_MINUTES" => "30")
  end

  def test_stock_variants_receive_prepared_core_main_binary_and_building_comfort_keeps_its_runner
    mock_runners
    STOCK_VARIANTS.each do |variant|
      output, errors, result = run_test(variant)
      assert result.success?, output + errors
      assert_equal %w[prepare stock], File.readlines(@calls, chomp: true)
      assert_equal ["main", "main", "drasi-nightly-test", @service, @server, variant],
                   File.readlines(@run_env, chomp: true)
    end
    output, errors, result = run_test("http_standard")
    assert result.success?, output + errors
    assert_equal ["comfort"], File.readlines(@calls, chomp: true)
  end

  def test_preparation_and_test_failures_are_not_hidden
    mock_runners
    [
      [STOCK_VARIANTS.first, { "PREPARE_RC" => "7" }, 7, ["prepare"]],
      [STOCK_VARIANTS.last, { "TEST_RC" => "9" }, 9, %w[prepare stock]],
      ["http_standard", { "TEST_RC" => "9" }, 9, ["comfort"]]
    ].each do |variant, env, expected_code, calls|
      output, errors, result = run_test(variant, env)
      assert_equal expected_code, result.exitstatus, output + errors
      assert_equal calls, File.readlines(@calls, chomp: true)
    end
  end

  def test_missing_or_empty_binary_metadata_cannot_fall_back_to_an_unpatched_server
    mock_runners
    [{}, { "binary" => nil }, { "binary" => "" }, { "binary" => 123 }].each do |metadata|
      output, errors, result = run_test(STOCK_VARIANTS.first, "SERVER_BUILD_JSON" => JSON.dump(metadata))
      refute result.success?, output + errors
      assert_equal ["prepare"], File.readlines(@calls, chomp: true)
    end
  end

  def test_stock_runner_keeps_prepared_binary_even_with_source_overrides
    function = File.read(File.join(__dir__, "run_test_ci.sh"))[/^download_drasi_server\(\) \{\n.*?^\}\n/m]
    refute_nil function
    stubs = <<~BASH
      log() { printf '%s\\n' "$*"; }
      build_drasi_server_from_source() { return 97; }
      download_drasi_server_release() { return 98; }
    BASH
    output, errors, result = execute(stubs + function + "\ndownload_drasi_server\n",
                                    settings.merge("DRASI_SERVER_BIN" => @server,
                                                   "DRASI_REPO_EXPLICIT" => "drasi-project/drasi-server"))
    assert result.success?, output + errors
    assert_includes output, "Using pre-set DRASI_SERVER_BIN=#{@server}"
  end
end
