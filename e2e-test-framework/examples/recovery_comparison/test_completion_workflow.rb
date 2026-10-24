# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

require "minitest/autorun"
require "yaml"

class CompletionWorkflowTest < Minitest::Test
  def setup
    path = File.expand_path("../../../.github/workflows/e2e-building-comfort-completion.yml", __dir__)
    @workflow = YAML.load_file(path)
    @steps = @workflow.fetch("jobs").fetch("completed-recovery").fetch("steps")
  end

  def test_reference_and_recovery_are_both_required
    reference = @steps.find { |step| step["name"]&.start_with?("Capture a genuinely") }
    recovery = @steps.find { |step| step["name"]&.start_with?("Require exact") }
    [reference, recovery].each do |step|
      refute_nil step
      refute step.key?("if")
      refute step.key?("continue-on-error")
      assert_includes step.fetch("run"), "qualify_building_comfort.py"
    end
    assert_includes reference.fetch("run"), '--output "$RESULTS/reference"'
    assert_includes recovery.fetch("run"), '--baseline "$RESULTS/reference/actual.json" --crash'
  end

  def test_libraries_are_built_from_the_selected_core_not_downloaded
    checkout = @steps.find { |step| step.dig("with", "repository") == "drasi-project/drasi-core" }
    assert_equal "${{ inputs.core_ref }}", checkout.fetch("with").fetch("ref")
    build = @steps.find { |step| step["name"] == "Build matching Server and gRPC libraries" }.fetch("run")
    assert_includes build, "-p drasi-source-grpc -p drasi-reaction-grpc"
    assert_includes build, "drasi-source-grpc/dynamic-plugin,drasi-reaction-grpc/dynamic-plugin"
    refute_includes build, "download"
  end

  def test_failure_evidence_is_retained
    upload = @steps.find { |step| step["uses"] == "actions/upload-artifact@v4" }
    assert_equal "always()", upload.fetch("if")
    assert_includes upload.fetch("with").fetch("path"), "recovery/*.json"
    assert_equal({ "contents" => "read" }, @workflow.fetch("permissions"))
  end
end
