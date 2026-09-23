#!/usr/bin/env ruby

require "minitest/autorun"
require "yaml"
require_relative "render_server_config"

class RenderServerConfigTest < Minitest::Test
  def setup
    config_path = File.join(__dir__, "drasi_server_config.yaml")
    @config = YAML.load_file(config_path)
  end

  def render(overrides = {})
    env = {
      "DRASI_ADMIN_PORT" => "8090",
      "QUERY_TUNING" => "10000",
      "PERSIST_INDEX" => "false",
      "STATE_STORE" => "false",
      "WAL_MAX_EVENTS" => "500000",
      "DRASI_PLUGIN_REGISTRY" => "",
      "DRASI_PLUGIN_TAG" => ""
    }.merge(overrides)
    StockMarketServerConfig.render(Marshal.load(Marshal.dump(@config)), env)
  end

  def test_default_profile_keeps_sources_volatile_and_sets_query_defaults
    rendered = render

    assert_equal 8090, rendered["port"]
    assert_equal false, rendered["persistIndex"]
    refute rendered.key?("stateStore")
    assert rendered.fetch("sources").none? { |source| source.key?("durability") }
    assert_equal 10_000, rendered.dig("queries", 0, "priorityQueueCapacity")
    assert_equal 1_000, rendered.dig("queries", 0, "dispatchBufferCapacity")
    assert_equal 10_000, rendered.dig("queries", 0, "bootstrapBufferSize")
  end

  def test_persistence_axes_are_independent
    [false, true].product([false, true]).each do |persist_index, state_store|
      rendered = render(
        "PERSIST_INDEX" => persist_index.to_s,
        "STATE_STORE" => state_store.to_s
      )

      assert_equal persist_index, rendered["persistIndex"]
      assert_equal state_store, rendered.key?("stateStore")
      assert_equal persist_index, rendered.fetch("sources").all? { |source| source.key?("durability") }
    end
  end

  def test_high_tuning_and_plugin_overrides
    rendered = render(
      "QUERY_TUNING" => "100000",
      "DRASI_PLUGIN_REGISTRY" => "ghcr.io/example",
      "DRASI_PLUGIN_TAG" => "candidate"
    )

    assert_equal 100_000, rendered.dig("queries", 0, "priorityQueueCapacity")
    assert_equal 10_000, rendered.dig("queries", 0, "dispatchBufferCapacity")
    assert_equal "ghcr.io/example", rendered["pluginRegistry"]
    assert rendered.fetch("plugins").all? { |plugin| plugin.fetch("ref").end_with?(":candidate") }
  end

  def test_rejects_invalid_query_tuning
    error = assert_raises(ArgumentError) { render("QUERY_TUNING" => "huge") }

    assert_match(/QUERY_TUNING/, error.message)
  end
end