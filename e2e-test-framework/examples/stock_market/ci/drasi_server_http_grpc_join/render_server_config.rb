#!/usr/bin/env ruby

require "yaml"

module StockMarketServerConfig
  QUERY_CAPACITIES = {
    "1000" => [1_000, 100, 1_000],
    "10000" => [10_000, 1_000, 10_000],
    "100000" => [100_000, 10_000, 100_000]
  }.freeze

  def self.enabled?(name, env)
    value = env.fetch(name, "false").downcase
    return value == "true" if %w[true false].include?(value)

    raise ArgumentError, "#{name} must be true or false"
  end

  def self.render(config, env = ENV)
    capacities = QUERY_CAPACITIES[env.fetch("QUERY_TUNING", "10000")]
    raise ArgumentError, "QUERY_TUNING must be 1000, 10000, or 100000" unless capacities

    priority_queue, dispatch_buffer, bootstrap_buffer = capacities
    persist_index = enabled?("PERSIST_INDEX", env)
    state_store = enabled?("STATE_STORE", env)
    wal_max_events = Integer(env.fetch("WAL_MAX_EVENTS", "500000"), 10)
    raise ArgumentError, "WAL_MAX_EVENTS must be positive" unless wal_max_events.positive?

    config["port"] = Integer(env.fetch("DRASI_ADMIN_PORT", "8090"), 10)
    config["persistIndex"] = persist_index

    if state_store
      config["stateStore"] = { "kind" => "redb", "path" => "./data/state.redb" }
    else
      config.delete("stateStore")
    end

    config.fetch("sources").each do |source|
      if persist_index
        source["durability"] = { "enabled" => true, "max_events" => wal_max_events }
      else
        source.delete("durability")
      end
    end

    config.fetch("queries").each do |query|
      query["priorityQueueCapacity"] = priority_queue
      query["dispatchBufferCapacity"] = dispatch_buffer
      query["bootstrapBufferSize"] = bootstrap_buffer
    end

    plugin_registry = env.fetch("DRASI_PLUGIN_REGISTRY", "")
    config["pluginRegistry"] = plugin_registry unless plugin_registry.empty?

    plugin_tag = env.fetch("DRASI_PLUGIN_TAG", "")
    unless plugin_tag.empty?
      config.fetch("plugins").each do |plugin|
        plugin["ref"] = "#{plugin.fetch("ref")}:#{plugin_tag}" unless plugin.fetch("ref").include?(":")
      end
    end

    config
  end
end

if $PROGRAM_NAME == __FILE__
  begin
    input, output = ARGV
    abort "usage: #{$PROGRAM_NAME} INPUT OUTPUT" unless input && output

    config = YAML.load_file(input)
    rendered = StockMarketServerConfig.render(config)
    File.write(output, YAML.dump(rendered))
  rescue ArgumentError, KeyError, TypeError => error
    warn "render_server_config: #{error.message}"
    exit 1
  end
end