echo -e "${GREEN}\n\nAdding Test Repository, Test Source, and Test Query...${RESET}"
curl -i -X POST -H "Content-Type: application/json" -d @examples/building_comfort/kind_drasi/cfg_repo_az_dev.json http://localhost:63123/test_repos
curl -i -X POST -H "Content-Type: application/json" -d @examples/building_comfort/kind_drasi/cfg_source.json http://localhost:63123/test_run_host/sources
curl -i -X POST -H "Content-Type: application/json" -d @examples/building_comfort/kind_drasi/cfg_query.json http://localhost:63123/test_run_host/queries



# Introduction

The TestRunHost is a a component of the Drasi E2E Test Framework. It allows developers and testers to create and use Drasi E2E Test Scripts. The TestRunHost supports multiple modes for event timing, including recorded, rebased, and live, providing flexibility in how tests are conducted. Additionally, it offers options for controlling the speed of event replay, ensuring that the system under test can handle different pacing scenarios effectively.


alljones-msft: @workspace suggest content for a section in this document describing how to run the TestRunHost, the configuration it needs and how to provide that configuration using wither command line arguments or environment variables. Give examples

Workspace: Sure, here is a suggested section for the README.md document:

```markdown
# Running The TestRunHost

To run the TestRunHost, you need to provide the necessary configuration either through command line arguments or environment variables. Below are the details on how to configure and run the service.

## Configuration

The TestRunHost requires the following configuration parameters:

- `config_file_path`: The path of the Test Run Config file. If not provided, the service will start in an uninitialized state and wait to be initialized through the Web API.
- `data_cache_path`: The path where data used and generated in the Test Run gets stored. Default is `./source_data_cache`.
- `port`: The port number the Web API will listen on. Default is `63123`.
- `event_output`: The output type for Source Change Events. Default is `none`.
- `telemetry_output`: The output type for Test Run Telemetry data. Default is `none`.
- `log_output`: The output type for Test Run Log data. Default is `none`.

## Running with Command Line Arguments

You can provide the configuration parameters directly through command line arguments when starting the service. Here is an example:

```sh
cargo run -- \
  --config /path/to/config.json \
  --data /path/to/data_cache \
  --port 8080 \
  --event_out publish \
  --telem_out publish \
  --log_out publish
```

## Running with Environment Variables

Alternatively, you can set the configuration parameters using environment variables. Here is an example:

```sh
export DRASI_CONFIG_FILE=/path/to/config.json
export DRASI_DATA_CACHE=/path/to/data_cache
export DRASI_PORT=8080
export DRASI_EVENT_OUTPUT=publish
export DRASI_TELEM_OUTPUT=publish
export DRASI_LOG_OUTPUT=publish

cargo run
```

## Running a Test

To run a test using the TestRunHost, you have two options for providing the configuration: passing a config file at startup or using the Web API to initialize the service. In this section, we will discuss both approaches and explain the contents of the config file and each argument.

### Config File

The TestRunHost requires a config file to be provided for proper initialization. The config file should be in JSON format and contain the following parameters:

- `config_file_path`: The path of the Test Run Config file. If not provided, the service will start in an uninitialized state and wait to be initialized through the Web API.
- `data_cache_path`: The path where data used and generated in the Test Run gets stored. The default value is `./source_data_cache`.
- `port`: The port number the Web API will listen on. The default value is `63123`.
- `event_output`: The output type for Source Change Events. The default value is `none`.
- `telemetry_output`: The output type for Test Run Telemetry data. The default value is `none`.
- `log_output`: The output type for Test Run Log data. The default value is `none`.

### Command Line Arguments

You can pass the configuration parameters directly through command line arguments when starting the service. Here is an example:

```sh
cargo run -- \
  --config /path/to/config.json \
  --data /path/to/data_cache \
  --port 8080 \
  --event_out publish \
  --telem_out publish \
  --log_out publish
```

Each argument corresponds to a configuration parameter mentioned earlier.

### Web API Initialization

Alternatively, you can initialize the TestRunHost through the Web API by providing the necessary configuration parameters. This allows you to dynamically configure the service without restarting it. Details on how to interact with the Web API can be found in the API documentation.

Remember to ensure that the configuration parameters are properly set before running the test.






## Time

The following times are associated with a SOurceCHangeEvent:
- time change occured in source
- time change was read from source change log
- time change was queued for 
- ...

We care about the time the change happened in the db.

In the test replay, need to think about the following things:

1. When do we want Drasi to think the events are happening during the test. Could be:
- RECORDED -> At the same time the happened when the data was captured.
- REBASED -> At a specific point in time that is important to whatever we are testing and different than the recorded time.
- LIVE -> At the time the test is executed.

2. How quickly do we want the events to be replayed. This should be indeoendent of the times in the events and Drasi should be able to deal with it. Could be:
- Using the same spacing as when they were recorded.
- Using a fixed spacing
- Using a compressed or stretched version of the recorded interval
- As fast as possible





# SourceChangeEvent

``` json
  {
    "schema": {},
    "payload": {
      "before": {
        "id": 1,
        "first_name": "Anne Marie",
        "last_name": "Smith",
        "email": "annes@noanswer.org"
      },
      "after": {
        "id": 1,
        "first_name": "Anne Marie",
        "last_name": "Kretchmar",
        "email": "annek@noanswer.org"
      },
      "source": {
        "ts_ms": 1559033904863,
        "ts_us": 1559033904863769,
        "ts_ns": 1559033904863769000,
        "db": "postgres",
        "schema": "public",
        "table": "customers",
        "txId": 556,
        "lsn": 24023128
      },
      "op": "u",
      "ts_ms": 1465584025523,
      "ts_us": 1465584025523514,
      "ts_ns": 1465584025523514964
    }
  }
```