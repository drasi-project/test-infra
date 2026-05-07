# gRPC Example

This example demonstrates how to use gRPC-based source change dispatchers and reaction handlers in the E2E Test Framework.

## Overview

The gRPC integration provides an alternative to HTTP-based communication for:
- **Source Change Dispatchers**: Send change events via gRPC instead of HTTP
- **Reaction Handlers**: Receive reaction invocations via gRPC instead of HTTP webhooks

## Features

- Protocol Buffers for efficient serialization
- Support for both batch and individual event dispatching
- Optional TLS support for secure communication
- Streaming support (future enhancement)

## Configuration

### Source Change Dispatcher

```json
{
  "kind": "Grpc",
  "host": "localhost",
  "port": 50051,
  "timeout_seconds": 30,
  "batch_events": true,
  "tls": false
}
```

### Reaction Handler

```json
{
  "kind": "Grpc",
  "host": "0.0.0.0",
  "port": 50052,
  "correlation_metadata_key": "x-correlation-id"
}
```

## Running the Example

1. Start the test service:
```bash
cargo run -p test-service
```

2. In another terminal, run the test:
```bash
curl -X POST http://localhost:8080/api/test_runs \
  -H "Content-Type: application/json" \
  -d @examples/grpc_example/config.json
```

3. Start the test run:
```bash
curl -X POST http://localhost:8080/api/test_runs/{run_id}/start
```

## gRPC Service Definitions

### Source Dispatcher Service

```protobuf
service SourceDispatcher {
    rpc DispatchBatch(SourceChangeEventBatch) returns (DispatchResponse);
    rpc DispatchSingle(SourceChangeEvent) returns (DispatchResponse);
}
```

### Reaction Handler Service

```protobuf
service ReactionHandler {
    rpc HandleInvocation(ReactionInvocation) returns (InvocationResponse);
    rpc StreamInvocations(stream ReactionInvocation) returns (stream InvocationResponse);
}
```

## Integration with External gRPC Servers

To integrate with your own gRPC servers:

1. Implement the `SourceDispatcher` service to receive change events
2. Configure the test to use the gRPC dispatcher with your server's address
3. For reactions, the test framework acts as the gRPC server and your application calls it

## Performance Considerations

- gRPC uses HTTP/2 for multiplexing and better connection management
- Protocol Buffers provide more efficient serialization than JSON
- Batch mode reduces network overhead for high-volume scenarios

## Troubleshooting

- Ensure ports 50051 and 50052 are available
- Check firewall settings if running across networks
- Enable debug logging with `RUST_LOG=debug` for detailed diagnostics