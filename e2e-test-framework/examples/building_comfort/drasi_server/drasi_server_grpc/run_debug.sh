#!/bin/bash

# Exit on any error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Drasi Server Test - gRPC (Debug Mode)${NC}"
echo "================================================"

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Navigate to e2e-test-framework root
cd "$SCRIPT_DIR/../../../.."
E2E_ROOT="$(pwd)"

# Navigate to drasi-server
DRASI_SERVER_DIR="$E2E_ROOT/../../drasi-server"
if [ ! -d "$DRASI_SERVER_DIR" ]; then
    echo -e "${RED}Error: Drasi Server directory not found at $DRASI_SERVER_DIR${NC}"
    exit 1
fi

# Kill any existing processes
echo -e "${YELLOW}Cleaning up any existing processes...${NC}"
pkill -f "drasi-server" 2>/dev/null || true
pkill -f "test-service.*building_comfort.*drasi_server_grpc" 2>/dev/null || true
sleep 2

# Build Drasi Server in debug mode
echo -e "${YELLOW}Building Drasi Server (Debug)...${NC}"
cd "$DRASI_SERVER_DIR"
cargo build

# Use the permanent server config file
CONFIG_FILE="$SCRIPT_DIR/server-config.yaml"

# Start Drasi Server in background with debug logging
echo -e "${YELLOW}Starting Drasi Server with debug logging...${NC}"
LOG_FILE="$SCRIPT_DIR/drasi-server-debug.log"
RUST_LOG=debug ./target/debug/drasi-server --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
DRASI_PID=$!
echo "Drasi Server PID: $DRASI_PID"
echo "Drasi Server log: $LOG_FILE"

# Wait for server to be ready
echo -e "${YELLOW}Waiting for Drasi Server to be ready...${NC}"
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}Drasi Server is ready!${NC}"
        break
    fi
    echo -n "."
    sleep 1
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo -e "${RED}Error: Drasi Server failed to start after 30 seconds${NC}"
    echo "Last 50 lines of server log:"
    tail -50 "$LOG_FILE"
    kill $DRASI_PID 2>/dev/null || true
    exit 1
fi

# Wait a bit more for gRPC source to be fully ready
sleep 2

# Run the E2E test with debug logging
echo -e "${YELLOW}Starting E2E Test Framework (Debug)...${NC}"
cd "$E2E_ROOT"
RUST_LOG=debug cargo run --manifest-path ./test-service/Cargo.toml -- \
    --config "$SCRIPT_DIR/config.json"

TEST_EXIT_CODE=$?

# Clean up
echo -e "${YELLOW}Cleaning up...${NC}"
kill $DRASI_PID 2>/dev/null || true
wait $DRASI_PID 2>/dev/null || true

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Test completed successfully!${NC}"
else
    echo -e "${RED}Test failed with exit code: $TEST_EXIT_CODE${NC}"
    echo "Check the server log at: $LOG_FILE"
fi

exit $TEST_EXIT_CODE