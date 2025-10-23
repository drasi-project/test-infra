#!/bin/bash

# Exit on any error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Drasi Server Test - Redis Communication${NC}"
echo "================================================="

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

# Function to cleanup processes
cleanup() {
    echo -e "${YELLOW}Cleaning up processes...${NC}"

    # Kill Drasi Server
    if [ ! -z "$DRASI_PID" ]; then
        kill $DRASI_PID 2>/dev/null || true
        wait $DRASI_PID 2>/dev/null || true
    fi

    # Stop Redis
    "$SCRIPT_DIR/run_redis.sh" stop 2>/dev/null || true

    # Kill any remaining test service processes
    pkill -f "test-service.*building_comfort.*drasi_server_redis" 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Kill any existing processes
echo -e "${YELLOW}Cleaning up any existing processes...${NC}"
pkill -f "drasi-server" 2>/dev/null || true
pkill -f "test-service.*building_comfort.*drasi_server_redis" 2>/dev/null || true
sleep 2

# Start Redis
echo -e "${BLUE}Starting Redis server...${NC}"
"$SCRIPT_DIR/run_redis.sh" start
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to start Redis${NC}"
    exit 1
fi

# Verify Redis is accessible
echo -e "${YELLOW}Verifying Redis connection...${NC}"
if ! docker exec drasi-test-redis redis-cli ping > /dev/null 2>&1; then
    echo -e "${RED}Error: Redis is not accessible${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Redis is ready${NC}"

# Build Drasi Server in release mode
echo -e "${YELLOW}Building Drasi Server (Release)...${NC}"
cd "$DRASI_SERVER_DIR"
cargo build --release

# Use the server config file
CONFIG_FILE="$SCRIPT_DIR/server-config.yaml"

# Start Drasi Server in background with info logging (suppressing noisy modules)
echo -e "${YELLOW}Starting Drasi Server with Redis support...${NC}"
LOG_FILE="$SCRIPT_DIR/drasi-server.log"
RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
    ./target/release/drasi-server --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
DRASI_PID=$!
echo "Drasi Server PID: $DRASI_PID"
echo "Drasi Server log: $LOG_FILE"

# Wait for server to be ready
echo -e "${YELLOW}Waiting for Drasi Server to be ready...${NC}"
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Drasi Server is ready!${NC}"
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
    exit 1
fi

# Wait a bit more for Redis sources/reactions to be fully ready
sleep 3

# Clear any existing data from Redis streams
echo -e "${YELLOW}Clearing Redis streams...${NC}"
docker exec drasi-test-redis redis-cli DEL drasi:source:facilities-db 2>/dev/null || true
docker exec drasi-test-redis redis-cli DEL drasi:reaction:building-comfort:results 2>/dev/null || true

# Run the E2E test with info logging
echo -e "${YELLOW}Starting E2E Test Framework...${NC}"
cd "$E2E_ROOT"
RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
    cargo run --release --manifest-path ./test-service/Cargo.toml -- \
    --config "$SCRIPT_DIR/config.json"

TEST_EXIT_CODE=$?

# Show test results
echo ""
echo -e "${BLUE}Test Summary:${NC}"
echo "================================"

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ Test completed successfully!${NC}"

    # Show Redis stream statistics
    echo ""
    echo -e "${BLUE}Redis Stream Statistics:${NC}"

    SOURCE_LEN=$(docker exec drasi-test-redis redis-cli XLEN drasi:source:facilities-db 2>/dev/null || echo "0")
    REACTION_LEN=$(docker exec drasi-test-redis redis-cli XLEN drasi:reaction:building-comfort:results 2>/dev/null || echo "0")

    echo "  Source stream length: $SOURCE_LEN"
    echo "  Reaction stream length: $REACTION_LEN"

    echo ""
    echo "Performance metrics saved to: $SCRIPT_DIR/test_data_cache"
else
    echo -e "${RED}✗ Test failed with exit code: $TEST_EXIT_CODE${NC}"
    echo ""
    echo "Debug information:"
    echo "  - Drasi Server log: $LOG_FILE"
    echo "  - Test data cache: $SCRIPT_DIR/test_data_cache"

    # Show last few lines of server log on failure
    echo ""
    echo "Last 20 lines of Drasi Server log:"
    tail -20 "$LOG_FILE"
fi

exit $TEST_EXIT_CODE