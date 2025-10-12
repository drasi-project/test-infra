#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Stopping Drasi Server Test - HTTP Adaptive${NC}"
echo "=========================================="

# Function to kill process by pattern
kill_process() {
    local pattern="$1"
    local name="$2"

    PIDS=$(pgrep -f "$pattern" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo -e "${YELLOW}Stopping $name processes: $PIDS${NC}"
        kill -TERM $PIDS 2>/dev/null

        # Wait up to 5 seconds for graceful shutdown
        for i in {1..5}; do
            if ! pgrep -f "$pattern" > /dev/null 2>&1; then
                echo -e "${GREEN}$name stopped gracefully${NC}"
                return 0
            fi
            sleep 1
        done

        # Force kill if still running
        PIDS=$(pgrep -f "$pattern" 2>/dev/null)
        if [ -n "$PIDS" ]; then
            echo -e "${YELLOW}Force killing $name processes: $PIDS${NC}"
            kill -KILL $PIDS 2>/dev/null
            sleep 1
        fi
    else
        echo -e "${GREEN}No $name processes found${NC}"
    fi
}

# Kill Drasi Server
kill_process "drasi-server.*server-config" "Drasi Server"

# Kill E2E Test Service for this specific test
kill_process "test-service.*building_comfort.*drasi_server_http_adaptive" "E2E Test Service"

# Clean up any orphaned processes on specific ports
echo -e "${YELLOW}Checking for processes on test ports...${NC}"

# Check port 8080 (Drasi Server API)
if lsof -i:8080 > /dev/null 2>&1; then
    echo -e "${YELLOW}Killing process on port 8080${NC}"
    lsof -ti:8080 | xargs kill -9 2>/dev/null || true
fi

# Check port 8081 (HTTP source)
if lsof -i:8081 > /dev/null 2>&1; then
    echo -e "${YELLOW}Killing process on port 8081${NC}"
    lsof -ti:8081 | xargs kill -9 2>/dev/null || true
fi

# Check port 3000 (HTTP reaction)
if lsof -i:3000 > /dev/null 2>&1; then
    echo -e "${YELLOW}Killing process on port 3000${NC}"
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
fi

echo -e "${GREEN}All processes stopped${NC}"
echo ""
echo "Log files (if any) are preserved at:"
echo "  - drasi-server-debug.log"
echo "  - drasi-server-release.log"