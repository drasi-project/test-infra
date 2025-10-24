#!/bin/bash

# Script to run Redis in a Docker container for Drasi Server Redis test

CONTAINER_NAME="drasi-test-redis"
REDIS_PORT=6379
REDIS_IMAGE="redis:7-alpine"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_message "$RED" "Error: Docker is not running or not installed"
        exit 1
    fi
}

# Function to check if container is already running
is_container_running() {
    docker ps --format "table {{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Function to check if container exists but is stopped
container_exists() {
    docker ps -a --format "table {{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Function to start Redis
start_redis() {
    check_docker

    if is_container_running; then
        print_message "$YELLOW" "Redis container '${CONTAINER_NAME}' is already running on port ${REDIS_PORT}"
        return 0
    fi

    if container_exists; then
        print_message "$YELLOW" "Starting existing Redis container '${CONTAINER_NAME}'..."
        docker start ${CONTAINER_NAME}
    else
        print_message "$GREEN" "Creating and starting new Redis container '${CONTAINER_NAME}'..."
        docker run -d \
            --name ${CONTAINER_NAME} \
            -p ${REDIS_PORT}:6379 \
            ${REDIS_IMAGE} \
            redis-server \
            --appendonly yes
    fi

    # Wait for Redis to be ready
    print_message "$YELLOW" "Waiting for Redis to be ready..."
    for i in {1..30}; do
        if docker exec ${CONTAINER_NAME} redis-cli ping >/dev/null 2>&1; then
            print_message "$GREEN" "✓ Redis is running and ready on port ${REDIS_PORT}"
            return 0
        fi
        sleep 0.5
    done

    print_message "$RED" "Error: Redis failed to start properly"
    return 1
}

# Function to stop Redis
stop_redis() {
    check_docker

    if is_container_running; then
        print_message "$YELLOW" "Stopping Redis container '${CONTAINER_NAME}'..."
        docker stop ${CONTAINER_NAME}
        print_message "$GREEN" "✓ Redis container stopped"
    else
        print_message "$YELLOW" "Redis container '${CONTAINER_NAME}' is not running"
    fi
}

# Function to remove Redis container
remove_redis() {
    check_docker

    if is_container_running; then
        print_message "$YELLOW" "Stopping Redis container '${CONTAINER_NAME}'..."
        docker stop ${CONTAINER_NAME}
    fi

    if container_exists; then
        print_message "$YELLOW" "Removing Redis container '${CONTAINER_NAME}'..."
        docker rm ${CONTAINER_NAME}
        print_message "$GREEN" "✓ Redis container removed"
    else
        print_message "$YELLOW" "Redis container '${CONTAINER_NAME}' does not exist"
    fi
}

# Function to check Redis status
status_redis() {
    check_docker

    if is_container_running; then
        print_message "$GREEN" "✓ Redis container '${CONTAINER_NAME}' is running on port ${REDIS_PORT}"

        # Test connection
        if docker exec ${CONTAINER_NAME} redis-cli ping >/dev/null 2>&1; then
            print_message "$GREEN" "✓ Redis is responding to ping"

            # Show stream info if any exist
            echo ""
            print_message "$YELLOW" "Redis Streams:"
            docker exec ${CONTAINER_NAME} redis-cli --raw KEYS "drasi:*" 2>/dev/null | while read stream; do
                if [ ! -z "$stream" ]; then
                    LENGTH=$(docker exec ${CONTAINER_NAME} redis-cli XLEN "$stream" 2>/dev/null || echo "0")
                    echo "  $stream: $LENGTH entries"
                fi
            done
        else
            print_message "$YELLOW" "⚠ Redis container is running but not responding to ping"
        fi

        # Show container details
        echo ""
        docker ps --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    else
        print_message "$YELLOW" "Redis container '${CONTAINER_NAME}' is not running"
    fi
}

# Function to show Redis logs
logs_redis() {
    check_docker

    if container_exists; then
        print_message "$YELLOW" "Showing Redis logs (last 50 lines)..."
        docker logs --tail 50 ${CONTAINER_NAME}
    else
        print_message "$RED" "Error: Redis container '${CONTAINER_NAME}' does not exist"
    fi
}

# Function to connect to Redis CLI
cli_redis() {
    check_docker

    if is_container_running; then
        print_message "$GREEN" "Connecting to Redis CLI..."
        print_message "$YELLOW" "Useful commands for Drasi streams:"
        echo "  XLEN drasi:source:facilities-db     # Check source stream length"
        echo "  XRANGE drasi:source:facilities-db - + COUNT 10     # View last 10 entries"
        echo "  XLEN drasi:reaction:building-comfort:results     # Check reaction stream length"
        echo "  KEYS drasi:*     # List all Drasi-related keys"
        echo ""
        docker exec -it ${CONTAINER_NAME} redis-cli
    else
        print_message "$RED" "Error: Redis container '${CONTAINER_NAME}' is not running"
        print_message "$YELLOW" "Run '$0 start' to start Redis"
    fi
}

# Function to clear Drasi streams
clear_streams() {
    check_docker

    if is_container_running; then
        print_message "$YELLOW" "Clearing Drasi Redis streams..."

        # Clear known Drasi streams
        docker exec ${CONTAINER_NAME} redis-cli DEL drasi:source:facilities-db 2>/dev/null
        docker exec ${CONTAINER_NAME} redis-cli DEL drasi:reaction:building-comfort:results 2>/dev/null

        # Clear any other Drasi-related keys
        docker exec ${CONTAINER_NAME} redis-cli --raw KEYS "drasi:*" 2>/dev/null | while read key; do
            if [ ! -z "$key" ]; then
                docker exec ${CONTAINER_NAME} redis-cli DEL "$key" >/dev/null 2>&1
                echo "  Cleared: $key"
            fi
        done

        print_message "$GREEN" "✓ Drasi streams cleared"
    else
        print_message "$RED" "Error: Redis container '${CONTAINER_NAME}' is not running"
    fi
}

# Function to show usage
usage() {
    cat << EOF
Usage: $0 {start|stop|restart|remove|status|logs|cli|clear|help}

Commands:
  start    - Start Redis container (creates if doesn't exist)
  stop     - Stop Redis container (keeps data)
  restart  - Restart Redis container
  remove   - Stop and remove Redis container (removes data)
  status   - Check if Redis is running and show stream info
  logs     - Show Redis container logs
  cli      - Connect to Redis CLI with Drasi hints
  clear    - Clear all Drasi-related Redis streams
  help     - Show this help message

Redis will be available on localhost:${REDIS_PORT}

Drasi Stream Names:
  Source:   drasi:source:facilities-db
  Reaction: drasi:reaction:building-comfort:results

Examples:
  $0 start           # Start Redis
  $0 status          # Check Redis and streams
  $0 clear           # Clear Drasi streams
  $0 cli             # Connect to Redis CLI
  $0 stop            # Stop Redis (data persists)
  $0 remove          # Remove Redis container and data
EOF
}

# Main script logic
case "${1:-}" in
    start)
        start_redis
        ;;
    stop)
        stop_redis
        ;;
    restart)
        stop_redis
        sleep 1
        start_redis
        ;;
    remove)
        remove_redis
        ;;
    status)
        status_redis
        ;;
    logs)
        logs_redis
        ;;
    cli)
        cli_redis
        ;;
    clear)
        clear_streams
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        if [ -z "$1" ]; then
            # No argument provided - default to start
            start_redis
        else
            print_message "$RED" "Error: Unknown command '$1'"
            echo ""
            usage
            exit 1
        fi
        ;;
esac