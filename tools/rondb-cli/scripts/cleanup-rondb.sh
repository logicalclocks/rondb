#!/bin/bash
#
# Clean up all RonDB Docker resources
#
# Usage:
#   ./cleanup-rondb.sh          # Stop containers and remove volumes
#   ./cleanup-rondb.sh --all    # Also remove downloaded rondb-docker repo
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RONDB_DOCKER_DIR="${SCRIPT_DIR}/rondb-docker"

echo ">>> Stopping all RonDB containers..."
docker stop $(docker ps -q --filter "name=mgmd" --filter "name=mysqld" --filter "name=ndbd" --filter "name=rest_" --filter "name=bench_") 2>/dev/null || true

echo ">>> Removing containers..."
docker rm -f $(docker ps -aq --filter "name=mgmd" --filter "name=mysqld" --filter "name=ndbd" --filter "name=rest_" --filter "name=bench_") 2>/dev/null || true
# Also remove rest_1 specifically (may have been created outside compose)
docker rm -f rest_1 2>/dev/null || true

echo ">>> Removing RonDB volumes..."
docker volume ls -q | grep -E "rondb|v24100" | xargs -r docker volume rm 2>/dev/null || true

echo ">>> Removing networks..."
docker network ls -q --filter "name=rondb\|v24100" | xargs -r docker network rm 2>/dev/null || true

if [ "$1" = "--all" ] && [ -d "$RONDB_DOCKER_DIR" ]; then
    echo ">>> Removing rondb-docker repository..."
    rm -rf "$RONDB_DOCKER_DIR"
fi

echo ""
echo ">>> Cleanup complete!"
echo ""
echo "To start fresh:"
echo "  ./start-rondb.sh"
