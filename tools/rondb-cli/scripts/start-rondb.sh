#!/bin/bash
#
# Start RonDB in Docker for local development/testing.
#
# Exposes:
#   - MySQL on port 3306
#   - Rondis (Redis protocol) on port 6379
#   - REST API on port 4406
#
# Usage:
#   ./start-rondb.sh          # Start RonDB
#   ./start-rondb.sh stop     # Stop RonDB
#   ./start-rondb.sh logs     # View logs
#   ./start-rondb.sh shell    # Open MySQL shell
#   ./start-rondb.sh redis    # Test Redis connection
#
# The script uses the official rondb-docker setup from Hopsworks.
# See: https://github.com/logicalclocks/rondb-docker
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RONDB_DOCKER_DIR="${SCRIPT_DIR}/rondb-docker"

show_usage() {
    cat <<EOF
Usage: $0 [command]

Commands:
  start   - Start RonDB cluster (default)
  stop    - Stop RonDB
  logs    - View container logs
  shell   - Open MySQL shell
  redis   - Test Rondis (Redis) connection
  status  - Show container status
  clean   - Stop and remove all containers and volumes

Exposed ports:
  3306  - MySQL (user: root, empty password)
  6379  - Rondis (Redis protocol)
  4406  - REST API
  1186  - NDB Management

Requirements:
  - Docker with Compose v2+
  - git (for first-time setup)
EOF
}

ensure_rondb_docker() {
    if [ ! -d "$RONDB_DOCKER_DIR" ]; then
        echo ">>> Cloning rondb-docker repository..."
        git clone --depth 1 https://github.com/logicalclocks/rondb-docker.git "$RONDB_DOCKER_DIR"
        echo ""
    fi
}

start_cluster() {
    ensure_rondb_docker
    cd "$RONDB_DOCKER_DIR"

    echo ">>> Starting RonDB cluster (mini size)..."
    echo ""

    # Use the official run.sh with mini size (2.5GB RAM, minimal setup)
    ./run.sh --size mini

    echo ""
    echo ">>> RonDB cluster starting!"
    echo ""
    echo "Wait ~60 seconds for full initialization, then:"
    echo ""
    echo "Endpoints:"
    echo "  MySQL:   mysql -h 127.0.0.1 -P 3306 -u root"
    echo "  Redis:   redis-cli -h 127.0.0.1 -p 6379"
    echo ""
    echo "CLI commands:"
    echo "  ./rondb status   - Check connection"
    echo "  ./rondb          - Start interactive shell"
}

case "${1:-start}" in
    start)
        start_cluster
        ;;

    stop)
        if [ -d "$RONDB_DOCKER_DIR" ]; then
            cd "$RONDB_DOCKER_DIR"
            docker compose down 2>/dev/null || docker-compose down
            echo "RonDB stopped."
        else
            echo "RonDB not set up. Run '$0 start' first."
            exit 1
        fi
        ;;

    logs)
        if [ -d "$RONDB_DOCKER_DIR" ]; then
            cd "$RONDB_DOCKER_DIR"
            docker compose logs -f "${2:-}" 2>/dev/null || docker-compose logs -f "${2:-}"
        else
            echo "RonDB not set up. Run '$0 start' first."
            exit 1
        fi
        ;;

    shell)
        if [ -d "$RONDB_DOCKER_DIR" ]; then
            cd "$RONDB_DOCKER_DIR"
            # Find the mysqld container
            MYSQL_CONTAINER=$(docker ps --filter "name=mysqld" --format "{{.Names}}" | head -1)
            if [ -n "$MYSQL_CONTAINER" ]; then
                docker exec -it "$MYSQL_CONTAINER" mysql -u root
            else
                echo "MySQL container not running. Start with '$0 start' first."
                exit 1
            fi
        else
            echo "RonDB not set up. Run '$0 start' first."
            exit 1
        fi
        ;;

    redis)
        echo "Testing Rondis connection..."
        if command -v redis-cli &>/dev/null; then
            redis-cli -h 127.0.0.1 -p 6379 PING
        else
            echo "redis-cli not found. Install it or use:"
            echo "  docker run --rm -it --network host redis redis-cli -h 127.0.0.1 -p 6379 PING"
        fi
        ;;

    status)
        if [ -d "$RONDB_DOCKER_DIR" ]; then
            cd "$RONDB_DOCKER_DIR"
            docker compose ps 2>/dev/null || docker-compose ps
        else
            echo "RonDB not set up. Run '$0 start' first."
            exit 1
        fi
        ;;

    clean)
        if [ -d "$RONDB_DOCKER_DIR" ]; then
            cd "$RONDB_DOCKER_DIR"
            docker compose down -v --remove-orphans 2>/dev/null || docker-compose down -v --remove-orphans
            echo "Containers and volumes removed."
        else
            echo "RonDB not set up."
        fi
        ;;

    -h|--help|help)
        show_usage
        ;;

    *)
        echo "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
