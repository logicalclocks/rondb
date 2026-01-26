#!/bin/bash
#
#   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License, version 2.0,
#   as published by the Free Software Foundation.
#
#   This program is designed to work with certain software (including
#   but not limited to OpenSSL) that is licensed under separate terms,
#   as designated in a particular file or component or in included license
#   documentation.  The authors of MySQL hereby grant you an additional
#   permission to link the program and your derivative works with the
#   separately licensed software that they have either included with
#   the program or referenced in the documentation.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License, version 2.0, for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
#
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
    # Run detached so we can continue with post-setup
    ./run.sh --size mini --detached

    echo ">>> Waiting for MySQL to be ready..."
    for i in {1..60}; do
        if docker exec mysqld_1 mysqladmin ping -uroot &>/dev/null; then
            break
        fi
        sleep 2
    done

    echo ">>> Configuring MySQL access..."
    # Allow root from any host (for CLI access from outside Docker)
    docker exec mysqld_1 mysql -uroot -e "
        CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '';
        GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
        FLUSH PRIVILEGES;
    " 2>/dev/null || true

    echo ">>> Initializing Rondis tables..."
    # Create Rondis tables
    docker exec mysqld_1 mysql -uroot -e "
        CREATE DATABASE IF NOT EXISTS redis_0;
        CREATE TABLE IF NOT EXISTS redis_0.string_keys(
            redis_key_id BIGINT UNSIGNED NOT NULL,
            redis_key VARBINARY(3000) NOT NULL,
            rondb_key BIGINT UNSIGNED AUTO_INCREMENT NULL,
            value_data_type INT UNSIGNED NOT NULL,
            tot_value_len INT UNSIGNED NOT NULL,
            num_rows INT UNSIGNED NOT NULL,
            value_start VARBINARY(4096) NOT NULL,
            expiry_date TIMESTAMP,
            KEY ttl_index(expiry_date),
            PRIMARY KEY (redis_key_id, redis_key) USING HASH,
            UNIQUE KEY (rondb_key) USING HASH
        ) ENGINE NDB CHARSET = latin1;
        CREATE TABLE IF NOT EXISTS redis_0.hset_keys(
            redis_key VARBINARY(3000) NOT NULL,
            redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            PRIMARY KEY (redis_key) USING HASH,
            UNIQUE KEY (redis_key_id) USING HASH
        ) ENGINE NDB CHARSET latin1;
        CREATE TABLE IF NOT EXISTS redis_0.string_values(
            rondb_key BIGINT UNSIGNED NOT NULL,
            ordinal INT UNSIGNED NOT NULL,
            expiry_date TIMESTAMP,
            value VARBINARY(29500) NOT NULL,
            KEY ttl_index(expiry_date),
            PRIMARY KEY (rondb_key, ordinal)
        ) ENGINE NDB CHARSET latin1;
    " 2>/dev/null || true

    echo ""
    echo ">>> Configuring Rondis..."

    # Find the autogenerated docker-compose file (use absolute path for docker volume mounts)
    COMPOSE_FILE=$(find "$RONDB_DOCKER_DIR/autogenerated_files" -name "docker_compose.yml" -type f 2>/dev/null | head -1)
    if [ -n "$COMPOSE_FILE" ]; then
        COMPOSE_DIR=$(dirname "$COMPOSE_FILE")

        # Patch docker-compose to use rdrs2 and expose port 6379
        if grep -q 'command: \["rdrs"' "$COMPOSE_FILE" 2>/dev/null; then
            # Use perl for portable in-place editing (works on macOS and Linux)
            perl -i -pe 's|command: \["rdrs", "-config=/srv/hops/mysql-cluster/rest_api.json"\]|entrypoint: ["/srv/hops/mysql-24.10.0/bin/rdrs2"]\n      command: ["--config", "/srv/hops/mysql-cluster/rest_api.json"]|' "$COMPOSE_FILE"
            # Add port 6379
            perl -i -pe 's|- 5406:5406|- 5406:5406\n      - 6379:6379|' "$COMPOSE_FILE"
        fi

        # Patch rest_api.json to enable Rondis
        REST_API_JSON="$COMPOSE_DIR/rest_api.json"
        if [ -f "$REST_API_JSON" ]; then
            cat > "$REST_API_JSON" << 'RESTJSON'
{
    "REST": {
        "Enable": true,
        "ServerIP": "0.0.0.0",
        "ServerPort": 4406
    },
    "Rondis": {
        "Enable": true,
        "ServerIP": "0.0.0.0",
        "ServerPort": 6379
    },
    "GRPC": {
        "Enable": false
    },
    "RonDB": {
        "Mgmds": [
            {
                "IP": "mgmd_1",
                "Port": 1186
            }
        ]
    },
    "Security": {
        "APIKey": {
            "UseHopsworksAPIKeys": false
        }
    }
}
RESTJSON
        fi

        # Get network name from compose
        NETWORK_NAME=$(docker network ls --filter "name=v24100" --format "{{.Name}}" | head -1)
        if [ -z "$NETWORK_NAME" ]; then
            NETWORK_NAME="rondb_v24100_m1_g1_r1_my1_ra1_bn1_default"
        fi

        # Remove old rest_1 if exists and start with correct config
        docker rm -f rest_1 2>/dev/null || true
        docker run -d --name rest_1 \
            --network "$NETWORK_NAME" \
            -v "$COMPOSE_DIR/rest_api.json:/srv/hops/mysql-cluster/rest_api.json:ro" \
            -p 4406:4406 -p 5406:5406 -p 6379:6379 \
            --user 501:20 \
            --entrypoint /srv/hops/mysql-24.10.0/bin/rdrs2 \
            hopsworks/rondb:24.10.0 \
            --config /srv/hops/mysql-cluster/rest_api.json >/dev/null 2>&1 || true
    fi

    # Wait for Rondis to be ready
    sleep 5

    echo ""
    echo ">>> RonDB cluster ready!"
    echo ""
    echo "Endpoints:"
    echo "  MySQL:   mysql -h 127.0.0.1 -P 3306 -u root"
    echo "  Rondis:  redis-cli -h 127.0.0.1 -p 6379"
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
