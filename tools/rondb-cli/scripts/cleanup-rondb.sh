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
