#!/bin/bash
#  Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, version 2.0,
#  as published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License, version 2.0, for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# Helper for rondis_hash_lifecycle.test. MTR's --exec runs the command
# directly (no shell), so $(...) substitution doesn't work and assertions
# that need a generated large value or a generated long argv must run
# inside a script. Each scenario emits exactly one deterministic OK line
# so the .result baseline stays stable.

set -e

REDIS_HOST="${RONDIS_HOST:-localhost}"
REDIS_PORT="${RONDIS_PORT:-6379}"
redis-cli() {
    command redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

scenario="${1:-}"
case "$scenario" in
    large_value)
        # large_value <hash> <field> <length>
        # HSETs a single field whose value is <length> 'x' bytes. Used to
        # exercise the >INLINE_VALUE_LEN ext-row branch in Phase 2 of the
        # single-trans HSET pipeline.
        hash="$2"; field="$3"; length="$4"
        value=$(head -c "$length" < /dev/zero | tr '\0' 'x')
        out=$(redis-cli HSET "$hash" "$field" "$value")
        echo "large_value $hash $field len=$length hset_reply=$out"
        ;;
    many_fields)
        # many_fields <hash> <count>
        # HSETs <count> distinct fields f1..fN with v1..vN in one call.
        # Used to exercise Phase 2 / Phase 3 chunk loops past
        # MAX_PARALLEL_KEY_OPS.
        hash="$2"; count="$3"
        args=()
        for i in $(seq 1 "$count"); do
            args+=("f$i" "v$i")
        done
        out=$(redis-cli HSET "$hash" "${args[@]}")
        echo "many_fields $hash count=$count hset_reply=$out"
        ;;
    *)
        echo "unknown scenario: $scenario" >&2
        exit 1
        ;;
esac
