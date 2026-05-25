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

# Helper for rondis_hash_concurrency.test. Spawns N parallel redis-cli
# HSETs against one hash and waits for all to complete, propagating any
# non-zero exit. Each worker writes a disjoint set of "w<id>_f<j>" fields
# plus a shared set of "s_f<j>" fields, so concurrent workers contend on
# both the hset_keys(key) X-lock (shared by all of them) and on the
# string_keys row writes for the shared field names.
#
# Distinct fields after all workers complete = workers * unique + shared.

set -e

REDIS_HOST="${RONDIS_HOST:-localhost}"
REDIS_PORT="${RONDIS_PORT:-6379}"
redis-cli() {
    command redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

hash="$1"
W="$2"           # workers
U="$3"           # unique fields per worker
S="$4"           # shared fields across all workers

worker_hset() {
    local id="$1"
    local args=()
    local j
    for j in $(seq 1 "$U"); do
        args+=("w${id}_f${j}" "v${id}_${j}")
    done
    for j in $(seq 1 "$S"); do
        args+=("s_f${j}" "vshared")
    done
    redis-cli HSET "$hash" "${args[@]}" > /dev/null
}

pids=()
for i in $(seq 1 "$W"); do
    worker_hset "$i" &
    pids+=($!)
done

fail=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "FAIL: at least one concurrent HSET worker exited non-zero" >&2
    exit 1
fi

distinct=$((W * U + S))
echo "concurrent_hsets $hash workers=$W unique=$U shared=$S distinct=$distinct"
