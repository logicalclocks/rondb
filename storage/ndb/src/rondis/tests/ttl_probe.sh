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

# Helper for rondis_keyinfo_ttl.test. Runs TTL / PTTL and range-checks
# the integer reply against a host-clock-jitter window. Emits a single
# deterministic PASS / FAIL line so the .result baseline does not
# embed live timestamps.
#
# Usage: ttl_probe.sh <command> <key> <lo> <hi>
#   <command>  - "TTL" or "PTTL"
#   <key>      - Redis key name
#   <lo> <hi>  - inclusive window for the integer reply

set -e

REDIS_HOST="${RONDIS_HOST:-localhost}"
REDIS_PORT="${RONDIS_PORT:-6379}"
redis-cli() {
    command redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

cmd="$1"
key="$2"
lo="$3"
hi="$4"

reply=$(redis-cli "$cmd" "$key")
if [[ "$reply" =~ ^-?[0-9]+$ ]] && (( reply >= lo )) && (( reply <= hi )); then
    echo "$cmd $key in_window=1"
else
    echo "$cmd $key reply=$reply not in [$lo,$hi]"
    exit 1
fi
