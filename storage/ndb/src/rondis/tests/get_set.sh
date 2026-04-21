#!/bin/bash

#  Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, version 2.0,
#  as published by the Free Software Foundation.
#
#  This program is designed to work with certain software (including
#  but not limited to OpenSSL) that is licensed under separate terms,
#  as designated in a particular file or component or in included license
#  documentation.  The authors of MySQL hereby grant you an additional
#  permission to link the program and your derivative works with the
#  separately licensed software that they have either included with
#  the program or referenced in the documentation.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License, version 2.0, for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

set -e

# Allow MTR / external harness to override Rondis host and port. The real
# redis-cli does not know about RONDIS_HOST/RONDIS_PORT, so wrap it in a
# function that injects -h / -p and keep every call site unchanged.
REDIS_HOST="${RONDIS_HOST:-localhost}"
REDIS_PORT="${RONDIS_PORT:-6379}"
redis-cli() {
    command redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

# Change key suffix using script arguments
KEY_SUFFIX=${1:-0}
KEY="test_key_$KEY_SUFFIX"

function check_set() {
    local key="$1"
    local value="$2"

    # SET the value in Redis
    if [[ -f "$value" ]]; then
        set_output=$(redis-cli --pipe <<EOF
SET $key $(< "$value")
EOF
)
    else
        set_output=$(redis-cli SET "$key" "$value")
    fi

    #echo $set_output
    if [[ $set_output == ERR* ]]; then
        echo "FAIL: Could not SET $key with given value" >&2
        exit 1
    fi
}

# Function to set a value and retrieve it, then verify if it matches
function set_and_get() {
    local key="$1"
    local value="$2"
    
    check_set "$key" "$value"

    # GET the value
    local result=$(redis-cli GET "$key")

    local expected_hash=$(echo -n "$value" | sha256sum | awk '{print $1}')
    local actual_hash=$(echo -n "$result" | sha256sum | awk '{print $1}')
    
    # Check if the retrieved value matches the expected value
    if [[ "$expected_hash" == "$actual_hash" ]]; then
        echo "PASS: $key with value length ${#value}"
    else
        echo "FAIL: $key with value length ${#value}; got length ${#result}" >&2
        echo "Expected hash:    $expected_hash" >&2
        echo "Received hash:    $actual_hash" >&2
        exit 1
    fi
    echo
}

generate_random_chars() {
  local length=$1
  local random_string=""

  while [ "${#random_string}" -lt "$length" ]; do
    random_string+=$(head /dev/urandom | LC_CTYPE=C tr -dc 'a-zA-Z0-9' | head -c "$length")
  done

  echo "${random_string:0:$length}"
}

# Test Cases

echo "Testing ping..."
redis-cli ping && echo

echo "Testing empty string..."
set_and_get "$KEY:empty" ""

redis-cli DEL "$KEY:empty"

echo "Testing small string..."
set_and_get "$KEY:small" "hello"
redis-cli DEL "$KEY:small"

# Minimal amount to create value rows: 30000
for NUM_CHARS in 100 10000 30000 50000 57000 60000 70000; do
    echo "Testing string with $NUM_CHARS characters..."
    test_value=$(generate_random_chars $NUM_CHARS)
    set_and_get "$KEY:$NUM_CHARS" "$test_value"
done
echo "redis-cli DEL $KEY:100 $KEY:10000 $KEY:30000 $KEY:50000 $KEY:57000 $KEY:60000 $KEY:70000"
redis-cli DEL $KEY:100 $KEY:10000 $KEY:30000 $KEY:50000 $KEY:57000 $KEY:60000 $KEY:70000

# echo "Testing xxl string (1,000,000 characters)..."
# xxl_file=$(mktemp)
# head -c 1000000 < /dev/zero | tr '\0' 'a' > "$xxl_file"
# set_and_get "$KEY:xxl" "$xxl_file"
# rm "$xxl_file"

echo "Testing non-ASCII string..."
set_and_get "$KEY:nonascii" "こんにちは世界"  # Japanese for "Hello, World"
redis-cli DEL "$KEY:nonascii"

echo "Testing binary data..."
binary_value=$(echo -e "\x01\x02\x03\x04\x05\x06\x07")
set_and_get "$KEY:binary" "$binary_value"

echo "Testing unicode characters..."
unicode_value="🔥💧🌳"
set_and_get "$KEY:unicode" "$unicode_value"
redis-cli DEL "$KEY:binary" "$KEY:unicode"

echo "Testing multiple keys..."
for i in {1..10}; do
    test_value="Value_$i"_$(head -c $((RANDOM % 100 + 1)) < /dev/zero | tr '\0' 'a')
    set_and_get "$KEY:multiple_$i" "$test_value"
done
redis-cli DEL "$KEY:multiple_1" "$KEY:multiple_2" "$KEY:multiple_3" "$KEY:multiple_4" "$KEY:multiple_5" "$KEY:multiple_6" "$KEY:multiple_7" "$KEY:multiple_8" "$KEY:multiple_9" "$KEY:multiple_10"

echo "Testing piped keys..."
for i in {1..10000}; do
    echo "SET $KEY:piped_$i value_$i"
done | redis-cli --pipe --verbose
for i in {1..10000}; do
    echo "DEL $KEY:piped_$i value_$i"
done | redis-cli --pipe --verbose

echo "Testing edge case large key length (Redis allows up to 512MB for the value)..."
edge_value=$(head -c 100000 < /dev/zero | tr '\0' 'b')
set_and_get "$KEY:edge_large" "$edge_value"
redis-cli DEL "$KEY:edge_large"

echo
incr_key="$KEY:incr${RANDOM}${RANDOM}"
incr_output=$(redis-cli INCR "$incr_key")
incr_result=$(redis-cli GET "$incr_key")
if [[ "$incr_result" == 1 ]]; then
    echo "PASS: Incrementing non-existing key $incr_key "
else
    echo "FAIL: Incrementing non-existing key $incr_key"
    echo "Expected: 1"
    echo "Received: $incr_result"
    echo "incr_output: $incr_output"
    exit 1
fi

echo
incr_start_value=$RANDOM
set_and_get "$incr_key" $incr_start_value
for i in {1..10}; do
    incr_output=$(redis-cli INCR "$incr_key")
    incr_result=$(redis-cli GET "$incr_key")
    incr_expected_value=$((incr_start_value + i))
    if [[ "$incr_result" == $incr_expected_value ]]; then
        echo "PASS: Incrementing key $incr_key to value $incr_result"
    else
        echo "FAIL: Incrementing key $incr_key from value $incr_start_value"
        echo "Expected: $incr_expected_value"
        echo "Received: $incr_result"
        exit 1
    fi
done
redis-cli DEL "$incr_key"

echo
incrby_key="$KEY:incrby${RANDOM}${RANDOM}"
incrby_output=$(redis-cli INCRBY "$incrby_key" "20")
incrby_result=$(redis-cli GET "$incrby_key")
if [[ "$incrby_result" == "20" ]]; then
    echo "PASS: Incrementing non-existing key $incrby_key by 20"
else
    echo "FAIL: Incrementing non-existing key $incrby_key by 20"
    echo "Expected: 20"
    echo "Received: $incrby_result"
    echo "incr_output: $incrby_output"
    exit 1
fi

echo
incrby_start_value=$RANDOM
set_and_get "$incrby_key" $incrby_start_value
for i in {1..10}; do
    incrby_output=$(redis-cli INCRBY "$incrby_key" "10")
    incrby_result=$(redis-cli GET "$incrby_key")
    incrby_expected_value=$((incrby_start_value + (i * 10)))
    if [[ "$incrby_result" == $incrby_expected_value ]]; then
        echo "PASS: Incrementing key $incrby_key by 10 to value $incrby_result"
    else
        echo "FAIL: Decrementing key $incrby_key by 10 from value $incrby_start_value"
        echo "Expected: $incrby_expected_value"
        echo "Received: $incrby_result"
        exit 1
    fi
done
redis-cli DEL "$incrby_key"

echo
decr_key="$KEY:decr${RANDOM}${RANDOM}"
decr_output=$(redis-cli DECR "$decr_key")
decr_result=$(redis-cli GET "$decr_key")
if [[ "$decr_result" == -1 ]]; then
    echo "PASS: Decrementing non-existing key $decr_key "
else
    echo "FAIL: Decrementing non-existing key $decr_key"
    echo "Expected: -1"
    echo "Received: $decr_result"
    echo "incr_output: $decr_output"
    exit 1
fi

echo
decr_start_value=$RANDOM
set_and_get "$decr_key" $decr_start_value
for i in {1..10}; do
    decr_output=$(redis-cli DECR "$decr_key")
    decr_result=$(redis-cli GET "$decr_key")
    decr_expected_value=$((decr_start_value - i))
    if [[ "$decr_result" == $decr_expected_value ]]; then
        echo "PASS: Decrementing key $decr_key to value $decr_result"
    else
        echo "FAIL: Decrementing key $decr_key from value $decr_start_value"
        echo "Expected: $decr_expected_value"
        echo "Received: $decr_result"
        exit 1
    fi
done
redis-cli DEL "$decr_key"

echo
decrby_key="$KEY:decr${RANDOM}${RANDOM}"
decrby_output=$(redis-cli DECRBY "$decrby_key" "20")
decrby_result=$(redis-cli GET "$decrby_key")
if [[ "$decrby_result" == "-20" ]]; then
    echo "PASS: Decrementing non-existing key $decrby_key by 20"
else
    echo "FAIL: Decrementing non-existing key $decrby_key by 20"
    echo "Expected: -20"
    echo "Received: $decrby_result"
    echo "incr_output: $decrby_output"
    exit 1
fi

echo
decrby_start_value=$RANDOM
set_and_get "$decrby_key" $decrby_start_value
for i in {1..10}; do
    decrby_output=$(redis-cli DECRBY "$decrby_key" "10")
    decrby_result=$(redis-cli GET "$decrby_key")
    decrby_expected_value=$((decrby_start_value - (i * 10)))
    if [[ "$decrby_result" == $decrby_expected_value ]]; then
        echo "PASS: Decrementing key $decrby_key by 10 to value $decrby_result"
    else
        echo "FAIL: Decrementing key $decrby_key by 10 from value $decrby_start_value"
        echo "Expected: $decrby_expected_value"
        echo "Received: $decrby_result"
        exit 1
    fi
done
redis-cli DEL "$decrby_key"

echo
# Create multi-value rows in parallel
run_client() {
    local client="$1"
    local key="$2"
    NUM_ITERATIONS=500
    for ((i=1; i<=$NUM_ITERATIONS; i++)); do
        # Generate a unique key for each client and iteration
        local test_value=$(generate_random_chars 50000)
        check_set "$key" "$test_value" > /dev/null
        redis-cli DEL "$key" > /dev/null
#       echo "PASS ($i/$NUM_ITERATIONS): client $client with key $key"
    done
}

echo "Testing multi-value rows in parallel..."
for ((client=1; client<=5; client++)); do
    run_client $client "${KEY}:parallel_key" &
    pids[$client]=$!
done

for pid in ${pids[*]}; do
    wait $pid
done
echo "PASS: All parallel clients completed."
echo "All tests completed."
