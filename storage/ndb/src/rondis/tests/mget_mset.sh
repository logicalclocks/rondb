#!/bin/bash
#  Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
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

# Change key suffix using script arguments
KEY_SUFFIX=${1:-0}
KEY="test_key_$KEY_SUFFIX"

function check_set() {
    local key="$1"
    local value="$2"

    # SET the value in Redis
    if [[ -f "$value" ]]; then
        mset_output=$(redis-cli --pipe <<EOF
MSET $key $(< "$value")
EOF
)
    else
        mset_output=$(redis-cli MSET "$key:0" "$value" "$key:1" "$value")
    fi

    if [[ $mset_output == ERR* ]]; then
        echo "FAIL: Could not MSET $key with given value" >&2
        exit 1
    fi
}

# Function to set a value and retrieve it, then verify if it matches
function set_and_get() {
    local key="$1"
    local value="$2"
    
    check_set "$key" "$value"

    # GET the value
    local result0=$(redis-cli MGET "$key:0")

    local expected_hash=$(echo -n "$value" | sha256sum | awk '{print $1}')
    local actual_hash0=$(echo -n "$result0" | sha256sum | awk '{print $1}')

    local result1=$(redis-cli MGET "$key:1")
    local actual_hash1=$(echo -n "$result1" | sha256sum | awk '{print $1}')
    
    # Check if the retrieved value matches the expected value
    if [[ "$expected_hash" == "$actual_hash0" ]]; then
        echo "PASS: $key:0 with value length ${#value}"
    else
        echo "FAIL: $key with value length ${#value}; got length ${#result}" >&2
        echo "Expected hash:    $expected_hash" >&2
        echo "Received hash:    $actual_hash0" >&2
        exit 1
    fi
    if [[ "$expected_hash" == "$actual_hash1" ]]; then
        echo "PASS: $key:1 with value length ${#value}"
    else
        echo "FAIL: $key with value length ${#value}; got length ${#result}" >&2
        echo "Expected hash:    $expected_hash" >&2
        echo "Received hash:    $actual_hash1" >&2
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

echo "Testing small string..."
set_and_get "$KEY:small" "hello"

NUM_CHARS=4096
test_value=$(generate_random_chars $NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=4095
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=4097
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=4095
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"

NUM_CHARS=4095
test_value=$(generate_random_chars $NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=4097
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=4096
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"

NUM_CHARS=33596
test_value=$(generate_random_chars $NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=33597
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=4094
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"

NUM_CHARS=31000
test_value=$(generate_random_chars $NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"
NEW_NUM_CHARS=1000
test_value=$(generate_random_chars $NEW_NUM_CHARS)
set_and_get "$KEY:$NUM_CHARS" "$test_value"

# Minimal amount to create value rows: 30000
for NUM_CHARS in 100 10000 31000 50000 57000 60000 70000 150000 400000; do
    echo "Testing string with $NUM_CHARS characters..."
    test_value=$(generate_random_chars $NUM_CHARS)
    set_and_get "$KEY:$NUM_CHARS" "$test_value"
done

# Minimal amount to create value rows: 30000
for NUM_CHARS in 31000 50000 57000 60000 70000 150000 400000; do
    echo "Testing string with $NUM_CHARS - 30000 characters..."
    ((NEW_NUM_CHARS = NUM_CHARS - 30000))
    test_value=$(generate_random_chars $NEW_NUM_CHARS)
    set_and_get "$KEY:$NUM_CHARS" "$test_value"
done

# echo "Testing xxl string (1,000,000 characters)..."
# xxl_file=$(mktemp)
# head -c 1000000 < /dev/zero | tr '\0' 'a' > "$xxl_file"
# set_and_get "$KEY:xxl" "$xxl_file"
# rm "$xxl_file"

echo "Testing non-ASCII string..."
set_and_get "$KEY:nonascii" "こんにちは世界"  # Japanese for "Hello, World"

echo "Testing binary data..."
binary_value=$(echo -e "\x01\x02\x03\x04\x05\x06\x07")
set_and_get "$KEY:binary" "$binary_value"

echo "Testing unicode characters..."
unicode_value="🔥💧🌳"
set_and_get "$KEY:unicode" "$unicode_value"

echo "Testing multiple keys..."
for i in {1..10}; do
    test_value="Value_$i"_$(head -c $((RANDOM % 100 + 1)) < /dev/zero | tr '\0' 'a')
    set_and_get "$KEY:multiple_$i" "$test_value"
done

echo "Testing piped keys..."
for i in {1..10000}; do
    echo "MSET $KEY:piped_$i value_$i"
done | redis-cli --pipe --verbose

echo "Testing edge case large key length (Redis allows up to 512MB for the value)..."
edge_value=$(head -c 200000 < /dev/zero | tr '\0' 'b')
set_and_get "$KEY:edge_large" "$edge_value"

# Create multi-value rows in parallel
run_client() {
    local client="$1"
    local key="$2"
    NUM_ITERATIONS=5
    for ((i=1; i<=$NUM_ITERATIONS; i++)); do
        # Generate a unique key for each client and iteration
        local test_value=$(generate_random_chars 32000)
        check_set "$key" "$test_value" > /dev/null
        echo "PASS ($i/$NUM_ITERATIONS): client $client with key $key"
    done
}

echo "All tests completed."
