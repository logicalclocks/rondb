#!/bin/bash
# Copyright (c) 2017, 2021, Oracle and/or its affiliates.
# Copyright (c) 2021, 2022, 2023 Hopsworks AB and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
set -e

# Fetch value from server config
# We use mysqld --verbose --help instead of my_print_defaults because the
# latter only show values present in config files, and not server defaults
_get_config() {
    local conf="$1"
    shift
    "$@" --verbose --help 2>/dev/null | grep "^$conf" | awk '$1 == "'"$conf"'" { print $2; exit }'
}

# Make sure that "--defaults-file" is always run as second argument
# Otherwise there is a risk that it might not be read
shift
set -- mysqld --defaults-file="$RONDB_DATA_DIR/my.cnf" "$@"
echo "[entrypoints/mysqld_configure.sh] \$@: $*"

# Test that the server can start. We redirect stdout to /dev/null so
# only the error messages are left.
result=0
output=$("$@" --validate-config) || result=$?
if [ ! "$result" = "0" ]; then
    echo >&2 '[entrypoints/mysqld_configure.sh] ERROR: Unable to start MySQL. Please check your configuration.'
    echo >&2 "[entrypoints/mysqld_configure.sh] $output"
    exit 1
fi
echo "[entrypoints/mysqld_configure.sh] The MySQL configuration has been validated"

echo '[entrypoints/mysqld_configure.sh] Initializing MySQL...'

# Technically, specifying the user here is unnecessary since that is
# the default user according to the Dockerfile
"$@" \
    --log-error-verbosity=3 \
    --initialize-insecure \
    --explicit_defaults_for_timestamp

echo '[entrypoints/mysqld_configure.sh] MySQL initialized'
