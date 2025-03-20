#!/bin/bash
# Copyright (c) 2017, 2021, Oracle and/or its affiliates.
# Copyright (c) 2021, 2022, Hopsworks AB and/or its affiliates.
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

# https://stackoverflow.com/a/246128/9068781
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source $SCRIPT_DIR/mysqld_configure.sh "$@"

export MYSQLD_PARENT_PID=$$

if [ ! -z "$MYSQL_INITIALIZE_DB" ]; then
    source $SCRIPT_DIR/mysqld_init_db.sh "$@"
else
    echo "[entrypoints/mysqld.sh] Not initializing MySQL databases"
fi

# This is not being used anymore
if [ -n "$MYSQL_INITIALIZE_ONLY" ]; then
    echo "[entrypoints/mysqld.sh] MYSQL_INITIALIZE_ONLY is set, so we're exiting without starting the MySQLd"
    exit 0
fi

echo '[entrypoints/mysqld.sh] Ready for starting up MySQLd'
echo "[entrypoints/mysqld.sh] Running: $*"
exec "$@"
