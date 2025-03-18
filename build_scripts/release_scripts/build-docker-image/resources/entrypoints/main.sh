#!/bin/bash
# Copyright (c) 2017, 2021, Oracle and/or its affiliates.
# Copyright (c) 2021, 2021, Hopsworks AB and/or its affiliates.
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

if [ "$1" = 'mysqld' ]; then
	"$SCRIPT_DIR/mysqld.sh" "$@"
else
	# "set" lets us set the arguments to the current script.
	# the command also has its own commands (see set --help).
	# to avoid accidentally using one of the set-commands,
	# we use "set --" to make clear that everything following
	# this is an argument to the script itself and not the set
	# command.

    # The default for mgmds & ndbmtds is to run as daemon processes
    if [ "$1" != "rdrs" ]; then
		set -- "$@" --nodaemon
	fi

	if [ "$1" == "rdrs" ]; then
        echo "[entrypoints/main.sh] Starting REST API server: $@"

	elif [ "$1" == "ndb_mgmd" ]; then
		echo "[entrypoints/main.sh] Starting ndb_mgmd"
		set -- "$@" -f "$RONDB_DATA_DIR/config.ini" --configdir="$RONDB_DATA_DIR/log"
	elif [ "$1" == "ndbmtd" ]; then
		echo "[entrypoints/main.sh] Starting ndbmtd"
		# Command for more verbosity with ndbmtds: `set -- "$@" --verbose=TRUE`

		# We have to run ndbmtd as a child process, since trap and exec
		# do not play nice.
		"$@"
		exit $?
	elif [ "$1" == "ndb_mgm" ]; then
		echo "[entrypoints/main.sh] Starting ndb_mgm"
	fi
    
    echo "[entrypoints/main.sh] Running: $*"
	exec "$@"
fi
