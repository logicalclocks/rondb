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

##############################################################
# The following should only be run once per cluster,
# on a single MySQLd container, but since that may be hard
# to control, it is attempted to be written in an idempotent way.
# We try not to keep state for our MySQLds.
##############################################################

########################
### RUN LOCAL MYSQLD ###
########################

echo '[entrypoints/mysqld_init_db.sh] Executing MySQLd as daemon with no networking allowed...'

echo "[entrypoints/mysqld_init_db.sh] Running $* --daemonize --skip-networking"

"$@" \
    --daemonize \
    --skip-networking

echo '[entrypoints/mysqld_init_db.sh] Successfully executed MySQLd with networking disabled, we can start changing users, passwords & permissions via a local socket without other clients interfering.'

# Get config
SOCKET="$(_get_config 'socket' "$@")"
echo "[entrypoints/mysqld_init_db.sh] SOCKET: $SOCKET"

echo "[entrypoints/mysqld_init_db.sh] Pinging MySQLd..."
for ping_attempt in {1..30}; do
    if mysqladmin --socket="$SOCKET" ping &>/dev/null; then
        echo "[entrypoints/mysqld_init_db.sh] Successfully pinged MySQLd on attempt $ping_attempt"
        break
    fi
    echo "[entrypoints/mysqld_init_db.sh] Failed pinging MySQLd on attempt $ping_attempt"
    sleep 1
done
if [ "$ping_attempt" = 30 ]; then
    echo >&2 '[entrypoints/mysqld_init_db.sh] Timeout during MySQL init.'
    exit 1
fi

###############################
### SETUP USERS & PASSWORDS ###
###############################

# If the password variable is a filename we use the contents of the file. We
# read this first to make sure that a proper error is generated for empty files.
if [ -f "$MYSQL_ROOT_PASSWORD" ]; then
    MYSQL_ROOT_PASSWORD="$(cat "$MYSQL_ROOT_PASSWORD")"
    if [ -z "$MYSQL_ROOT_PASSWORD" ]; then
        echo >&2 '[entrypoints/mysqld_init_db.sh] Empty MYSQL_ROOT_PASSWORD file specified.'
        exit 1
    fi
fi

if [ -z "$MYSQL_ROOT_PASSWORD" ]; then
    echo >&2 '[entrypoints/mysqld_init_db.sh] No password option specified for root user.'
    if [ -z "$MYSQL_ALLOW_EMPTY_PASSWORD" ]; then
        echo >&2 '[entrypoints/mysqld_init_db.sh] Set MYSQL_ALLOW_EMPTY_PASSWORD to allow the server to start with an empty root password'
        exit 1
    fi
fi

# Defining the client command used throughout the script
# Since networking is not permitted for this mysql server, we have to use a socket to connect to it
# "SET @@SESSION.SQL_LOG_BIN=0;" is required for products like group replication to work properly
DUMMY_ROOT_PASSWORD=
function mysql() { command mysql -uroot -hlocalhost --password="$DUMMY_ROOT_PASSWORD" --protocol=socket --socket="$SOCKET" --init-command="SET @@SESSION.SQL_LOG_BIN=0;"; }
echo '[entrypoints/mysqld_init_db.sh] Overwrote the mysql client command for this script'

echo '[entrypoints/mysqld_init_db.sh] Changing the root user password'
mysql <<EOF
    ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
    FLUSH PRIVILEGES;
EOF

DUMMY_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD

####################################
### SETUP BENCHMARKING DATABASES ###
####################################

# Benchmarking table; all other tables will be created by the benchmakrs themselves
echo "CREATE DATABASE IF NOT EXISTS \`dbt2\` ;" | mysql
echo "CREATE DATABASE IF NOT EXISTS \`ycsb\` ;" | mysql

# shellcheck disable=SC2153
if [ "$MYSQL_BENCH_USER" ]; then
    echo "[entrypoints/mysqld_init_db.sh] Initializing benchmarking user $MYSQL_BENCH_USER"
    
    echo "CREATE USER IF NOT EXISTS '$MYSQL_BENCH_USER'@'%' IDENTIFIED BY '$MYSQL_BENCH_PASSWORD' ;" | mysql

    # Grant MYSQL_BENCH_USER rights to all benchmarking databases
    echo "GRANT NDB_STORED_USER ON *.* TO '$MYSQL_BENCH_USER'@'%' ;" | mysql
    echo "GRANT ALL PRIVILEGES ON \`sysbench%\`.* TO '$MYSQL_BENCH_USER'@'%' ;" | mysql
    echo "GRANT ALL PRIVILEGES ON \`dbt%\`.* TO '$MYSQL_BENCH_USER'@'%' ;" | mysql
    echo "GRANT ALL PRIVILEGES ON \`sbtest%\`.* TO '$MYSQL_BENCH_USER'@'%' ;" | mysql
    echo "GRANT ALL PRIVILEGES ON \`ycsb%\`.* TO '$MYSQL_BENCH_USER'@'%' ;" | mysql
else
    echo '[entrypoints/mysqld_init_db.sh] Not creating benchmark user. MYSQL_BENCH_USER and MYSQL_BENCH_PASSWORD must be specified to do so.'
fi

##############################
### RUN CUSTOM SQL SCRIPTS ###
##############################

for f in $SQL_INIT_SCRIPTS_DIR/*; do
    case "$f" in
    *.sh)
        echo "[entrypoints/mysqld_init_db.sh] Running $f"
        . "$f"
        ;;
    *.sql)
        echo "[entrypoints/mysqld_init_db.sh] Running $f"
        cat $f | mysql
        ;;
    *) echo "[entrypoints/mysqld_init_db.sh] Ignoring $f" ;;
    esac
done

#########################
### STOP LOCAL MYSQLD ###
#########################

# When using a local socket, mysqladmin shutdown will only complete when the
# server is actually down.
echo '[entrypoints/mysqld_init_db.sh] Shutting down MySQLd via mysqladmin...'
mysqladmin -uroot --password="$MYSQL_ROOT_PASSWORD" shutdown --socket="$SOCKET"
echo "[entrypoints/mysqld_init_db.sh] Successfully shut down MySQLd"

echo '[entrypoints/mysqld_init_db.sh] MySQL databases have been initialized'
