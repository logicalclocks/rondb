#!/usr/bin/env sh

FORCE=0
if [ $# -gt 0 ]; then
    if [ "$1" = "--force" ]; then
        FORCE=1
    else
        echo "Incorrect parameter. Usage: <prog> [--force]"
        exit 1
    fi
fi

MYSQL_SOCKET=$(/srv/hops/mysql-cluster/ndb/scripts/get-mysql-socket.sh)

PID_FILE=/srv/hops/mysql-cluster/log/mysqld.pid
/srv/hops/mysql-cluster/ndb/scripts/util/kill-process.sh mysqld $PID_FILE 0 $FORCE
exit $?
