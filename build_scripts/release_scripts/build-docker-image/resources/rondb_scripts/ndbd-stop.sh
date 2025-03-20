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

PID_FILE=/srv/hops/mysql-cluster/log/ndbmtd.pid
/srv/hops/mysql-cluster/ndb/scripts/util/kill-process.sh ndbmtd $PID_FILE 1 $FORCE
exit $?
