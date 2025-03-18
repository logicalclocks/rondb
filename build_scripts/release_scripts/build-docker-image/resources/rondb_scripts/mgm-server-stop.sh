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

PID_FILE=/srv/hops/mysql-cluster/log/ndb_mgmd.pid
/srv/hops/mysql-cluster/ndb/scripts/util/kill-process.sh ndb_mgmd $PID_FILE 0 $FORCE
exit $?
