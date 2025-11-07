#!/usr/bin/env bash
# Input:
#   OLD_BASE_DIR (e.g. /Users/mikael/mysql_trees/rondb_2410/debug_build
#   NEW_BASE_DIR (e.g. /Users/mikael/mysql_trees/rondb_2510/debug_build
#
cd $OLD_BASE_DIR/mysql-test
./mtr --suite=ndb --start-and-exit > $OLD_BASE_DIR/start_output
cat $OLD_BASE_DIR/start_output | grep Started > $OLD_BASE_DIR/started_output
rm $OLD_BASE_DIR/start_output
readarray -t procs < <(
  awk -F'[][]' '{
    for (i=2; i<=NF; i+=2) {
      if ($i ~ /pid:/) {
        split($i,a," ");
        proc=a[1];
        split(proc,p,".");
        base=p[1];
        pid="";
        for (j=1; j<=NF; j++)
          if (a[j]=="pid:") pid=a[j+1];
        if (pid != "") {
          if (base=="ndb_mgmd") group=1;
          else if (base=="mysqld") group=2;
          else if (base=="ndbd") group=3;
          printf "%d %s %s\n", group, base, pid;
        }
      }
    }
  }' "$OLD_BASE_DIR/started_output" \
  | sort -k1,1n \
  | awk '{print $2 " " $3}'
)
NUM_NDBD_NODES=0
for line in "${procs[@]}"; do
  name=$(awk '{print $1}' <<<"$line")
  if [[ "$name" == "ndbd" ]]; then
    ((NUM_NDBD_NODES++))
  fi
done

echo "Running with $NUM_NDBD_NODES ndbmtd nodes"
for line in "${procs[@]}"; do
  read -r name pid <<<"$line"
  pid=${pid//,/}
  echo "$name is running with pid $pid"
done

# Loop through the array in pairs (process name + pid)
NUM_MYSQLD="1"
NUM_NDBD="1"
for line in "${procs[@]}"; do
  echo
  read -r name pid <<<"$line"
  pid=${pid//,/}
  echo "Stop $name with pid $pid from $NEW_BASE_DIR/bin"
  CHILD_PIDS=
  if [[ "$name" == "ndbd" ]]; then
    CHILD_PID=$(pgrep -P $pid)
    echo "Kill also child process $CHILD_PID for $pid"
    kill -9 "$pid"
    pid="$CHILD_PID"
  fi
  TIMEOUT=60
  kill -TERM "$pid"
  while kill -0 "$pid" 2>/dev/null && [ ${TIMEOUT} -gt 0 ]; do
    sleep 1
    ((timeout--))
  done
  kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
  echo "$name process is stopped"
  if [[ "$name" == "ndb_mgmd" ]]; then
    $NEW_BASE_DIR/bin/ndb_mgmd --defaults-group-suffix=.1.1 \
      --defaults-file=$OLD_BASE_DIR/mysql-test/var/my.cnf --nodaemon \
      --configdir=$OLD_BASE_DIR/mysql-test/var/mysql_cluster.1/ndb_mgmd.1 >> \
      "$OLD_BASE_DIR/mysql-test/var/mysql_cluster.1/ndb_mgmd.1/ndb_mgmd.log" 2>&1 &
    NDB_MGMD_PID=$!
    echo "Started ndb_mgmd with PID $NDB_MGMD_PID"
    sleep 5
  elif [[ "$name" == "mysqld" ]]; then
    $NEW_BASE_DIR/bin/mysqld --defaults-group-suffix=.${NUM_MYSQLD}.1 \
      --defaults-file=$OLD_BASE_DIR/mysql-test/var/my.cnf --log-output=file \
      --explain-format=TRADITIONAL_STRICT --loose-debug-sync-timeout=600 \
      --core-file >> "$OLD_BASE_DIR/mysql-test/var/log/mysqld.${NUM_MYSQLD}.1.err" 2>&1 &
    MYSQLD_PID[${NUM_MYSQLD}]=$!
    echo "Started mysqld with PID ${MYSQLD_PID[${NUM_MYSQLD}]}"
    ((NUM_MYSQLD++))
    sleep 10
  elif [[ "$name" == "ndbd" ]]; then
    $NEW_BASE_DIR/bin/ndbmtd --defaults-group-suffix=.${NUM_NDBD}.1 \
      --defaults-file=$OLD_BASE_DIR/mysql-test/var/my.cnf --nodaemon --foreground \
      --core-file >> "$OLD_BASE_DIR/mysql-test/var/mysql_cluster.1/ndbd.${NUM_NDBD}/ndbd.log" &
    NDBD_PID[${NUM_NDBD}]=$!
    echo "Started ndbmtd with PID ${NDBD_PID[${NUM_NDBD}]}"
    ((NUM_NDBD++))
    sleep 60
  fi
done
echo "Shutdown the cluster and remove files"
kill -9 "$NDB_MGMD_PID"
kill -9 "${MYSQLD_PID[1]}"
kill -9 "${MYSQLD_PID[2]}"
kill -9 "${NDBD_PID[1]}"
kill -9 "${NDBD_PID[2]}"
rm $OLD_BASE_DIR/started_output
