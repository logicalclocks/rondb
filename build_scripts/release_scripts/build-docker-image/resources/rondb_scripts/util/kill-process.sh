#!/usr/bin/env sh

if [ $# -ne 4 ]; then
    echo "Incorrect args. Usage: kill-process name pid_file inc_pid force"
    exit 1
fi

NAME=$1
PID_FILE=$2
INC_PID=$3
FORCE=$4

if [ "$FORCE" -eq 1 ]; then
    killall -9 "$NAME"
fi

if [ ! -f "$PID_FILE" ]; then
    echo "No $NAME process to stop - no PID file found at $PID_FILE."
    exit 0
fi

PID=$(cat "$PID_FILE")
if [ "$INC_PID" -eq 1 ]; then
    echo "Incremeting the PID by 1 to skip over watchdog process"
    PID=$((PID + 1))
fi
echo "Killing $NAME with process-id $PID "
(kill -TERM "$PID") 2>/dev/null
RES=$?
wait_pid_removed=10
timeout=0
while [ $timeout -lt $wait_pid_removed ]; do
    sleep 1
    (! kill -0 "$PID") 2>/dev/null && break
    echo -n "."
    timeout=$((timeout + 1))
done
if [ "$timeout" -eq $wait_pid_removed ]; then
    kill -9 "$PID"
    RES=$?
fi

exit $RES
