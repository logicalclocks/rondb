#!/usr/bin/env sh

USERID=$(id | sed -e 's/).*//; s/^.*(//;')
if [ "X$USERID" != "Xmysql" ]; then
    echo "You should have started the cluster as user: 'mysql'."
    echo "If you continue, you will change ownership of database files"
    echo "from 'mysql' to '$USERID'."
    exit -3
fi

echo "Testing to see if a cluster is already running on 1186 ..."
# Whitespace after 1186 is important in order to ignore MySQLd on 11860
ss -Hltu | grep "1186 "

if [ $? -eq 0 ]; then
    echo "A management server is already running on 1186"
    exit 2
else
    echo "No management server is running on 1186; we're good to go"
fi

if [ ! -e /srv/hops/mysql/bin/ndb_mgmd ]; then
    echo "Error: could not find file: /srv/hops/mysql/bin/ndb_mgmd"
    exit 3
fi

# TODO: Add this to the original cloud setup
INITIAL_START_ARG=
if [ -n "$INITIAL_START" ]; then
    INITIAL_START_ARG="--initial"
fi

# TODO: Add this to the original cloud setup
SERVICE_ARG=
if [ -n "$SERVICE_NAME" ]; then
    SERVICE_ARG="--service-name $SERVICE_NAME"
fi

mgmd_command="/srv/hops/mysql/bin/ndb_mgmd --ndb-nodeid=$NDB_MGMD_NODE_ID -f /srv/hops/mysql-cluster/config.ini  --configdir=/srv/hops/mysql-cluster/mgmd --reload $INITIAL_START_ARG $SERVICE_ARG"

# This is not in the original cloud setup;
# It is used for alternative process managers such as supervsisord
# that cannot daemonize processes.
if [ -n "$NO_DAEMON" ]; then
    echo "Starting the MySQL Management as a foreground process"
    mgmd_command="$mgmd_command --nodaemon"
    echo "Running command '$mgmd_command'"
    exec $mgmd_command
fi

echo "Started the MySQL Management server - ndb_mgmd."
eval $mgmd_command

RES=$(echo $?)
if [ "$RES" -ne 0 ]; then
    echo ""
    echo "Error when starting the management server: $?."
    echo ""
    exit 1
fi
exit "$RES"
