#!/usr/bin/env sh

USERID=$(id | sed -e 's/).*//; s/^.*(//;')
if [ "X$USERID" != "Xmysql" ]; then
    echo "You should have started the cluster as user: 'mysql'."
    echo "If you continue, you will change ownership of database files"
    echo "from 'mysql' to '$USERID'."
    exit -3
fi

# TODO: Add this to the original cloud setup
INITIAL_START_ARG=
if [ -n "$INITIAL_START" ]; then
    INITIAL_START_ARG="--initial"

    # We flip this back in case the ndbd crashes and restarts using this script.
    # If we only have one replica, we would erase our data.
    sed -i 's/^INITIAL_START=.*$/INITIAL_START=/g' /srv/hops/mysql-cluster/ndb/scripts/ndbd_env_variables
fi

# TODO: Add this to the original cloud setup
SERVICE_ARG=
if [ -n "$SERVICE_NAME" ]; then
    SERVICE_ARG="--service-name $SERVICE_NAME"
fi

MGM_CONN=$MGM_CONN_STRING

# comma separated list of node-ids of nodes not to wait for when starting this ndbmtd
NOWAIT_NODES_LIST=

while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help | -help)
        echo "usage: <prog> [ -c | --connectstring MGMD_HOST:MGMD_PORT ] "
        echo ""
        echo "connectstring is set to "
        exit 0
        ;;
    -c | --connectstring)
        shift
        MGM_CONN=$1
        break
        ;;
    --nowait-nodes)
        NOWAIT_NODES_LIST="--no-wait-nodes=$1"
        break
        ;;
    *)
        echo "Unknown option '$1'"
        exit -1
        ;;
    esac
    shift
done

ndbd_command="/srv/hops/mysql/bin/ndbmtd -c "$MGM_CONN" --ndb-nodeid=$NDB_NDBD_NODE_ID  --connect-retries=-1 --connect-delay=10 $INITIAL_START_ARG $NOWAIT_NODES_LIST $SERVICE_ARG"

# This is not in the original cloud setup;
# It is used for alternative process managers such as supervsisord
# that cannot daemonize processes.
if [ -n "$NO_DAEMON" ]; then
    ndbd_command="$ndbd_command --foreground"
    echo "Running command '$ndbd_command'"
    exec $ndbd_command
else
    echo "Running command '$ndbd_command'"
fi

echo "Starting Data Node $NDB_NDBD_NODE_ID"
# --connect-retries == -1 implies that the ndbd keeps trying forever to connect to the ndb_mgmd
#su mysql -c "/srv/hops/mysql/bin/ndbmtd -c $MGM_CONN --ndb-nodeid=$NDB_NDBD_NODE_ID  --connect-retries=-1 --connect-delay=10"
$ndbd_command

RES=$(echo $?)
exit $RES
