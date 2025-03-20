#!/usr/bin/env sh

USERID=$(id | sed -e 's/).*//; s/^.*(//;')
if [ "X$USERID" != "Xmysql" ]; then
    echo "You should have started the cluster as user: 'mysql'."
    echo "If you continue, you will change ownership of database files"
    echo "from 'mysql' to '$USERID'."
    exit -3
fi

MGM_CONN=$MGM_CONN_STRING
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
    *)
        echo "Unknown option '$1'"
        exit -1
        ;;
    esac
    shift
done

echo "Initializing Data Node 1."
echo ""

#su mysql -c "/srv/hops/mysql/bin/ndbmtd -c $MGM_CONN --initial --ndb-nodeid=$NDB_NDBD_NODE_ID"
/srv/hops/mysql/bin/ndbmtd -c "$MGM_CONN" --initial --ndb-nodeid=$NDB_NDBD_NODE_ID
RES=$(echo $?)
exit
