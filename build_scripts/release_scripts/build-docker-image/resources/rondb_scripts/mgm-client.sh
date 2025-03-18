#!/usr/bin/env sh

MGM_CONN=$MGM_CONN_STRING
PARAMS=
EXEC=
while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help | -help)
        echo "usage: <prog> [ -c | --connectstring MGMD_HOST:MGMD_PORT ] ] [ -e [command] ] "
        echo ""
        echo "Default connectstring parameter = "
        echo ""
        echo "To view the state of the cluster (which nodes are connected), type:"
        echo "./mgm-client.sh -e show"
        echo ""
        exit 0
        ;;
    -e)
        shift
        EXEC="-e"
        while [ $# -gt 0 ]; do
            PARAMS="$PARAMS $1"
            shift
        done
        break
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

/srv/hops/mysql/bin/ndb_mgm -c "$MGM_CONN" $EXEC "$PARAMS"
exit $?
