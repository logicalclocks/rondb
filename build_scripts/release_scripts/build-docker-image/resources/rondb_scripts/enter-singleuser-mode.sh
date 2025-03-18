#!/usr/bin/env sh

usage()
{
    echo ""
    echo "Usage: prog_name NODE-ID"
    echo ""
    echo "MySQL Cluster enters single-user mode, where the only MySQL Server or API user"
    echo "that can access the cluster has a nodeID of NODE-ID."
    echo "MySQL Cluster must be in single-user mode"
    echo "in order to convert columns or whole tables from in-memory to disk-based."
    echo "Other operations that require single-user mode can be found here:"
    echo "http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-single-user-mode.html"
    echo ""
}

case $1 in
    "-h"|"--help"|"-help")
	usage
	exit 0
	;;      
    *)
      # do nothing
	;;
esac 

if [ $# -ne 1 ] || [ "$1" -lt 1 ] || [ "$1" -gt 255 ] ; then
    usage
    exit 1
fi

/srv/hops/mysql/bin/ndb_mgm -c $MGM_CONN_STRING -e "ENTER SINGLE USER MODE $1"
exit $?
