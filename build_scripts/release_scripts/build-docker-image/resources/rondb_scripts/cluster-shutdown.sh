#!/usr/bin/env sh 


MGM_CONN=$MGM_CONN_STRING
SKIP_MYSQLDS=0

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help|-help)
              echo "usage: <prog> [ -c | --connectstring MGMD_HOST:MGMD_PORT ] [ -s|--skip-mysqlds ]"
	      echo ""
	      echo "connectstring is set to "
	      exit 0 
	      ;;
    -c|--connectstring)
              shift
	      MGM_CONN=$1
	      break 
	      ;;
    -s|--skip-mysqlds)
              SKIP_MYSQLDS=1
	      ;;
	   * )
              echo "Unknown option '$1'" 
              exit -1
  esac
  shift       
done




if [ $SKIP_MYSQLDS -eq 0 ] ; then
    echo "Stopping a MySQL Server on host: 10.0.0.79 "
    echo "For username: mysql "
  ssh mysql@10.0.0.79 "/srv/hops/mysql-cluster/ndb/scripts/mysql-server-stop.sh"

  if [ $? -ne 0 ] ; then
    echo "Problem stopping a MySQL Server Node on host 10.0.0.79."
    echo "Please read the logs on host 10.0.0.79 in:"
    echo "/srv/hops/mysql-cluster/log/mysql__out.log"
  fi
else
  echo "Skipping stopping mysqld-52 at 10.0.0.79"
fi

#if [ -e /srv/hops/mysql-cluster/ndb/scripts/mysql-server-stop.sh ] ; then
#    /srv/hops/mysql-cluster/ndb/scripts/mysql-server-stop.sh
#fi

/srv/hops/mysql/bin/ndb_mgm -c "$MGM_CONN" -e "shutdown"
