#!/usr/bin/env bash 

SKIP_NDBDS=0
SKIP_MYSQLDS=0
SKIP_USER_CHECK=0
TIME_WAIT=10800

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help|-help)
              echo "usage: <prog> [--skip-ndbds (do not start data nodes)] [--skip-mysqlds (do not start mysql servers)] [-sc (delete old configurations)] [-f skip user check]"
	      echo ""
	      echo "connectstring is set to $MGM_CONN_STRING"
	      exit 0 
	      ;;
    -f|--force)
              SKIP_USER_CHECK=1
	      ;;
    -t|--time-wait-for-start)
	      shift
              TIME_WAIT=$1
	      ;;
    -sc)
              echo "Deleting old configurations from //srv/hops/mysql-cluster/mgmd/"
              rm -rf ..//srv/hops/mysql-cluster/mgmd/*
	      ;;
    --skip-ndbds)
              SKIP_NDBDS=1
	      ;;
    --skip-mysqlds)
              SKIP_MYSQLDS=1
	      ;;
	   * )
              echo "Unknown option '$1'" 
              exit -1
  esac
  shift       
done


  echo "" 
  echo "Testing to see if a cluster is already running on host '$MGM_CONN_STRING' ..." 
  echo "" 
  /srv/hops/mysql/bin/ndb_mgm -c $MGM_CONN_STRING -t 2 -e show 1> /dev/null

  if [ $? -eq 0 ] ; then
      echo ""      
      echo "A management server is already running on $MGM_CONN_STRING" 
      echo "" 	
      exit 2
  fi



USERID=$(id | sed -e 's/).*//; s/^.*(//;')
if [ "X$USERID" = "Xroot" ]; then
    echo ""
    echo "You started cluster as user: 'root'."

    echo ""
    start_as_wrong_user() 
    {
	echo -n "Do you really want to start the cluster as user \"$USERID\"? y/n/h(help) "
	read ACCEPT
	case $ACCEPT in
	    y | Y)
	    ;;
	    n | N)
		echo ""
		echo "Bye.."
		echo ""
		exit 1
		;;
	    *)
		echo ""
		echo -n "Please enter 'y' or 'n'." 
		start_as_wrong_user
		;;
	esac
    }
    start_as_wrong_user
fi

echo "Your are initializing and starting the MySQL Cluster database."
echo "If you have already initialised the Cluster, exit this script"
echo "and run the 'start-noinit-' script, instead."
echo ""

really_start() 
{
  echo -n "Do an initial start of NDB? This will DELETE any existing data!  y/n/h(help) "
  read ACCEPT
  case $ACCEPT in
   y | Y)
      ;;
   n | N)
      exit 1
      ;;
    *)
      echo ""
      echo "Please enter 'y' or 'n'." 
      really_start
      ;;
   esac
}
if [ $SKIP_USER_CHECK -eq 0 ] ; then
 really_start
fi
      

echo "Cluster Startup may take a few minutes."  


echo "Truncating the cluster log file: /srv/hops/mysql-cluster/log/cluster.log"
rm /srv/hops/mysql-cluster/log/cluster.log 

echo "Starting the Management Server ....."

if [ -e /srv/hops/mysql-cluster/log/cluster.log ] ; then
    SIZE_CL=$(wc -l < /srv/hops/mysql-cluster/log/cluster.log)
else
    SIZE_CL=0
fi

#remove memory of old configurations
#echo If you want to remove warnings for incompatible configuration changes, run rm -rf ../$NDB_DIR/$MGM_DATADIR/*

/srv/hops/mysql/bin/ndb_mgmd -f /srv/hops/mysql-cluster/config.ini  --configdir=/srv/hops/mysql-cluster/mgmd --reload   

if [ $? -ne 0 ] ; then
  echo "Problem starting the Management Server."
  echo "Please read the logs in:"
  echo "/srv/hops/mysql-cluster/log."
  echo "/srv/hops/mysql-cluster/mgmd_1"
  exit 1
fi

MGMD_STARTED=0

echo "Waiting for ndb_mgmd to start"
    

MGMD_TIMEOUT=200
MGMD_COUNT=0
while [ $MGMD_STARTED -eq 0 ] ; do

  if [ -e /srv/hops/mysql-cluster/log/cluster.log ] ; then
    UPDATED_SIZE_CL=$(wc -l < /srv/hops/mysql-cluster/log/cluster.log)
  else
    UPDATED_SIZE_CL=0
  fi
    
  if [ $SIZE_CL -lt $UPDATED_SIZE_CL ] ; then
	MGMD_STARTED=1;
	echo ""
  else

      if [ ${MGMD_COUNT} -eq 0 ] ; then
	  echo -n "Seconds left before timeout: "
      fi

      echo -n "$((MGMD_TIMEOUT - MGMD_COUNT))"
      sleep 1
      MGMD_COUNT=$((MGMD_COUNT + 1))

      if [ "$MGMD_COUNT" -gt $MGMD_TIMEOUT ] ; then
	  MGMD_STARTED=2;
      fi
  fi

done

  if [ $MGMD_STARTED -ne 1 ] ; then
   echo ""
   echo "Failure when starting the 'ndb_mgmd'."
   echo "Please check for errors in your configuration file or report a bug."
   echo ""
   echo "You now need to kill the ndb_mgmd process."
   echo "To find the proceses-id of ndb_mgmd: 'ps -ef | grep ndb_mgmd | grep mysql' "
   echo "To kill the process: kill -9 [process-id]"
   exit 2
  fi


sleep 1

echo ""
if [ $SKIP_NDBDS -eq 0 ] ; then
    echo "Starting a Data Node on host: 10.0.0.107 "
    echo "For username: mysql "
  ssh mysql@10.0.0.107 /srv/hops/mysql-cluster/ndb/scripts/ndbd-init.sh

  if [ $? -ne 0 ] ; then
    echo "Problem starting a Data Node on host 10.0.0.107."
    echo "Please read the logs on host 10.0.0.107 in:"
    echo "/srv/hops/mysql-cluster/log"
  fi
else
  echo "Skipping starting ndbd-1 at 10.0.0.107"
fi


echo "Waiting for the cluster to be ready by calling:"
echo "ndb_waiter -c $MGM_CONN_STRING --timeout=$TIME_WAIT"
echo "This can take a few minutes..."

/srv/hops/mysql/bin/ndb_waiter -c $MGM_CONN_STRING --timeout=$TIME_WAIT 2>&1 > /dev/null

if [ $? -ne 0 ] ; then
    echo "Error when waiting on the cluster to be ready."
    echo "Exiting..."
    exit 3
fi


if [ $SKIP_MYSQLDS -eq 0 ] ; then
    echo "Starting a MySQL Server on host: 10.0.0.79 "
    echo "For username: mysql "
  ssh mysql@10.0.0.79 "/srv/hops/mysql-cluster/ndb/scripts/mysql-server-start.sh --force"

  if [ $? -ne 0 ] ; then
    echo "Problem starting a MySQL Server Node on host 10.0.0.79."
    echo "Please read the logs on host 10.0.0.79 in:"
    echo "/srv/hops/mysql-cluster/log/mysql__out.log"
  fi
else
  echo "Skipping starting mysqld-52 at 10.0.0.79"
fi


sleep 3

/srv/hops/mysql-cluster/ndb/scripts/mgm-client.sh -e show

exit 0
