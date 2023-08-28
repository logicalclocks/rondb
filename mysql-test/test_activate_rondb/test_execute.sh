SLEEP_TIME="3"
SLEEP_START_TIME="60"
SLEEP_STOP_TIME="20"
HOSTNAME_SERVER="192.168.0.103"
rm -rf $HOME/test_activate/ndb
mkdir -p $HOME/test_activate
mkdir -p $HOME/test_activate/ndb
mkdir -p $HOME/test_activate/mgm_65
mkdir -p $HOME/test_activate/mgm_66
export NDB_CONNECTSTRING="$HOSTNAME_SERVER:1186"
echo "## Kill all MGM servers and RonDB data nodes"
killall ndb_mgmd ndbmtd
sleep ${SLEEP_TIME}
killall ndb_mgmd ndbmtd
#
echo "## Start MGM Server, nodeid 65"
ndb_mgmd --configdir=$HOME/test_activate/mgm_65 --ndb-nodeid=65 -f config.ini  --initial --verbose > tmp_file
sleep ${SLEEP_TIME}
ndb_mgm -e "show"
#
echo "## Activate 65, expect it to be already active"
ndb_mgm -e "65 activate"
sleep ${SLEEP_TIME}
#
echo "## Start MGM Server, nodeid 66, expect it to fail since deactivated"
ndb_mgmd --configdir=$HOME/test_activate/mgm_66 --ndb-nodeid=66 --initial --verbose >> tmp_file
ndb_mgm -e "show"
export NDB_CONNECTSTRING="$HOSTNAME_SERVER:1187"
echo "## Connect to MGM server 66 to verify that it failed to start"
ndb_mgm --connect-retries=2 -e "66 activate"
#
echo "## Activate 66, expect it to succeed"
export NDB_CONNECTSTRING="$HOSTNAME_SERVER:1186"
ndb_mgm -e "66 hostname $HOSTNAME_SERVER"
ndb_mgm -e "66 activate"
echo "## Start MGM Server, nodeid 66, expect success"
ndb_mgmd --configdir=$HOME/test_activate/mgm_66 --ndb-nodeid=66 --initial --verbose >> tmp_file
sleep ${SLEEP_TIME}
ndb_mgm -e "show"
#
echo "## Connect to MGM server at $HOSTNAME_SERVER:1187, node 66, deactivate node 65, expect success"
export NDB_CONNECTSTRING="$HOSTNAME_SERVER:1187"
ndb_mgm -e "65 deactivate"
sleep ${SLEEP_STOP_TIME}
ndb_mgm -e "show"
#
echo "## Deactivate API node 67, expect success"
ndb_mgm -e "67 deactivate"
sleep ${SLEEP_TIME}
ndb_mgm -e "show"
#
echo "## Start data node 1, expect success"
ndbmtd --ndb-nodeid=1 --initial >> tmp_file
#
echo "## Wait for data node to start"
sleep ${SLEEP_START_TIME}
ndb_mgm -e "show"
#
echo "Run ndb_waiter --timeout=30"
ndb_waiter --timeout=30
#
echo "## Run ndb_desc using node 67, should not connect"
ndb_desc --ndb-nodeid=67
ndb_mgm -e "show"
#
echo "## Start data node 2, expect failure"
ndb_mgm -e "2 hostname $HOSTNAME_SERVER"
ndbmtd --ndb-nodeid=2 --initial >> tmp_file
sleep ${SLEEP_TIME}
ndb_mgm -e "show"
#
echo "## Activate node 2, expect success"
ndb_mgm -e "2 activate"
echo "## ndb_waiter, allow partial start, expect success"
ndb_waiter --timeout=5 --allow-partial-start
#
echo "## Now expecting successful start of node 2"
ndbmtd --ndb-nodeid=2 --initial >> tmp_file
echo "Wait for data node to start"
sleep ${SLEEP_START_TIME}
ndb_mgm -e "show"
echo "## ndb_waiter, expect success"
ndb_waiter --timeout=5
#
echo "## Activate API node 67, expect success"
ndb_mgm -e "67 activate"
ndb_mgm -e "show"
#
echo "## Run ndb_desc using node 67, should connect now"
ndb_desc --ndb-nodeid=67
#
echo "## Activate MGM node 65, expect success"
ndb_mgm -e "65 activate"
ndb_mgm -e "show"
#
echo "## Start MGM node 65, expect success"
ndb_mgmd --configdir=$HOME/test_activate/mgm_65 --ndb-nodeid=65 --verbose --initial >> tmp_file
sleep ${SLEEP_TIME}
ndb_mgm -e "show"
#
export NDB_CONNECTSTRING="$HOSTNAME_SERVER:1186"
echo "## Activate node 3, expect success"
ndb_mgm -e "3 hostname $HOSTNAME_SERVER"
ndb_mgm -e "3 activate"
ndb_mgm -e "show"
#
echo "## Activate API node 68, expect success"
ndb_mgm -e "68 hostname $HOSTNAME_SERVER"
ndb_mgm -e "68 activate"
ndb_mgm -e "show"
#
echo "## Run ndb_desc using node 67, should connect now"
ndb_desc --ndb-nodeid=67
#
echo "## Expecting successful start of node 3"
ndbmtd --ndb-nodeid=3 --initial >> tmp_file
echo "## Wait for data node to start"
sleep ${SLEEP_START_TIME}
ndb_mgm -e "show"
echo "## Deactivate node 2, expect success"
ndb_mgm -e "2 deactivate"
sleep ${SLEEP_STOP_TIME}
ndb_mgm -e "show"
echo "## Deactivate node 1, expect success"
ndb_mgm -e "1 deactivate"
sleep ${SLEEP_STOP_TIME}
ndb_mgm -e "show"
echo "## Deactivate node 3, expect failure"
ndb_mgm -e "3 deactivate"
sleep ${SLEEP_STOP_TIME}
ndb_mgm -e "show"
#
echo "## Deactivating MGM node 66, expect success"
ndb_mgm -e "66 deactivate"
sleep ${SLEEP_STOP_TIME}
ndb_mgm -e "show"
echo "### Test finalised, now shutting down cluster"
ndb_mgm -e "shutdown"
ndb_mgm -e "show"
