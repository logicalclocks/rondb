#
# Setup and check that there are two MySQL Servers with
# support for ndbcluster available
#

# Check that required variables are defined(these are normally set in the
# suite or test specific my.cnf file)
if (!$MASTER_MYPORT)
{
  die Need MASTER_MYPORT variable;
}
if (!$MASTER_MYPORT1)
{
  die Need MASTER_MYPORT1 variable;
}

# Connect to both MySQL Servers, allowing transient TCP connection failures.
# Retry only CR_CONN_HOST_ERROR, at most 60 attempts per server.
let $_multi_ndb_server= 1;
while ($_multi_ndb_server <= 2)
{
  let $_multi_ndb_port= $MASTER_MYPORT;
  if ($_multi_ndb_server == 2)
  {
    let $_multi_ndb_port= $MASTER_MYPORT1;
  }
  let $_multi_ndb_connection= server$_multi_ndb_server;
  let $_multi_ndb_attempts= 60;
  while ($_multi_ndb_attempts)
  {
    --disable_query_log ONCE
    --disable_result_log ONCE
    --error 0,CR_CONN_HOST_ERROR
    connect ($_multi_ndb_connection,127.0.0.1,root,,test,$_multi_ndb_port,);
    if (!$mysql_errno)
    {
      let $_multi_ndb_attempts= 0;
    }
    if ($mysql_errno)
    {
      dec $_multi_ndb_attempts;
      if (!$_multi_ndb_attempts)
      {
        die Cannot connect to $_multi_ndb_connection at 127.0.0.1:$_multi_ndb_port after 60 attempts (CR_CONN_HOST_ERROR);
      }
      sleep 1;
    }
  }
  inc $_multi_ndb_server;
}

# Check that server1 has NDB support
connection server1;
--source include/have_ndb.inc

# Check that server2 has NDB support
connection server2;
--source include/have_ndb.inc

# Set default connection
connection server1;
