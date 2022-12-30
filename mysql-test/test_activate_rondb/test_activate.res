## Kill all MGM servers and RonDB data nodes
## Start MGM Server, nodeid 65
Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, accepting connect from 192.168.0.101)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66 (not connected, node is deactivated)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Activate 65, expect it to be already active
Connected to Management Server at: 192.168.0.101:1186
Node 65 is already activated

## Start MGM Server, nodeid 66, expect it to fail since deactivated
Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, accepting connect from 192.168.0.101)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66 (not connected, node is deactivated)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Connect to MGM server 66 to verify that it failed to start
ERROR: Unable to connect with connect string: nodeid=0,192.168.0.101:1187
Retrying every 5 seconds. Attempts left: 1, failed.
## Activate 66, expect it to succeed
Connected to Management Server at: 192.168.0.101:1186
Configuration changed to reflect new hostname of node 66
Now changing hostname in the cluster
Node 66 now has hostname 192.168.0.101 in the cluster

Connected to Management Server at: 192.168.0.101:1186
Configuration changed to reflect activated node
Now activating the node in the cluster
Node 66 is now activated in the cluster

## Start MGM Server, nodeid 66, expect success
Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, accepting connect from 192.168.0.101)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Connect to MGM server at 192.168.0.101:1187, node 66, deactivate node 65, expect success
Connected to Management Server at: 192.168.0.101:1187
Node 65 is a MGM server, need to deactivate it before stopping it
since otherwise the config change transaction will fail
Configuration changed to reflect deactivated node
Now deactivating the node in the cluster
Node 65 is now deactivated in the cluster
Stopping node 65
Node 65 was successfully stopped

Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, accepting connect from 192.168.0.101)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Deactivate API node 67, expect success
Connected to Management Server at: 192.168.0.101:1187
Deactivating node 67 means no API nodes can connect to the cluster, this is allowed, so proceeding
Node 67 is already down, proceeding to deactivation immediately
Configuration changed to reflect deactivated node
Now deactivating the node in the cluster
Node 67 is now deactivated in the cluster

Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, accepting connect from 192.168.0.101)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, node is deactivated)
id=68 (not connected, node is deactivated)

## Start data node 1, expect success
## Wait for data node to start
Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, node is deactivated)
id=68 (not connected, node is deactivated)

Run ndb_waiter --timeout=30
Connecting to mgmsrv at (null)
Node 2: INACTIVE
Node 3: INACTIVE
Node 1: STARTED
## Run ndb_desc using node 67, should not connect
Unable to connect to management server.

NDBT_ProgramExit: 1 - Failed

Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, node is deactivated)
id=68 (not connected, node is deactivated)

## Start data node 2, expect failure
Connected to Management Server at: 192.168.0.101:1187
Configuration changed to reflect new hostname of node 2
Now changing hostname in the cluster
Node 2 now has hostname 192.168.0.101 in the cluster

Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2 (not connected, node is deactivated)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, node is deactivated)
id=68 (not connected, node is deactivated)

## Activate node 2, expect success
Connected to Management Server at: 192.168.0.101:1187
Configuration changed to reflect activated node
Now activating the node in the cluster
Node 2 is now activated in the cluster

## ndb_waiter, allow partial start, expect success
Connecting to mgmsrv at (null)
Node 3: INACTIVE
Node 1: STARTED
Node 2: NO_CONTACT
## Now expecting successful start of node 2
Wait for data node to start
Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, node is deactivated)
id=68 (not connected, node is deactivated)

## ndb_waiter, expect success
Connecting to mgmsrv at (null)
Node 3: INACTIVE
Node 1: STARTED
Node 2: STARTED
## Activate API node 67, expect success
Connected to Management Server at: 192.168.0.101:1187
Configuration changed to reflect activated node
Now activating the node in the cluster
Node 67 is now activated in the cluster

Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, node is deactivated)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Run ndb_desc using node 67, should connect now

NDBT_ProgramExit: 0 - OK

## Activate MGM node 65, expect success
Connected to Management Server at: 192.168.0.101:1187
Configuration changed to reflect activated node
Now activating the node in the cluster
Node 65 is now activated in the cluster

Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65 (not connected, accepting connect from 192.168.0.101)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Start MGM node 65, expect success
Connected to Management Server at: 192.168.0.101:1187
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3 (not connected, node is deactivated)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Activate node 3, expect success
Connected to Management Server at: 192.168.0.101:1186
Configuration changed to reflect new hostname of node 3
Now changing hostname in the cluster
Node 3 now has hostname 192.168.0.101 in the cluster

Connected to Management Server at: 192.168.0.101:1186
Configuration changed to reflect activated node
Now activating the node in the cluster
Node 3 is now activated in the cluster

Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3 (not connected, accepting connect from 192.168.0.101)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, node is deactivated)

## Activate API node 68, expect success
Connected to Management Server at: 192.168.0.101:1186
Configuration changed to reflect new hostname of node 68
Now changing hostname in the cluster
Node 68 now has hostname 192.168.0.101 in the cluster

Connected to Management Server at: 192.168.0.101:1186
Configuration changed to reflect activated node
Now activating the node in the cluster
Node 68 is now activated in the cluster

Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3 (not connected, accepting connect from 192.168.0.101)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

## Run ndb_desc using node 67, should connect now

NDBT_ProgramExit: 0 - OK

## Expecting successful start of node 3
## Wait for data node to start
Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)
id=3	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

## Deactivate node 2, expect success
Connected to Management Server at: 192.168.0.101:1186
Stopping node 2
Node 2 was successfully stopped
Configuration changed to reflect deactivated node
Now deactivating the node in the cluster
Node 2 is now deactivated in the cluster

Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)
id=2 (not connected, node is deactivated)
id=3	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

## Deactivate node 1, expect success
Connected to Management Server at: 192.168.0.101:1186
Stopping node 1
Node 1 was successfully stopped
Configuration changed to reflect deactivated node
Now deactivating the node in the cluster
Node 1 is now deactivated in the cluster

Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, node is deactivated)
id=2 (not connected, node is deactivated)
id=3	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

## Deactivate node 3, expect failure
Connected to Management Server at: 192.168.0.101:1186
Cannot deactivate node 3 since we need at least one active Data node in a cluster

Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, node is deactivated)
id=2 (not connected, node is deactivated)
id=3	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66	@192.168.0.101  (RonDB-22.10.0)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

## Deactivating MGM node 66, expect success
Connected to Management Server at: 192.168.0.101:1186
Node 66 is a MGM server, need to deactivate it before stopping it
since otherwise the config change transaction will fail
Configuration changed to reflect deactivated node
Now deactivating the node in the cluster
Node 66 is now deactivated in the cluster
Stopping node 66
Node 66 was successfully stopped

Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, node is deactivated)
id=2 (not connected, node is deactivated)
id=3	@192.168.0.101  (RonDB-22.10.0, Nodegroup: 0, *)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66 (not connected, node is deactivated)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

### Test finalised, now shutting down cluster
Connected to Management Server at: 192.168.0.101:1186
2 RonDB node(s) have shutdown.
Disconnecting to allow management server to shutdown.
Connected to Management Server at: 192.168.0.101:1186
Cluster Configuration
---------------------
[ndbd(NDB)]	3 node(s)
id=1 (not connected, node is deactivated)
id=2 (not connected, node is deactivated)
id=3 (not connected, accepting connect from 192.168.0.101)

[ndb_mgmd(MGM)]	2 node(s)
id=65	@192.168.0.101  (RonDB-22.10.0)
id=66 (not connected, node is deactivated)

[mysqld(API)]	2 node(s)
id=67 (not connected, accepting connect from 192.168.0.101)
id=68 (not connected, accepting connect from 192.168.0.101)

