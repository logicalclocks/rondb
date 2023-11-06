#!/bin/bash
set -e
./mtr --suite=ndb ndb_basic ndb_dd_basic clusterj
./mtr --suite=ndb_opt
./mtr --suite=ndbcluster
./mtr --suite=ndb_rpl ndb_rpl_basic
./mtr --suite=innodb innodb-index
./mtr --suite=rdrs 
