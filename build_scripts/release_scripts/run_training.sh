#!/bin/bash
set -e

mysql_connector=mysql-connector-java-8.0.21-bin.jar
wget https://repo.hops.works/master/$mysql_connector
full_path=$(readlink -f $mysql_connector)
export MTR_CLASSPATH=$full_path

./mtr --suite=ndb ndb_basic ndb_dd_basic clusterj
./mtr --suite=ndb_opt
./mtr --suite=ndbcluster
./mtr --suite=ndb_rpl ndb_rpl_basic
./mtr --suite=innodb innodb-index
./mtr --suite=rdrs 
