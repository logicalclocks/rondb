#!/bin/bash
set -e

mysql_connector=mysql-connector-java-8.0.21-bin.jar
wget https://repo.hops.works/master/$mysql_connector
full_path=$(readlink -f $mysql_connector)
export MTR_CLASSPATH=$full_path

set +e
./mtr --suite=ndb --force ndb_basic ndb_dd_basic clusterj
./mtr --suite=ndb_opt --force
./mtr --suite=ndbcluster --force
./mtr --suite=ndb_rpl --force ndb_rpl_basic
./mtr --suite=innodb --force innodb-index
#./mtr --suite=rdrs2-golang --force
#./mtr --suite=ronsql --force ronsql_constants ronsql_date_sub ronsql_dbt3_1_2 ronsql_filtering ronsql_regressions
set -e
