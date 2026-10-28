#!/bin/bash
# Runs the MTR suites against the build whose mysql-test directory is the
# current directory. build_all.sh runs it twice in a release build: on the
# PGO training build, where it also collects the profiles, and on the final
# build, the binaries that ship.
#
# Fails early and loud: the first suite with a failing test stops the run
# with a non-zero exit status. That suite itself runs to its end, so all of
# its failures are listed. A test that fails and then passes on mtr's retry
# counts as failing (mtr's default). MTR_EXTRA_ARGS is passed to every mtr
# invocation.
set -e

mysql_connector=mysql-connector-java-8.0.21-bin.jar
wget -N https://repo.hops.works/master/$mysql_connector
full_path=$(readlink -f $mysql_connector)
export MTR_CLASSPATH=$full_path

# --max-test-fail=0: with --force alone mtr still aborts a suite after 10
# failed tests, cutting the list of failures short.
run_mtr() {
  if ! ./mtr --force --max-test-fail=0 ${MTR_EXTRA_ARGS:-} "$@"; then
    echo "################################################################"
    echo "## MTR FAILED: $*"
    echo "## Stopping; the remaining suites were not run."
    echo "################################################################"
    exit 1
  fi
}

run_mtr --suite=ndb ndb_basic ndb_dd_basic clusterj
run_mtr --suite=ndb_opt
run_mtr --suite=ndbcluster
run_mtr --suite=ndb_rpl ndb_rpl_basic
run_mtr --suite=innodb innodb-index
run_mtr --suite=rdrs2-golang
#run_mtr --suite=ronsql ronsql_constants ronsql_date_sub ronsql_dbt3_1_2 ronsql_filtering ronsql_regressions

echo "MTR: all suites passed"
