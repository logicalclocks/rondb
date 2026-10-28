#!/bin/bash
# Runs the MTR suites against the build whose mysql-test directory is the
# current directory. build_all.sh runs it twice in a release build: on the
# PGO training build, where it also collects the profiles, and on the final
# build, the binaries that ship.
#
# Every suite always runs to the end, so the profiles are complete and every
# failure is listed. The exit status is non-zero if any suite had a failing
# test. MTR_EXTRA_ARGS is passed to every mtr invocation.
set -e

mysql_connector=mysql-connector-java-8.0.21-bin.jar
wget -N https://repo.hops.works/master/$mysql_connector
full_path=$(readlink -f $mysql_connector)
export MTR_CLASSPATH=$full_path

# --max-test-fail=0: with --force alone mtr still aborts a suite after 10
# failed tests, cutting both the profile and the list of failures short.
FAILED_SUITES=()
run_mtr() {
  ./mtr --force --max-test-fail=0 ${MTR_EXTRA_ARGS:-} "$@" ||
    FAILED_SUITES+=("$*")
}

set +e
run_mtr --suite=ndb ndb_basic ndb_dd_basic clusterj
run_mtr --suite=ndb_opt
run_mtr --suite=ndbcluster
run_mtr --suite=ndb_rpl ndb_rpl_basic
run_mtr --suite=innodb innodb-index
run_mtr --suite=rdrs2-golang
#run_mtr --suite=ronsql ronsql_constants ronsql_date_sub ronsql_dbt3_1_2 ronsql_filtering ronsql_regressions
set -e

if [ ${#FAILED_SUITES[@]} -ne 0 ]; then
  echo "MTR: tests failed in:"
  printf '  %s\n' "${FAILED_SUITES[@]}"
  exit 1
fi
echo "MTR: all suites passed"
