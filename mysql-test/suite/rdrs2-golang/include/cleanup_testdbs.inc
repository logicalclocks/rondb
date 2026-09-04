# Drop everything the RDRS2 test-database seed (InitialiseTesting in the
# Go test module) creates, so check-testcase sees no leftover state. Shared
# by rdrs2-golang_gotest and rdrs2-golang_cpptest: both seed the databases
# (whichever Go test runs first creates the sentinel DB and seeds the rest)
# and both must therefore clean them up.
#
# Cleanup on default connection (mysqld.1.1)
--disable_query_log
--disable_warnings

# The Golang tests create and drop many databases in quick succession;
# under load the schema distribution can time out transiently and mysqld
# logs a warning that would otherwise fail the test's error log check.
call mtr.add_suppression("Schema dist coordinator detected timeout");

DROP DATABASE IF EXISTS hopsworks;
DROP DATABASE IF EXISTS db000;
DROP DATABASE IF EXISTS db001;
DROP DATABASE IF EXISTS db002;
DROP DATABASE IF EXISTS db003;
DROP DATABASE IF EXISTS db004;
DROP DATABASE IF EXISTS db005;
DROP DATABASE IF EXISTS db006;
DROP DATABASE IF EXISTS db007;
DROP DATABASE IF EXISTS db008;
DROP DATABASE IF EXISTS db009;
DROP DATABASE IF EXISTS db010;
DROP DATABASE IF EXISTS db011;
DROP DATABASE IF EXISTS db012;
DROP DATABASE IF EXISTS db013;
DROP DATABASE IF EXISTS db014;
DROP DATABASE IF EXISTS db015;
DROP DATABASE IF EXISTS db016;
DROP DATABASE IF EXISTS db017;
DROP DATABASE IF EXISTS db018;
DROP DATABASE IF EXISTS db019;
DROP DATABASE IF EXISTS db020;
DROP DATABASE IF EXISTS db021;
DROP DATABASE IF EXISTS db022;
DROP DATABASE IF EXISTS db023;
DROP DATABASE IF EXISTS db024;
DROP DATABASE IF EXISTS db025;
DROP DATABASE IF EXISTS db026;
DROP DATABASE IF EXISTS db027;
DROP DATABASE IF EXISTS db028;
DROP DATABASE IF EXISTS db029;
DROP DATABASE IF EXISTS db030;
DROP DATABASE IF EXISTS fsdb001;
DROP DATABASE IF EXISTS fsdb002;
DROP DATABASE IF EXISTS fsdb003;
DROP DATABASE IF EXISTS fsdb004;
DROP DATABASE IF EXISTS usera_project;
DROP DATABASE IF EXISTS rdrs_bench;
DROP DATABASE IF EXISTS sentinel;

# Also cleanup on mysqld.2.1 to avoid NDB schema distribution delay
# causing check-testcase to see stale databases.
--connect(cleanup_server2, 127.0.0.1, root, , test, $MASTER_MYPORT1)

call mtr.add_suppression("Schema dist coordinator detected timeout");

DROP DATABASE IF EXISTS hopsworks;
DROP DATABASE IF EXISTS db000;
DROP DATABASE IF EXISTS db001;
DROP DATABASE IF EXISTS db002;
DROP DATABASE IF EXISTS db003;
DROP DATABASE IF EXISTS db004;
DROP DATABASE IF EXISTS db005;
DROP DATABASE IF EXISTS db006;
DROP DATABASE IF EXISTS db007;
DROP DATABASE IF EXISTS db008;
DROP DATABASE IF EXISTS db009;
DROP DATABASE IF EXISTS db010;
DROP DATABASE IF EXISTS db011;
DROP DATABASE IF EXISTS db012;
DROP DATABASE IF EXISTS db013;
DROP DATABASE IF EXISTS db014;
DROP DATABASE IF EXISTS db015;
DROP DATABASE IF EXISTS db016;
DROP DATABASE IF EXISTS db017;
DROP DATABASE IF EXISTS db018;
DROP DATABASE IF EXISTS db019;
DROP DATABASE IF EXISTS db020;
DROP DATABASE IF EXISTS db021;
DROP DATABASE IF EXISTS db022;
DROP DATABASE IF EXISTS db023;
DROP DATABASE IF EXISTS db024;
DROP DATABASE IF EXISTS db025;
DROP DATABASE IF EXISTS db026;
DROP DATABASE IF EXISTS db027;
DROP DATABASE IF EXISTS db028;
DROP DATABASE IF EXISTS db029;
DROP DATABASE IF EXISTS db030;
DROP DATABASE IF EXISTS fsdb001;
DROP DATABASE IF EXISTS fsdb002;
DROP DATABASE IF EXISTS fsdb003;
DROP DATABASE IF EXISTS fsdb004;
DROP DATABASE IF EXISTS usera_project;
DROP DATABASE IF EXISTS rdrs_bench;
DROP DATABASE IF EXISTS sentinel;

--disconnect cleanup_server2
--connection default

# The Go tests provision a USER rate-limit entity for the test API key
# prefix (testutils.ProvisionDefaultRateLimitUsers, key constant
# HOPSWORKS_TEST_API_KEY). Drop it here so check-testcase sees no leftover
# $-entity in the data nodes. Tolerate failure: the entity does not exist
# when the Go tests were skipped. The result log is disabled because the
# command's output depends on whether the entity existed.
--disable_result_log
--error 0,255,65280
--exec $NDB_MGM -e "USER DROP bkYjEz6OTZyevbqt"
--enable_result_log

--enable_warnings
--enable_query_log
