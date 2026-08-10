/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

/*
 * RONDB-1104: unit tests for the NDB error classification used by the
 * request-serving paths. These tests are pure and do not need a running
 * cluster.
 */

#include <gtest/gtest.h>

#include <NdbApi.hpp>
#include <NdbMutex.h>
#include <my_base.h>

#include "db_operations/pk/common.hpp"
#include "ndb_api_helper.hpp"
#include "status.hpp"

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as api_key_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

/*
 * A dictionary getTable() == nullptr must only be reported as
 * "Database/Table does not exist" (404) when the dictionary positively
 * says so. 723/709 are the "no such table" codes; 0 means the dictionary
 * reports no error at all (preserves historic behavior).
 */
TEST(NdbDictObjectMissing, TableMissingCodesMapToMissing) {
  EXPECT_TRUE(ndb_dict_object_missing(0));
  EXPECT_TRUE(ndb_dict_object_missing(709));
  EXPECT_TRUE(ndb_dict_object_missing(723));
}

/*
 * Cluster-unavailability and other dictionary failures must NOT be
 * classified as a missing table. This is the exact bug behind RONDB-1104:
 * error 4009 (no data nodes) was reported as 404, and the feature store
 * translated that into HTTP 200 with status MISSING.
 */
TEST(NdbDictObjectMissing, ClusterFailureCodesAreNotMissing) {
  EXPECT_FALSE(ndb_dict_object_missing(4009));  // Cluster failure
  EXPECT_FALSE(ndb_dict_object_missing(4035));  // Cluster temporarily unavailable
  EXPECT_FALSE(ndb_dict_object_missing(4037));  // Nodes starting
  EXPECT_FALSE(ndb_dict_object_missing(4010));  // Node failure caused abort
  EXPECT_FALSE(ndb_dict_object_missing(241));   // Invalid schema object version
  EXPECT_FALSE(ndb_dict_object_missing(283));   // Table is being dropped
  EXPECT_FALSE(ndb_dict_object_missing(284));   // Table not defined in TC
  EXPECT_FALSE(ndb_dict_object_missing(1226));  // Table is being dropped
  EXPECT_FALSE(ndb_dict_object_missing(4012));  // Request ndbd time-out
}

/*
 * The reconnection trigger set: only the states where the API node has
 * lost its transporter connection to EVERY data node. These strand the
 * NDB API in CS_waiting_for_clean_cache, and they also guarantee the
 * event subscribers received TE_CLUSTER_FAILURE so the teardown's
 * wait-for-objects converges.
 */
TEST(NdbErrorClusterUnavailable, TotalConnectivityLossTriggersReconnect) {
  EXPECT_TRUE(ndb_error_cluster_unavailable(4009));  // Cluster failure
  EXPECT_TRUE(ndb_error_cluster_unavailable(4035));  // None alive
  EXPECT_TRUE(ndb_error_cluster_unavailable(4040));  // Never connected
}

/*
 * Errors that occur while some data node is still reachable must not
 * trigger a full reconnection: the NDB API recovers on its own, tearing
 * down costs ~a minute of downtime, and the event subscribers never get
 * TE_CLUSTER_FAILURE so the teardown would stall on their objects. A
 * rolling restart (4037) or single user mode (4041) must never turn into
 * a self-inflicted outage.
 */
TEST(NdbErrorClusterUnavailable, PartialFailuresAreNotUnavailability) {
  EXPECT_FALSE(ndb_error_cluster_unavailable(0));
  EXPECT_FALSE(ndb_error_cluster_unavailable(-1));
  EXPECT_FALSE(ndb_error_cluster_unavailable(626));   // No data found
  EXPECT_FALSE(ndb_error_cluster_unavailable(723));   // No such table
  EXPECT_FALSE(ndb_error_cluster_unavailable(4010));  // Node failure, abort
  EXPECT_FALSE(ndb_error_cluster_unavailable(4012));  // Request time-out
  EXPECT_FALSE(ndb_error_cluster_unavailable(241));   // Schema version
  EXPECT_FALSE(ndb_error_cluster_unavailable(4036));  // Weird transient
  EXPECT_FALSE(ndb_error_cluster_unavailable(4037));  // Nodes starting
  EXPECT_FALSE(ndb_error_cluster_unavailable(4038));  // Version mismatch
  EXPECT_FALSE(ndb_error_cluster_unavailable(4039));  // Nodes stopping
  EXPECT_FALSE(ndb_error_cluster_unavailable(4041));  // Single user mode
}

/* Build the RS_Status an NDB error produces in the serving path. */
static RS_Status ndbErrorStatus(int status, int classification, int code,
                                int mysql_code) {
  return __RS_ERROR(SERVER_ERROR, status, classification, code, mysql_code,
                    "test", __LINE__, __MYFILENAME__);
}

/*
 * CanRetryOperation compares against NdbError::TemporaryError, which is a
 * Status enum value. It must therefore inspect RS_Status.status, not
 * RS_Status.classification (Classification value 1 is ApplicationError):
 * the old field mix-up retried permanent application errors and never
 * retried genuine temporary errors.
 */
TEST(CanRetryOperation, TemporaryStatusIsRetried) {
  // 4010 'Node failure caused abort': status TemporaryError,
  // classification NodeRecoveryError.
  RS_Status s = ndbErrorStatus(NdbError::TemporaryError,
                               NdbError::NodeRecoveryError, 4010, -1);
  EXPECT_TRUE(CanRetryOperation(s));

  // 410 'REDO log overloaded': status TemporaryError, classification
  // TemporaryResourceError.
  s = ndbErrorStatus(NdbError::TemporaryError,
                     NdbError::OverloadError, 410, -1);
  EXPECT_TRUE(CanRetryOperation(s));
}

TEST(CanRetryOperation, PermanentErrorsAreNotRetried) {
  // 626 'No data found': status PermanentError, classification NoDataFound.
  RS_Status s = ndbErrorStatus(NdbError::PermanentError,
                               NdbError::NoDataFound, 626, HA_ERR_KEY_NOT_FOUND);
  EXPECT_FALSE(CanRetryOperation(s));

  // Application errors are permanent; the old code retried these because
  // Classification::ApplicationError == 1 == Status::TemporaryError.
  s = ndbErrorStatus(NdbError::PermanentError,
                     NdbError::ApplicationError, 897, -1);
  EXPECT_FALSE(CanRetryOperation(s));

  // 4009 cluster unavailable: status UnknownResult - retrying cannot help
  // until the connection is re-established.
  s = ndbErrorStatus(NdbError::UnknownResult,
                     NdbError::UnknownResultError, 4009, -1);
  EXPECT_FALSE(CanRetryOperation(s));

  // Manufactured statuses (RS_SERVER_ERROR etc.) carry -1 fields.
  s = ndbErrorStatus(-1, -1, -1, -1);
  EXPECT_FALSE(CanRetryOperation(s));
}

TEST(CanRetryOperation, SchemaChangeErrorsAreRetriedViaUnloadSchema) {
  // 241 'Invalid schema object version' retries through UnloadSchema,
  // matched on code + mysql_code independent of the status field.
  RS_Status s = ndbErrorStatus(NdbError::PermanentError,
                               NdbError::SchemaError, 241,
                               HA_ERR_TABLE_DEF_CHANGED);
  EXPECT_TRUE(UnloadSchema(s));
  EXPECT_TRUE(CanRetryOperation(s));

  s = ndbErrorStatus(NdbError::PermanentError, NdbError::SchemaError, 283,
                     HA_ERR_NO_SUCH_TABLE);
  EXPECT_TRUE(UnloadSchema(s));
  EXPECT_TRUE(CanRetryOperation(s));
}

/*
 * ndb_init() creates g_eventLogger, which CanRetryOperation's retry
 * logging needs (same pattern as api_key_test.cpp).
 */
int main(int argc, char **argv) {
  ndb_init();
  testing::InitGoogleTest(&argc, argv);
  int rc = RUN_ALL_TESTS();
  ndb_end(0);
  return rc;
}
