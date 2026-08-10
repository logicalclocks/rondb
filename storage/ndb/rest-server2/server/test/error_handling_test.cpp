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

#include "ndb_api_helper.hpp"

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
 * The cluster-unavailable set must match what
 * ClusterMgr::is_cluster_completely_unavailable() can produce. These are
 * the errors that should mark the connection for reconnection.
 */
TEST(NdbErrorClusterUnavailable, CompleteUnavailabilitySet) {
  EXPECT_TRUE(ndb_error_cluster_unavailable(4009));
  EXPECT_TRUE(ndb_error_cluster_unavailable(4035));
  EXPECT_TRUE(ndb_error_cluster_unavailable(4037));
  EXPECT_TRUE(ndb_error_cluster_unavailable(4038));
  EXPECT_TRUE(ndb_error_cluster_unavailable(4039));
  EXPECT_TRUE(ndb_error_cluster_unavailable(4040));
  EXPECT_TRUE(ndb_error_cluster_unavailable(4041));
}

/*
 * Errors that occur while the cluster can still answer must not trigger a
 * full reconnection: a single node failure (4010), a timeout (4012) or a
 * schema error can happen on a healthy cluster, and tearing down the
 * connection then would be far worse than the error itself.
 */
TEST(NdbErrorClusterUnavailable, PartialFailuresAreNotUnavailability) {
  EXPECT_FALSE(ndb_error_cluster_unavailable(0));
  EXPECT_FALSE(ndb_error_cluster_unavailable(-1));
  EXPECT_FALSE(ndb_error_cluster_unavailable(626));   // No data found
  EXPECT_FALSE(ndb_error_cluster_unavailable(723));   // No such table
  EXPECT_FALSE(ndb_error_cluster_unavailable(4010));  // Node failure, abort
  EXPECT_FALSE(ndb_error_cluster_unavailable(4012));  // Request time-out
  EXPECT_FALSE(ndb_error_cluster_unavailable(241));   // Schema version
}
