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
 * RONDB-1104: end-to-end tests for the reconnection trigger path. A
 * request that fails with a cluster-unavailability error must start a
 * reconnection; thread-cached Ndb objects must be handed back so the
 * teardown converges without waiting for its 120s timeout; and the pool
 * must serve working Ndb objects again once the reconnection completes.
 *
 * Needs a running cluster (RDRS_CONFIG_FILE), like the other C++ tests.
 * The cluster stays up throughout: the outage is simulated by returning
 * an Ndb object together with a manufactured 4009 status, which is
 * exactly what the serving path produces since RONDB-1104.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>

#include <drogon/HttpTypes.h>
#include <NdbApi.hpp>
#include <NdbMutex.h>
#include <NdbSleep.h>
#include <NdbTick.h>

#include "config_structs.hpp"
#include "connection.hpp"
#include "rdrs_dal.h"
#include "rdrs_rondb_connection_pool.hpp"
#include "status.hpp"

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as api_key_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

static constexpr Uint32 NUM_THREADS = 4;

class ReconnectTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    RS_Status status = RonDBConnection::init_rondb_connection(
      globalConfigs.ronDB, globalConfigs.ronDBMetadataCluster, NUM_THREADS);
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  static void TearDownTestSuite() {
    RS_Status status = RonDBConnection::shutdown_rondb_connection();
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  /* The RS_Status the serving path produces when the dictionary cannot be
   * read because no data node is available. */
  static RS_Status clusterDownStatus() {
    return __RS_ERROR(SERVER_ERROR, NdbError::UnknownResult,
                      NdbError::UnknownResultError, 4009, -1,
                      "No data node(s) available", __LINE__, __MYFILENAME__);
  }

  /* Wait until no reconnection is in progress and the connection reports
   * CONNECTED. Returns false on timeout. */
  static bool waitForRecovery(Uint32 max_ms) {
    const NDB_TICKS start = NdbTick_getCurrentTicks();
    while (true) {
      RonDB_Stats stats;
      get_rondb_stats(&stats);
      if (!stats.is_reconnection_in_progress &&
          stats.connection_state == CONNECTED) {
        return true;
      }
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      if (NdbTick_Elapsed(start, now).milliSec() > max_ms) {
        return false;
      }
      NdbSleep_MilliSleep(10);
    }
  }

  /* Prove an Ndb object talks to a live cluster: a dictionary lookup of a
   * nonexistent table must come back with 723 'No such table' - an answer
   * only the data nodes can give. */
  static void expectUsable(Ndb *ndb) {
    ASSERT_NE(ndb, nullptr);
    ASSERT_EQ(ndb->setCatalogName("test"), 0);
    NdbDictionary::Dictionary *dict = ndb->getDictionary();
    const NdbDictionary::Table *tab =
      dict->getTable("rondb_1104_no_such_table");
    EXPECT_EQ(tab, nullptr);
    EXPECT_EQ(dict->getNdbError().code, 723)
      << "Dictionary lookup did not reach the cluster: "
      << dict->getNdbError().message;
  }
};

/*
 * A cluster-unavailability status returned with an Ndb object must
 * trigger a reconnection, reclaim the idle thread-cached object of the
 * other thread, converge without the 120s teardown timeout, and serve
 * working objects afterwards.
 */
TEST_F(ReconnectTest, ClusterUnavailableStatusTriggersReconnect) {
  // Thread 0: acquire and return cleanly; the object stays cached idle.
  Ndb *ndb0 = nullptr;
  ASSERT_EQ(rdrsRonDBConnectionPool->GetNdbObject(&ndb0, 0).http_code,
            SUCCESS);
  RS_Status ok = RS_OK;
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb0, &ok, 0);

  // Thread 1: acquire and fail the request with the 4009 status the
  // serving path produces during an outage.
  Ndb *ndb1 = nullptr;
  ASSERT_EQ(rdrsRonDBConnectionPool->GetNdbObject(&ndb1, 1).http_code,
            SUCCESS);

  Uint64 generation_before =
    rdrsRonDBConnectionPool->GetDataConnectionGeneration(0);
  const NDB_TICKS start = NdbTick_getCurrentTicks();

  RS_Status down = clusterDownStatus();
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb1, &down, 1);

  // The reconnection must have started: generation bumped.
  EXPECT_GT(rdrsRonDBConnectionPool->GetDataConnectionGeneration(0),
            generation_before)
    << "4009 return did not trigger a reconnection";

  // With every object handed back the teardown must converge quickly -
  // far below its 120s wait-for-objects timeout.
  ASSERT_TRUE(waitForRecovery(60 * 1000))
    << "Reconnection did not complete";
  const NDB_TICKS end = NdbTick_getCurrentTicks();
  EXPECT_LT(NdbTick_Elapsed(start, end).milliSec(), 60 * 1000);

  // Both threads must get working objects from the new connection.
  Ndb *fresh0 = nullptr;
  Ndb *fresh1 = nullptr;
  ASSERT_EQ(rdrsRonDBConnectionPool->GetNdbObject(&fresh0, 0).http_code,
            SUCCESS);
  ASSERT_EQ(rdrsRonDBConnectionPool->GetNdbObject(&fresh1, 1).http_code,
            SUCCESS);
  expectUsable(fresh0);
  expectUsable(fresh1);
  rdrsRonDBConnectionPool->ReturnNdbObject(fresh0, &ok, 0);
  rdrsRonDBConnectionPool->ReturnNdbObject(fresh1, &ok, 1);
}

/*
 * Errors that do not mean "the cluster is gone" - a node failure abort, a
 * timeout, a schema error, a plain 404 - must NOT tear down the
 * connection.
 */
TEST_F(ReconnectTest, ClusterUpErrorsDoNotReconnect) {
  ASSERT_TRUE(waitForRecovery(60 * 1000));
  Uint64 generation_before =
    rdrsRonDBConnectionPool->GetDataConnectionGeneration(0);

  struct { int status; int classification; int code; } cases[] = {
    {NdbError::TemporaryError, NdbError::NodeRecoveryError, 4010},
    {NdbError::TemporaryError, NdbError::TimeoutExpired, 4012},
    {NdbError::PermanentError, NdbError::SchemaError, 241},
    {NdbError::PermanentError, NdbError::NoDataFound, 626},
    // Nodes starting / single user mode: transient administrative states,
    // reconnecting would turn them into a self-inflicted outage.
    {NdbError::UnknownResult, NdbError::UnknownResultError, 4037},
    {NdbError::UnknownResult, NdbError::UnknownResultError, 4041},
  };
  for (const auto &c : cases) {
    Ndb *ndb = nullptr;
    ASSERT_EQ(rdrsRonDBConnectionPool->GetNdbObject(&ndb, 2).http_code,
              SUCCESS);
    RS_Status err = __RS_ERROR(SERVER_ERROR, c.status, c.classification,
                               c.code, -1, "test", __LINE__, __MYFILENAME__);
    rdrsRonDBConnectionPool->ReturnNdbObject(ndb, &err, 2);
  }

  EXPECT_EQ(rdrsRonDBConnectionPool->GetDataConnectionGeneration(0),
            generation_before)
    << "A cluster-up error tore down the connection";

  RonDB_Stats stats;
  get_rondb_stats(&stats);
  EXPECT_EQ(stats.connection_state, CONNECTED);
  EXPECT_FALSE(stats.is_reconnection_in_progress);
  // The /health readiness signal: reachable data nodes on a live cluster.
  EXPECT_GT(get_num_ready_data_nodes(), 0);

  // The cached object must still be usable - no reconnection happened.
  Ndb *ndb = nullptr;
  ASSERT_EQ(rdrsRonDBConnectionPool->GetNdbObject(&ndb, 2).http_code,
            SUCCESS);
  expectUsable(ndb);
  RS_Status ok = RS_OK;
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb, &ok, 2);
}

int main(int argc, char **argv) {
  ndb_init();
  globalConfigsMutex = NdbMutex_Create();

  std::string configFile;
  const char *env_config_file_path = std::getenv("RDRS_CONFIG_FILE");
  if (env_config_file_path != nullptr) {
    configFile = env_config_file_path;
  }
  RS_Status status = AllConfigs::init(configFile);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    std::cerr << "Error loading config: " << status.message << std::endl;
    return 1;
  }

  testing::InitGoogleTest(&argc, argv);
  int rc = RUN_ALL_TESTS();
  NdbMutex_Destroy(globalConfigsMutex);
  ndb_end(0);
  return rc;
}
