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
 * RONDB-1158: the rate limit group DAL and expansion the RDRS sync loop is
 * built on. Reads the fixture rows seeded by rate_limit_groups_data.sql
 * (groups 1-4 on self-contained users rlga1b2/rlgb3c4 and projects
 * RLGrp01/RateLimitGroupVeryLongProj, What rows on the fixed stores) and
 * asserts the expanded desired state: Who rows become the online-FS MySQL
 * account names - every membership of a uid row, every member of a project
 * row, clip31 applied - and What rows become table/database scopes, in
 * group-id order.
 */

#include "../src/config_structs.hpp"
#include "connection.hpp"
#include "resources/seed_check.hpp"
#include "rdrs_dal.h"
#include "rdrs_hopsworks_dal.h"
#include "rdrs_rondb_connection_pool.hpp"
#include <NdbMutex.h>

#include <drogon/HttpTypes.h>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

NdbMutex *globalConfigsMutex = nullptr;

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

class RateLimitGroupsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    RequireSeededTestDatabases();
    RS_Status status = RonDBConnection::init_rondb_connection(
      globalConfigs.ronDB, globalConfigs.ronDBMetadataCluster, 4);
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

  // Borrow a metadata NDB object for the duration of one test.
  class MetadataNdb {
   public:
    MetadataNdb() {
      RS_Status status =
        rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb_);
      EXPECT_EQ(status.http_code,
                static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
        << status.message;
    }
    ~MetadataNdb() {
      RS_Status status;
      rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb_, &status);
    }
    Ndb *get() { return ndb_; }

   private:
    Ndb *ndb_ = nullptr;
  };
};

TEST_F(RateLimitGroupsTest, VersionCounter) {
  MetadataNdb ndb;
  unsigned long long version = 0;
  bool found = false;
  RS_Status status =
    read_rate_limit_group_version(ndb.get(), &version, &found);
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << status.message;
  EXPECT_TRUE(found) << "version row seeded by the fixture was not found";
  EXPECT_EQ(version, 42ULL);
}

TEST_F(RateLimitGroupsTest, ExpandDesiredState) {
  MetadataNdb ndb;
  std::vector<RateLimitDesiredGroup> groups;
  RS_Status status = expand_rate_limit_groups(ndb.get(), &groups);
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << status.message;
  ASSERT_EQ(groups.size(), 4u);

  // Group 1: Who = project RLGrp01 -> every member; What = store fsdb002.
  EXPECT_EQ(groups[0].group_id, 1);
  EXPECT_EQ(groups[0].name, "rlg_team_rl");
  EXPECT_EQ(groups[0].budget_percent_hundredths, 2000);
  EXPECT_FALSE(groups[0].all_scope);
  EXPECT_EQ(groups[0].accounts,
            (std::vector<std::string>{"RLGrp01_rlga1b2", "RLGrp01_rlgb3c4"}));
  ASSERT_EQ(groups[0].scopes.size(), 1u);
  EXPECT_FALSE(groups[0].scopes[0].is_table);
  EXPECT_EQ(groups[0].scopes[0].db, "fsdb002");
  EXPECT_TRUE(groups[0].scopes[0].table.empty());

  // Group 2: Who = uid rlga1b2 -> one account per membership, original
  // project-name case preserved (FSDB003) and the 34-char identity of the
  // long project clipped to 31; What = feature group 31
  // (sample_complex_type v1 in fsdb002).
  EXPECT_EQ(groups[1].group_id, 2);
  EXPECT_EQ(groups[1].name, "rlg_clicks_rl");
  EXPECT_EQ(groups[1].budget_percent_hundredths, 200);
  EXPECT_FALSE(groups[1].all_scope);
  EXPECT_EQ(groups[1].accounts,
            (std::vector<std::string>{"FSDB003_rlga1b2", "RLGrp01_rlga1b2",
                                      "RateLimitGroupVeryLongProj_rlga"}));
  ASSERT_EQ(groups[1].scopes.size(), 1u);
  EXPECT_TRUE(groups[1].scopes[0].is_table);
  EXPECT_EQ(groups[1].scopes[0].db, "fsdb002");
  EXPECT_EQ(groups[1].scopes[0].table, "sample_complex_type_1");

  // Group 3: ALL scope - no What rows by definition.
  EXPECT_EQ(groups[2].group_id, 3);
  EXPECT_EQ(groups[2].name, "rlg_all_rl");
  EXPECT_EQ(groups[2].budget_percent_hundredths, 500);
  EXPECT_TRUE(groups[2].all_scope);
  EXPECT_EQ(groups[2].accounts,
            (std::vector<std::string>{"RLGrp01_rlgb3c4"}));
  EXPECT_TRUE(groups[2].scopes.empty());

  // Group 4: SELECTED with no What rows - inert, but still expanded so the
  // sync can report it as such.
  EXPECT_EQ(groups[3].group_id, 4);
  EXPECT_EQ(groups[3].name, "rlg_inert_rl");
  EXPECT_EQ(groups[3].budget_percent_hundredths, 50);
  EXPECT_FALSE(groups[3].all_scope);
  EXPECT_EQ(groups[3].accounts,
            (std::vector<std::string>{"RLGrp01_rlgb3c4"}));
  EXPECT_TRUE(groups[3].scopes.empty());
}

int main(int argc, char **argv) {
  ndb_init();
  globalConfigsMutex = NdbMutex_Create();

  // Load config from RDRS_CONFIG_FILE (needed for correct NDB connection
  // settings)
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
