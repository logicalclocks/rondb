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
 * RONDB-1158: the sync loop against a live cluster. One run_once() must
 * install the fixture groups (rate_limit_groups_data.sql) as USER /
 * USER ALIAS entities, the next run must short-circuit on the unchanged
 * version counter, and a full resync must repair a manual drop. The test
 * cleans its entities up at the end, and the suite cleanup include drops
 * them as a backstop.
 */

#include "../src/config_structs.hpp"
#include "../src/rate_limit_sync_loop.hpp"
#include "connection.hpp"
#include "resources/seed_check.hpp"
#include "rdrs_dal.h"
#include <NdbMutex.h>

#include <mgmapi.h>
#include <mysql_com.h>  // NAME_LEN, which ndb_limits.h uses
#include <ndb_limits.h>  // RNIL, the user/alias list end marker
#include <mysql.h>

#include <drogon/HttpTypes.h>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

NdbMutex *globalConfigsMutex = nullptr;

static const char *kGroupUsers[] = {"rlg_team_rl", "rlg_clicks_rl",
                                    "rlg_all_rl", "rlg_inert_rl"};
static const char *kAliasAccounts[] = {"RLGrp01_rlga1b2", "RLGrp01_rlgb3c4",
                                       "FSDB003_rlga1b2",
                                       "RateLimitGroupVeryLongProj_rlga"};

class RateLimitSyncClusterTest : public ::testing::Test {
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
    // Drop everything the loop installed so MTR's check-testcase sees no
    // leftover entities; failures are fine (a test may have dropped some).
    // The state rows do not cascade (the fixture groups stay), so the
    // check-testcase baseline requires removing them here.
    query_sql("DELETE FROM hopsworks.rate_limit_group_cluster_state"
              " WHERE cluster_name = 'default'");
    NdbMgmHandle mgm = connect_mgm();
    if (mgm != nullptr) {
      for (const char *account : kAliasAccounts) {
        ndb_mgm_drop_user_alias(mgm, account, -1, "");
      }
      for (const char *user : kGroupUsers) {
        ndb_mgm_drop_user(mgm, user);
      }
      ndb_mgm_disconnect(mgm);
      ndb_mgm_destroy_handle(&mgm);
    }
    RS_Status status = RonDBConnection::shutdown_rondb_connection();
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  // Read one string column from the metadata mysqld, one row per element.
  static std::vector<std::string> query_sql(const std::string &statement) {
    std::vector<std::string> rows;
    auto &mySQL = globalConfigs.testing.mySQL;
    MYSQL *conn = mysql_init(nullptr);
    unsigned int protocol = MYSQL_PROTOCOL_TCP;
    mysql_options(conn, MYSQL_OPT_PROTOCOL, &protocol);
    if (mysql_real_connect(conn, mySQL.servers[0].IP.c_str(),
                           mySQL.user.c_str(), mySQL.password.c_str(),
                           nullptr, mySQL.servers[0].port, nullptr,
                           0) == nullptr) {
      ADD_FAILURE() << "mysql connect failed: " << mysql_error(conn);
      mysql_close(conn);
      return rows;
    }
    if (mysql_query(conn, statement.c_str()) != 0) {
      ADD_FAILURE() << "SQL failed: " << statement << ": "
                    << mysql_error(conn);
      mysql_close(conn);
      return rows;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    if (result != nullptr) {
      MYSQL_ROW row;
      while ((row = mysql_fetch_row(result)) != nullptr) {
        rows.emplace_back(row[0] == nullptr ? "" : row[0]);
      }
      mysql_free_result(result);
    }
    mysql_close(conn);
    return rows;
  }

  static NdbMgmHandle connect_mgm() {
    NdbMgmHandle mgm = ndb_mgm_create_handle();
    if (mgm == nullptr) {
      return nullptr;
    }
    std::string connect_string =
      globalConfigs.ronDB.generate_Mgmd_connect_string();
    if (ndb_mgm_set_connectstring(mgm, connect_string.c_str()) != 0 ||
        ndb_mgm_connect(mgm, 0, 0, 0) != 0) {
      ndb_mgm_destroy_handle(&mgm);
      return nullptr;
    }
    return mgm;
  }

  static std::string list_users() {
    NdbMgmHandle mgm = connect_mgm();
    EXPECT_NE(mgm, nullptr);
    std::string rows;
    Uint32 next_id = 0;
    do {
      char *buf = nullptr;
      if (ndb_mgm_list_users(mgm, &next_id, &buf) != 0) {
        ADD_FAILURE() << "USER LIST failed";
        break;
      }
      if (next_id == RNIL) {
        free(buf);
        break;
      }
      rows += buf;
      free(buf);
    } while (true);
    ndb_mgm_disconnect(mgm);
    ndb_mgm_destroy_handle(&mgm);
    return rows;
  }

  static std::string list_aliases() {
    NdbMgmHandle mgm = connect_mgm();
    EXPECT_NE(mgm, nullptr);
    std::string rows;
    Uint32 next_id = 0;
    do {
      char *buf = nullptr;
      if (ndb_mgm_list_user_aliases(mgm, &next_id, &buf) != 0) {
        ADD_FAILURE() << "USER ALIAS LIST failed";
        break;
      }
      if (next_id == RNIL) {
        delete[] buf;
        break;
      }
      rows += buf;
      delete[] buf;
    } while (true);
    ndb_mgm_disconnect(mgm);
    ndb_mgm_destroy_handle(&mgm);
    return rows;
  }

  static unsigned long long user_rate(
    const std::vector<RateLimitActualUser> &users, const std::string &name) {
    for (const RateLimitActualUser &user : users) {
      if (user.name == name) {
        return user.rate_per_sec;
      }
    }
    return 0;
  }

  static const RateLimitActualAlias *find_alias(
    const std::vector<RateLimitActualAlias> &aliases,
    const std::string &name) {
    for (const RateLimitActualAlias &alias : aliases) {
      if (alias.name == name) {
        return &alias;
      }
    }
    return nullptr;
  }
};

TEST_F(RateLimitSyncClusterTest, InstallShortCircuitAndRepair) {
  RateLimitSyncLoop loop;

  // 1. The first run installs the fixture groups.
  RateLimitSyncLoop::RunResult result;
  RS_Status status = loop.run_once(&result);
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << status.message;
  EXPECT_TRUE(result.ran);
  EXPECT_GT(result.commands_executed, 0u);
  EXPECT_EQ(result.commands_failed, 0u);

  std::vector<RateLimitActualUser> users;
  ASSERT_TRUE(rate_limit_parse_user_list(list_users(), &users));
  unsigned long long rate1 = user_rate(users, "rlg_team_rl");    // 20.00%
  unsigned long long rate2 = user_rate(users, "rlg_clicks_rl");  // 2.00%
  unsigned long long rate3 = user_rate(users, "rlg_all_rl");     // 5.00%
  unsigned long long rate4 = user_rate(users, "rlg_inert_rl");   // 0.50%
  ASSERT_GT(rate4, 0ULL) << "inert group's USER was not installed";
  // The rates are hundredths * threads * 100; the thread count is the
  // cluster's own, so assert the exact proportions instead of a number.
  EXPECT_EQ(rate1, 40ULL * rate4);
  EXPECT_EQ(rate2, 4ULL * rate4);
  EXPECT_EQ(rate3, 10ULL * rate4);
  EXPECT_EQ(rate4 % 50ULL, 0ULL);  // hundredths divide out to an integer

  std::vector<RateLimitActualAlias> aliases;
  ASSERT_TRUE(rate_limit_parse_alias_list(list_aliases(), &aliases));
  const RateLimitActualAlias *a1 = find_alias(aliases, "RLGrp01_rlga1b2");
  ASSERT_NE(a1, nullptr);
  // Group 1's database rule and group 2's table rule.
  EXPECT_EQ(a1->rules.size(), 2u);
  const RateLimitActualAlias *a2 = find_alias(aliases, "RLGrp01_rlgb3c4");
  ASSERT_NE(a2, nullptr);
  // Group 1's database rule and group 3's ALL rule.
  EXPECT_EQ(a2->rules.size(), 2u);
  const RateLimitActualAlias *a3 = find_alias(aliases, "FSDB003_rlga1b2");
  ASSERT_NE(a3, nullptr);
  EXPECT_EQ(a3->rules.size(), 1u);
  const RateLimitActualAlias *a4 =
    find_alias(aliases, "RateLimitGroupVeryLongProj_rlga");
  ASSERT_NE(a4, nullptr) << "clip31 identity missing";
  EXPECT_EQ(a4->rules.size(), 1u);

  // The run reported its per-group states back to Hopsworks: the three
  // live groups as INSTALLED and the rule-less one as INERT, under this
  // cluster's configured name.
  std::vector<std::string> states = query_sql(
    "SELECT CONCAT(group_id, ':', state)"
    " FROM hopsworks.rate_limit_group_cluster_state"
    " WHERE cluster_name = 'default' ORDER BY group_id");
  ASSERT_EQ(states.size(), 4u);
  EXPECT_EQ(states[0], "1:INSTALLED");
  EXPECT_EQ(states[1], "2:INSTALLED");
  EXPECT_EQ(states[2], "3:INSTALLED");
  EXPECT_EQ(states[3], "4:INERT");

  // 2. The next run short-circuits on the unchanged version counter.
  status = loop.run_once(&result);
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
  EXPECT_FALSE(result.ran);

  // 3. A full resync repairs a manual drop without touching the rest.
  {
    NdbMgmHandle mgm = connect_mgm();
    ASSERT_NE(mgm, nullptr);
    EXPECT_EQ(ndb_mgm_drop_user(mgm, "rlg_all_rl"), 0);
    ndb_mgm_disconnect(mgm);
    ndb_mgm_destroy_handle(&mgm);
  }
  unsigned saved_resync_runs = globalConfigs.rateLimit.syncFullResyncRuns;
  globalConfigs.rateLimit.syncFullResyncRuns = 1;  // every run reconciles
  status = loop.run_once(&result);
  globalConfigs.rateLimit.syncFullResyncRuns = saved_resync_runs;
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
  EXPECT_TRUE(result.ran);
  users.clear();
  ASSERT_TRUE(rate_limit_parse_user_list(list_users(), &users));
  EXPECT_EQ(user_rate(users, "rlg_all_rl"), rate3);
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
