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
 * RONDB-1158: concurrent sync at fleet scale (design doc section 7.5). Ten
 * independent loop instances - each with its own two-run drop memory, as
 * ten RDRS replicas would be - reconcile one cluster simultaneously and
 * staggered, a group is saved mid-run, and a deletion must survive the
 * conservative-delete rules: converged entities after every phase, no
 * flapping DROPs, and a management server that holds up under the combined
 * command rate.
 */

#include "../src/config_structs.hpp"
#include "../src/rate_limit_sync_loop.hpp"
#include "connection.hpp"
#include "resources/seed_check.hpp"
#include "rdrs_dal.h"
#include <NdbMutex.h>

#include <mgmapi.h>
#include <mysql.h>
#include <mysql_com.h>  // NAME_LEN, which ndb_limits.h uses
#include <ndb_limits.h>  // RNIL, the user/alias list end marker

#include <drogon/HttpTypes.h>
#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

NdbMutex *globalConfigsMutex = nullptr;

static constexpr int kFleetSize = 10;

static const char *kGroupUsers[] = {"rlg_team_rl", "rlg_clicks_rl",
                                    "rlg_all_rl", "rlg_inert_rl",
                                    "rlg_fleet_rl"};
static const char *kAliasAccounts[] = {"RLGrp01_rlga1b2", "RLGrp01_rlgb3c4",
                                       "FSDB003_rlga1b2",
                                       "RateLimitGroupVeryLongProj_rlga"};

class RateLimitSyncFleetTest : public ::testing::Test {
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
    // Remove the mid-run group's rows (idempotent) and every entity the
    // loops installed, so check-testcase sees a clean cluster.
    run_sql("DELETE FROM hopsworks.rate_limit_group WHERE id = 10");
    // The fixture groups stay, so their state rows do not cascade away.
    run_sql("DELETE FROM hopsworks.rate_limit_group_cluster_state"
            " WHERE cluster_name = 'default'");
    run_sql("UPDATE hopsworks.rate_limit_group_version"
            " SET version = version + 1 WHERE id = 1");
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

  // Run one statement on the metadata mysqld (the group tables live in the
  // hopsworks schema the Go seed created).
  static bool run_sql(const std::string &statement) {
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
      return false;
    }
    bool ok = mysql_query(conn, statement.c_str()) == 0;
    if (!ok) {
      ADD_FAILURE() << "SQL failed: " << statement << ": "
                    << mysql_error(conn);
    }
    mysql_close(conn);
    return ok;
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

  static bool user_exists(const std::string &name) {
    std::vector<RateLimitActualUser> users;
    rate_limit_parse_user_list(list_users(), &users);
    for (const RateLimitActualUser &user : users) {
      if (user.name == name) {
        return true;
      }
    }
    return false;
  }

  static size_t alias_rule_count(const std::string &name) {
    std::vector<RateLimitActualAlias> aliases;
    rate_limit_parse_alias_list(list_aliases(), &aliases);
    for (const RateLimitActualAlias &alias : aliases) {
      if (alias.name == name) {
        return alias.rules.size();
      }
    }
    return 0;
  }

  // Every loop runs once; failures surface as test failures.
  static void run_all(std::vector<RateLimitSyncLoop> *loops) {
    for (RateLimitSyncLoop &loop : *loops) {
      RateLimitSyncLoop::RunResult result;
      RS_Status status = loop.run_once(&result);
      EXPECT_EQ(status.http_code,
                static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
        << status.message;
    }
  }
};

TEST_F(RateLimitSyncFleetTest, FleetConverges) {
  // The whole fleet reconciles every run: the version short-circuit is for
  // steady-state cost, not what this test exercises, and the two-run drop
  // needs reconciling runs to count.
  unsigned saved_resync_runs = globalConfigs.rateLimit.syncFullResyncRuns;
  globalConfigs.rateLimit.syncFullResyncRuns = 1;

  std::vector<RateLimitSyncLoop> loops(kFleetSize);

  // Phase 1: simultaneous first runs against an empty cluster. Races on
  // USER SET are expected (one instance wins, the kernel refuses the
  // rest); what matters is that every instance completes and the cluster
  // converges to the desired state.
  {
    std::vector<std::thread> threads;
    for (int i = 0; i < kFleetSize; i++) {
      threads.emplace_back([&loops, i]() {
        RateLimitSyncLoop::RunResult result;
        RS_Status status = loops[i].run_once(&result);
        EXPECT_EQ(status.http_code,
                  static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
          << "loop " << i << ": " << status.message;
      });
    }
    for (std::thread &thread : threads) {
      thread.join();
    }
  }
  EXPECT_TRUE(user_exists("rlg_team_rl"));
  EXPECT_TRUE(user_exists("rlg_inert_rl"));
  EXPECT_EQ(alias_rule_count("RLGrp01_rlga1b2"), 2u);
  EXPECT_EQ(alias_rule_count("RLGrp01_rlgb3c4"), 2u);
  EXPECT_EQ(alias_rule_count("RateLimitGroupVeryLongProj_rlga"), 1u);

  // A converged fleet issues nothing more.
  {
    RateLimitSyncLoop::RunResult result;
    ASSERT_EQ(loops[0].run_once(&result).http_code,
              static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
    EXPECT_EQ(result.commands_failed, 0u);
    EXPECT_EQ(result.commands_executed, 0u);
  }

  // Phase 2: a group saved mid-run while the fleet reconciles staggered.
  // Instances that read before the save hold a stale snapshot; they must
  // not drop what the others install.
  {
    std::vector<std::thread> threads;
    for (int i = 0; i < kFleetSize; i++) {
      threads.emplace_back([&loops, i]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20 * i));
        for (int run = 0; run < 2; run++) {
          RateLimitSyncLoop::RunResult result;
          RS_Status status = loops[i].run_once(&result);
          EXPECT_EQ(status.http_code,
                    static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
            << "loop " << i << ": " << status.message;
        }
      });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(run_sql(
      "INSERT INTO hopsworks.rate_limit_group"
      " (id, name, budget_percent, data_scope)"
      " VALUES (10, 'rlg_fleet_rl', 1.00, 'SELECTED')"));
    ASSERT_TRUE(run_sql(
      "INSERT INTO hopsworks.rate_limit_group_user (id, group_id, uid)"
      " VALUES (10, 10, 10101)"));
    ASSERT_TRUE(run_sql(
      "INSERT INTO hopsworks.rate_limit_group_data"
      " (id, group_id, feature_store_id) VALUES (10, 10, 67)"));
    ASSERT_TRUE(run_sql(
      "UPDATE hopsworks.rate_limit_group_version"
      " SET version = version + 1 WHERE id = 1"));
    for (std::thread &thread : threads) {
      thread.join();
    }
  }
  // One more fleet pass: every instance has now seen the save.
  run_all(&loops);
  EXPECT_TRUE(user_exists("rlg_fleet_rl"));
  // rlgb3c4's alias gained group 10's fsdb001 rule on top of its two.
  EXPECT_EQ(alias_rule_count("RLGrp01_rlgb3c4"), 3u);
  EXPECT_EQ(alias_rule_count("RLGrp01_rlga1b2"), 2u);

  // Phase 3: the group is deleted. The first reconciling run after the
  // deletion must drop nothing (two-run rule); the second drops exactly
  // the group's entities - the shared alias survives with its other rules.
  ASSERT_TRUE(run_sql("DELETE FROM hopsworks.rate_limit_group"
                      " WHERE id = 10"));
  ASSERT_TRUE(run_sql("UPDATE hopsworks.rate_limit_group_version"
                      " SET version = version + 1 WHERE id = 1"));
  run_all(&loops);
  EXPECT_TRUE(user_exists("rlg_fleet_rl"))
    << "dropped on the first absent run - the two-run rule is broken";
  EXPECT_EQ(alias_rule_count("RLGrp01_rlgb3c4"), 3u);
  run_all(&loops);
  EXPECT_FALSE(user_exists("rlg_fleet_rl"));
  EXPECT_EQ(alias_rule_count("RLGrp01_rlgb3c4"), 2u);
  EXPECT_TRUE(user_exists("rlg_team_rl"));
  EXPECT_TRUE(user_exists("rlg_clicks_rl"));
  EXPECT_TRUE(user_exists("rlg_all_rl"));

  globalConfigs.rateLimit.syncFullResyncRuns = saved_resync_runs;
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
