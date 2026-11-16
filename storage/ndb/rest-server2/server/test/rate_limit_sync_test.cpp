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
 * RONDB-1158: the reconcile engine, pure-function tests. Each test builds a
 * (desired, actual, memory) triple and asserts the exact ordered command
 * list, so a change in ordering or in the conservative-delete rules fails
 * loudly. Cluster-free.
 */

#include "../src/rate_limit_sync.hpp"
#include "../src/rate_limit_sync_loop.hpp"

#include <gtest/gtest.h>
#include <NdbMutex.h>

#include <string>
#include <vector>

// Referenced by rdrs2_lib's config machinery; this binary never locks it.
NdbMutex *globalConfigsMutex = nullptr;

namespace {

constexpr unsigned kThreads = 20;

RateLimitDesiredGroup make_group(int id, const std::string &name,
                                 int hundredths, bool all_scope,
                                 std::vector<std::string> accounts,
                                 std::vector<RateLimitScope> scopes) {
  RateLimitDesiredGroup group;
  group.group_id = id;
  group.name = name;
  group.budget_percent_hundredths = hundredths;
  group.all_scope = all_scope;
  group.accounts = std::move(accounts);
  group.scopes = std::move(scopes);
  return group;
}

RateLimitScope table_scope(const std::string &db, const std::string &table) {
  return RateLimitScope{true, db, table};
}
RateLimitScope db_scope(const std::string &db) {
  return RateLimitScope{false, db, ""};
}
RateLimitScope all_scope() {
  return RateLimitScope{false, "", ""};
}

RateLimitCommand user_set(const std::string &name,
                          unsigned long long rate) {
  RateLimitCommand cmd;
  cmd.kind = RateLimitCommand::Kind::UserSet;
  cmd.name = name;
  cmd.rate_per_sec = rate;
  return cmd;
}
RateLimitCommand user_alter(const std::string &name,
                            unsigned long long rate) {
  RateLimitCommand cmd;
  cmd.kind = RateLimitCommand::Kind::UserAlter;
  cmd.name = name;
  cmd.rate_per_sec = rate;
  return cmd;
}
RateLimitCommand user_drop(const std::string &name) {
  RateLimitCommand cmd;
  cmd.kind = RateLimitCommand::Kind::UserDrop;
  cmd.name = name;
  return cmd;
}
RateLimitCommand alias_set(const std::string &name,
                           const RateLimitScope &scope,
                           const std::string &target) {
  RateLimitCommand cmd;
  cmd.kind = RateLimitCommand::Kind::AliasSetRule;
  cmd.name = name;
  cmd.scope = scope;
  cmd.target = target;
  return cmd;
}
RateLimitCommand alias_drop_rule(const std::string &name,
                                 const RateLimitScope &scope) {
  RateLimitCommand cmd;
  cmd.kind = RateLimitCommand::Kind::AliasDropRule;
  cmd.name = name;
  cmd.scope = scope;
  return cmd;
}
RateLimitCommand alias_drop(const std::string &name) {
  RateLimitCommand cmd;
  cmd.kind = RateLimitCommand::Kind::AliasDrop;
  cmd.name = name;
  return cmd;
}

// The doc's three example groups against an empty cluster.
std::vector<RateLimitDesiredGroup> example_desired() {
  return {
    make_group(1, "recommender_team_rl", 2000, true,
               {"Recommender_alice4f2", "Recommender_ava3b7c0"}, {}),
    make_group(2, "marketing_clicks_rl", 200, false,
               {"Marketing_mary01ab", "Marketing_mike7c3d"},
               {table_scope("marketing", "marketing_clicks_1")}),
    make_group(3, "recommender2_marketing_rl", 500, false,
               {"Recommender2_rosa9e1f"}, {db_scope("marketing")}),
  };
}

// The cluster state after example_desired() was fully applied.
void example_actual(std::vector<RateLimitActualUser> *users,
                    std::vector<RateLimitActualAlias> *aliases) {
  *users = {{"recommender_team_rl", 4000000ULL},
            {"marketing_clicks_rl", 400000ULL},
            {"recommender2_marketing_rl", 1000000ULL}};
  *aliases = {
    {"Recommender_alice4f2", {{all_scope(), "recommender_team_rl"}}},
    {"Recommender_ava3b7c0", {{all_scope(), "recommender_team_rl"}}},
    {"Marketing_mary01ab",
     {{table_scope("marketing", "marketing_clicks_1"),
       "marketing_clicks_rl"}}},
    {"Marketing_mike7c3d",
     {{table_scope("marketing", "marketing_clicks_1"),
       "marketing_clicks_rl"}}},
    {"Recommender2_rosa9e1f",
     {{db_scope("marketing"), "recommender2_marketing_rl"}}},
  };
}

// Run reconcile with fresh outputs.
void run(const std::vector<RateLimitDesiredGroup> &desired,
         const std::vector<RateLimitActualUser> &users,
         const std::vector<RateLimitActualAlias> &aliases,
         RateLimitSyncMemory *memory,
         std::vector<RateLimitCommand> *commands,
         std::vector<RateLimitGroupState> *states) {
  reconcile_rate_limits(desired, kThreads, users, aliases, memory, commands,
                        states);
}

TEST(RateLimitSyncTest, BudgetToRate) {
  // Section 13 of the design doc, 20 threads.
  EXPECT_EQ(rate_limit_budget_to_rate(2000, 20), 4000000ULL);  // 20%
  EXPECT_EQ(rate_limit_budget_to_rate(200, 20), 400000ULL);    // 2%
  EXPECT_EQ(rate_limit_budget_to_rate(500, 20), 1000000ULL);   // 5%
  EXPECT_EQ(rate_limit_budget_to_rate(50, 8), 40000ULL);       // 0.50%, 8 thr
}

TEST(RateLimitSyncTest, FreshInstall) {
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(example_desired(), {}, {}, &memory, &commands, &states);

  // Users first in group order, then alias rules in group order: targets
  // always exist before the rules that name them.
  std::vector<RateLimitCommand> expected = {
    user_set("recommender_team_rl", 4000000ULL),
    user_set("marketing_clicks_rl", 400000ULL),
    user_set("recommender2_marketing_rl", 1000000ULL),
    alias_set("Recommender_alice4f2", all_scope(), "recommender_team_rl"),
    alias_set("Recommender_ava3b7c0", all_scope(), "recommender_team_rl"),
    alias_set("Marketing_mary01ab",
              table_scope("marketing", "marketing_clicks_1"),
              "marketing_clicks_rl"),
    alias_set("Marketing_mike7c3d",
              table_scope("marketing", "marketing_clicks_1"),
              "marketing_clicks_rl"),
    alias_set("Recommender2_rosa9e1f", db_scope("marketing"),
              "recommender2_marketing_rl"),
  };
  EXPECT_EQ(commands, expected);

  ASSERT_EQ(states.size(), 3u);
  EXPECT_EQ(states[0].state, RateLimitGroupState::State::Pending);
  EXPECT_EQ(states[1].state, RateLimitGroupState::State::Pending);
  EXPECT_EQ(states[2].state, RateLimitGroupState::State::Pending);
}

TEST(RateLimitSyncTest, ConvergedIssuesNothing) {
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(example_desired(), users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());
  ASSERT_EQ(states.size(), 3u);
  EXPECT_EQ(states[0].state, RateLimitGroupState::State::Installed);
  EXPECT_EQ(states[1].state, RateLimitGroupState::State::Installed);
  EXPECT_EQ(states[2].state, RateLimitGroupState::State::Installed);
}

TEST(RateLimitSyncTest, BudgetChangeBecomesAlter) {
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);
  auto desired = example_desired();
  desired[1].budget_percent_hundredths = 300;  // 2% -> 3%
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(desired, users, aliases, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    user_alter("marketing_clicks_rl", 600000ULL),
  };
  EXPECT_EQ(commands, expected);
}

TEST(RateLimitSyncTest, TargetChangeDropsTheRuleFirst) {
  // The kernel refuses a same-scope different-target SET, so moving an
  // account to another group must drop the old rule before the new SET.
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);
  auto desired = example_desired();
  // mary moves from group 2's table rule to group 3's target on the same
  // scope.
  desired[1].accounts = {"Marketing_mike7c3d"};
  desired[2].accounts.push_back("Marketing_mary01ab");
  desired[2].scopes = {table_scope("marketing", "marketing_clicks_1")};
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(desired, users, aliases, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    alias_drop_rule("Marketing_mary01ab",
                    table_scope("marketing", "marketing_clicks_1")),
    alias_set("Marketing_mary01ab",
              table_scope("marketing", "marketing_clicks_1"),
              "recommender2_marketing_rl"),
    alias_set("Recommender2_rosa9e1f",
              table_scope("marketing", "marketing_clicks_1"),
              "recommender2_marketing_rl"),
    // rosa's old db rule is now stale, but a drop needs two absent runs.
  };
  EXPECT_EQ(commands, expected);
}

TEST(RateLimitSyncTest, DropsOnlyAfterTwoAbsentRuns) {
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);

  // Run 1: full desired state - everything converged, memory learns the
  // object keys.
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(example_desired(), users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());

  // Run 2: group 2 deleted in Hopsworks. First absence: nothing dropped.
  auto desired = example_desired();
  desired.erase(desired.begin() + 1);
  run(desired, users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());

  // Run 3: still absent: now the drops come, aliases before the user.
  run(desired, users, aliases, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    alias_drop("Marketing_mary01ab"),
    alias_drop("Marketing_mike7c3d"),
    user_drop("marketing_clicks_rl"),
  };
  EXPECT_EQ(commands, expected);
}

TEST(RateLimitSyncTest, StaleSnapshotCannotFlap) {
  // An instance whose snapshot predates a just-saved group sees the
  // cluster objects another instance installed. They were never part of
  // ITS desired state, so they are not its property: no drop, ever.
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);
  auto stale_desired = example_desired();
  stale_desired.pop_back();  // group 3 not in this instance's stale read

  RateLimitSyncMemory memory;  // fresh instance: no history at all
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  for (int run_no = 0; run_no < 3; run_no++) {
    run(stale_desired, users, aliases, &memory, &commands, &states);
    EXPECT_TRUE(commands.empty()) << "run " << run_no;
  }

  // The snapshot catches up: group 3 reappears before its objects were
  // ever considered for dropping - and the pending set resets, so a flap
  // of the snapshot (missing on one run, back on the next, missing again)
  // still never produces a drop.
  run(example_desired(), users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());
  auto desired = example_desired();
  desired.pop_back();
  run(desired, users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());  // first absence again: only pending
  run(example_desired(), users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());  // back again: pending forgotten
}

TEST(RateLimitSyncTest, NeverDropsForeignObjects) {
  // A USER and an alias an operator created by hand (e.g. plain RONDB-978
  // per-account limits) are not derivable from the group rows and must
  // survive any number of runs.
  std::vector<RateLimitActualUser> users = {{"metered_user", 1ULL}};
  std::vector<RateLimitActualAlias> aliases = {
    {"SomeProj_someuser", {{all_scope(), "metered_user"}}}};
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  for (int run_no = 0; run_no < 3; run_no++) {
    run({}, users, aliases, &memory, &commands, &states);
    EXPECT_TRUE(commands.empty()) << "run " << run_no;
  }
}

TEST(RateLimitSyncTest, IntraSnapshotTieLowerGroupWins) {
  // Two groups claiming the same (account, scope) can only come from a
  // stale read (Hopsworks refuses the save); the engine emits only the
  // lower group id's rule, matching the kernel's conflict rule for the
  // commands racing instances would issue.
  std::vector<RateLimitDesiredGroup> desired = {
    make_group(1, "group_a_rl", 1000, true, {"Proj_user1"}, {}),
    make_group(2, "group_b_rl", 500, true, {"Proj_user1"}, {}),
  };
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(desired, {}, {}, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    user_set("group_a_rl", 2000000ULL),
    user_set("group_b_rl", 1000000ULL),
    alias_set("Proj_user1", all_scope(), "group_a_rl"),
  };
  EXPECT_EQ(commands, expected);
}

TEST(RateLimitSyncTest, MemberLeavingDropsOnlyTheirAlias) {
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);

  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(example_desired(), users, aliases, &memory, &commands, &states);

  // mike leaves Marketing: his account disappears from group 2.
  auto desired = example_desired();
  desired[1].accounts = {"Marketing_mary01ab"};
  run(desired, users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());  // first absence
  run(desired, users, aliases, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    alias_drop("Marketing_mike7c3d"),
  };
  EXPECT_EQ(commands, expected);
}

TEST(RateLimitSyncTest, StaleRuleOfSurvivingAliasIsDropped) {
  std::vector<RateLimitActualUser> users;
  std::vector<RateLimitActualAlias> aliases;
  example_actual(&users, &aliases);
  // rosa's alias currently carries an extra table rule from a scope that
  // was removed from group 3.
  aliases[4].rules.push_back(
    {table_scope("marketing", "old_table_1"), "recommender2_marketing_rl"});
  auto with_old_scope = example_desired();
  with_old_scope[2].scopes.push_back(table_scope("marketing", "old_table_1"));

  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(with_old_scope, users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());

  run(example_desired(), users, aliases, &memory, &commands, &states);
  EXPECT_TRUE(commands.empty());  // first absence
  run(example_desired(), users, aliases, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    alias_drop_rule("Recommender2_rosa9e1f",
                    table_scope("marketing", "old_table_1")),
  };
  EXPECT_EQ(commands, expected);
}

TEST(RateLimitSyncTest, ParseUserList) {
  // The management server's USER LIST reply rows, as printed by
  // MgmtSrvr's user listing (see ndb_user_alias_lifecycle.result).
  const std::string text =
    "User rate limits for team_rl\n"
    "  userId = 7\n"
    "  userVersion = 1\n"
    "  RatePerSec = 100000\n"
    "  MaxTransactionSize = 0\n"
    "  MaxParallelTransactions = 0\n"
    "  MaxParallelComplexQueries = 0\n"
    "User rate limits for clicks_rl\n"
    "  userId = 8\n"
    "  userVersion = 1\n"
    "  RatePerSec = 4000000\n"
    "  MaxTransactionSize = 0\n"
    "  MaxParallelTransactions = 0\n"
    "  MaxParallelComplexQueries = 0\n";
  std::vector<RateLimitActualUser> users;
  ASSERT_TRUE(rate_limit_parse_user_list(text, &users));
  ASSERT_EQ(users.size(), 2u);
  EXPECT_EQ(users[0].name, "team_rl");
  EXPECT_EQ(users[0].rate_per_sec, 100000ULL);
  EXPECT_EQ(users[1].name, "clicks_rl");
  EXPECT_EQ(users[1].rate_per_sec, 4000000ULL);
}

TEST(RateLimitSyncTest, ParseAliasList) {
  const std::string text =
    "User alias rules for mary01ab\n"
    "aliasId = 12\n"
    "numRules = 3\n"
    "  --table=marketing.clicks_1 --target=clicks_rl\n"
    "  --database=marketing --target=team_rl\n"
    "  --target=team_rl\n"
    "User alias rules for rosa9e1f\n"
    "aliasId = 13\n"
    "numRules = 1\n"
    "  --database=marketing --target=recommender2_marketing_rl\n";
  std::vector<RateLimitActualAlias> aliases;
  ASSERT_TRUE(rate_limit_parse_alias_list(text, &aliases));
  ASSERT_EQ(aliases.size(), 2u);
  EXPECT_EQ(aliases[0].name, "mary01ab");
  ASSERT_EQ(aliases[0].rules.size(), 3u);
  EXPECT_EQ(aliases[0].rules[0].scope,
            table_scope("marketing", "clicks_1"));
  EXPECT_EQ(aliases[0].rules[0].target, "clicks_rl");
  EXPECT_EQ(aliases[0].rules[1].scope, db_scope("marketing"));
  EXPECT_EQ(aliases[0].rules[1].target, "team_rl");
  EXPECT_EQ(aliases[0].rules[2].scope, all_scope());
  EXPECT_EQ(aliases[0].rules[2].target, "team_rl");
  EXPECT_EQ(aliases[1].name, "rosa9e1f");
  ASSERT_EQ(aliases[1].rules.size(), 1u);
}

TEST(RateLimitSyncTest, LocalFilter) {
  // marketing exists on this cluster, salesdb does not; the group whose
  // every scope is foreign disappears, the mixed group keeps its local
  // scope, and ALL and inert groups always stay.
  std::vector<RateLimitDesiredGroup> groups = {
    make_group(1, "all_rl", 100, true, {"P_u1"}, {}),
    make_group(2, "mixed_rl", 100, false, {"P_u1"},
               {db_scope("marketing"), db_scope("salesdb")}),
    make_group(3, "foreign_rl", 100, false, {"P_u1"},
               {table_scope("salesdb", "t1_1")}),
    make_group(4, "inert_rl", 100, false, {"P_u1"}, {}),
  };
  std::set<std::string> local_dbs = {"marketing", "hopsworks"};
  rate_limit_filter_local(local_dbs, &groups);
  ASSERT_EQ(groups.size(), 3u);
  EXPECT_EQ(groups[0].name, "all_rl");
  EXPECT_EQ(groups[1].name, "mixed_rl");
  ASSERT_EQ(groups[1].scopes.size(), 1u);
  EXPECT_EQ(groups[1].scopes[0], db_scope("marketing"));
  EXPECT_EQ(groups[2].name, "inert_rl");
}

TEST(RateLimitSyncTest, InertGroupInstallsUserButNoRules) {
  std::vector<RateLimitDesiredGroup> desired = {
    make_group(7, "inert_rl", 100, false, {"Proj_user1"}, {}),
  };
  RateLimitSyncMemory memory;
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> states;
  run(desired, {}, {}, &memory, &commands, &states);
  std::vector<RateLimitCommand> expected = {
    user_set("inert_rl", 200000ULL),
  };
  EXPECT_EQ(commands, expected);
  ASSERT_EQ(states.size(), 1u);
  EXPECT_EQ(states[0].state, RateLimitGroupState::State::Inert);
}

}  // namespace
