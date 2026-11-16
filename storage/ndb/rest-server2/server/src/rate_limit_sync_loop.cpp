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

#include "rate_limit_sync_loop.hpp"

#include "config_structs.hpp"
#include "logger.hpp"
#include "rdrs_hopsworks_dal.h"
#include "rdrs_rondb_connection_pool.hpp"
#include "status.hpp"

#include <mgmapi.h>
#include <mysql_com.h>  // NAME_LEN, which ndb_limits.h uses
#include <ndb_limits.h>  // RNIL, the user/alias list end marker
#include <NdbApi.hpp>
#include "../../../src/ndbapi/NdbInfo.hpp"

#include <chrono>
#include <cstdlib>
#include <random>
#include <sstream>

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

/*
 * Kernel error codes the loop maps to the Refused state: no budget left
 * under the 50% create cap, and an alias rule conflicting with one another
 * group installed first (DictTabInfo.hpp / CreateTable.hpp).
 */
static constexpr int kErrCreateDbNoAvailableRates = 1235;
static constexpr int kErrAliasRuleConflict = 1247;

bool rate_limit_parse_user_list(const std::string &text,
                                std::vector<RateLimitActualUser> *users) {
  std::istringstream stream(text);
  std::string line;
  static const std::string kNamePrefix = "User rate limits for ";
  static const std::string kRatePrefix = "RatePerSec = ";
  RateLimitActualUser current;
  bool in_user = false;
  while (std::getline(stream, line)) {
    size_t start = line.find_first_not_of(' ');
    if (start == std::string::npos) {
      continue;
    }
    std::string trimmed = line.substr(start);
    if (trimmed.rfind(kNamePrefix, 0) == 0) {
      if (in_user) {
        users->push_back(current);
      }
      current = RateLimitActualUser{};
      current.name = trimmed.substr(kNamePrefix.size());
      in_user = true;
    } else if (in_user && trimmed.rfind(kRatePrefix, 0) == 0) {
      current.rate_per_sec =
        strtoull(trimmed.c_str() + kRatePrefix.size(), nullptr, 10);
    }
  }
  if (in_user) {
    users->push_back(current);
  }
  return true;
}

bool rate_limit_parse_alias_list(const std::string &text,
                                 std::vector<RateLimitActualAlias> *aliases) {
  std::istringstream stream(text);
  std::string line;
  static const std::string kNamePrefix = "User alias rules for ";
  static const std::string kTable = "--table=";
  static const std::string kDatabase = "--database=";
  static const std::string kTarget = "--target=";
  RateLimitActualAlias current;
  bool in_alias = false;
  while (std::getline(stream, line)) {
    size_t start = line.find_first_not_of(' ');
    if (start == std::string::npos) {
      continue;
    }
    std::string trimmed = line.substr(start);
    if (trimmed.rfind(kNamePrefix, 0) == 0) {
      if (in_alias) {
        aliases->push_back(current);
      }
      current = RateLimitActualAlias{};
      current.name = trimmed.substr(kNamePrefix.size());
      in_alias = true;
      continue;
    }
    if (!in_alias || trimmed.rfind("--", 0) != 0) {
      continue;  // aliasId / numRules / noise
    }
    // A rule line: [--table=db.tbl | --database=db] --target=t
    RateLimitActualAliasRule rule;
    size_t target_pos = trimmed.find(kTarget);
    if (target_pos == std::string::npos) {
      return false;
    }
    rule.target = trimmed.substr(target_pos + kTarget.size());
    size_t target_end = rule.target.find(' ');
    if (target_end != std::string::npos) {
      rule.target.resize(target_end);
    }
    if (trimmed.rfind(kTable, 0) == 0) {
      std::string scope = trimmed.substr(
        kTable.size(), trimmed.find(' ') - kTable.size());
      size_t dot = scope.find('.');
      if (dot == std::string::npos) {
        return false;
      }
      rule.scope =
        RateLimitScope{true, scope.substr(0, dot), scope.substr(dot + 1)};
    } else if (trimmed.rfind(kDatabase, 0) == 0) {
      std::string scope = trimmed.substr(
        kDatabase.size(), trimmed.find(' ') - kDatabase.size());
      rule.scope = RateLimitScope{false, scope, ""};
    } else {
      rule.scope = RateLimitScope{false, "", ""};  // ALL
    }
    current.rules.push_back(std::move(rule));
  }
  if (in_alias) {
    aliases->push_back(current);
  }
  return true;
}

void rate_limit_filter_local(const std::set<std::string> &local_dbs,
                             std::vector<RateLimitDesiredGroup> *groups) {
  std::vector<RateLimitDesiredGroup> kept;
  for (RateLimitDesiredGroup &group : *groups) {
    if (group.all_scope || group.scopes.empty()) {
      // ALL groups apply everywhere; inert groups are kept so their state
      // is reported.
      kept.push_back(std::move(group));
      continue;
    }
    std::vector<RateLimitScope> local_scopes;
    for (RateLimitScope &scope : group.scopes) {
      if (local_dbs.count(scope.db) != 0) {
        local_scopes.push_back(std::move(scope));
      }
    }
    if (!local_scopes.empty()) {
      group.scopes = std::move(local_scopes);
      kept.push_back(std::move(group));
    }
    // Every scope lives on another cluster: not this cluster's group.
  }
  *groups = std::move(kept);
}

RateLimitSyncLoop::RateLimitSyncLoop() = default;

RateLimitSyncLoop::~RateLimitSyncLoop() { stop(); }

std::map<int, RateLimitSyncLoop::GroupState>
RateLimitSyncLoop::group_states() {
  std::lock_guard<std::mutex> guard(state_mutex_);
  return states_;
}

/*
 * The data-cluster facts of this run: the databases the cluster serves
 * (dictionary list) and, once, the data-node thread count the kernel
 * computes its budget caps from. ndbinfo.threads reports one row per block
 * thread plus one per send thread (Thrman), which is exactly the kernel's
 * getNumThreads() + getNumSendThreads(); heterogeneous nodes are handled
 * with the minimum so a budget never exceeds the smallest node's cap.
 */
RS_Status RateLimitSyncLoop::read_cluster_facts(
  std::set<std::string> *local_dbs) {
  Ndb *ndb_object = nullptr;
  RS_Status status =
    rdrsRonDBConnectionPool->GetRateLimitSyncNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }
  NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  NdbDictionary::Dictionary::List list;
  if (dict->listObjects(list, NdbDictionary::Object::UserTable) != 0) {
    status = RS_RONDB_SERVER_ERROR(dict->getNdbError(),
                                   "rate limit sync: listObjects failed");
    rdrsRonDBConnectionPool->ReturnRateLimitSyncNdbObject(ndb_object,
                                                          &status);
    return status;
  }
  for (unsigned i = 0; i < list.count; i++) {
    const NdbDictionary::Dictionary::List::Element &element =
      list.elements[i];
    if (element.database != nullptr) {
      local_dbs->insert(element.database);
    }
  }
  if (data_node_threads_ == 0) {
    NdbInfo info(&ndb_object->get_ndb_cluster_connection(), "");
    if (info.init()) {
      const NdbInfo::Table *table = nullptr;
      if (info.openTable("threads", &table) == 0) {
        NdbInfoScanOperation *scan = nullptr;
        if (info.createScanOperation(table, &scan) == 0) {
          const NdbInfoRecAttr *node_id_attr = nullptr;
          if (scan->readTuples() == 0 &&
              (node_id_attr = scan->getValue("node_id")) != nullptr &&
              scan->execute() == 0) {
            std::map<unsigned, unsigned> per_node;
            while (scan->nextResult() == 1) {
              per_node[node_id_attr->u_32_value()]++;
            }
            unsigned min_threads = 0;
            for (const auto &node : per_node) {
              if (min_threads == 0 || node.second < min_threads) {
                min_threads = node.second;
              }
            }
            data_node_threads_ = min_threads;
          }
          info.releaseScanOperation(scan);
        }
        info.closeTable(table);
      }
    }
  }
  RS_Status ok = RS_OK;
  rdrsRonDBConnectionPool->ReturnRateLimitSyncNdbObject(ndb_object, &ok);
  if (data_node_threads_ == 0) {
    return RS_SERVER_ERROR(
      "rate limit sync: could not read the data node thread count from"
      " ndbinfo.threads");
  }
  return RS_OK;
}

RS_Status RateLimitSyncLoop::run_once(RunResult *result) {
  std::lock_guard<std::mutex> guard(run_mutex_);
  return run_once_locked(result);
}

RS_Status RateLimitSyncLoop::run_once_locked(RunResult *result) {
  *result = RunResult{};

  // 1. The version counter: the cheap steady-state exit.
  Ndb *meta_ndb = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetMetadataNdbObject(&meta_ndb);
  if (status.http_code != SUCCESS) {
    return status;
  }
  unsigned long long version = 0;
  bool found = false;
  status = read_rate_limit_group_version(meta_ndb, &version, &found);
  if (status.http_code != SUCCESS || !found) {
    // Not deployed (or unreadable): never mistake this for "no groups".
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(meta_ndb, &status);
    return status;
  }
  run_count_++;
  bool full_resync =
    globalConfigs.rateLimit.syncFullResyncRuns != 0 &&
    (run_count_ % globalConfigs.rateLimit.syncFullResyncRuns) == 0;
  if (have_applied_version_ && version == last_applied_version_ &&
      !full_resync) {
    RS_Status ok = RS_OK;
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(meta_ndb, &ok);
    return RS_OK;
  }

  // 2. Desired state from the group tables.
  std::vector<RateLimitDesiredGroup> desired;
  status = expand_rate_limit_groups(meta_ndb, &desired);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(meta_ndb, &status);
  if (status.http_code != SUCCESS) {
    // A failed read skips the run; an empty result must not be mistaken
    // for "no groups".
    return status;
  }

  // 3. What this cluster can enforce.
  std::set<std::string> local_dbs;
  status = read_cluster_facts(&local_dbs);
  if (status.http_code != SUCCESS) {
    return status;
  }
  rate_limit_filter_local(local_dbs, &desired);

  // 4. The cluster's current USER / alias state.
  NdbMgmHandle mgm = ndb_mgm_create_handle();
  if (mgm == nullptr) {
    return RS_SERVER_ERROR("rate limit sync: ndb_mgm_create_handle failed");
  }
  std::string connect_string =
    globalConfigs.ronDB.generate_Mgmd_connect_string();
  if (ndb_mgm_set_connectstring(mgm, connect_string.c_str()) != 0 ||
      ndb_mgm_connect(mgm, 0, 0, 0) != 0) {
    ndb_mgm_destroy_handle(&mgm);
    return RS_SERVER_ERROR(
      "rate limit sync: cannot connect to the management server");
  }

  std::vector<RateLimitActualUser> actual_users;
  std::vector<RateLimitActualAlias> actual_aliases;
  {
    std::string rows;
    Uint32 next_id = 0;
    do {
      char *buf = nullptr;
      if (ndb_mgm_list_users(mgm, &next_id, &buf) != 0) {
        ndb_mgm_disconnect(mgm);
        ndb_mgm_destroy_handle(&mgm);
        return RS_SERVER_ERROR("rate limit sync: USER LIST failed");
      }
      if (next_id == RNIL) {
        free(buf);
        break;
      }
      rows += buf;
      free(buf);
    } while (true);
    rate_limit_parse_user_list(rows, &actual_users);

    rows.clear();
    next_id = 0;
    do {
      char *buf = nullptr;
      if (ndb_mgm_list_user_aliases(mgm, &next_id, &buf) != 0) {
        ndb_mgm_disconnect(mgm);
        ndb_mgm_destroy_handle(&mgm);
        return RS_SERVER_ERROR("rate limit sync: USER ALIAS LIST failed");
      }
      if (next_id == RNIL) {
        delete[] buf;
        break;
      }
      rows += buf;
      delete[] buf;
    } while (true);
    rate_limit_parse_alias_list(rows, &actual_aliases);
  }

  // 5. Reconcile and execute.
  std::vector<RateLimitCommand> commands;
  std::vector<RateLimitGroupState> engine_states;
  reconcile_rate_limits(desired, data_node_threads_, actual_users,
                        actual_aliases, &memory_, &commands, &engine_states);

  // The names to map a refused command back to its group: a UserSet names
  // the group's USER, an AliasSetRule names it as the target.
  std::map<std::string, int> group_by_user;
  for (const RateLimitDesiredGroup &group : desired) {
    group_by_user[group.name] = group.group_id;
  }
  std::map<int, GroupState> new_states;
  for (const RateLimitGroupState &state : engine_states) {
    switch (state.state) {
      case RateLimitGroupState::State::Pending:
        new_states[state.group_id] = GroupState::Pending;
        break;
      case RateLimitGroupState::State::Installed:
        new_states[state.group_id] = GroupState::Installed;
        break;
      case RateLimitGroupState::State::Inert:
        new_states[state.group_id] = GroupState::Inert;
        break;
    }
  }

  for (const RateLimitCommand &cmd : commands) {
    int rc = 0;
    switch (cmd.kind) {
      case RateLimitCommand::Kind::UserSet:
        rc = ndb_mgm_set_user(mgm, cmd.name.c_str(),
                              (Uint32)cmd.rate_per_sec, 0, 0, 0);
        break;
      case RateLimitCommand::Kind::UserAlter:
        rc = ndb_mgm_alter_user(mgm, cmd.name.c_str(),
                                (Uint32)cmd.rate_per_sec, 0, 0, 0);
        break;
      case RateLimitCommand::Kind::UserDrop:
        rc = ndb_mgm_drop_user(mgm, cmd.name.c_str());
        break;
      case RateLimitCommand::Kind::AliasSetRule:
      case RateLimitCommand::Kind::AliasDropRule: {
        int scope_type;
        std::string scope_name;
        if (cmd.scope.db.empty()) {
          scope_type = 2;  // all
        } else if (cmd.scope.is_table) {
          scope_type = 0;
          scope_name = cmd.scope.db + "." + cmd.scope.table;
        } else {
          scope_type = 1;
          scope_name = cmd.scope.db;
        }
        if (cmd.kind == RateLimitCommand::Kind::AliasSetRule) {
          rc = ndb_mgm_set_user_alias(mgm, cmd.name.c_str(),
                                      cmd.target.c_str(), scope_type,
                                      scope_name.c_str());
        } else {
          rc = ndb_mgm_drop_user_alias(mgm, cmd.name.c_str(), scope_type,
                                       scope_name.c_str());
        }
        break;
      }
      case RateLimitCommand::Kind::AliasDrop:
        rc = ndb_mgm_drop_user_alias(mgm, cmd.name.c_str(), -1, "");
        break;
    }
    if (rc == 0) {
      result->commands_executed++;
      continue;
    }
    result->commands_failed++;
    int mgm_error = ndb_mgm_get_latest_error(mgm);
    const std::string &group_name =
      (cmd.kind == RateLimitCommand::Kind::AliasSetRule) ? cmd.target
                                                         : cmd.name;
    auto group_it = group_by_user.find(group_name);
    if (group_it != group_by_user.end() &&
        (mgm_error == kErrCreateDbNoAvailableRates ||
         mgm_error == kErrAliasRuleConflict)) {
      new_states[group_it->second] = GroupState::Refused;
    }
    rdrs_logger::error(
      std::string("rate limit sync: command on '") + cmd.name +
      "' failed with management server error " + std::to_string(mgm_error));
  }
  ndb_mgm_disconnect(mgm);
  ndb_mgm_destroy_handle(&mgm);

  /*
   * The version is recorded as applied even when commands were refused:
   * a refusal (budget cap full, rule conflict) does not heal by itself,
   * so retrying it every period would be the refused-command loop the
   * design forbids. The periodic full resync and the next Hopsworks save
   * retry it, and the state map shows Refused meanwhile.
   */
  last_applied_version_ = version;
  have_applied_version_ = true;
  {
    std::lock_guard<std::mutex> guard(state_mutex_);
    states_ = std::move(new_states);
  }
  result->ran = true;
  return RS_OK;
}

void RateLimitSyncLoop::start() {
  if (started_) {
    return;
  }
  started_ = true;
  stop_requested_ = false;
  thread_ = std::thread([this]() {
    std::mt19937 jitter_rng{std::random_device{}()};
    for (;;) {
      unsigned period_s = globalConfigs.rateLimit.syncPeriodS;
      // Up to 20% jitter so a fleet of replicas spreads its runs.
      std::uniform_int_distribution<unsigned> jitter(
        0, period_s > 4 ? period_s / 5 : 1);
      unsigned sleep_s = period_s + jitter(jitter_rng);
      {
        std::unique_lock<std::mutex> lock(sleep_mutex_);
        sleep_cond_.wait_for(lock, std::chrono::seconds(sleep_s),
                             [this]() { return stop_requested_; });
        if (stop_requested_) {
          return;
        }
      }
      RunResult result;
      RS_Status status = run_once(&result);
      if (status.http_code != SUCCESS) {
        rdrs_logger::error(std::string("rate limit sync run failed: ") +
                           status.message);
      } else if (result.ran) {
        rdrs_logger::info(
          "rate limit sync: executed " +
          std::to_string(result.commands_executed) + " command(s), " +
          std::to_string(result.commands_failed) + " failed");
      }
    }
  });
}

void RateLimitSyncLoop::stop() {
  if (!started_) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(sleep_mutex_);
    stop_requested_ = true;
  }
  sleep_cond_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
  started_ = false;
}
