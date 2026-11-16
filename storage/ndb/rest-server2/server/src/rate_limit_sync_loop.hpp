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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_SYNC_LOOP_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_SYNC_LOOP_HPP_

/*
 * RONDB-1158: the rate limit group sync loop (design doc section 8). On a
 * timer with per-instance jitter, each sync-enabled RDRS replica expands
 * the hopsworks.rate_limit_group tables (metadata connection) into the
 * desired USER / USER ALIAS state, filters it to what its own data cluster
 * can enforce, reconciles against USER LIST / USER ALIAS LIST and executes
 * the difference through the management server. Runs independently on
 * every sync-enabled replica: commands are idempotent and the reconcile
 * engine's conservative-delete rules (rate_limit_sync.hpp) keep concurrent
 * and stale instances from flapping each other's work.
 */

#include "rate_limit_sync.hpp"
#include "rdrs_dal.h"

#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

/*
 * Parse the row text of USER LIST into users. The format is the
 * management server's reply rows:
 *   User rate limits for <name>
 *     userId = N ... RatePerSec = N ...
 * Returns false on text that does not parse.
 */
bool rate_limit_parse_user_list(const std::string &text,
                                std::vector<RateLimitActualUser> *users);

/*
 * Parse the row text of USER ALIAS LIST into aliases with their rules:
 *   User alias rules for <name>
 *   aliasId = N
 *   numRules = N
 *     --table=db.tbl --target=t | --database=db --target=t | --target=t
 */
bool rate_limit_parse_alias_list(const std::string &text,
                                 std::vector<RateLimitActualAlias> *aliases);

/*
 * Keep only what this cluster can enforce: scopes whose database is in
 * local_dbs; then groups that kept at least one scope, ALL groups, and
 * inert groups (no scopes anywhere - kept so their state is reported).
 * A SELECTED group whose every scope lives on another cluster is removed.
 */
void rate_limit_filter_local(const std::set<std::string> &local_dbs,
                             std::vector<RateLimitDesiredGroup> *groups);

class RateLimitSyncLoop {
 public:
  struct RunResult {
    bool ran = false;  // false: skipped (version unchanged or not deployed)
    unsigned commands_executed = 0;
    unsigned commands_failed = 0;
  };

  enum class GroupState { Pending, Installed, Inert, Refused };

  RateLimitSyncLoop();
  ~RateLimitSyncLoop();

  // One reconcile pass; also the test entry point. Thread-compatible with
  // the background thread only through stop() - tests call it directly
  // without start().
  RS_Status run_once(RunResult *result);

  // Start/stop the background timer thread (period and jitter from
  // .RateLimit in the configuration).
  void start();
  void stop();

  // The last run's per-group judgement, keyed by group id (section 8
  // states; read back by the admin page through Hopsworks).
  std::map<int, GroupState> group_states();

 private:
  RS_Status run_once_locked(RunResult *result);
  RS_Status read_cluster_facts(std::set<std::string> *local_dbs);

  std::mutex run_mutex_;           // one run at a time (timer vs tests)
  std::mutex state_mutex_;         // guards states_
  std::map<int, GroupState> states_;

  RateLimitSyncMemory memory_;
  unsigned long long last_applied_version_ = 0;
  bool have_applied_version_ = false;
  unsigned long long run_count_ = 0;
  unsigned data_node_threads_ = 0;  // cached after the first ndbinfo read

  std::thread thread_;
  std::mutex sleep_mutex_;
  std::condition_variable sleep_cond_;
  bool stop_requested_ = false;
  bool started_ = false;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_SYNC_LOOP_HPP_
