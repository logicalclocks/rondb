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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_SYNC_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_SYNC_HPP_

/*
 * RONDB-1158: the reconcile engine of the rate limit group sync loop. A
 * pure function from (desired state, cluster state, per-instance memory) to
 * an ordered management command list; all I/O - reading the group tables,
 * USER LIST / USER ALIAS LIST, executing commands - belongs to the caller.
 *
 * The rules it encodes (design doc section 8):
 * - Commands are ordered so targets exist before the rules that reference
 *   them: USER SET/ALTER in group-id order, then alias rule SETs in group-id
 *   order (so on a tie the kernel keeps the lower group id's rule), then
 *   rule drops, alias drops and USER drops last.
 * - A SET or ALTER acts on the current snapshot, but a DROP is issued only
 *   for an object that has been absent from this instance's desired state
 *   for two consecutive runs, so an instance holding a stale snapshot
 *   cannot flap an object another instance just installed.
 * - Only names this instance has itself derived from the group rows are
 *   ever dropped: a USER or alias an operator created by hand is not the
 *   sync's property.
 */

#include "rdrs_hopsworks_dal.h"

#include <set>
#include <string>
#include <vector>

// One USER row of the cluster's USER LIST (aliases excluded).
struct RateLimitActualUser {
  std::string name;
  unsigned long long rate_per_sec;
};

// One alias of the cluster's USER ALIAS LIST with its rule list. An ALL
// rule carries an empty scope (is_table false, db empty).
struct RateLimitActualAliasRule {
  RateLimitScope scope;
  std::string target;
};
struct RateLimitActualAlias {
  std::string name;
  std::vector<RateLimitActualAliasRule> rules;
};

// One management command, in execution order.
struct RateLimitCommand {
  enum class Kind {
    UserSet,        // USER SET <name> --rate-per-sec=<rate>
    UserAlter,      // USER ALTER <name> --rate-per-sec=<rate>
    AliasSetRule,   // USER ALIAS SET <name> --target=<target> [scope]
    AliasDropRule,  // USER ALIAS DROP <name> [scope]
    AliasDrop,      // USER ALIAS DROP <name>
    UserDrop,       // USER DROP <name>
  };
  Kind kind;
  std::string name;
  unsigned long long rate_per_sec = 0;  // UserSet / UserAlter
  RateLimitScope scope;                 // rule commands; empty db = ALL rule
  std::string target;                   // AliasSetRule

  bool operator==(const RateLimitCommand &o) const {
    return kind == o.kind && name == o.name &&
           rate_per_sec == o.rate_per_sec && scope == o.scope &&
           target == o.target;
  }
};

// The reconcile's judgement per group. Refused (the kernel rejected the
// install, e.g. CreateDbNoAvailableRates) is a runtime outcome the loop
// records when it executes the commands; the pure engine cannot know it.
struct RateLimitGroupState {
  int group_id;
  enum class State {
    Pending,    // the group's USER does not exist on the cluster yet
    Installed,  // USER present; alias rules converge this run
    Inert,      // SELECTED with no scopes: matches nothing by design
  } state;
};

// Per-instance memory carried between runs: which object keys the previous
// run derived from the group rows (what "ours" means), and which of those
// have been absent from desired exactly once (dropped when absent twice).
struct RateLimitSyncMemory {
  std::set<std::string> prev_desired_keys;
  std::set<std::string> pending_drops;
};

/*
 * Convert a group budget into the kernel's rate-per-sec: one data-node
 * thread executes 1e9 exclusive nanoseconds per second but the kernel
 * accounts rate-per-sec in microsecond-granularity units, so one thread is
 * 1,000,000/s and <percent> of <threads> is
 * hundredths * threads * 100 (design doc section 13: 20% of 20 threads =
 * 4,000,000).
 */
inline unsigned long long rate_limit_budget_to_rate(
  int budget_percent_hundredths, unsigned data_node_threads) {
  return (unsigned long long)budget_percent_hundredths *
         (unsigned long long)data_node_threads * 100ULL;
}

/*
 * Reconcile the desired groups (group-id sorted, already filtered to what
 * this cluster can enforce) against the cluster's USER and alias lists.
 * Appends the commands to execute in order, the per-group states, and
 * updates memory for the next run.
 */
void reconcile_rate_limits(
  const std::vector<RateLimitDesiredGroup> &desired,
  unsigned data_node_threads,
  const std::vector<RateLimitActualUser> &actual_users,
  const std::vector<RateLimitActualAlias> &actual_aliases,
  RateLimitSyncMemory *memory,
  std::vector<RateLimitCommand> *commands,
  std::vector<RateLimitGroupState> *states);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_SYNC_HPP_
