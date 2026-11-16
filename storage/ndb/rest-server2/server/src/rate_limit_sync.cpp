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

#include "rate_limit_sync.hpp"

#include <algorithm>
#include <map>
#include <utility>

/*
 * Object keys for the two-run drop memory. The scope is part of a rule's
 * identity but the target is not: a rule whose target changed is modified
 * in place (drop + set this run), never remembered as absent.
 */
static std::string scope_key(const RateLimitScope &scope) {
  if (scope.db.empty()) {
    return "all";
  }
  if (scope.is_table) {
    return "tbl:" + scope.db + "." + scope.table;
  }
  return "db:" + scope.db;
}
static std::string user_key(const std::string &name) {
  return "u:" + name;
}
static std::string alias_key(const std::string &name) {
  return "a:" + name;
}
static std::string rule_key(const std::string &alias,
                            const RateLimitScope &scope) {
  return "r:" + alias + "|" + scope_key(scope);
}

// One desired alias rule: where it came from (for ordering and ties) and
// what it does.
struct DesiredRule {
  int group_id;
  RateLimitScope scope;
  std::string target;
};

void reconcile_rate_limits(
  const std::vector<RateLimitDesiredGroup> &desired,
  unsigned data_node_threads,
  const std::vector<RateLimitActualUser> &actual_users,
  const std::vector<RateLimitActualAlias> &actual_aliases,
  RateLimitSyncMemory *memory,
  std::vector<RateLimitCommand> *commands,
  std::vector<RateLimitGroupState> *states) {
  commands->clear();
  states->clear();

  std::map<std::string, unsigned long long> actual_user_rate;
  for (const RateLimitActualUser &user : actual_users) {
    actual_user_rate[user.name] = user.rate_per_sec;
  }
  // (alias, scope key) -> target
  std::map<std::string, std::map<std::string, std::string>> actual_rules;
  for (const RateLimitActualAlias &alias : actual_aliases) {
    auto &rules = actual_rules[alias.name];
    for (const RateLimitActualAliasRule &rule : alias.rules) {
      rules[scope_key(rule.scope)] = rule.target;
    }
  }

  /*
   * Desired alias rules, deduplicated on (alias, scope) in group-id order:
   * desired is group-id sorted, so on an intra-snapshot tie (possible only
   * from a stale read, Hopsworks refuses ties at save) the lower group id
   * claims the scope, matching the kernel's refusal rule for the commands
   * the instances race on.
   */
  std::map<std::string, std::vector<DesiredRule>> desired_rules;
  std::set<std::string> desired_keys;
  for (const RateLimitDesiredGroup &group : desired) {
    desired_keys.insert(user_key(group.name));
    for (const std::string &account : group.accounts) {
      desired_keys.insert(alias_key(account));
      std::vector<DesiredRule> &rules = desired_rules[account];
      // An ALL group contributes the scopeless match-everything rule;
      // a SELECTED group one rule per scope.
      std::vector<RateLimitScope> scopes = group.scopes;
      if (group.all_scope) {
        scopes.push_back(RateLimitScope{false, "", ""});
      }
      for (const RateLimitScope &scope : scopes) {
        std::string skey = scope_key(scope);
        bool claimed = false;
        for (const DesiredRule &existing : rules) {
          if (scope_key(existing.scope) == skey) {
            claimed = true;  // lower group id got here first
            break;
          }
        }
        if (!claimed) {
          rules.push_back(DesiredRule{group.group_id, scope, group.name});
          desired_keys.insert(rule_key(account, scope));
        }
      }
    }
  }

  // 1. USER SET / USER ALTER, group-id order.
  for (const RateLimitDesiredGroup &group : desired) {
    unsigned long long rate = rate_limit_budget_to_rate(
      group.budget_percent_hundredths, data_node_threads);
    auto it = actual_user_rate.find(group.name);
    RateLimitGroupState state;
    state.group_id = group.group_id;
    if (!group.all_scope && group.scopes.empty()) {
      state.state = RateLimitGroupState::State::Inert;
    } else if (it != actual_user_rate.end()) {
      state.state = RateLimitGroupState::State::Installed;
    } else {
      state.state = RateLimitGroupState::State::Pending;
    }
    states->push_back(state);
    if (it == actual_user_rate.end()) {
      RateLimitCommand cmd;
      cmd.kind = RateLimitCommand::Kind::UserSet;
      cmd.name = group.name;
      cmd.rate_per_sec = rate;
      commands->push_back(std::move(cmd));
    } else if (it->second != rate) {
      RateLimitCommand cmd;
      cmd.kind = RateLimitCommand::Kind::UserAlter;
      cmd.name = group.name;
      cmd.rate_per_sec = rate;
      commands->push_back(std::move(cmd));
    }
  }

  // 2. Alias rule SETs, group-id order (then account order within a group
  // via the map), with a scoped drop first when the scope exists with a
  // different target - the kernel refuses a same-scope different-target SET.
  std::vector<std::pair<int, RateLimitCommand>> rule_sets;
  for (const auto &alias_entry : desired_rules) {
    const std::string &account = alias_entry.first;
    auto actual_it = actual_rules.find(account);
    for (const DesiredRule &rule : alias_entry.second) {
      const std::string skey = scope_key(rule.scope);
      bool need_set = true;
      bool need_drop_first = false;
      if (actual_it != actual_rules.end()) {
        auto rule_it = actual_it->second.find(skey);
        if (rule_it != actual_it->second.end()) {
          if (rule_it->second == rule.target) {
            need_set = false;
          } else {
            need_drop_first = true;
          }
        }
      }
      if (need_drop_first) {
        RateLimitCommand drop;
        drop.kind = RateLimitCommand::Kind::AliasDropRule;
        drop.name = account;
        drop.scope = rule.scope;
        rule_sets.emplace_back(rule.group_id, std::move(drop));
      }
      if (need_set) {
        RateLimitCommand cmd;
        cmd.kind = RateLimitCommand::Kind::AliasSetRule;
        cmd.name = account;
        cmd.scope = rule.scope;
        cmd.target = rule.target;
        rule_sets.emplace_back(rule.group_id, std::move(cmd));
      }
    }
  }
  std::stable_sort(rule_sets.begin(), rule_sets.end(),
                   [](const std::pair<int, RateLimitCommand> &a,
                      const std::pair<int, RateLimitCommand> &b) {
                     return a.first < b.first;
                   });
  for (auto &entry : rule_sets) {
    commands->push_back(std::move(entry.second));
  }

  /*
   * 3. Deletions, under the two-run rule, most specific first: stale rules
   * of surviving aliases, whole aliases, users last (so no rule ever
   * points at a target the same run already dropped). Only keys the
   * previous run derived from the group rows are candidates: anything
   * else on the cluster is an operator's, not ours.
   */
  std::set<std::string> new_pending;
  std::vector<RateLimitCommand> drops;
  auto consider_drop = [&](const std::string &key,
                           RateLimitCommand &&cmd) {
    if (memory->pending_drops.count(key) != 0) {
      drops.push_back(std::move(cmd));  // absent twice: drop now
    } else if (memory->prev_desired_keys.count(key) != 0) {
      new_pending.insert(key);  // ours, absent once: remember
    }
    // Neither: not ours, never touch it.
  };

  for (const auto &alias_entry : actual_rules) {
    const std::string &account = alias_entry.first;
    bool alias_desired = desired_rules.count(account) != 0;
    if (!alias_desired) {
      // The whole alias is stale; a single USER ALIAS DROP covers its
      // rules, so they are not considered one by one.
      RateLimitCommand cmd;
      cmd.kind = RateLimitCommand::Kind::AliasDrop;
      cmd.name = account;
      consider_drop(alias_key(account), std::move(cmd));
      continue;
    }
    const std::vector<DesiredRule> &rules = desired_rules.at(account);
    for (const auto &actual_rule : alias_entry.second) {
      const std::string &skey = actual_rule.first;
      bool rule_desired = false;
      for (const DesiredRule &rule : rules) {
        if (scope_key(rule.scope) == skey) {
          rule_desired = true;
          break;
        }
      }
      if (!rule_desired) {
        RateLimitCommand cmd;
        cmd.kind = RateLimitCommand::Kind::AliasDropRule;
        cmd.name = account;
        // Reconstruct the scope from its key for the command.
        if (skey == "all") {
          cmd.scope = RateLimitScope{false, "", ""};
        } else if (skey.rfind("db:", 0) == 0) {
          cmd.scope = RateLimitScope{false, skey.substr(3), ""};
        } else {
          std::string rest = skey.substr(4);
          size_t dot = rest.find('.');
          cmd.scope =
            RateLimitScope{true, rest.substr(0, dot), rest.substr(dot + 1)};
        }
        consider_drop(rule_key(account, cmd.scope), std::move(cmd));
      }
    }
  }
  for (const RateLimitActualUser &user : actual_users) {
    if (desired_keys.count(user_key(user.name)) == 0) {
      RateLimitCommand cmd;
      cmd.kind = RateLimitCommand::Kind::UserDrop;
      cmd.name = user.name;
      consider_drop(user_key(user.name), std::move(cmd));
    }
  }
  // Rules first, then aliases, then users; stable within each kind.
  std::stable_sort(drops.begin(), drops.end(),
                   [](const RateLimitCommand &a, const RateLimitCommand &b) {
                     return (int)a.kind < (int)b.kind;
                   });
  for (RateLimitCommand &cmd : drops) {
    commands->push_back(std::move(cmd));
  }

  memory->prev_desired_keys = std::move(desired_keys);
  memory->pending_drops = std::move(new_pending);
}
