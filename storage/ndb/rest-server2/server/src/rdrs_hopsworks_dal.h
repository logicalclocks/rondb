/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_HOPSWORKS_DAL_H_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_HOPSWORKS_DAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "rdrs_dal.h"
#include "rdrs_const.h"

// API Key table
typedef struct HopsworksAPIKey {
  char secret[API_KEY_SECRET_SIZE];
  char salt[API_KEY_SALT_SIZE];
  char name[API_KEY_NAME_SIZE];
  int user_id;
  // api_key.expiry as unix epoch seconds; 0 = NULL = never expires
  long long expiry_epoch;
} HopsworksAPIKey;

// User table
typedef struct HopsworksUsers {
  char email[USERS_EMAIL_SIZE];
  // 8-char generated Hopsworks login ([a-z0-9]{8}); used to build the
  // project-user rate limit identity (RONDB-978)
  char username[USERS_USERNAME_SIZE];
} HopsworksUsers;

// project_team table
typedef struct HopsworksProjectTeam {
  int project_id;
  char team_role[PROJECT_TEAM_TEAM_ROLE_SIZE];
} HopsworksProjectTeam;

// project table
typedef struct HopsworksProject {
  int id;
  char projectname[PROJECT_PROJECTNAME_SIZE];
} HopsworksProject;

/**
 * Find api key row for given secret
 */
RS_Status find_api_key(const char *prefix, HopsworksAPIKey *api_key);

#ifdef __cplusplus
}  // extern "C"

#include <string>
#include <vector>

// A shared_feature_store row reduced to what the caller needs: the shared
// store and which of the API key owner's own projects received the share.
struct SharedStoreRef {
  int store_id;
  int recipient_project_id;
};

// A database the user reaches only through a share, paired with the project
// of theirs that received it (RONDB-978). Hopsworks grants the share to the
// MySQL account "<RecipientProject>_<username>"
// (OnlineFeaturestoreController.shareOnlineFeatureStore), so SQL reads of the
// shared database are billed to that account; RDRS bills the same bucket so
// shared-store REST traffic is metered identically instead of running free.
struct HopsworksSharedDbBilling {
  std::string db;                 // the shared store's database name
  std::string recipient_project;  // original-case name of the receiving project
};

// One fine-grained data grant: a single online table the user may read,
// either entirely (columns empty) or restricted to the listed columns.
// Sourced from shared_feature_group/shared_feature (grantee = project) and
// restricted_feature_group_access/restricted_feature_access (grantee = user).
struct HopsworksFineGrant {
  std::string db;                    // producer feature store database
  std::string table;                 // online table name: <fg_name>_<version>
  std::vector<std::string> columns;  // empty = the whole table is granted
};

// Everything an API key user may access, resolved from the Hopsworks
// membership and sharing tables.
struct HopsworksUserGrants {
  // Full-database data access: the user's own (non-restricted) projects
  // plus feature stores shared entirely with any of those projects.
  std::vector<std::string> full_dbs;
  // Databases the user may resolve feature-view metadata from but not read
  // wholesale: restricted memberships and placeholder store shares
  // (shared_feature_store.shared_entirely = 0).
  std::vector<std::string> visible_dbs;
  // Table/column-level data grants.
  std::vector<HopsworksFineGrant> fine_grants;
  // hopsworks.users.username of the key's owner (RONDB-978)
  std::string username;
  // Original-case project names of ALL the user's project_team
  // memberships (any role, including 'Feature store restricted'): the
  // projects Hopsworks creates an online-FS MySQL account
  // "<ProjectName>_<username>" for. Unlike full_dbs this excludes shared
  // stores and preserves case (RONDB-978 rate limit identities).
  std::vector<std::string> member_projects;
  // Databases reachable only through a store share, each with the receiving
  // member project that determines the rate limit identity (RONDB-978).
  // A database that is also one of the owner's own projects never appears
  // here - membership always wins.
  std::vector<HopsworksSharedDbBilling> shared_db_billing;
};

/*
 * Resolve all databases, tables and columns the api key's user can access.
 * Members with the 'Feature store restricted' project role get no member
 * access: their project is only visible and their data access comes
 * exclusively from the restricted_* grant rows.
 */
RS_Status find_user_databases(int uid, HopsworksUserGrants *grants);

struct HopsworksAPIKeyEntry {
  std::string prefix;
  std::string secret;
  std::string salt;
  int user_id;
  // api_key.expiry as unix epoch seconds; 0 = NULL = never expires
  long long expiry_epoch;
};

// Decode a nullable DATETIME NdbRecAttr to unix epoch seconds (0 = NULL).
// Used for api_key.expiry by the DAL readers and the api_key event watcher.
class NdbRecAttr;
long long datetime_attr_to_epoch(const NdbRecAttr *attr, unsigned precision);

RS_Status find_all_api_keys(std::vector<HopsworksAPIKeyEntry> *keys);

// PK read by integer id — returns prefix, secret, salt, user_id
RS_Status find_api_key_by_id(int id, HopsworksAPIKeyEntry *entry);

#endif  // __cplusplus

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_HOPSWORKS_DAL_H_
