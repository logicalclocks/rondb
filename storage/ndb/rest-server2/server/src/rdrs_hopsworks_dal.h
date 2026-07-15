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
} HopsworksAPIKey;

// User table
typedef struct HopsworksUsers {
  char email[USERS_EMAIL_SIZE];
} HopsworksUsers;

// project_team table
typedef struct HopsworksProjectTeam {
  int project_id;
} HopsworksProjectTeam;

// project_team table
typedef struct HopsworksProject {
  char projectname[PROJECT_PROJECTNAME_SIZE];
} HopsworksProject;

/**
 * Find api key row for given secret
 */
RS_Status find_api_key(const char *prefix, HopsworksAPIKey *api_key);

/*
 * Find all databases the api key's user can access: the user's own
 * projects plus feature stores shared entirely with any of those projects
 * (hopsworks.shared_feature_store, shared_entirely = 1)
 */
RS_Status find_user_databases(int uid, char ***projects, int *count);

#ifdef __cplusplus
}  // extern "C"

#include <string>
#include <vector>

struct HopsworksAPIKeyEntry {
  std::string prefix;
  std::string secret;
  std::string salt;
  int user_id;
};

RS_Status find_all_api_keys(std::vector<HopsworksAPIKeyEntry> *keys);

// PK read by integer id — returns prefix, secret, salt, user_id
RS_Status find_api_key_by_id(int id, HopsworksAPIKeyEntry *entry);

#endif  // __cplusplus

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_HOPSWORKS_DAL_H_
