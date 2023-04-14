/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_HOPSWORKS_DAL_H_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_HOPSWORKS_DAL_H_

#include "rdrs-dal.h"

// API Key table
typedef struct HopsworksAPIKey {
  char secret[513];
  char salt[257];
  char name[46];
  int user_id;
} HopsworksAPIKey;

// User table
typedef struct HopsworksUsers {
  char email[151];
} HopsworksUsers;

// project_team table
typedef struct HopsworksProjectTeam {
  int project_id;
} HopsworksProjectTeam;

// project_team table
typedef struct HopsworksProject {
  char porjectname[101];
} HopsworksProject;

/**
 * Find api key row for given secret
 */
RS_Status find_api_key(const char *prefix, HopsworksAPIKey *api_key);

/*
 * Find all projects for the api key
 */
RS_Status find_all_projects(int uid, char ***projects, int *count);

#endif

#ifdef __cplusplus
}
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_HOPSWORKS_DAL_H_
