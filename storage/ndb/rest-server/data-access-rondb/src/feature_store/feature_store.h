/*
 * Copyright (C) 2023 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_FEATURE_STORE_FEATURE_STORE_H_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_FEATURE_STORE_FEATURE_STORE_H_

#include "../rdrs-dal.h"

// some DS
typedef struct FSKey {
  char secret[513];
} FSKey;

/**
 * Find project ID using the feature store name
 */
RS_Status find_project_id(const char *feature_store_name, int *project_id);

#endif
#ifdef __cplusplus
}
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_FEATURE_STORE_FEATURE_STORE_H_
