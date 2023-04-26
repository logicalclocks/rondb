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
 * SELECT id AS project_id FROM project WHERE projectname = feature_store_name
 */
RS_Status find_project_id(const char *feature_store_name, int *project_id);

/**
 * Find feature store ID using the feature store name
 * SELECT id AS feature_store_id FROM feature_store WHERE _name = {feature_store_name}
 */
RS_Status find_feature_store_id(const char *feature_store_name, int *feature_store_id);

/**
 * Find feature view ID using the feature store id
 * SELECT id AS feature_view_id FROM feature_view WHERE name = {feature_view_name} AND version =
 * {feature_view_version} AND feature_store_id = {feature_store_id}
 */
RS_Status find_feature_view_id(int feature_store_id, const char *feature_view_name,
                               int feature_view_version, int *feature_view_id);

/**
 * Find training dataset join data
 * SELECT id AS td_join_id, feature_group AS feature_group_id, prefix FROM training_dataset_join
 * WHERE feature_view_id = {feature_view_id}
 */
RS_Status find_training_dataset_join_data(int feature_view_id, int *td_join_id,
                                          int *feature_group_id, char *prefix);

#endif
#ifdef __cplusplus
}
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_FEATURE_STORE_FEATURE_STORE_H_
