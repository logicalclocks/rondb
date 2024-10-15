/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONSTANTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONSTANTS_HPP_

#include <string>

#define API_VERSION "0.1.0"
#define PING        "ping"
#define HEALTH      "health"
#define PKREAD      "pk-read"
#define BATCH       "batch"
#define RONSQL      "ronsql"

#define MAKE_PATH(version, endpoint) "/" version "/" endpoint

#define HEALTH_PATH MAKE_PATH(API_VERSION, HEALTH)
#define PING_PATH  MAKE_PATH(API_VERSION, PING)
#define BATCH_PATH MAKE_PATH(API_VERSION, BATCH)

#define PKREAD_PATH              "/" API_VERSION "/{db}/{table}/" PKREAD
#define RONSQL_PATH "/" API_VERSION "/" RONSQL
#define FEATURE_STORE_PATH       "/" API_VERSION "/feature_store"
#define BATCH_FEATURE_STORE_PATH "/" API_VERSION "/batch_feature_store"


constexpr const char *POST                           = "POST";
constexpr const char *GET                            = "GET";
constexpr const char *OPERATIONS                     = "operations";
constexpr const char *METHOD                         = "method";
constexpr const char *RELATIVE_URL                   = "relative-url";
constexpr const char *BODY                           = "body";
constexpr const char *FILTERS                        = "filters";
constexpr const char *COLUMN                         = "column";
constexpr const char *VALUE                          = "value";
constexpr const char *READCOLUMNS                    = "readColumns";
constexpr const char *DATA_RETURN_TYPE               = "dataReturnType";
constexpr const char *OPERATION_ID                   = "operationId";
constexpr const char *INFO                           = "INFO";
constexpr const char *API_KEY_NAME_LOWER_CASE =
    "x-api-key";  // Drogon always receives the header as lowercase
constexpr const char *FEATURE_STORE_NAME       = "featureStoreName";
constexpr const char *FEATURE_VIEW_NAME        = "featureViewName";
constexpr const char *FEATURE_VIEW_VERSION     = "featureViewVersion";
constexpr const char *PASSED_FEATURES          = "passedFeatures";
constexpr const char *ENTRIES                  = "entries";
constexpr const char *METADATA_OPTIONS         = "metadataOptions";
constexpr const char *FEATURE_NAME             = "featureName";
constexpr const char *FEATURE_TYPE             = "featureType";
constexpr const char *OPTIONS                  = "options";
constexpr const char *VALIDATE_PASSED_FEATURES = "validatePassedFeatures";
constexpr const char *INCLUDE_DETAILED_STATUS  = "includeDetailedStatus";
constexpr const char *FEATURE_STORE_OPERATION  = "feature_store";
constexpr const char *SEQUENCE_SEPARATOR       = "#";

constexpr const char *ERROR_NOT_FOUND = "Not Found";

// Default configuration values are in config_structs.cpp

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONSTANTS_HPP_
