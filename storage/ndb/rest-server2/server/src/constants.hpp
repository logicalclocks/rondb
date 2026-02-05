/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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


// Names of Endpoints
#define API_VERSION         "0.1.0"
#define API_VERSION_2       "0.2.0"
#define PING                "ping"
#define HEALTH              "health"
#define PKREAD              "pk-read"
#define PKDELETE            "pk-delete"
#define PKWRITE             "pk-write"
#define PKUPDATE            "pk-update"
#define PKINSERT            "pk-insert"
#define BATCH               "batch"
#define BATCHDELETE         "batchdelete"
#define BATCHWRITE          "batchwrite"
#define RONSQL              "ronsql"
#define METRICS             "metrics"
#define FEATURE_STORE       "feature_store"
#define BATCH_FEATURE_STORE "batch_feature_store"
#define SCAN                "scan"

#define MAKE_PATH(version, endpoint) "/" version "/" endpoint

// API version 0.1.0 paths (plain text errors)
#define HEALTH_PATH MAKE_PATH(API_VERSION, HEALTH)
#define PING_PATH   MAKE_PATH(API_VERSION, PING)
#define BATCH_PATH        MAKE_PATH(API_VERSION, BATCH)
#define BATCHDELETE_PATH  MAKE_PATH(API_VERSION, BATCHDELETE)
#define BATCHWRITE_PATH   MAKE_PATH(API_VERSION, BATCHWRITE)
#define SCAN_PATH  "/" API_VERSION "/{db}/{table}/" SCAN

#define PKREAD_PATH              "/" API_VERSION "/{db}/{table}/" PKREAD
#define RONSQL_PATH              "/" API_VERSION "/" RONSQL
#define FEATURE_STORE_PATH       "/" API_VERSION "/" FEATURE_STORE
#define BATCH_FEATURE_STORE_PATH "/" API_VERSION "/" BATCH_FEATURE_STORE
#define PROMETHEUS_METRICS_PATH  "/" METRICS

// API version 0.2.0 paths (JSON errors)
#define HEALTH_PATH_V2 MAKE_PATH(API_VERSION_2, HEALTH)
#define PING_PATH_V2   MAKE_PATH(API_VERSION_2, PING)
#define BATCH_PATH_V2        MAKE_PATH(API_VERSION_2, BATCH)
#define BATCHDELETE_PATH_V2  MAKE_PATH(API_VERSION_2, BATCHDELETE)
#define BATCHWRITE_PATH_V2   MAKE_PATH(API_VERSION_2, BATCHWRITE)

#define PKREAD_PATH_V2              "/" API_VERSION_2 "/{db}/{table}/" PKREAD
#define RONSQL_PATH_V2              "/" API_VERSION_2 "/" RONSQL
#define FEATURE_STORE_PATH_V2       "/" API_VERSION_2 "/" FEATURE_STORE
#define BATCH_FEATURE_STORE_PATH_V2 "/" API_VERSION_2 "/" BATCH_FEATURE_STORE

// TTL Purge API paths
#define TTL_PURGE              "ttl-purge"
#define TTL_PURGE_PATH         MAKE_PATH(API_VERSION, TTL_PURGE)
#define TTL_PURGE_CONFIG_PATH  "/" API_VERSION "/" TTL_PURGE "/config"
#define TTL_PURGE_STATUS_PATH  "/" API_VERSION "/" TTL_PURGE "/status"
#define TTL_PURGE_METRICS_PATH "/" API_VERSION "/" TTL_PURGE "/metrics"
#define TTL_PURGE_TABLES_PATH  "/" API_VERSION "/" TTL_PURGE "/tables"
#define TTL_PURGE_TABLE_PATH   "/" API_VERSION "/" TTL_PURGE "/tables/{db}/{table}"

constexpr const char *POST                           = "POST";
constexpr const char *GET                            = "GET";
constexpr const char *DELETE                         = "DELETE";
constexpr const char *OPERATIONS                     = "operations";
constexpr const char *METHOD                         = "method";
constexpr const char *RELATIVE_URL                   = "relative-url";
constexpr const char *BODY                           = "body";
constexpr const char *FILTERS                        = "filters";
constexpr const char *INDEX                          = "index";
constexpr const char *COLUMN                         = "column";
constexpr const char *VALUE                          = "value";
constexpr const char *READCOLUMNS                    = "readColumns";
constexpr const char *WRITECOLUMNS                   = "writeColumns";
constexpr const char *DATA_RETURN_TYPE               = "dataReturnType";
constexpr const char *OPERATION_ID                   = "operationId";
constexpr const char *LIMIT                          = "limit";
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

#define RDRS_MIN_NUM_THREADS 2
#define MACRO_TO_STRING_CONSTANT(X) _MACRO_TO_STRING_CONSTANT(X)
#define _MACRO_TO_STRING_CONSTANT(X) #X

// Default configuration values are in config_structs.cpp

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONSTANTS_HPP_
