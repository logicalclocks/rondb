/*
 * Copyright (C) 2023, 2026 Hopsworks AB
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

package config

import "hopsworks.ai/rdrs2/version"

const API_KEY_NAME = "X-API-KEY"

// Path Prefixes (PP)
const DB_PP = "db"
const TABLE_PP = "table"
const DB_TABLE_PP = "/:" + DB_PP + "/:" + TABLE_PP + "/"

const VERSION_GROUP = "/" + version.API_VERSION
const DB_OPS_EP_GROUP = VERSION_GROUP + DB_TABLE_PP
const DBS_OPS_EP_GROUP = VERSION_GROUP + "/"

const PING_OPERATION = "ping"
const STAT_OPERATION = "stat"
const HEALTH_OPERATION = "health"
const PK_DB_OPERATION = "pk-read"
const PK_DELETE_OPERATION = "pk-delete"
const PK_WRITE_OPERATION = "pk-write"
const PK_UPDATE_OPERATION = "pk-update"
const PK_INSERT_OPERATION = "pk-insert"
const BATCH_OPERATION = "batch"
const BATCH_DELETE_OPERATION = "batchdelete"
const BATCH_WRITE_OPERATION = "batchwrite"
const FEATURE_STORE_OPERATION = "feature_store"
const BATCH_FEATURE_STORE_OPERATION = "batch_feature_store"

const PK_HTTP_VERB = "POST"
const BATCH_HTTP_VERB = "POST"
const STAT_HTTP_VERB = "GET"
const HEALTH_HTTP_VERB = "GET"
const FEATURE_STORE_HTTP_VERB = "POST"

const GRPC_API_TYPE = "GRPC"
const REST_API_TYPE = "REST"

/*
 Env variables
*/

const CONFIG_FILE_PATH = "RDRS_CONFIG_FILE"
