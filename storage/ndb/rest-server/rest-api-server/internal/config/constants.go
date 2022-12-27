/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package config

import "hopsworks.ai/rdrs/version"

const API_KEY_NAME = "X-API-KEY"

const DB_PP = "db"
const TABLE_PP = "table"

const VERSION_GROUP = "/" + version.API_VERSION
const DB_OPS_EP_GROUP = VERSION_GROUP + "/:" + DB_PP + "/:" + TABLE_PP + "/"
const DBS_OPS_EP_GROUP = VERSION_GROUP + "/"

const PK_DB_OPERATION = "pk-read"
const BATCH_OPERATION = "batch"
const STAT_OPERATION = "stat"

const PK_HTTP_VERB = "POST"
const BATCH_HTTP_VERB = "POST"
const STAT_HTTP_VERB = "GET"

/*
	Env variables
*/

const CONFIG_FILE_PATH = "RDRS_CONFIG_FILE"
