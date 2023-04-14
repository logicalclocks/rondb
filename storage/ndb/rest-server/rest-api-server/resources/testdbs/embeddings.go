/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package testdbs

import _ "embed"

/*
	Dynamic schemes
*/

//go:embed dynamic/benchmark.sql
var BenchmarkScheme string

const Benchmark = "rdrs_bench"
const benchmarkSed_COLUMN_LENGTH = "COLUMN_LENGTH"
const benchmarkSed_VARBINARY_PK_LENGTH_LENGTH = "VARBINARY_PK_LENGTH"
const benchmarkSed_MANY_IDENTICAL_COLUMNS = "MANY_IDENTICAL_COLUMNS"

//go:embed dynamic/benchmark_add_row.sql
var BenchmarkAddRow string

// Seding values
const BenchAddRow_TABLE_NAME = "TABLE_NAME"
const BenchAddRow_COLUMN_VALUES_TO_INSERT = "COLUMN_VALUES_TO_INSERT"

//go:embed dynamic/hopsworks_add_project.sql
var HopsworksAddProject string

const hopsworksAddProject_PROJECT_NAME = "PROJECT_NAME"
const hopsworksAddProject_PROJECT_NUMBER = "PROJECT_NUMBER"

//go:embed dynamic/hopsworks_api_key.sql
var HopsworksAPIKey string

const HopsworksAPIKey_KEY_ID = "KEY_ID"
const HopsworksAPIKey_KEY_PREFIX = "KEY_PREFIX"
const HopsworksAPIKey_KEY_NAME = "KEY_NAME"
const HopsworksAPIKey_ADDITIONAL_KEYS = 512
const HopsworksAPIKey_SECRET = "ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"

//go:embed dynamic/textual_columns.sql
var TextualColumns string

const textualColumns_DATABASE_NAME = "DATABASE_NAME"
const textualColumns_COLUMN_TYPE = "COLUMN_TYPE"
const textualColumns_COLUMN_LENGTH = "COLUMN_LENGTH"

const DB012 = "db012"
const DB014 = "db014"
const DB015 = "db015"
const DB016 = "db016"
const DB017 = "db017"
const DB018 = "db018"

/*
	Fixed schemes

	TODO:

	Add the following constants for all tables to avoid dynamic typing of the tables all over the code:
	const DB001_table1 = "table_1"
	const DB002_table1 = "table_1"
*/

//go:embed fixed/hopsworks.sql
var HopsworksScheme string

const HOPSWORKS_DB_NAME = "hopsworks"

//go:embed fixed/DB000.sql
var DB000Scheme string

const DB000 = "db000"

//go:embed fixed/DB001.sql
var DB001Scheme string

const DB001 = "db001"

//go:embed fixed/DB002.sql
var DB002Scheme string

const DB002 = "db002"

//go:embed fixed/DB003.sql
var DB003Scheme string

const DB003 = "db003"

//go:embed fixed/DB004.sql
var DB004Scheme string

const DB004 = "db004"

//go:embed fixed/DB005.sql
var DB005Scheme string

const DB005 = "db005"

//go:embed fixed/DB006.sql
var DB006Scheme string

const DB006 = "db006"

//go:embed fixed/DB007.sql
var DB007Scheme string

const DB007 = "db007"

//go:embed fixed/DB008.sql
var DB008Scheme string

const DB008 = "db008"

//go:embed fixed/DB009.sql
var DB009Scheme string

const DB009 = "db009"

//go:embed fixed/DB010.sql
var DB010Scheme string

const DB010 = "db010"

//go:embed fixed/DB011.sql
var DB011Scheme string

const DB011 = "db011"

//go:embed fixed/DB013.sql
var DB013Scheme string

const DB013 = "db013"

//go:embed fixed/DB019.sql
var DB019Scheme string

const DB019 = "db019"

//go:embed fixed/DB020.sql
var DB020Scheme string

const DB020 = "db020"

//go:embed fixed/DB021.sql
var DB021Scheme string

const DB021 = "db021"

//go:embed fixed/DB022.sql
var DB022Scheme string

const DB022 = "db022"

//go:embed fixed/DB023.sql
var DB023Scheme string

const DB023 = "db023"

//go:embed fixed/DB024.sql
var DB024Scheme string

const DB024 = "db024"

//go:embed fixed/DB025.sql
var DB025Scheme string

const DB025 = "db025"

//go:embed fixed/DB025-Update.sql
var DB025UpdateScheme string

//go:embed fixed/DB026.sql
var DB026Scheme string

const DB026 = "db026"

//go:embed fixed/DB027.sql
var DB027Scheme string

const DB027 = "db027"

//go:embed fixed/FSDB001.sql
var FSDB001Scheme string

const FSDB001 = "fsdb001"

// This is sentinel DB
// If this exists then we have successfully initialized all the DBs
//
//go:embed fixed/sentinel.sql
var SentinelDBScheme string

const SentinelDB = "sentinel"
