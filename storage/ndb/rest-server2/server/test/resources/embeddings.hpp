/*
 * Copyright (C) 2024 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_EMBEDDINGS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_EMBEDDINGS_HPP_

#include "embedded_content.hpp"

#include <string>

// Constants
const std::string Benchmark = "rdrs_bench";
const std::string benchmarkSed_COLUMN_LENGTH = "COLUMN_LENGTH";

const std::string benchmarkSed_VARBINARY_PK_LENGTH_LENGTH = "VARBINARY_PK_LENGTH";
const std::string benchmarkSed_MANY_IDENTICAL_COLUMNS = "MANY_IDENTICAL_COLUMNS";

//go:embed dynamic/benchmark_add_row.sql
const std::string BenchmarkAddRow = sqlFiles.at("dynamic/benchmark_add_row.sql");

// Seding values
const std::string BenchAddRow_TABLE_NAME = "TABLE_NAME";
const std::string BenchAddRow_COLUMN_VALUES_TO_INSERT = "COLUMN_VALUES_TO_INSERT";

//go:embed fixed/hopsworks_34.sql
const std::string HopsworksScheme = sqlFiles.at("fixed/hopsworks_34.sql");

const std::string HOPSWORKS_DB_NAME = "hopsworks";

//go:embed dynamic/hopsworks_34_add_project.sql
const std::string HopsworksAddProject = sqlFiles.at("dynamic/hopsworks_34_add_project.sql");

const std::string hopsworksAddProject_PROJECT_NAME = "PROJECT_NAME";
const std::string hopsworksAddProject_PROJECT_NUMBER = "PROJECT_NUMBER";

//go:embed dynamic/hopsworks_api_key.sql
const std::string HopsworksAPI_Key = sqlFiles.at("dynamic/hopsworks_api_key.sql");

const std::string HopsworksAPIKey_KEY_ID = "KEY_ID";
const std::string HopsworksAPIKey_KEY_PREFIX = "KEY_PREFIX";
const std::string HopsworksAPIKey_KEY_NAME = "KEY_NAME";
const int HopsworksAPIKey_ADDITIONAL_KEYS = 512;
const std::string HopsworksAPIKey_SECRET = "ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";

//go:embed dynamic/textual_columns.sql
const std::string TextualColumns = sqlFiles.at("dynamic/textual_columns.sql");

const std::string textualColumns_DATABASE_NAME = "DATABASE_NAME";
const std::string textualColumns_COLUMN_TYPE = "COLUMN_TYPE";
const std::string textualColumns_COLUMN_LENGTH = "COLUMN_LENGTH";

const std::string DB012 = "db012";
const std::string DB014 = "db014";
const std::string DB015 = "db015";
const std::string DB016 = "db016";
const std::string DB017 = "db017";
const std::string DB018 = "db018";

/*
	Fixed schemes

	TODO:

	Add the following constants for all tables to avoid dynamic typing of the tables all over the code:
	const DB001_table1 = "table_1"
	const DB002_table1 = "table_1"
*/

//go:embed fixed/DB000.sql
const std::string DB000Scheme = sqlFiles.at("fixed/DB000.sql");

const std::string DB000 = "db000";

//go:embed fixed/DB001.sql
const std::string DB001Scheme = sqlFiles.at("fixed/DB001.sql");

const std::string DB001 = "db001";

//go:embed fixed/DB002.sql
const std::string DB002Scheme = sqlFiles.at("fixed/DB002.sql");

const std::string DB002 = "db002";

//go:embed fixed/DB003.sql
const std::string DB003Scheme = sqlFiles.at("fixed/DB003.sql");

const std::string DB003 = "db003";

//go:embed fixed/DB004.sql
const std::string DB004Scheme = sqlFiles.at("fixed/DB004.sql");

const std::string DB004 = "db004";

//go:embed fixed/DB005.sql
const std::string DB005Scheme = sqlFiles.at("fixed/DB005.sql");

const std::string DB005 = "db005";

//go:embed fixed/DB006.sql
const std::string DB006Scheme = sqlFiles.at("fixed/DB006.sql");

const std::string DB006 = "db006";

//go:embed fixed/DB007.sql
const std::string DB007Scheme = sqlFiles.at("fixed/DB007.sql");

const std::string DB007 = "db007";

//go:embed fixed/DB008.sql
const std::string DB008Scheme = sqlFiles.at("fixed/DB008.sql");

const std::string DB008 = "db008";

//go:embed fixed/DB009.sql
const std::string DB009Scheme = sqlFiles.at("fixed/DB009.sql");

const std::string DB009 = "db009";

//go:embed fixed/DB010.sql
const std::string DB010Scheme = sqlFiles.at("fixed/DB010.sql");

const std::string DB010 = "db010";

//go:embed fixed/DB011.sql
const std::string DB011Scheme = sqlFiles.at("fixed/DB011.sql");

const std::string DB011 = "db011";

//go:embed fixed/DB013.sql
const std::string DB013Scheme = sqlFiles.at("fixed/DB013.sql");

const std::string DB013 = "db013";

//go:embed fixed/DB019.sql
const std::string DB019Scheme = sqlFiles.at("fixed/DB019.sql");

const std::string DB019 = "db019";

//go:embed fixed/DB020.sql
const std::string DB020Scheme = sqlFiles.at("fixed/DB020.sql");

const std::string DB020 = "db020";

//go:embed fixed/DB021.sql
const std::string DB021Scheme = sqlFiles.at("fixed/DB021.sql");

const std::string DB021 = "db021";

//go:embed fixed/DB022.sql
const std::string DB022Scheme = sqlFiles.at("fixed/DB022.sql");

const std::string DB022 = "db022";

//go:embed fixed/DB023.sql
const std::string DB023Scheme = sqlFiles.at("fixed/DB023.sql");

const std::string DB023 = "db023";

//go:embed fixed/DB024.sql
const std::string DB024Scheme = sqlFiles.at("fixed/DB024.sql");

const std::string DB024 = "db024";

//go:embed fixed/DB025.sql
const std::string DB025Scheme = sqlFiles.at("fixed/DB025.sql");

const std::string DB025 = "db025";

//go:embed fixed/DB025-Update.sql
const std::string DB025UpdateScheme = sqlFiles.at("fixed/DB025-Update.sql");

//go:embed fixed/DB026.sql
const std::string DB026Scheme = sqlFiles.at("fixed/DB026.sql");

const std::string DB026 = "db026";

//go:embed fixed/DB027.sql
const std::string DB027Scheme = sqlFiles.at("fixed/DB027.sql");

const std::string DB027 = "db027";

//go:embed fixed/DB028.sql
const std::string DB028Scheme = sqlFiles.at("fixed/DB028.sql");

const std::string DB028 = "db028";

//go:embed fixed/FSDB001.sql
const std::string FSDB001Scheme = sqlFiles.at("fixed/FSDB001.sql");

const std::string FSDB001 = "fsdb001";

//go:embed fixed/FSDB002.sql
const std::string FSDB002Scheme = sqlFiles.at("fixed/FSDB002.sql");

const std::string FSDB002 = "fsdb002";

// This is sentinel DB
// If this exists then we have successfully initialized all the DBs
//
//go:embed fixed/sentinel.sql
const std::string SentinelDBScheme = sqlFiles.at("fixed/sentinel.sql");

const std::string SentinelDB = "sentinel";


#endif