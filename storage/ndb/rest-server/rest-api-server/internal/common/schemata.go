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

package common

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
)

const HOPSWORKS_SCHEMA_NAME = "hopsworks"

var databases map[string][][]string = make(map[string][][]string)

func init() {
	db := "bench"
	databases[db] = [][]string{
		createBenchmarkSchema(db, 1000),
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB000"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB001"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,
			"CREATE TABLE table_1(id0 VARCHAR(10), col_0 VARCHAR(100), col_1 VARCHAR(100), col_2 VARCHAR(100), PRIMARY KEY(id0)) ENGINE=ndbcluster",
			"INSERT INTO table_1 VALUES('id0_data', 'col_0_data', 'col_1_data', 'col_2_data')",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB002"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,
			"CREATE TABLE table_1(id0 VARCHAR(10), id1 VARCHAR(10), col_0 VARCHAR(100), col_1 VARCHAR(100), col_2 VARCHAR(100), PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO table_1 VALUES('id0_data', 'id1_data', 'col_0_data', 'col_1_data', 'col_2_data')",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB003"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE `date_table` ( `id0` int NOT NULL, `col0` date DEFAULT NULL, `col1` time DEFAULT NULL, `col2` datetime DEFAULT NULL, `col3` timestamp NULL DEFAULT NULL, `col4` year DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into date_table values(1, \"1111-11-11\", \"11:11:11\", \"1111-11-11 11:11:11\", \"1970-11-11 11:11:11\", \"11\")",
			"insert into date_table set id0=2",

			"CREATE TABLE `arrays_table` ( `id0` int NOT NULL, `col0` char(100) DEFAULT NULL, `col2` varchar(100) DEFAULT NULL, `col3` binary(100) DEFAULT NULL, `col4` varbinary(100)      DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into arrays_table values (1, \"abcd\", \"abcd\", 0xFFFF, 0xFFFF)",
			"insert into arrays_table set id0=2",

			"CREATE TABLE `set_table` ( `id0` int NOT NULL, `col0` enum('a','b','c','d') DEFAULT NULL, `col1` set('a','b','c','d') DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"INSERT INTO `set_table` VALUES (1,'a','a')",
			"INSERT INTO `set_table` VALUES (2,'b','a,b')",
			"insert into set_table set id0=3",

			"CREATE TABLE `special_table` ( `id0` int NOT NULL, `col0` geometry DEFAULT NULL, `col1` point DEFAULT NULL, `col2` linestring DEFAULT NULL, `col3` polygon DEFAULT NULL,       `col4` geomcollection DEFAULT NULL, `col5` multilinestring DEFAULT NULL, `col6` multipoint DEFAULT NULL, `col7` multipolygon DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into special_table set id0=1, col0=ST_GeomFromText('POINT(1 1)'), col1=ST_GeomFromText('POINT(1 1)'), col2=ST_GeomFromText('LineString(1 1,2 2,3 3)'), col3=ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))'), col7=ST_GeomFromText('MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'),col4=ST_GeomFromText('GeometryCollection(Point(1 1),LineString(2 2, 3 3))'),col6=ST_MPointFromText('MULTIPOINT (1 1, 2 2, 3 3)'),col5=ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))')",
			"insert into special_table set id0=2",

			"CREATE TABLE `number_table` ( `id0` int NOT NULL, `col0` tinyint DEFAULT NULL, `col1` smallint DEFAULT NULL, `col2` mediumint DEFAULT NULL, `col3` int DEFAULT NULL, `col4` bigint DEFAULT NULL, `col5` decimal(10, 0) DEFAULT NULL, `col6` float DEFAULT NULL, `col7` double DEFAULT NULL, `col8` bit(1) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"INSERT INTO `number_table` VALUES (1,99,99,99,99,99,99,99.99,99.99,true)",
			"insert into number_table set id0=2",

			"CREATE TABLE `blob_table` ( `id0` int NOT NULL, `col0` tinyblob, `col1` blob, `col2` mediumblob, `col3` longblob, `col4` tinytext, `col5` mediumtext, `col6` longtext, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into blob_table values(1, 0xFFFF, 0xFFFF, 0xFFFF,  0xFFFF, \"abcd\", \"abcd\", \"abcd\")",
			"insert into blob_table set id0=2",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	// signed and unsigned number data types
	db = "DB004"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE int_table(id0 INT, id1 INT UNSIGNED, col0 INT, col1 INT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  int_table VALUES(2147483647,4294967295,2147483647,4294967295)",
			"INSERT INTO  int_table VALUES(-2147483648,0,-2147483648,0)",
			"INSERT INTO  int_table VALUES(0,0,0,0)",
			"INSERT INTO  int_table set id0=1, id1=1", // NULL values for non primary columns

			// this table only has primary keys
			"CREATE TABLE int_table1(id0 INT, id1 INT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  int_table1 VALUES(0,0)",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB005"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE bigint_table(id0 BIGINT, id1 BIGINT UNSIGNED, col0 BIGINT, col1 BIGINT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  bigint_table VALUES(9223372036854775807,18446744073709551615,9223372036854775807,18446744073709551615)",
			"INSERT INTO  bigint_table VALUES(-9223372036854775808,0,-9223372036854775808,0)",
			"INSERT INTO  bigint_table VALUES(0,0,0,0)",
			"INSERT INTO  bigint_table set id0=1, id1=1", // NULL values for non primary columns
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB006"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE tinyint_table(id0 TINYINT, id1 TINYINT UNSIGNED, col0 TINYINT, col1 TINYINT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  tinyint_table VALUES(127,255,127,255)",
			"INSERT INTO  tinyint_table VALUES(-128,0,-128,0)",
			"INSERT INTO  tinyint_table VALUES(0,0,0,0)",
			"INSERT INTO  tinyint_table set id0=1, id1=1", // NULL values for non primary columns
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB007"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE smallint_table(id0 SMALLINT, id1 SMALLINT UNSIGNED, col0 SMALLINT, col1 SMALLINT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  smallint_table VALUES(32767,65535,32767,65535)",
			"INSERT INTO  smallint_table VALUES(-32768,0,-32768,0)",
			"INSERT INTO  smallint_table VALUES(0,0,0,0)",
			"INSERT INTO  smallint_table set id0=1, id1=1", // NULL values for non primary columns
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB008"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE mediumint_table(id0 MEDIUMINT, id1 MEDIUMINT UNSIGNED, col0 MEDIUMINT, col1 MEDIUMINT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  mediumint_table VALUES(8388607,16777215,8388607,16777215)",
			"INSERT INTO  mediumint_table VALUES(-8388608,0,-8388608,0)",
			"INSERT INTO  mediumint_table VALUES(0,0,0,0)",
			"INSERT INTO  mediumint_table set id0=1, id1=1", // NULL values for non primary columns
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB009"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE float_table1(id0 INT, col0 FLOAT, col1 FLOAT UNSIGNED, PRIMARY KEY(id0)) ENGINE=ndbcluster",
			"INSERT INTO  float_table1 VALUES(1,-123.123,123.123)",
			"INSERT INTO  float_table1 VALUES(0,0,0)",
			"INSERT INTO  float_table1 set id0=2", // NULL values for non primary columns

			"CREATE TABLE float_table2(id0 FLOAT, col0 FLOAT, col1 FLOAT UNSIGNED, PRIMARY KEY(id0)) ENGINE=ndbcluster",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB010"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE double_table1(id0 INT, col0 DOUBLE, col1 DOUBLE UNSIGNED, PRIMARY KEY(id0)) ENGINE=ndbcluster",
			"INSERT INTO  double_table1 VALUES(1,-123.123,123.123)",
			"INSERT INTO  double_table1 VALUES(0,0,0)",
			"INSERT INTO  double_table1 set id0=2", // NULL values for non primary columns

			"CREATE TABLE double_table2(id0 DOUBLE, col0 DOUBLE, col1 DOUBLE UNSIGNED, PRIMARY KEY(id0)) ENGINE=ndbcluster",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB011"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE decimal_table(id0 DECIMAL(10,5), id1 DECIMAL(10,5) UNSIGNED, col0 DECIMAL(10,5), col1 DECIMAL(10,5) UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE=ndbcluster",
			"INSERT INTO  decimal_table VALUES(-12345.12345,12345.12345,-12345.12345,12345.12345)",
			"INSERT INTO  decimal_table set id0=-67890.12345, id1=67890.12345",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB012"
	databases[db] = SchemaTextualColumns("char", db, 100)

	db = "DB013"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE blob_table(id0 int, col0 blob, col1 int,  PRIMARY KEY(id0)) ENGINE=ndbcluster",
			"INSERT INTO  blob_table VALUES(1,0xFFFF, 1)",
			"CREATE TABLE text_table(id0 int, col0 text, col1 int, PRIMARY KEY(id0)) ENGINE=ndbcluster",
			"INSERT INTO  text_table VALUES(1,\"FFFF\", 1)",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB014" // varchar
	databases[db] = SchemaTextualColumns("VARCHAR", db, 50)

	db = "DB015" // long varchar
	databases[db] = SchemaTextualColumns("VARCHAR", db, 256)

	db = "DB016" // binary fix size
	databases[db] = SchemaTextualColumns("BINARY", db, 100)

	db = "DB017" // varbinary
	databases[db] = SchemaTextualColumns("VARBINARY", db, 100)

	db = "DB018" // long varbinary
	databases[db] = SchemaTextualColumns("VARBINARY", db, 256)

	db = "DB019"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `date_table` ( `id0`  date, `col0` date DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into date_table values( \"1111-11-11\", \"1111:11:11\")",
			"insert into date_table set id0= \"1111-11-12\" ",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB020"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `date_table0` ( `id0`  datetime(0), `col0` datetime(0) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into date_table0 values( \"1111-11-11 11:11:11\", \"1111-11-11 11:11:11\")",
			"insert into date_table0 set id0= \"1111-11-12 11:11:11\"",

			"CREATE TABLE `date_table3` ( `id0`  datetime(3), `col0` datetime(3) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into date_table3 values( \"1111-11-11 11:11:11.123\", \"1111-11-11 11:11:11.123\")",
			"insert into date_table3 set id0= \"1111-11-12 11:11:11.123\"",

			"CREATE TABLE `date_table6` ( `id0`  datetime(6), `col0` datetime(6) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into date_table6 values( \"1111-11-11 11:11:11.123456\", \"1111-11-11 11:11:11.123456\")",
			"insert into date_table6 set id0= \"1111-11-12 11:11:11.123456\"",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB021"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `time_table0` ( `id0`  time(0), `col0` time(0) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into time_table0 values( \"11:11:11\", \"11:11:11\")",
			"insert into time_table0 set id0= \"12:11:11\"",

			"CREATE TABLE `time_table3` ( `id0`  time(3), `col0` time(3) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into time_table3 values( \"11:11:11.123\", \"11:11:11.123\")",
			"insert into time_table3 set id0= \"12:11:11.123\"",

			"CREATE TABLE `time_table6` ( `id0` time(6), `col0` time(6) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into time_table6 values( \"11:11:11.123456\", \"11:11:11.123456\")",
			"insert into time_table6 set id0= \"12:11:11.123456\"",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB022"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `ts_table0` ( `id0`  timestamp(0), `col0` timestamp(0) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into ts_table0 values( \"2022-11-11 11:11:11\", \"2022-11-11 11:11:11\")",
			"insert into ts_table0 set id0= \"2022-11-12 11:11:11\"",

			"CREATE TABLE `ts_table3` ( `id0`  timestamp(3), `col0` timestamp(3) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into ts_table3 values( \"2022-11-11 11:11:11.123\", \"2022-11-11 11:11:11.123\")",
			"insert into ts_table3 set id0= \"2022-11-12 11:11:11.123\"",

			"CREATE TABLE `ts_table6` ( `id0`  timestamp(6), `col0` timestamp(6) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into ts_table6 values( \"2022-11-11 11:11:11.123456\", \"2022-11-11 11:11:11.123456\")",
			"insert into ts_table6 set id0= \"2022-11-12 11:11:11.123456\"",
		},
		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB023"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `year_table` ( `id0`  year, `col0` year DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into year_table values( \"2022\", \"2022\")",
			"insert into year_table set id0=\"2023\"",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB024"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `bit_table` ( `id0`  binary(100), `col0` bit(1) DEFAULT NULL, `col1` bit(3) DEFAULT NULL, `col2` bit(25) DEFAULT NULL,`col3` bit(39) DEFAULT NULL, col4 bit(64) DEFAULT NULL, PRIMARY KEY (`id0`)) ENGINE=ndbcluster",
			"insert into bit_table values(1,  b'1',  b'111', b'1111111111111111111111111', b'111111111111111111111111111111111111111', b'1111111111111111111111111111111111111111111111111111111111111111')",
			"insert into bit_table values(2,  b'0',  b'000', b'0000000000000000000000000', b'000000000000000000000000000000000000000', b'0000000000000000000000000000000000000000000000000000000000000000')",
			"insert into bit_table set id0=\"3\"",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	GenerateHWSchema(db)
}

func GenerateHWSchema(userProjects ...string) [][]string {
	createSchema := []string{
		// setup commands
		"DROP DATABASE IF EXISTS " + HOPSWORKS_SCHEMA_NAME,
		"CREATE DATABASE " + HOPSWORKS_SCHEMA_NAME,
		"USE " + HOPSWORKS_SCHEMA_NAME,

		"CREATE TABLE `users` (" +
			"`uid` int NOT NULL AUTO_INCREMENT," +
			"`username` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`password` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`email` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL," +
			"`fname` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL," +
			"`lname` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL," +
			"`activated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`title` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT '-'," +
			"`false_login` int NOT NULL DEFAULT '-1'," +
			"`status` int NOT NULL DEFAULT '-1'," +
			"`isonline` int NOT NULL DEFAULT '-1'," +
			"`secret` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL," +
			"`validation_key` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL," +
			"`validation_key_updated` timestamp NULL DEFAULT NULL," +
			"`validation_key_type` varchar(20) COLLATE latin1_general_cs DEFAULT NULL," +
			"`mode` int NOT NULL DEFAULT '0'," +
			"`password_changed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`notes` varchar(500) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT '-'," +
			"`max_num_projects` int NOT NULL," +
			"`num_active_projects` int NOT NULL DEFAULT '0'," +
			"`num_created_projects` int NOT NULL DEFAULT '0'," +
			"`two_factor` tinyint(1) NOT NULL DEFAULT '1'," +
			"`tours_state` tinyint(1) NOT NULL DEFAULT '0'," +
			"`salt` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT ''," +
			"PRIMARY KEY (`uid`)," +
			"UNIQUE KEY `username` (`username`)," +
			"UNIQUE KEY `email` (`email`)) ENGINE=ndbcluster",

		"CREATE TABLE `project` (" +
			"`id` int NOT NULL AUTO_INCREMENT," +
			"`inode_pid` bigint NOT NULL," +
			"`inode_name` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`partition_id` bigint NOT NULL," +
			"`projectname` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`username` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`retention_period` date DEFAULT NULL," +
			"`archived` tinyint(1) DEFAULT '0'," +
			"`logs` tinyint(1) DEFAULT '0'," +
			"`deleted` tinyint(1) DEFAULT '0'," +
			"`description` varchar(2000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL," +
			"`payment_type` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT 'PREPAID'," +
			"`last_quota_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`kafka_max_num_topics` int NOT NULL DEFAULT '100'," +
			"`docker_image` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL," +
			"`python_env_id` int DEFAULT NULL," +
			"PRIMARY KEY (`id`)," +
			"UNIQUE KEY `projectname` (`projectname`)," +
			"UNIQUE KEY `inode_pid` (`inode_pid`,`inode_name`,`partition_id`)," +
			"KEY `user_idx` (`username`)," +
			// "CONSTRAINT `FK_149_289` FOREIGN KEY (`inode_pid`, `inode_name`, `partition_id`) REFERENCES `hops`.`hdfs_inodes` (`parent_id`, `name`, `partition_id`) ON DELETE CASCADE," +
			"CONSTRAINT `FK_262_290` FOREIGN KEY (`username`) REFERENCES `users` (`email`)) ENGINE=ndbcluster",

		"CREATE TABLE `project_team` (" +
			"`project_id` int NOT NULL," +
			"`team_member` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`team_role` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
			"`added` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
			"PRIMARY KEY (`project_id`,`team_member`)," +
			"KEY `team_member` (`team_member`)," +
			"CONSTRAINT `FK_262_304` FOREIGN KEY (`team_member`) REFERENCES `users` (`email`) ON DELETE CASCADE," +
			"CONSTRAINT `FK_284_303` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ) ENGINE=ndbcluster",

		"CREATE TABLE `api_key` (" +
			"`id` int NOT NULL AUTO_INCREMENT," +
			"`prefix` varchar(45) COLLATE latin1_general_cs NOT NULL," +
			"`secret` varchar(512) COLLATE latin1_general_cs NOT NULL," +
			"`salt` varchar(256) COLLATE latin1_general_cs NOT NULL," +
			"`created` timestamp NOT NULL," +
			"`modified` timestamp NOT NULL," +
			"`name` varchar(45) COLLATE latin1_general_cs NOT NULL," +
			"`user_id` int NOT NULL," +
			"`reserved` tinyint(1) DEFAULT '0'," +
			"PRIMARY KEY (`id`)," +
			"UNIQUE KEY `prefix_UNIQUE` (`prefix`)," +
			"UNIQUE KEY `index4` (`user_id`,`name`)," +
			"KEY `fk_api_key_1_idx` (`user_id`)," +
			"CONSTRAINT `fk_api_key_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`uid`) ON DELETE CASCADE) ENGINE=ndbcluster",

		"INSERT INTO `users` VALUES (999,'macho','12fa520ec8f65d3a6feacfa97a705e622e1fea95b80b521ec016e43874dfed5a','macho@hopsworks.ai','','macho','2015-05-15 10:22:36','Mr',0,2,1,'V3WBPS4G2WMQ53VA',NULL,NULL,NULL,0,'2015-04-28 15:18:42',NULL,30,2,1,0,3,'+9mTLmYSpnZROFEJEaednw8+GDH/s2J1QuRZy8okxW5myI/q8ek8Xu+ab5CyE9GzhWX6Sa4cr7KX8cAHi5IC4g==');",

		"INSERT INTO `project` VALUES (999,322,'demo0',322,'demo0','macho@hopsworks.ai'," +
			"'2022-05-30 14:17:22','2032-05-30',0,0,NULL,'A demo project for getting started with featurestore'," +
			"'NOLIMIT','2022-05-30 14:17:38',100,'demo_fs_meb10000:1653921933268-2.6.0-SNAPSHOT.1',1)",

		"INSERT INTO `project_team` VALUES (999,'macho@hopsworks.ai','Data scientist','2022-06-01 13:28:05')",

		// 1  bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub
		"INSERT INTO `api_key` VALUES (2049 , 'bkYjEz6OTZyevbqt' , '709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4' , '1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==' , '2022-06-14 10:27:03' , '2022-06-14 10:27:03' , 'myapikey1'             ,   999 ,        0 )",
	}

	for i, project := range userProjects {
		createSchema = append(createSchema, fmt.Sprintf("INSERT INTO `project` VALUES (%d,322,'%s',322,'%s','macho@hopsworks.ai',"+
			"'2022-05-30 14:17:22','2032-05-30',0,0,NULL,'A demo project for getting started with featurestore',"+
			"'NOLIMIT','2022-05-30 14:17:38',100,'demo_fs_meb10000:1653921933268-2.6.0-SNAPSHOT.1',1)", i+1, project, project))
	}

	for i := 1; i <= len(userProjects); i++ {
		createSchema = append(createSchema, fmt.Sprintf("INSERT INTO `project_team` VALUES (%d,'macho@hopsworks.ai','Data scientist','2022-06-01 13:28:05')", i))
	}

	dropSchema := []string{ // clean up commands
		"DROP DATABASE " + HOPSWORKS_SCHEMA_NAME,
	}

	commands := [][]string{createSchema, dropSchema}
	databases[HOPSWORKS_SCHEMA_NAME] = commands

	return commands
}

func SchemaTextualColumns(colType string, db string, length int) [][]string {
	if strings.EqualFold(colType, "varbinary") || strings.EqualFold(colType, "binary") ||
		strings.EqualFold(colType, "char") || strings.EqualFold(colType, "varchar") {
		return [][]string{
			{
				// setup commands
				"DROP DATABASE IF EXISTS " + db,
				"CREATE DATABASE " + db,
				"USE " + db,

				// blobs in PK is not supported by RonDB
				"CREATE TABLE table1(id0 " + colType + "(" + strconv.Itoa(length) + "), col0 " + colType + "(" + strconv.Itoa(length) + "),  PRIMARY KEY(id0)) ENGINE=ndbcluster",
				`INSERT INTO  table1 VALUES("1","这是一个测验。 我不知道怎么读中文。")`,
				`INSERT INTO  table1 VALUES("2",0x660066)`,
				`INSERT INTO  table1 VALUES("3","a\nb")`,
				`INSERT INTO  table1 VALUES("这是一个测验","12345")`,
				`INSERT INTO  table1 VALUES("4","ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïð")`, // some chars
				`INSERT INTO  table1 set id0=5`,
				`INSERT INTO  table1 VALUES("6","\"\\\b\f\n\r\t$%_?")`, // in mysql \f is replaced by f
				`INSERT INTO  table1 VALUES("\"7\"","abc")`,            // testing quoted primary key
			},
			{ // clean up commands
				"DROP DATABASE " + db,
			},
		}
	} else {
		panic("Data type not supported")
	}
}

func createBenchmarkSchema(db string, count int) []string {
	colWidth := 1000
	dummyData := ""
	for i := 0; i < colWidth; i++ {
		dummyData += "$"
	}

	schema := []string{
		// setup commands
		"DROP DATABASE IF EXISTS " + db,
		"CREATE DATABASE " + db,
		"USE " + db,
		"CREATE TABLE table_1(id0 INT, col_0 VARCHAR(" + strconv.Itoa(colWidth) + "), PRIMARY KEY(id0)) ENGINE=ndbcluster",
	}

	for i := 0; i < count; i++ {
		schema = append(schema, fmt.Sprintf("INSERT INTO table_1 VALUES(%d, \"%s\")", i, dummyData))
	}

	return schema
}

func Database(name string) [][]string {
	db, ok := databases[name]
	if !ok {
		return [][]string{}
	}
	return db
}

func CreateDatabases(t testing.TB, dbNames ...string) {
	if config.Configuration().Security.UseHopsWorksAPIKeys {
		GenerateHWSchema(dbNames...)
		dbNames = append(dbNames, HOPSWORKS_SCHEMA_NAME)
	}
	createOrDestroyDatabases(t, true, dbNames...)
}

func DropDatabases(t testing.TB, dbNames ...string) {
	if config.Configuration().Security.UseHopsWorksAPIKeys {
		GenerateHWSchema(dbNames...)
		dbNames = append(dbNames, HOPSWORKS_SCHEMA_NAME)
	}
	createOrDestroyDatabases(t, false, dbNames...)
}

func createOrDestroyDatabases(t testing.TB, create bool, dbNames ...string) {
	if len(dbNames) == 0 {
		t.Fatal("No database specified")
	}

	createAndDestroySchemata := [][][]string{}
	for _, dbName := range dbNames {
		createAndDestroySchemata = append(createAndDestroySchemata, Database(dbName))
	}

	t.Helper()
	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	// user:password@tcp(IP:Port)/
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.Configuration().MySQLServer.User,
		config.Configuration().MySQLServer.Password,
		config.Configuration().MySQLServer.IP,
		config.Configuration().MySQLServer.Port)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		t.Fatalf("failed to connect to db. %v", err)
	}
	defer dbConnection.Close()

	for _, createDestroyScheme := range createAndDestroySchemata {
		if len(createDestroyScheme) != 2 {
			t.Fatal("expecting the setup array to contain two sub arrays where the first " +
				"sub array contains commands to setup the DBs, " +
				"and the second sub array contains commands to clean up the DBs")
		}
		if create {
			runSQLQueries(t, dbConnection, createDestroyScheme[0])
		} else { // drop
			runSQLQueries(t, dbConnection, createDestroyScheme[1])
		}
	}
}

func runSQLQueries(t testing.TB, db *sql.DB, setup []string) {
	t.Helper()
	for _, command := range setup {
		_, err := db.Exec(command)
		if err != nil {
			t.Fatalf("failed to run command. %s. Error: %v", command, err)
		}
	}
}
