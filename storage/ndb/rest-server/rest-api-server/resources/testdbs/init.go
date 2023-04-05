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

import (
	"fmt"
	"strconv"
	"strings"
)

/*
	This package supplies a handful of different SQL schemata, which
	can be used to test the RDRS. Apart from the init() function, all
	the variables in this package can essentially be seen as constants.
*/

const BENCH_DB_COLUMN_LENGTH = 1000
const BENCH_DB_NUM_ROWS = 1000

// Mapping database names to their schemes
var databaseCreateSchemes = map[string]string{
	// TODO: Fix the ordering of 11, 13 and 19
	DB000: DB000Scheme,
	DB001: DB001Scheme,
	DB002: DB002Scheme,
	DB003: DB003Scheme,
	DB004: DB004Scheme,
	DB005: DB005Scheme,
	DB006: DB006Scheme,
	DB007: DB007Scheme,
	DB008: DB008Scheme,
	DB009: DB009Scheme,
	DB010: DB010Scheme,
	DB011: DB011Scheme,

	DB013: DB013Scheme,

	DB019:      DB019Scheme,
	DB020:      DB020Scheme,
	DB021:      DB021Scheme,
	DB022:      DB022Scheme,
	DB023:      DB023Scheme,
	DB024:      DB024Scheme,
	DB025:      DB025Scheme,
	SentinelDB: SentinelDBScheme,
}

/*
Here we are adding dynamic schema to databaseCreateSchemes;
*/
func init() {
	if true {

		benchSchema := createBenchmarkSchema()
		databaseCreateSchemes[Benchmark] = benchSchema

		// char
		DB012Scheme := createTextualColumnsSchema(DB012, "char", 100)
		databaseCreateSchemes[DB012] = DB012Scheme

		// varchar
		DB014Scheme := createTextualColumnsSchema(DB014, "VARCHAR", 50)
		databaseCreateSchemes[DB014] = DB014Scheme

		// long varchar
		DB015Scheme := createTextualColumnsSchema(DB015, "VARCHAR", 256)
		databaseCreateSchemes[DB015] = DB015Scheme

		// binary fix size
		DB016Scheme := createTextualColumnsSchema(DB016, "BINARY", 100)
		databaseCreateSchemes[DB016] = DB016Scheme

		// varbinary
		DB017Scheme := createTextualColumnsSchema(DB017, "VARBINARY", 100)
		databaseCreateSchemes[DB017] = DB017Scheme

		// long varbinary
		DB018Scheme := createTextualColumnsSchema(DB018, "VARBINARY", 256)
		databaseCreateSchemes[DB018] = DB018Scheme
	}
}

func createBenchmarkSchema() string {
	// Create custom benchmark scheme
	benchScheme := strings.ReplaceAll(BenchmarkScheme, benchmarkSed_COLUMN_LENGTH, strconv.Itoa(BENCH_DB_COLUMN_LENGTH))

	// Create an amount of INSERT statements for the benchmark scheme
	benchAddRows := ""
	col2DummyData := strings.Repeat("$", BENCH_DB_COLUMN_LENGTH)
	for rowId := 0; rowId < BENCH_DB_NUM_ROWS; rowId++ {
		addNewRow := strings.ReplaceAll(BenchmarkAddRow, BenchmarkAddRowSed_VALUE_COLUMN_1, strconv.Itoa(rowId))
		addNewRow = strings.ReplaceAll(addNewRow, BenchmarkAddRowSed_VALUE_COLUMN_2, col2DummyData)
		benchAddRows += addNewRow
	}
	benchScheme += benchAddRows
	return benchScheme
}

func createTextualColumnsSchema(dbName string, columnType string, columnLength int) string {
	if !(strings.EqualFold(columnType, "varbinary") ||
		strings.EqualFold(columnType, "binary") ||
		strings.EqualFold(columnType, "char") ||
		strings.EqualFold(columnType, "varchar")) {
		panic("Data type not supported")
	}

	/*
		Replacing the following "magic values" in the embedded sql statements
		DATABASE_NAME --> db
		COLUMN_TYPE --> colType
		COLUMN_LENGTH --> length
	*/

	textColsScheme := strings.ReplaceAll(TextualColumns, textualColumns_DATABASE_NAME, dbName)
	textColsScheme = strings.ReplaceAll(textColsScheme, textualColumns_COLUMN_TYPE, columnType)
	textColsScheme = strings.ReplaceAll(textColsScheme, textualColumns_COLUMN_LENGTH, strconv.Itoa(columnLength))

	return textColsScheme
}

/*
Simply export databaseCreateSchemes as slice
*/
func GetAllDBs() []string {
	allDBs := []string{}
	for db := range databaseCreateSchemes {
		allDBs = append(allDBs, db)
	}
	return allDBs
}

/*
This function can be used to export the embedded schemata of different databases. In case
it is wished to use the databases in conjunction with Hopsworks as an authentication layer,
this function also supplies the required Hopsworks schemata.
*/
func GetCreationSchemaPerDB(registerAsHopsworksProjects bool, dbs ...string) (map[string]string, error) {
	createSchemata := make(map[string]string)
	for _, db := range dbs {
		schema, ok := databaseCreateSchemes[db]
		if !ok {
			return nil, fmt.Errorf("database %s does not exist in registered schemas", schema)
		}
		createSchemata[db] = schema
	}
	if registerAsHopsworksProjects {
		createSchemata[HOPSWORKS_DB_NAME] = createHopsworksSchema(dbs...)
	}
	return createSchemata, nil
}

/*
If we require a Hopsworks API key, the databases the client wants to access need to be
registered as projects in the Hopsworks database. This function creates the standard Hopsworks
scheme and inserts the databases as projects.
*/
func createHopsworksSchema(dbsToRegister ...string) string {
	hopsworksScheme := HopsworksScheme
	for idx, projectName := range dbsToRegister {
		/*
			Replacing the following "magic values" in the embedded sql statements
			PROJECT_NAME  --> project
			PROJECT_NUMBER --> idx+1
		*/
		addNewProject := strings.ReplaceAll(HopsworksAddProject, hopsworksAddProject_PROJECT_NAME, projectName)
		addNewProject = strings.ReplaceAll(addNewProject, hopsworksAddProject_PROJECT_NUMBER, strconv.Itoa(idx+1))
		hopsworksScheme += addNewProject
	}

	// register additional API Keys
	for i := 0; i < HopsworksAPIKey_ADDITIONAL_KEYS; i++ {
		addAPIKey := strings.ReplaceAll(HopsworksAPIKey, HopsworksAPIKey_KEY_ID, fmt.Sprintf("%d", i+2 /* id 1 is already taken*/))
		addAPIKey = strings.ReplaceAll(addAPIKey, HopsworksAPIKey_KEY_PREFIX, fmt.Sprintf("%016d", i))
		addAPIKey = strings.ReplaceAll(addAPIKey, HopsworksAPIKey_KEY_NAME, fmt.Sprintf("name%d", i))
		hopsworksScheme += addAPIKey
	}

	return hopsworksScheme
}
