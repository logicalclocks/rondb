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

package testutils

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

func CreateDatabases(
	registerAsHopsworksProjects bool,
	dbNames ...string,
) (cleanupDbs func(), err error) {

	createSchemata, err := testdbs.GetCreationSchemaPerDB(registerAsHopsworksProjects, dbNames...)
	if err != nil {
		return cleanupDbs, err
	}
	cleanupDbs = func() {}

	dataDbConn, err := CreateMySQLConnectionDataCluster()
	if err != nil {
		return
	}
	defer dataDbConn.Close()

	metadataDbConn, err := CreateMySQLConnectionMetadataCluster()
	if err != nil {
		return
	}
	defer metadataDbConn.Close()

	dropDatabases := ""
	cleanupDbsWrapper := func(dropDatabases string) func() {
		return func() {
			// We need a new DB connection since this might be called after the
			// initial connection is closed.

			// delete from both clusters
			err = RunQueriesOnDataCluster(dropDatabases)
			if err != nil {
				log.Errorf("failed cleaning up databases; error: %v", err)
			}

			err = RunQueriesOnMetadataCluster(dropDatabases)
			if err != nil {
				log.Errorf("failed cleaning up databases; error: %v", err)
			}
		}
	}
	for db, createSchema := range createSchemata {
		var err error
		if db == testdbs.HOPSWORKS_DB_NAME {
			err = runQueriesWithConnection(createSchema, metadataDbConn)
		} else {
			err = runQueriesWithConnection(createSchema, dataDbConn)
		}
		if err != nil {
			cleanupDbsWrapper(dropDatabases)()
			err = errors.New(fmt.Sprintf("failed running createSchema for db '%s'; error: %v", db, err))
			return func() {}, err
		}
		log.Debugf("successfully ran all queries to instantiate db '%s'", db)
		dropDatabases += fmt.Sprintf("DROP IF NOT EXISTS DATABASE %s;\n", db)
	}
	return cleanupDbsWrapper(dropDatabases), nil
}

func RunQueriesOnDataCluster(sqlQueries string) error {
	dbConn, err := CreateMySQLConnectionDataCluster()
	if err != nil {
		return err
	}
	defer dbConn.Close()
	return runQueriesWithConnection(sqlQueries, dbConn)
}

func RunQueriesOnMetadataCluster(sqlQueries string) error {
	dbConn, err := CreateMySQLConnectionMetadataCluster()
	if err != nil {
		return err
	}
	defer dbConn.Close()
	return runQueriesWithConnection(sqlQueries, dbConn)
}

func runQueriesWithConnection(sqlQueries string, dbConnection *sql.DB) error {
	if sqlQueries == "" {
		return nil
	}

	//remove comments
	regex, err := regexp.Compile("--.*")
	if err != nil {
		return err
	}
	sqlQueries = regex.ReplaceAllString(sqlQueries, "")
	splitQueries := strings.Split(sqlQueries, ";")

	if len(splitQueries) == 0 {
		return nil
	}
	// the last semi-colon will produce an empty last element
	splitQueries = splitQueries[:len(splitQueries)-1]

	for _, query := range splitQueries {
		query := strings.TrimSpace(query)
		log.Debugf("running query: \n%s", query)
		_, err := dbConnection.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to run SQL query '%s'; error: %v", query, err)
		}
	}
	return nil
}

func SentinelDBExists() bool {
	dbConn, err := CreateMySQLConnectionDataCluster()
	if err != nil {
		return false
	}
	defer dbConn.Close()

	testQuery := "select SCHEMA_NAME from information_schema.SCHEMATA where SCHEMA_NAME=\"sentinel\""
	rows, err := dbConn.Query(testQuery)
	if err == nil && rows.Next() {
		return true
	} else {
		return false
	}
}
