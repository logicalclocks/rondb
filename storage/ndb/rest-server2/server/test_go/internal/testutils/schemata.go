/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023, 2026 Hopsworks AB
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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// seedMaxAttempts and seedRetryDelay bound the per-database retries in
// CreateDatabases when the cluster is still working through schema churn.
const seedMaxAttempts = 4
const seedRetryDelay = 5 * time.Second

// isTransientSeedError reports whether a seeding failure is worth retrying:
// lock wait timeouts and deadlocks (MySQL 1205/1213 — NDB maps busy-cluster
// transaction timeouts to 1205 and the error text itself says "try
// restarting transaction"), and 3604 ("Storage engine can't drop table"),
// which a DROP DATABASE hits transiently while NDB binlog event teardown
// from earlier churn is still settling.
func isTransientSeedError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Error 1205") ||
		strings.Contains(msg, "Error 1213") ||
		strings.Contains(msg, "Error 3604")
}

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
		// Heavy NDB schema churn (this seed can run right after another
		// MTR test dropped all of these databases, and some tests re-seed
		// in a loop) keeps the data nodes busy with background drop work,
		// and a statement can then time out with 1205/1213 ("try
		// restarting transaction") or a drop can transiently fail with
		// 3604 while binlog event teardown settles. Every fixture starts
		// with DROP DATABASE IF EXISTS, so re-running one database's whole
		// schema is idempotent — retry it a few times before giving up.
		for attempt := 1; ; attempt++ {
			// Only the hopsworks database lives on the metadata cluster;
			// every other database is written to the data cluster.
			if db == testdbs.HOPSWORKS_DB_NAME {
				err = runQueriesWithConnection(createSchema, metadataDbConn)
			} else {
				err = runQueriesWithConnection(createSchema, dataDbConn)
			}
			if err == nil || attempt >= seedMaxAttempts ||
				!isTransientSeedError(err) {
				break
			}
			log.Warnf("transient error seeding db '%s' (attempt %d/%d), "+
				"retrying: %v", db, attempt, seedMaxAttempts, err)
			time.Sleep(seedRetryDelay)
		}
		if err != nil {
			cleanupDbsWrapper(dropDatabases)()
			err = errors.New(fmt.Sprintf("failed running createSchema for db '%s'; error: %v", db, err))
			return func() {}, err
		}
		log.Debugf("successfully ran all queries to instantiate db '%s'", db)
		dropDatabases += fmt.Sprintf("DROP DATABASE IF EXISTS %s;\n", db)
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

	// Pin a single underlying connection for all statements.  sql.DB is
	// a connection pool — each Exec() may pick a different connection,
	// which breaks session variables like SET FOREIGN_KEY_CHECKS.
	conn, err := dbConnection.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get dedicated connection: %w", err)
	}
	defer conn.Close()

	// Insert each database's data in one big transaction, the way Hopsworks
	// does in production (the feature view metadata graph is persisted by a
	// single cascaded JPA transaction). With statement-level autocommit a
	// server-side event watcher (e.g. the RDRS feature view metadata cache)
	// can observe a feature_view row whose features, joins and serving keys
	// are still uncommitted, and cache that partial state. DDL statements
	// implicitly commit, so in effect every run of DML between DDL
	// statements becomes one transaction; the fixture files keep all data
	// after all DDL. Do NOT replace this with periodic "commit every N
	// statements": an arbitrary commit boundary can split a feature view's
	// rows and reintroduce the partial-read window.
	_, err = conn.ExecContext(context.Background(), "SET autocommit = 0")
	if err != nil {
		return fmt.Errorf("failed to disable autocommit: %w", err)
	}

	for _, query := range splitQueries {
		query := strings.TrimSpace(query)
		log.Debugf("running query: \n%s", query)
		_, err := conn.ExecContext(context.Background(), query)
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return fmt.Errorf("failed to run SQL query '%s'; error: %v", query, err)
		}
	}
	_, err = conn.ExecContext(context.Background(), "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit seeding transaction: %w", err)
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
