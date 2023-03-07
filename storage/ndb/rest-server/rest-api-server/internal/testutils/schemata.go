package testutils

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func CreateDatabases(
	t testing.TB,
	registerAsHopsworksProjects bool,
	dbNames ...string,
) (err error, cleanupDbs func()) {
	t.Helper()

	createSchemata, err := testdbs.GetCreationSchemaPerDB(registerAsHopsworksProjects, dbNames...)
	if err != nil {
		return err, cleanupDbs
	}
	cleanupDbs = func() {}

	dbConn, err := CreateMySQLConnection()
	if err != nil {
		return
	}
	defer dbConn.Close()

	dropDatabases := ""
	for db, createSchema := range createSchemata {
		err = runQueries(t, createSchema, dbConn)
		if err != nil {
			cleanupDbs()
			return fmt.Errorf("failed running createSchema for db '%s'; error: %w", db, err), cleanupDbs
		}
		t.Logf("successfully ran all queries to instantiate db '%s'", db)
		cleanupDbs = func() {
			dropDatabases += fmt.Sprintf("DROP DATABASE %s;\n", db)
			// We need a new DB connection since this might be called after the
			// initial connection is closed.
			err = runQueriesWithConnection(t, dropDatabases)
			if err != nil {
				t.Errorf("failed cleaning up databases; error: %v", err)
			}
		}
	}
	return
}

func runQueriesWithConnection(t testing.TB, sqlQueries string) error {
	dbConn, err := CreateMySQLConnection()
	if err != nil {
		return err
	}
	defer dbConn.Close()
	return runQueries(t, sqlQueries, dbConn)
}

func runQueries(t testing.TB, sqlQueries string, dbConnection *sql.DB) error {
	t.Helper()
	if !*WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	if sqlQueries == "" {
		return nil
	}
	splitQueries := strings.Split(sqlQueries, ";")
	if len(splitQueries) == 0 {
		return nil
	}
	// the last semi-colon will produce an empty last element
	splitQueries = splitQueries[:len(splitQueries)-1]

	for _, query := range splitQueries {
		query := strings.TrimSpace(query)
		t.Logf("running query: \n%s", query)
		_, err := dbConnection.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to run SQL query '%s'; error: %v", query, err)
		}
	}
	return nil
}
