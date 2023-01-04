package testutils

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
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

	dropDatabases := ""
	cleanupDbs = func() {}
	for db, createSchema := range createSchemata {
		err = runQueries(t, createSchema)
		if err != nil {
			cleanupDbs()
			return fmt.Errorf("failed running createSchema for db '%s'; error: %w", db, err), cleanupDbs
		}
		cleanupDbs = func() {
			dropDatabases += fmt.Sprintf("DROP DATABASE %s;\n", db)
			err = runQueries(t, dropDatabases)
			if err != nil {
				t.Errorf("failed cleaning up databases; error: %v", err)
			}
		}
	}
	return
}

func runQueries(t testing.TB, sqlQueries string) error {
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

	conf := config.GetAll()
	connectionString := config.GenerateConnectionString(conf)
	t.Logf("Connecting to mysqld with '%s'", connectionString)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to db; error: %v", err)
	}
	defer dbConnection.Close()

	for _, query := range splitQueries {
		_, err := dbConnection.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to run SQL query '%s'; error: %v", query, err)
		}
	}
	return nil
}
