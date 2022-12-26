package testutils

import (
	"database/sql"
	"fmt"
	"testing"

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
)

func CreateDatabases(t testing.TB, dbNames ...string) {
	t.Helper()
	if config.Configuration().Security.UseHopsWorksAPIKeys {
		common.GenerateHopsworksSchema(dbNames...)
		dbNames = append(dbNames, common.HOPSWORKS_SCHEMA_NAME)
	}
	createOrDestroyDatabases(t, true, dbNames...)
}

func DropDatabases(t testing.TB, dbNames ...string) {
	t.Helper()
	if config.Configuration().Security.UseHopsWorksAPIKeys {
		common.GenerateHopsworksSchema(dbNames...)
		dbNames = append(dbNames, common.HOPSWORKS_SCHEMA_NAME)
	}
	createOrDestroyDatabases(t, false, dbNames...)
}

func createOrDestroyDatabases(t testing.TB, create bool, dbNames ...string) {
	t.Helper()
	if !*WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	if len(dbNames) == 0 {
		t.Fatal("No database specified")
	}

	createAndDestroySchemata := [][][]string{}
	for _, dbName := range dbNames {
		createAndDestroySchemata = append(createAndDestroySchemata, common.GetCreateAndDestroySchemata(dbName))
	}

	// user:password@tcp(IP:Port)/
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.Configuration().MySQLServer.User,
		config.Configuration().MySQLServer.Password,
		config.Configuration().MySQLServer.IP,
		config.Configuration().MySQLServer.Port)
	t.Logf("Connecting to mysqld with '%s'", connectionString)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		t.Fatalf("failed to connect to db; error: %v", err)
	}
	defer dbConnection.Close()

	for _, createDestroyScheme := range createAndDestroySchemata {
		if len(createDestroyScheme) != 2 {
			t.Fatalf("expecting the setup array to contain two sub arrays where the first " +
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
			t.Fatalf("failed to run command '%s'; error: %v", command, err)
		}
	}
}
