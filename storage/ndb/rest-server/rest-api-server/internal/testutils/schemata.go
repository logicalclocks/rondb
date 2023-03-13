package testutils

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func CreateDatabases(
	registerAsHopsworksProjects bool,
	dbNames ...string,
) (err error, cleanupDbs func()) {

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
	cleanupDbsWrapper := func(dropDatabases string) func() {
		return func() {
			// We need a new DB connection since this might be called after the
			// initial connection is closed.
			err = runQueriesWithConnection(dropDatabases)
			if err != nil {
				log.Errorf("failed cleaning up databases; error: %v", err)
			}
		}
	}
	for db, createSchema := range createSchemata {
		err = runQueries(createSchema, dbConn)
		if err != nil {
			cleanupDbsWrapper(dropDatabases)()
			err = fmt.Errorf("failed running createSchema for db '%s'; error: %w", db, err)
			return err, func() {}
		}
		log.Debugf("successfully ran all queries to instantiate db '%s'", db)
		dropDatabases += fmt.Sprintf("DROP DATABASE %s;\n", db)
	}
	return nil, cleanupDbsWrapper(dropDatabases)
}

func runQueriesWithConnection(sqlQueries string) error {
	dbConn, err := CreateMySQLConnection()
	if err != nil {
		return err
	}
	defer dbConn.Close()
	return runQueries(sqlQueries, dbConn)
}

func runQueries(sqlQueries string, dbConnection *sql.DB) error {
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
		log.Debugf("running query: \n%s", query)
		_, err := dbConnection.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to run SQL query '%s'; error: %v", query, err)
		}
	}
	return nil
}
