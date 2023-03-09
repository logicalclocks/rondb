package testutils

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/config"
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

	dropDatabases := ""
	cleanupDbs = func() {}
	for db, createSchema := range createSchemata {
		err = runQueries(createSchema)
		if err != nil {
			cleanupDbs()
			err = fmt.Errorf("failed running createSchema for db '%s'; error: %w", db, err)
			return err, func() {}
		}
		log.Debugf("successfully ran all queries to instantiate db '%s'", db)
		cleanupDbs = func() {
			dropDatabases += fmt.Sprintf("DROP DATABASE %s;\n", db)
			err = runQueries(dropDatabases)
			if err != nil {
				log.Errorf("failed cleaning up databases; error: %v", err)
			}
		}
	}
	return
}

func runQueries(sqlQueries string) error {

	if sqlQueries == "" {
		return nil
	}
	splitQueries := strings.Split(sqlQueries, ";")
	if len(splitQueries) == 0 {
		return nil
	}
	// the last semi-colon will produce an empty last element
	splitQueries = splitQueries[:len(splitQueries)-1]

	conf := config.GetAll()
	connectionString := config.GenerateMysqldConnectString(conf)
	log.Debugf("Connecting to mysqld with '%s'", connectionString)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to db; error: %v", err)
	}
	defer dbConnection.Close()

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
