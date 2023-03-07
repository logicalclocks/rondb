package testutils

import (
	"database/sql"
	"flag"
	"fmt"

	"hopsworks.ai/rdrs/internal/config"
)

const HOPSWORKS_TEST_API_KEY = "bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"

var WithRonDB = flag.Bool("with-rondb", true, "test with a running RonDB instance")

func CreateMySQLConnection() (*sql.DB, error) {
	conf := config.GetAll()
	connectionString := config.GenerateMysqldConnectString(conf)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		err = fmt.Errorf("failed to connect to db; error: %w", err)
	}
	return dbConnection, err
}
