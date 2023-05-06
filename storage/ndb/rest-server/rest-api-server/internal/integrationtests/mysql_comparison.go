package integrationtests

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

/*
	Query the same data via the MySQL server and check whether the REST API server has
	returned the correct response.
*/

func CompareDataWithDB(
	t testing.TB,
	db string,
	table string,
	filters *[]api.Filter,
	colName *string,
	colDataFromRestServer *string, // REST and MySQL
	isBinaryData bool,
) {
	dbVal, err := getColumnDataFromDB(t, db, table, filters, *colName, isBinaryData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if (colDataFromRestServer == nil || dbVal == nil) && !(colDataFromRestServer == nil && dbVal == nil) { // if one of prts is nil
		t.Fatalf("The read value for key %s does not match.", *colName)
	}

	if !((colDataFromRestServer == nil && dbVal == nil) || (*colDataFromRestServer == *dbVal)) {
		t.Fatalf("The read value for key %s does not match. Got from our server: %s, Got from MYSQL Server: %s", *colName, *colDataFromRestServer, *dbVal)
	}
}

func getColumnDataFromDB(
	t testing.TB,
	db string,
	table string,
	filters *[]api.Filter,
	col string,
	isBinary bool,
) (*string, error) {
	dbConn, err := testutils.CreateMySQLConnection()
	if err != nil {
		t.Fatalf("failed to connect to db. %v", err)
	}
	defer func() {
		err = dbConn.Close()
		if err != nil {
			t.Log(err.Error())
		}
	}()

	command := "use " + db
	_, err = dbConn.Exec(command)
	if err != nil {
		t.Fatalf("failed to run command. %s. Error: %v", command, err)
	}

	if isBinary {
		command = fmt.Sprintf("select replace(replace(to_base64(%s), '\\r',''), '\\n', '') from %s where ", col, table)
	} else {
		command = fmt.Sprintf("select %s from %s where ", col, table)
	}
	where := ""
	for _, filter := range *filters {
		if where != "" {
			where += " and "
		}
		if isBinary {
			where = fmt.Sprintf("%s %s = from_base64(%s)", where, *filter.Column, string(*filter.Value))
		} else {
			where = fmt.Sprintf("%s %s = %s", where, *filter.Column, string(*filter.Value))
		}
	}

	command = fmt.Sprintf(" %s %s\n ", command, where)
	rows, err := dbConn.Query(command)
	defer func() {
		err = rows.Close()
		if err != nil {
			t.Log(err.Error())
		}
	}()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, 1)
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// get rawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		var value *string
		for _, col := range values {

			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = nil
			} else {
				v := string(col)
				value = &v
			}
			return value, nil
		}
	}

	return nil, fmt.Errorf("did not find data in the database %s", command)
}
