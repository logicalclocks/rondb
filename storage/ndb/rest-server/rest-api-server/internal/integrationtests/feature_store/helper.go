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

package feature_store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func CreateFeatureStoreRequest(
	fsName string,
	fvName string,
	fvVersion int,
	pk []string,
	values []interface{},
	passedFeaturesKey []string,
	passedFeaturesValue []interface{},
) *api.FeatureStoreRequest {
	var entries = make(map[string]*json.RawMessage)
	for i, key := range pk {
		val := json.RawMessage(values[i].([]byte))
		entries[key] = &val
	}
	var passedFeatures = make(map[string]*json.RawMessage)
	for i, key := range passedFeaturesKey {
		val := json.RawMessage(passedFeaturesValue[i].([]byte))
		passedFeatures[key] = &val
	}
	req := api.FeatureStoreRequest{
		FeatureStoreName:   &fsName,
		FeatureViewName:    &fvName,
		FeatureViewVersion: &fvVersion,
		Entries:            &entries,
		PassedFeatures:     &passedFeatures,
	}
	return &req
}

func GetSampleData(database string, table string) ([][]interface{}, []string, []string, error) {
	dbConn, err := testutils.CreateMySQLConnection()
	if err != nil {
		return nil, nil, nil, err
	}
	defer dbConn.Close()

	nResult := 2

	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", database, table, nResult)
	rows, err := dbConn.Query(query)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, err
	}

	valueBatch := make([][]interface{}, 0)
	columnName, pks, colTypes, err := getColumnInfo(database, table)
	if log.IsDebug() {
		var colDebug []string
		for i := range columnName {
			colDebug = append(colDebug, fmt.Sprintf("%s (type: %s)", columnName[i], colTypes[i]))
		}
		log.Debugf("Read columns: %s", strings.Join(colDebug, ", "))

	}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(sql.RawBytes)
		}

		err := rows.Scan(values...)
		if err != nil {
			return nil, nil, nil, err
		}
		rawValue := make([]interface{}, len(columns))
		for i := range values {
			rawBytes := values[i].(*sql.RawBytes)
			if isColNumerical(colTypes[i]) {
				rawValue[i] = []byte(*rawBytes)
			} else {
				rawValue[i] = []byte("\"" + string(*rawBytes) + "\"")
			}
			valueBatch = append(valueBatch, rawValue)
		}
	}
	if len(valueBatch) == 0 {
		return nil, nil, nil, fmt.Errorf("No sample data is fetched.\n")
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return valueBatch, pks, columnName, nil
}

func GetSampleDataWithJoin(database string, table string, rightDatabase string, rightTable string, rightPrefix string) ([][]interface{}, []string, []string, error) {
	fg1Rows, fg1Pks, fg1Cols, err := GetSampleData(database, table)
	if err != nil {
		return nil, nil, nil, err
	}
	fg2Rows, fg2Pks, fg2Cols, err := GetSampleData(rightDatabase, rightTable)
	if err != nil {
		return nil, nil, nil, err
	}
	var numFeatures = len(fg1Rows[0]) + len(fg2Rows[0])
	var rows = make([][]interface{}, numFeatures)
	var pks, cols []string
	for i, fg1Row := range fg1Rows {
		rows[i] = append(fg1Row, fg2Rows[i]...)
	}
	pks = fg1Pks
	for _, pk := range fg2Pks {
		pks = append(pks, rightPrefix+pk)
	}
	cols = fg1Cols
	for _, col := range fg2Cols {
		cols = append(cols, rightPrefix+col)
	}
	return rows, pks, cols, nil
}

func isColNumerical(colType string) bool {
	var numericalType = make(map[string]bool)
	numericalType["TINYINT"] = true
	numericalType["SMALLINT"] = true
	numericalType["MEDIUMINT"] = true
	numericalType["INT"] = true
	numericalType["INTEGER"] = true
	numericalType["BIGINT"] = true
	numericalType["DECIMAL"] = true
	numericalType["FLOAT"] = true
	numericalType["DOUBLE"] = true
	numericalType["REAL"] = true
	return numericalType[strings.ToUpper(colType)]
}

func isRawBytesNumerical(v *sql.RawBytes) bool {
	if _, err := strconv.ParseFloat(string(*v), 64); err == nil {
		return true
	} else if _, err := strconv.ParseInt(string(*v), 10, 64); err == nil {
		return true
	} else {
		return false
	}
}

func getColumnInfo(dbName string, tableName string) ([]string, []string, []string, error) {
	dbConn, err := testutils.CreateMySQLConnection()

	var colTypes []string
	var columns []string
	var pks []string

	query := fmt.Sprintf("SELECT DATA_TYPE, COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbName, tableName)
	rows, err := dbConn.Query(query)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnType, columnName, columnKey string
		err := rows.Scan(&columnType, &columnName, &columnKey)
		if err != nil {
			return nil, nil, nil, err
		}
		if columnKey == "PRI" {
			pks = append(pks, columnName)
		}
		columns = append(columns, columnName)
		colTypes = append(colTypes, columnType)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	return columns, pks, colTypes, nil
}

func GetFeatureStoreResponse(t *testing.T, req *api.FeatureStoreRequest) *api.FeatureStoreResponse {
	return GetFeatureStoreResponseWithDetail(t, req, "", http.StatusOK)
}

func GetFeatureStoreResponseWithDetail(t *testing.T, req *api.FeatureStoreRequest, message string, status int) *api.FeatureStoreResponse {
	reqBody := fmt.Sprintf("%s", req)
	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(), reqBody, message, status)
	if int(status/100) == 2 {
		fsResp := api.FeatureStoreResponse{}
		err := json.Unmarshal([]byte(respBody), &fsResp)
		if err != nil {
			t.Fatalf("Unmarshal failed %s ", err)
		}
		log.Debugf("Response data is %s", fsResp.String())
		return &fsResp
	} else {
		return nil
	}
}

func getPkValues(row *[]interface{}, pks *[]string, cols *[]string) *[]interface{} {
	pkSet := make(map[string]bool)

	for _, pk := range *pks {
		pkSet[pk] = true
	}

	var pkValue = make([]interface{}, 0)
	for i, col := range *cols {
		if _, ok := pkSet[col]; ok {
			log.Debugf("Primary key type sent to API: %s, value %s\n", reflect.TypeOf((*row)[i]), (*row)[i])
			pkValue = append(pkValue, (*row)[i])
		}
	}
	return &pkValue
}

func ValidateResponseWithData(t *testing.T, data *[]interface{}, cols *[]string, resp *api.FeatureStoreResponse) {
	colToIndex := make(map[string]int)
	for i, col := range *cols {
		colToIndex[col] = i
	}
	for i, metadata := range (*resp).Metadata {
		got := ((*resp).Features)[i]
		expected := ((*data)[colToIndex[metadata.Name]]).([]byte)
		var expectedJson interface{}
		err := json.Unmarshal(expected, &expectedJson)
		if err != nil {
			t.Errorf("Cannot unmarshal %s, got error: %s\n", expected, err)
		}
		// Decode binary data got from feature vector
		if metadata.Type == "binary" {
			var binary []byte
			err := json.Unmarshal([]byte(fmt.Sprintf(`"%s"`, got.(string))), &binary)
			if err != nil {
				t.Errorf("Cannot unmarshal %s, got error: %s\n", []byte(fmt.Sprintf(`"%s"`, got.(string))), err)
			}
			got = string(binary)
		}
		if !reflect.DeepEqual(got, expectedJson) {
			t.Errorf("Got %s (%s) but expect %s (%s)\n", got, reflect.TypeOf(got), expectedJson, reflect.TypeOf(expectedJson))
			break
		}
	}
}
