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
		return nil, nil, nil,err
	}
	defer dbConn.Close()

	nResult := 2

	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", database, table, nResult)
	rows, err := dbConn.Query(query)
	if err != nil {
		return nil, nil, nil,err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil,err
	}

	valueBatch := make([][]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(sql.RawBytes)
		}

		err := rows.Scan(values...)
		if err != nil {
			return nil, nil, nil,err
		}
		rawValue := make([]interface{}, len(columns))
		for i := range values {
			rawBytes := values[i].(*sql.RawBytes)
			if isRawBytesNumerical(rawBytes) {
				rawValue[i] = []byte(*rawBytes)
			} else {
				rawValue[i] = []byte("\"" + string(*rawBytes) + "\"")
			}
			valueBatch = append(valueBatch, rawValue)
		}
	}
	columnName, pks, err := getColumnName(database, table)
	if err != nil {
		return nil, nil, nil,err
	}
	return valueBatch, pks, columnName, nil
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

func getColumnName(dbName string, tableName string) ([]string, []string, error){
	dbConn, err := testutils.CreateMySQLConnection()

	var columns []string
	var pks []string

	query := fmt.Sprintf("SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbName, tableName)
	rows, err := dbConn.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, columnKey string
		err := rows.Scan(&columnName, &columnKey)
		if err != nil {
			return nil, nil, err
		}
		if columnKey == "PRI" {
			pks = append(pks, columnName)
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return columns, pks, nil
}

func GetFeatureStoreResponse(t *testing.T, req *api.FeatureStoreRequest) *api.FeatureStoreResponse {
	reqBody := fmt.Sprintf("%s", req)
	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(), reqBody, "", http.StatusOK)
	fsResp := api.FeatureStoreResponse{}
	err := json.Unmarshal([]byte(respBody), &fsResp)
	if err != nil {
		t.Fatalf("Unmarshal failed %s ", err)
	}
	return &fsResp
}

func getPkValues(row *[]interface{}, pks *[]string, cols *[]string) *[]interface{} {
	pkSet := make(map[string]bool)
	
	for _, pk := range *pks {
		pkSet[pk] = true
	}

	var pkValue = make([]interface{}, 0)
	for i, col := range *cols {
		if _, ok := pkSet[col]; ok {
			fmt.Printf("Primary key type sent to API: %s, value %s\n", reflect.TypeOf((*row)[i]), (*row)[i])
			pkValue = append(pkValue, (*row)[i])
		}
	}
	return &pkValue
}

func ValidateResponseWithData(t *testing.T, data *[]interface{}, cols *[]string, resp *api.FeatureStoreResponse) {
	log.Debugf("Response data is %s", resp.String())
	colToIndex := make(map[string]int)
	for i, col := range *cols {
		colToIndex[col] = i
 	}
	for i, metadata := range (*resp).Metadata {
		got := ((*resp).Features)[i]
		if metadata.Name == "ts" {
			continue
		}
		expected := ((*data)[colToIndex[metadata.Name]]).([]byte)
		var expectedJson interface{}
		err := json.Unmarshal(expected, &expectedJson)
		if err != nil {
			t.Errorf("Cannot unmarshal %s, got error: %s\n", expected, err)
		}
		if !reflect.DeepEqual(got, expectedJson) {
			t.Errorf("Got %s (%s) but expect %s (%s)\n", got, reflect.TypeOf(got), expectedJson, reflect.TypeOf(expectedJson))
			break
		}
	}
}

func checkJsonType(value *json.RawMessage) {
	var v interface{}
	json.Unmarshal(*value, &v)
	fmt.Printf("Primary key type: %s, value %s\n", reflect.TypeOf(v), value)
}
