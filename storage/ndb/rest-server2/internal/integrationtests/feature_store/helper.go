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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
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
	return GetNSampleData(database, table, 2)
}

func GetNSampleData(database string, table string, n int) ([][]interface{}, []string, []string, error) {

	columnName, pks, colTypes, err := getColumnInfo(database, table)
	if err != nil {
		return nil, nil, nil, err
	}
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", database, table, n)
	var valueBatch, err1 = fetchDataRows(query, colTypes)
	if err1 != nil {
		return nil, nil, nil, err1
	}
	return *valueBatch, pks, columnName, nil
}

func GetNSampleDataColumns(database string, table string, n int, cols []string) ([][]interface{}, []string, []string, error) {

	columnName, pks, colTypes, err := getColumnInfo(database, table)
	if err != nil {
		return nil, nil, nil, err
	}
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT %d", strings.Join(cols, ", "), database, table, n)
	var valueBatch, err1 = fetchDataRows(query, colTypes)
	if err1 != nil {
		return nil, nil, nil, err1
	}
	return *valueBatch, pks, columnName, nil
}

func fetchDataRows(query string, colTypes []string) (*[][]interface{}, error) {
	dbConn, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		return nil, fmt.Errorf("Cannot create MYSQLConnection" + err.Error())
	}
	defer dbConn.Close()

	return fetchRowsInt(query, colTypes, dbConn)
}

func fetchMetadataRows(query string, colTypes []string) (*[][]interface{}, error) {
	dbConn, err := testutils.CreateMySQLConnectionMetadataCluster()
	if err != nil {
		return nil, fmt.Errorf("Cannot create MYSQLConnection" + err.Error())
	}
	defer dbConn.Close()

	return fetchRowsInt(query, colTypes, dbConn)
}

func fetchRowsInt(query string, colTypes []string, dbConn *sql.DB) (*[][]interface{}, error) {
	rows, err := dbConn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var nCol = len(columns)

	valueBatch := make([][]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, nCol)
		for i := range values {
			values[i] = new(sql.RawBytes)
		}

		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		rawValue := make([]interface{}, nCol)
		for i := range values {
			rawBytes := values[i].(*sql.RawBytes)
			if isColNumerical(colTypes[i]) {
				rawValue[i] = []byte(*rawBytes)
			} else {
				rawValue[i] = []byte("\"" + string(*rawBytes) + "\"")
			}
		}
		valueBatch = append(valueBatch, rawValue)
	}
	if len(valueBatch) == 0 {
		return nil, fmt.Errorf("no sample data is fetched")
	}
	return &valueBatch, nil
}

func GetSampleDataWithJoin(database string, table string, rightDatabase string, rightTable string, rightPrefix string) ([][]interface{}, []string, []string, error) {
	return GetNSampleDataWithJoin(2, database, table, rightDatabase, rightTable, rightPrefix)
}

func GetNSampleDataWithJoin(n int, database string, table string, rightDatabase string, rightTable string, rightPrefix string) ([][]interface{}, []string, []string, error) {
	return GetNSampleDataWithJoinAndKey(n, database, table, rightDatabase, rightTable, rightPrefix, make(map[string]string), []string{}, []string{})
}

func GetNSampleDataWithJoinAndKey(n int, database string, table string, rightDatabase string,
	rightTable string, rightPrefix string, joinKey map[string]string,
	leftTargetCols []string, rightTargetCols []string) ([][]interface{}, []string, []string, error) {

	fg1Cols, fg1Pks, fg1ColTypes, err := getColumnInfo(database, table)
	if err != nil {
		return nil, nil, nil, err
	}
	fg2Cols, fg2Pks, fg2ColTypes, err := getColumnInfo(rightDatabase, rightTable)
	if err != nil {
		return nil, nil, nil, err
	}

	var onClause []string
	if len(joinKey) == 0 {
		for _, pk := range fg1Pks {
			onClause = append(onClause, fmt.Sprintf("fg0.%s = fg1.%s", pk, pk))
		}
	} else {
		for k, v := range joinKey {
			onClause = append(onClause, fmt.Sprintf("fg0.%s = fg1.%s", k, v))
		}
	}

	var selectedCols string
	var pks, cols []string
	var colTypes []string
	pks = fg1Pks
	for _, pk := range fg2Pks {
		pks = append(pks, rightPrefix+pk)
	}
	if (leftTargetCols == nil || len(leftTargetCols) == 0) && (rightTargetCols == nil || len(rightTargetCols) == 0) {
		selectedCols = "fg0.*, fg1.*"
		cols = fg1Cols
		for _, col := range fg2Cols {
			cols = append(cols, rightPrefix+col)
		}
		colTypes = append(fg1ColTypes, fg2ColTypes...)
	} else {
		for i, col := range leftTargetCols {
			if i > 0 {
				selectedCols = selectedCols + ", "
			}
			selectedCols = selectedCols + "fg0." + col
			cols = append(cols, col)
			for i, c := range fg1Cols {
				if c == col {
					colTypes = append(colTypes, fg1ColTypes[i])
				}
			}
		}
		if selectedCols != "" {
			selectedCols = selectedCols + ", "
		}
		for i, col := range rightTargetCols {
			if i > 0 {
				selectedCols = selectedCols + ", "
			}
			selectedCols = selectedCols + "fg1." + col
			cols = append(cols, rightPrefix+col)
			for i, c := range fg2Cols {
				if c == col {
					colTypes = append(colTypes, fg2ColTypes[i])
				}
			}
		}
	}

	query := fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` fg0 INNER JOIN `%s`.`%s` fg1 ON (%s) LIMIT %d",
		selectedCols, database, table, rightDatabase, rightTable, strings.Join(onClause, " AND "), n)

	var valueBatch, err1 = fetchDataRows(query, colTypes)
	if err1 != nil {
		return nil, nil, nil, err1
	}
	return *valueBatch, pks, cols, nil
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

func getColumnInfo(dbName string, tableName string) ([]string, []string, []string, error) {
	dbConn, _ := testutils.CreateMySQLConnectionDataCluster()
	defer dbConn.Close()
	var colTypes []string
	var columns []string
	var pks []string

	query := fmt.Sprintf("SELECT DATA_TYPE, COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION", dbName, tableName)
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
	if log.IsDebug() {
		var colDebug []string
		for i := range columns {
			colDebug = append(colDebug, fmt.Sprintf("%s (type: %s)", columns[i], colTypes[i]))
		}
		log.Debugf("Read columns: %s", strings.Join(colDebug, ", "))

	}
	return columns, pks, colTypes, nil
}

func GetFeatureStoreResponse(t *testing.T, req *api.FeatureStoreRequest) *api.FeatureStoreResponse {
	return GetFeatureStoreResponseWithDetail(t, req, "", http.StatusOK)
}

func GetFeatureStoreResponseWithDetail(t *testing.T, req *api.FeatureStoreRequest, message string, status int) *api.FeatureStoreResponse {
	reqBody := req.String()
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

func GetPkValues(row *[]interface{}, pks *[]string, cols *[]string) *[]interface{} {
	pkSet := make(map[string]bool)

	for _, pk := range *pks {
		pkSet[pk] = true
	}

	var pkValue = make([]interface{}, 0)
	for i, col := range *cols {
		if _, ok := pkSet[col]; ok {
			pkValue = append(pkValue, (*row)[i])
		}
	}
	return &pkValue
}

func GetPkValuesExclude(row *[]interface{}, pks *[]string, cols *[]string, exclude []string) (*[]string, *[]interface{}) {
	pkSet := make(map[string]bool)
	exSet := make(map[string]bool)

	for _, pk := range *pks {
		pkSet[pk] = true
	}

	for _, ex := range exclude {
		exSet[ex] = true
	}

	var pkValue = make([]interface{}, 0)
	var pksFiltered = make([]string, 0)
	for i, col := range *cols {
		_, ok := pkSet[col]
		_, ex := exSet[col]
		if ok && !ex {
			pkValue = append(pkValue, (*row)[i])
			pksFiltered = append(pksFiltered, col)
		}
	}
	return &pksFiltered, &pkValue
}

func ValidateResponseWithData(t *testing.T, data *[]interface{}, cols *[]string, resp *api.FeatureStoreResponse) {
	var exCols = make(map[string]bool)
	ValidateResponseWithDataExcludeCols(t, data, cols, &exCols, resp)
}

func ValidateResponseWithDataExcludeCols(t *testing.T, data *[]interface{}, cols *[]string, exCols *map[string]bool, resp *api.FeatureStoreResponse) {
	var status = api.FEATURE_STATUS_COMPLETE
	if len(*data) == 0 {
		status = api.FEATURE_STATUS_ERROR
	}
	var i = -1
	for k, _data := range *data {
		if (*exCols)[(*cols)[k]] {
			continue
		}
		i++
		gotRaw := ((*resp).Features)[i]
		if gotRaw == nil && _data != nil {
			t.Errorf("Got nil but expect %s \n", (_data).([]byte))
		} else if gotRaw != nil && _data == nil {
			t.Errorf("Got %s but expect nil \n", gotRaw.([]byte))
		} else if gotRaw == nil && _data == nil {
			status = api.FEATURE_STATUS_MISSING
			continue
		}
		var got interface{}
		if reflect.TypeOf(gotRaw) == reflect.TypeOf([]byte{}) {
			err := json.Unmarshal(gotRaw.([]byte), &got)
			if err != nil {
				t.Errorf("cannot unmarshal %s, got error: %s", gotRaw, err)
			}
		} else {
			got = gotRaw
		}
		var expectedJson interface{}
		switch _data.(type) {
		case []byte:
			expected := (_data).([]byte)
			err := json.Unmarshal(expected, &expectedJson)
			if err != nil {
				t.Errorf("Cannot unmarshal %s, got error: %s\n", expected, err)
			}
		case interface{}:
			// Need to Marshal it first so that the data type of the interface{} is converted to json data type
			dataJson, err := json.Marshal(_data)
			if err != nil {
				t.Errorf("Cannot marshal %v, got error: %s\n", dataJson, err)
			}
			err = json.Unmarshal(dataJson, &expectedJson)
			if err != nil {
				t.Errorf("Cannot unmarshal %s, got error: %s\n", dataJson, err)
			}
		default:
			t.Fatal("Wrong data type.")
		}
		// Decode binary data got from feature vector
		if strings.Contains((*cols)[i], "binary") {
			var binary []byte
			err := json.Unmarshal([]byte(fmt.Sprintf(`"%s"`, got.(string))), &binary)
			if err != nil {
				t.Errorf("Cannot unmarshal %s, got error: %s\n", []byte(fmt.Sprintf(`"%s"`, got.(string))), err)
			}
			got = string(binary)
		}
		if !reflect.DeepEqual(got, expectedJson) {
			t.Errorf("col: %s; Got %s (%s) but expect %s (%s)\n", (*cols)[k], got, reflect.TypeOf(got), expectedJson, reflect.TypeOf(expectedJson))
			break
		}
	}
	if resp.Status != status {
		t.Errorf("Got status %s but expect %s", resp.Status, status)
	}
}

func ValidateResponseMetadata(t *testing.T, metadata *[]*api.FeatureMetadata, metadataRequest *api.MetadataRequest, fsName, fvName string, fvVersion int) {
	var exCol = make(map[string]bool)
	ValidateResponseMetadataExCol(t, metadata, metadataRequest, &exCol, fsName, fvName, fvVersion)
}

func ValidateResponseMetadataExCol(t *testing.T, metadata *[]*api.FeatureMetadata, metadataRequest *api.MetadataRequest, exCol *map[string]bool, fsName, fvName string, fvVersion int) {
	var rows, err = fetchMetadataRows(fmt.Sprintf(`SELECT id from hopsworks.feature_store where name = "%s"`, fsName), []string{"bigint"})
	if err != nil {
		t.Errorf("Fetch rows failed with error: %s\n", err)
	}
	var fsIdStr = string((*rows)[0][0].([]byte))
	var fsId, strErr = strconv.Atoi(fsIdStr)
	if strErr != nil {
		t.Errorf("Cannot convert %s to integer with error: %s\n", fsIdStr, err)
	}
	rows, err = fetchMetadataRows(fmt.Sprintf(`SELECT id from hopsworks.feature_view where feature_store_id = %d and name = "%s" and version = %d`, fsId, fvName, fvVersion), []string{"bigint"})
	if err != nil {
		t.Errorf("Fetch rows failed with error: %s\n", err)
	}
	var fvIdStr = string((*rows)[0][0].([]byte))
	var fvId, strErr1 = strconv.Atoi(fvIdStr)
	if strErr1 != nil {
		t.Errorf("Cannot convert %s to integer with error: %s\n", fvIdStr, err)
	}

	rows, err = fetchMetadataRows(fmt.Sprintf(`SELECT tdf.name, tdf.type, tdj.prefix from hopsworks.training_dataset_feature tdf inner join hopsworks.training_dataset_join tdj on tdf.td_join = tdj.id where tdf.feature_view_id = %d order by tdf.idx`, fvId), []string{"varchar", "varchar", "varchar"})
	if err != nil {
		t.Errorf("Fetch rows failed with error: %s\n", err)
	}
	var expected = make([]api.FeatureMetadata, 0)
	for _, row := range *rows {
		var meta = api.FeatureMetadata{}
		var prefix = strings.Replace(string(row[2].([]byte)), `"`, "", -1)
		var name = prefix + strings.Replace(string(row[0].([]byte)), `"`, "", -1)
		if (*exCol)[name] {
			continue
		}
		if metadataRequest.FeatureName {
			meta.Name = &name
		}
		if metadataRequest.FeatureType {
			var typ = strings.Replace(string(row[1].([]byte)), `"`, "", -1)
			meta.Type = &typ
		}

		expected = append(expected, meta)
	}
	for i := range *metadata {
		var got = *(*metadata)[i]
		var expect = expected[i]
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Got: %s, Expected: %s", got.String(), expect.String())
			break
		}
		log.Debugf("Validated metadata. %s", got.String())
	}
}

func removeQuotes(input string) string {
	// Check if the string starts and ends with double quotes
	if len(input) >= 2 && input[0] == '"' && input[len(input)-1] == '"' {
		// Remove the first and last character (double quotes)
		return input[1 : len(input)-1]
	}
	return input // Return unchanged if not quoted
}

func ConvertBinaryToJsonMessage(data interface{}) (*json.RawMessage, error) {
	// string to base64string
	log.Debug(string(data.([]byte)))
	base64Str := base64.StdEncoding.EncodeToString([]byte(removeQuotes(string(data.([]byte)))))
	log.Debug(base64Str)
	out := json.RawMessage([]byte(fmt.Sprintf(`"%s"`, base64Str)))
	return &out, nil
}
