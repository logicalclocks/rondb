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
package integrationtests

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func SendHttpRequest(
	t testing.TB,
	httpVerb string,
	url string,
	body string,
	expectedErrMsg string,
	expectedStatus ...int,
) (int, string) {
	t.Helper()

	client := testutils.SetupHttpClient(t)
	var req *http.Request
	var resp *http.Response
	var err error
	switch httpVerb {
	case http.MethodPost:
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

	case http.MethodGet:
		req, err = http.NewRequest(http.MethodGet, url, nil)

	default:
		t.Fatalf("HTTP verb '%s' is not implemented", httpVerb)
	}

	if err != nil {
		t.Fatalf("failed to create request; error: %v", err)
	}

	conf := config.GetAll()
	if conf.Security.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("failed to perform HTTP request towards url: '%s'\nrequest body: '%s'\nerror: %v", url, body, err)
	}
	defer resp.Body.Close()

	respCode := resp.StatusCode
	respBodyBtyes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read HTTP response body for url: '%s'\nrequest body: '%s'\nresponse code: %d\nerror: %v", url, body, respCode, err)
	}
	respBody := string(respBodyBtyes)

	idx := -1
	for i, c := range expectedStatus {
		if c == respCode {
			idx = i
		}
	}
	if idx == -1 {
		t.Fatalf("received unexpected status '%d'\nexpected status: '%v'\nurl: '%s'\nbody: '%s'\nresponse body: %v ", respCode, expectedStatus, url, body, respBody)
	}

	if respCode != http.StatusOK && !strings.Contains(respBody, expectedErrMsg) {
		t.Fatalf("response error body does not contain '%s'; received response body: '%s'", expectedErrMsg, respBody)
	}

	return respCode, respBody
}

func ValidateResHttp(t testing.TB, testInfo api.PKTestInfo, resp string, isBinaryData bool) {
	t.Helper()
	for i := 0; i < len(testInfo.RespKVs); i++ {
		key := string(testInfo.RespKVs[i].(string))

		var pkResponse api.PKReadResponseJSON
		err := json.Unmarshal([]byte(resp), &pkResponse)
		if err != nil {
			t.Fatalf("Failed to unmarshal response object %v", err)
		}

		jsonVal, found := getColumnDataFromJson(t, key, &pkResponse)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		compareDataWithDB(t, testInfo.Db, testInfo.Table, testInfo.PkReq.Filters,
			&key, jsonVal, isBinaryData)
	}
}

func ValidateResGRPC(
	t testing.TB,
	testInfo api.PKTestInfo,
	resp *api.PKReadResponseGRPC,
	isBinaryData bool,
) {
	t.Helper()
	for i := 0; i < len(testInfo.RespKVs); i++ {
		key := string(testInfo.RespKVs[i].(string))

		val, found := getColumnDataFromGRPC(t, key, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		var err error
		if val != nil {
			quotedVal := fmt.Sprintf("\"%s\"", *val) // you have to surround the string with "s
			*val, err = strconv.Unquote(quotedVal)
			if err != nil {
				t.Fatalf("Unquote failed %v\n", err)
			}
		}

		compareDataWithDB(t, testInfo.Db, testInfo.Table, testInfo.PkReq.Filters,
			&key, val, isBinaryData)
	}
}

func compareDataWithDB(t testing.TB, db string, table string, filters *[]api.Filter,
	colName *string, colDataFromRestServer *string, isBinaryData bool) {
	dbVal, err := getColumnDataFromDB(t, db, table, filters, *colName, isBinaryData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if (colDataFromRestServer == nil || dbVal == nil) && !(colDataFromRestServer == nil && dbVal == nil) { // if one of prts is nil
		t.Fatalf("The read value for key %s does not match.", *colName)
	}

	if !((colDataFromRestServer == nil && dbVal == nil) || (*colDataFromRestServer == *dbVal)) {
		t.Fatalf("The read value for key %s does not match. Got from REST Server: %s, Got from MYSQL Server: %s", *colName, *colDataFromRestServer, *dbVal)
	}
}

func getColumnDataFromGRPC(t testing.TB, colName string, pkResponse *api.PKReadResponseGRPC) (*string, bool) {
	t.Helper()
	val, ok := (*pkResponse.Data)[colName]
	if !ok {
		return nil, ok
	}
	return val, ok
}

func getColumnDataFromJson(t testing.TB, colName string, pkResponse *api.PKReadResponseJSON) (*string, bool) {
	t.Helper()

	kvMap := make(map[string]*string)
	for colName, colValue := range *pkResponse.Data {
		if colValue == nil {
			kvMap[colName] = nil
			continue
		}
		value := string([]byte(*colValue))
		if value[0] == '"' {
			var err error
			value, err = strconv.Unquote(value)
			if err != nil {
				t.Fatal(err)
			}
		}
		kvMap[colName] = &value
	}

	val, ok := kvMap[colName]
	if !ok {
		return nil, ok
	}
	return val, ok
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
	defer dbConn.Close()

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
	for i := 0; i < len(*filters); i++ {
		if where != "" {
			where += " and "
		}
		if isBinary {
			where = fmt.Sprintf("%s %s = from_base64(%s)", where, *(*filters)[i].Column, string(*(*filters)[i].Value))
		} else {
			where = fmt.Sprintf("%s %s = %s", where, *(*filters)[i].Column, string(*(*filters)[i].Value))
		}
	}

	command = fmt.Sprintf(" %s %s\n ", command, where)
	rows, err := dbConn.Query(command)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, 1)
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// get RawBytes from data
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

	return nil, fmt.Errorf("Did not find data in the database %s", command)
}

func RawBytes(a interface{}) json.RawMessage {
	var value json.RawMessage
	if a == nil {
		return []byte("null")
	}

	switch a.(type) {
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint, float32, float64:
		value = []byte(fmt.Sprintf("%v", a))
	case string:
		value = []byte(strconv.Quote(a.(string)))
	default:
		panic(fmt.Errorf("Unsupported data type. Type: %v", reflect.TypeOf(a)))
	}
	return value
}

func NewReadColumns(prefix string, numReadColumns int) *[]api.ReadColumn {
	readColumns := make([]api.ReadColumn, numReadColumns)
	for i := 0; i < numReadColumns; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		drt := api.DRT_DEFAULT
		readColumns[i].Column = &col
		readColumns[i].DataReturnType = &drt
	}
	return &readColumns
}

func NewReadColumn(col string) *[]api.ReadColumn {
	readColumns := make([]api.ReadColumn, 1)
	drt := string(api.DRT_DEFAULT)
	readColumns[0].Column = &col
	readColumns[0].DataReturnType = &drt
	return &readColumns
}

func NewOperationID(size int) *string {
	opID := RandString(size)
	return &opID
}

func NewPKReadReqBodyTBD() api.PKReadBody {
	param := api.PKReadBody{
		Filters:     NewFilters("filter_col_", 3),
		ReadColumns: NewReadColumns("read_col_", 5),
		OperationID: NewOperationID(64),
	}
	return param
}

// creates dummy filter columns of type string
func NewFilters(prefix string, numFilters int) *[]api.Filter {
	filters := make([]api.Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		v := RawBytes(val)
		filters[i] = api.Filter{Column: &col, Value: &v}
	}
	return &filters
}

func NewFilter(column *string, a interface{}) *[]api.Filter {
	filter := make([]api.Filter, 1)

	filter[0] = api.Filter{Column: column}
	v := RawBytes(a)
	filter[0].Value = &v
	return &filter
}

func NewFiltersKVs(vals ...interface{}) *[]api.Filter {
	if len(vals)%2 != 0 {
		log.Panic("Expecting key value pairs")
	}

	filters := make([]api.Filter, len(vals)/2)
	fidx := 0
	for i := 0; i < len(vals); {
		c := fmt.Sprintf("%v", vals[i])
		v := RawBytes(vals[i+1])
		filters[fidx] = api.Filter{Column: &c, Value: &v}
		fidx++
		i += 2
	}
	return &filters
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandString(n int) string {
	b := make([]rune, n)
	rand.Seed(int64(time.Now().Nanosecond()))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func PkTestMultiple(t *testing.T, tests map[string]api.PKTestInfo, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			PkTest(t, testInfo, isBinaryData, true)
		})
	}
}

func PkTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	pkRESTTest(t, testInfo, isBinaryData, validate)
	pkGRPCTest(t, testInfo, isBinaryData, validate)
}

func pkGRPCTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	respCode, resp := sendGRPCPKReadRequest(t, testInfo)
	if respCode == http.StatusOK && validate {
		ValidateResGRPC(t, testInfo, resp, isBinaryData)
	}
}

func sendGRPCPKReadRequest(
	t testing.TB,
	testInfo api.PKTestInfo,
) (int, *api.PKReadResponseGRPC) {

	client := api.NewRonDBRESTClient(GetGRPCConnction())

	// Create Request
	pkReadParams := api.PKReadParams{
		DB:          &testInfo.Db,
		Table:       &testInfo.Table,
		Filters:     testInfo.PkReq.Filters,
		OperationID: testInfo.PkReq.OperationID,
		ReadColumns: testInfo.PkReq.ReadColumns,
	}

	reqProto := api.ConvertPKReadParams(&pkReadParams)

	expectedStatus := testInfo.HttpCode
	respCode := 200
	var errStr string
	respProto, err := client.PKRead(context.Background(), reqProto)
	if err != nil {
		respCode = GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Received unexpected status; Expected: %d, Got: %d; Complete Error Message: '%s'", expectedStatus, respCode, errStr)
	}

	if respCode != http.StatusOK && !strings.Contains(errStr, testInfo.ErrMsgContains) {
		t.Fatalf("Received unexpected error message; It does not contain string: '%s'; Complete Error Message: '%s'", testInfo.ErrMsgContains, errStr)
	}

	if respCode == http.StatusOK {
		resp := api.ConvertPKReadResponseProto(respProto)
		return respCode, resp
	} else {
		return respCode, nil
	}
}

func GetStatusCodeFromError(t testing.TB, err error) int {
	status, ok := status.FromError(err)
	if !ok {
		t.Fatalf("could not find gRPC status in error: %v", err)
	}

	return common.GrpcCodeToHttpStatus(status.Code())
}

func pkRESTTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	url := testutils.NewPKReadURL(testInfo.Db, testInfo.Table)
	body, err := json.MarshalIndent(testInfo.PkReq, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}

	httpCode, res := SendHttpRequest(
		t,
		config.PK_HTTP_VERB,
		url,
		string(body),
		testInfo.ErrMsgContains,
		testInfo.HttpCode,
	)
	if httpCode == http.StatusOK && validate {
		ValidateResHttp(t, testInfo, res, isBinaryData)
	}
}

func BatchTest(t *testing.T, tests map[string]api.BatchOperationTestInfo, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			// This will mean that REST calls to Handler will be slightly faster
			BatchRESTTest(t, testInfo, isBinaryData, true)
			BatchGRPCTest(t, testInfo, isBinaryData, true)
		})
	}
}

func BatchGRPCTest(t *testing.T, testInfo api.BatchOperationTestInfo, isBinaryData bool, validateData bool) {
	httpCode, res := sendGRPCBatchRequest(t, testInfo)
	if httpCode == http.StatusOK {
		validateBatchResponseGRPC(t, testInfo, res, isBinaryData, validateData)
	}
}

var grpcConn *grpc.ClientConn
var grpcConnLock sync.Mutex

// Create only one gRPC connection.
// Tests start to fail if too many connections are opened and closed in a short time
func InitGRPCConnction() (*grpc.ClientConn, error) {
	if grpcConn != nil {
		return grpcConn, nil
	}

	grpcConnLock.Lock()
	grpcConnLock.Unlock()
	var err error
	if grpcConn == nil {
		conf := config.GetAll()
		grpcConn, err = testutils.CreateGrpcConn(conf.Security.UseHopsworksAPIKeys, conf.Security.EnableTLS)
		if err != nil {
			return nil, err
		}
	}
	return grpcConn, nil
}

func GetGRPCConnction() *grpc.ClientConn {
	return grpcConn
}

func sendGRPCBatchRequest(t *testing.T, testInfo api.BatchOperationTestInfo) (int, *api.BatchResponseGRPC) {

	gRPCClient := api.NewRonDBRESTClient(GetGRPCConnction())

	// Create request
	batchOpRequest := make([]*api.PKReadParams, len(testInfo.Operations))
	for i := 0; i < len(testInfo.Operations); i++ {
		op := testInfo.Operations[i]
		pkReadParams := &api.PKReadParams{
			DB:          &op.DB,
			Table:       &op.Table,
			Filters:     op.SubOperation.Body.Filters,
			OperationID: op.SubOperation.Body.OperationID,
			ReadColumns: op.SubOperation.Body.ReadColumns,
		}
		batchOpRequest[i] = pkReadParams
	}

	batchRequestProto := api.ConvertBatchOpRequest(batchOpRequest)

	respCode := 200
	var errStr string
	respProto, err := gRPCClient.Batch(context.Background(), batchRequestProto)
	if err != nil {
		respCode = GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	idx := -1
	for i, expCode := range testInfo.HttpCode {
		if expCode == respCode {
			idx = i
		}
	}

	if idx == -1 {
		t.Fatalf("Received unexpected status; Expected: %v, Got: %d; Complete Error Message: '%s'", testInfo.HttpCode, respCode, errStr)
	}

	if respCode != http.StatusOK && !strings.Contains(errStr, testInfo.ErrMsgContains) {
		t.Fatalf("Received unexpected error message; It does not contain string: '%s'; Complete Error Message: '%s'", testInfo.ErrMsgContains, errStr)
	}

	if respCode == http.StatusOK {
		resp := api.ConvertBatchResponseProto(respProto)
		return respCode, resp
	} else {
		return respCode, nil
	}
}

func BatchRESTTest(t *testing.T, testInfo api.BatchOperationTestInfo, isBinaryData bool, validateData bool) {
	httpCode, res := sendHttpBatchRequest(t, testInfo, isBinaryData)
	if httpCode == http.StatusOK {
		validateBatchResponseHttp(t, testInfo, res, isBinaryData, validateData)
	}
}

func sendHttpBatchRequest(t *testing.T, testInfo api.BatchOperationTestInfo, isBinaryData bool) (httpCode int, res string) {
	subOps := []api.BatchSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchOpRequest{Operations: &subOps}

	url := testutils.NewBatchReadURL()
	body, err := json.MarshalIndent(batch, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}
	httpCode, res = SendHttpRequest(t, config.BATCH_HTTP_VERB, url,
		string(body), testInfo.ErrMsgContains, testInfo.HttpCode[:]...)
	return
}

func validateBatchResponseHttp(t testing.TB, testInfo api.BatchOperationTestInfo, resp string, isBinaryData bool, validateData bool) {
	t.Helper()
	validateBatchResponseOpIdsNCodeHttp(t, testInfo, resp)
	if validateData {
		validateBatchResponseValuesHttp(t, testInfo, resp, isBinaryData)
	}
}

func validateBatchResponseGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC,
	isBinaryData bool, validateData bool) {
	t.Helper()
	validateBatchResponseOpIdsNCodeGRPC(t, testInfo, resp)
	if validateData {
		validateBatchResponseValuesGRPC(t, testInfo, resp, isBinaryData)
	}
}

func validateBatchResponseOpIdsNCodeGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC) {
	if len(*resp.Result) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i, subResp := range *resp.Result {
		checkOpIDandStatus(t, testInfo.Operations[i], subResp.Body.OperationID,
			int(*subResp.Code), subResp)
	}
}

func checkOpIDandStatus(
	t testing.TB,
	testInfo api.BatchSubOperationTestInfo,
	opIDGot *string,
	statusGot int,
	subResponse api.PKReadResponseWithCode,
) {
	expectingOpID := testInfo.SubOperation.Body.OperationID
	expectingStatus := testInfo.HttpCode

	if expectingOpID != nil {
		if *expectingOpID != *opIDGot {
			t.Fatalf("Operation ID does not match. Expecting: %s, Got: %s. TestInfo: %v",
				*expectingOpID, *opIDGot, testInfo)
		}
	}

	idx := -1
	for i, c := range expectingStatus {
		if c == statusGot {
			idx = i
		}
	}
	if idx == -1 {
		t.Fatalf("Return code does not match. Expecting: %v, Got: %d. TestInfo: %v. Body: %v.",
			expectingStatus, statusGot, testInfo, subResponse.String())
	}
}

func validateBatchResponseOpIdsNCodeHttp(t testing.TB,
	testInfo api.BatchOperationTestInfo, resp string) {
	var res api.BatchResponseJSON
	err := json.Unmarshal([]byte(resp), &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch response. Error %v", err)
	}

	if len(*res.Result) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i, subResp := range *res.Result {
		checkOpIDandStatus(t, testInfo.Operations[i], subResp.Body.OperationID,
			int(*subResp.Code), subResp)
	}
}

func validateBatchResponseValuesHttp(t testing.TB, testInfo api.BatchOperationTestInfo,
	resp string, isBinaryData bool) {
	var res api.BatchResponseJSON
	err := json.Unmarshal([]byte(resp), &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch response. Error %v", err)
	}

	for o := 0; o < len(testInfo.Operations); o++ {
		if *(*res.Result)[o].Code != http.StatusOK {
			continue // data is null if the status is not OK
		}

		operation := testInfo.Operations[o]
		pkresponse := (*res.Result)[o].Body
		for i := 0; i < len(operation.RespKVs); i++ {
			key := string(operation.RespKVs[i].(string))
			val, found := getColumnDataFromJson(t, key, pkresponse)
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}

			compareDataWithDB(t, operation.DB, operation.Table, operation.SubOperation.Body.Filters,
				&key, val, isBinaryData)
		}
	}
}

func validateBatchResponseValuesGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC, isBinaryData bool) {
	for o := 0; o < len(testInfo.Operations); o++ {
		if *(*resp.Result)[o].Code != http.StatusOK {
			continue // data is null if the status is not OK
		}

		operation := testInfo.Operations[o]
		pkresponse := (*resp.Result)[o].Body
		for i := 0; i < len(operation.RespKVs); i++ {
			key := string(operation.RespKVs[i].(string))
			val, found := getColumnDataFromGRPC(t, key, pkresponse)
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}

			var err error
			if val != nil {
				quotedVal := fmt.Sprintf("\"%s\"", *val) // you have to surround the string with "s
				*val, err = strconv.Unquote(quotedVal)
				if err != nil {
					t.Fatalf("Unquote failed %v\n", err)
				}
			}

			compareDataWithDB(t, operation.DB, operation.Table, operation.SubOperation.Body.Filters,
				&key, val, isBinaryData)
		}
	}
}

func Encode(data string, binary bool, colWidth int, padding bool) string {
	if binary {
		newData := []byte(data)
		if padding {
			length := colWidth
			if length < len(data) {
				length = len(data)
			}

			newData = make([]byte, length)
			for i := 0; i < length; i++ {
				newData[i] = 0x00
			}
			for i := 0; i < len(data); i++ {
				newData[i] = data[i]
			}
		}
		return base64.StdEncoding.EncodeToString(newData)
	} else {
		return data
	}
}
