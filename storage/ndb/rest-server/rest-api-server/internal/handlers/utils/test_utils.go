/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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
package utils

import (
	"context"
	"crypto/tls"
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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
	"hopsworks.ai/rdrs/internal/server"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/version"
)

func SendHttpRequest(t testing.TB, tlsCtx testutils.TlsContext, httpVerb string,
	url string, body string, expectedStatus int, expectedErrMsg string) (int, string) {
	t.Helper()

	conf := config.GetAll()

	client := setupHttpClient(t, tlsCtx)
	var req *http.Request
	var resp *http.Response
	var err error
	switch httpVerb {
	case "POST":
		req, err = http.NewRequest("POST", url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

	case "GET":
		req, err = http.NewRequest("GET", url, nil)

	default:
		t.Fatalf("Http verb not yet implemented. Verb %s", httpVerb)
	}

	logMsg := fmt.Sprintf("HTTP request with url: %s; request body: %s", url, body)
	if err != nil {
		t.Fatalf("Test failed to create request for %s; Error: %v", logMsg, err)
	}

	if conf.Security.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Test failed to perform request for %s; Error: %v", logMsg, err)
	}

	respCode := resp.StatusCode
	respBodyBtyes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Test failed to read response body for %s; Error: %v", logMsg, err)
	}
	respBody := string(respBodyBtyes)

	if respCode != expectedStatus {
		t.Fatalf("got wrong HTTP status; expected: %d; got: %d; response body: %s; for %s", expectedStatus, respCode, respBody, logMsg)
	}

	if respCode != http.StatusOK && !strings.Contains(respBody, expectedErrMsg) {
		t.Fatalf("Response error body does not contain %s. response body: %s; for %s", expectedErrMsg, respBody, logMsg)
	}

	return respCode, respBody
}

func setupHttpClient(t testing.TB, tlsCtx testutils.TlsContext) *http.Client {
	c := &http.Client{}
	c.Transport = &http.Transport{TLSClientConfig: GetClientTLSConfig(t, tlsCtx)}
	return c
}

func GetClientTLSConfig(t testing.TB, tlsCtx testutils.TlsContext) *tls.Config {
	clientTLSConfig := tls.Config{}
	conf := config.GetAll()
	if conf.Security.RootCACertFile != "" {
		clientTLSConfig.RootCAs = tlsutils.TrustedCAs(tlsCtx.RootCACertFile)
	}

	if conf.Security.RequireAndVerifyClientCert {
		clientCert, err := tls.LoadX509KeyPair(tlsCtx.ClientCertFile, tlsCtx.ClientKeyFile)
		if err != nil {
			t.Fatalf("%v", err)
		}
		clientTLSConfig.Certificates = []tls.Certificate{clientCert}
	}
	return &clientTLSConfig
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

		err = compareDataWithDB(t, testInfo.Db, testInfo.Table, testInfo.PkReq.Filters,
			&key, jsonVal, isBinaryData)
		if err != nil {
			t.Fatalf("failed validating HTTP response; error: %v", err)
		}
	}
}

func ValidateResGRPC(t testing.TB, testInfo api.PKTestInfo,
	resp *api.PKReadResponseGRPC, isBinaryData bool) {
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

		err = compareDataWithDB(t, testInfo.Db, testInfo.Table, testInfo.PkReq.Filters,
			&key, val, isBinaryData)
		if err != nil {
			t.Fatalf("failed validating gRPC response; error: %v", err)
		}
	}
}

func compareDataWithDB(t testing.TB, db string, table string, filters *[]api.Filter,
	colName *string, colDataFromRestServer *string, isBinaryData bool) error {
	dbVal, err := getColumnDataFromDB(t, db, table, filters, *colName, isBinaryData)
	if err != nil {
		return err
	}

	if (colDataFromRestServer == nil || dbVal == nil) && !(colDataFromRestServer == nil && dbVal == nil) { // if one of prts is nill
		return fmt.Errorf("The read value for key %s does not match.", *colName)
	}

	if !((colDataFromRestServer == nil && dbVal == nil) || (*colDataFromRestServer == *dbVal)) {
		return fmt.Errorf(
			"The read value for key '%s' does not match when querying '%s.%s'. "+
				"Result from REST Server: '%s'; "+
				"Result from MYSQL Server: '%s'; "+
				"Filters: %v",
			*colName, db, table, *colDataFromRestServer, *dbVal, *filters,
		)
	}
	return nil
}

func getColumnDataFromGRPC(t testing.TB, colName string, pkResponse *api.PKReadResponseGRPC) (*string, bool) {
	t.Helper()
	val, ok := (*pkResponse.Data)[colName]
	if !ok {
		return nil, ok
	} else {
		return val, ok
	}
}

func getColumnDataFromJson(t testing.TB, colName string, pkResponse *api.PKReadResponseJSON) (*string, bool) {
	t.Helper()

	kvMap := make(map[string]*string)
	for colName, colValue := range *pkResponse.Data {
		if colValue != nil {
			value := string([]byte(*colValue))
			var err error
			if value[0] == '"' {
				value, err = strconv.Unquote(value)
				if err != nil {
					t.Fatal(err)
				}
			}
			kvMap[colName] = &value
		} else {
			kvMap[colName] = nil
		}
	}

	val, ok := kvMap[colName]
	if !ok {
		return nil, ok
	} else {
		return val, ok
	}
}

func getColumnDataFromDB(t testing.TB, db string, table string, filters *[]api.Filter, col string, isBinary bool) (*string, error) {

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

func NewPKReadURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d%s%s", conf.REST.ServerIP,
		conf.REST.ServerPort,
		config.DB_OPS_EP_GROUP, config.PK_DB_OPERATION)
	url = strings.Replace(url, ":"+config.DB_PP, db, 1)
	url = strings.Replace(url, ":"+config.TABLE_PP, table, 1)
	appendURLProtocol(&url)
	return url
}

func NewBatchReadURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s", conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION, config.BATCH_OPERATION)
	appendURLProtocol(&url)
	return url
}

func NewStatURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s", conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION, config.STAT_OPERATION)
	appendURLProtocol(&url)
	return url
}

func appendURLProtocol(url *string) {
	conf := config.GetAll()
	if conf.Security.EnableTLS {
		*url = fmt.Sprintf("https://%s", *url)
	} else {
		*url = fmt.Sprintf("http://%s", *url)
	}
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

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func WithDBs(
	t testing.TB,
	dbs []string,
	handlers *handlers.AllHandlers,
	executer func(tc testutils.TlsContext),
) {
	t.Helper()

	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	conf := config.GetAll()

	// init logger
	log.InitLogger(conf.Log)

	var err error
	var tlsCtx testutils.TlsContext
	var cleanup func()
	if conf.Security.EnableTLS {
		tlsCtx, cleanup, err = testutils.CreateAllTLSCerts()
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()
	}

	rand.Seed(int64(time.Now().Nanosecond()))

	err, removeDatabases := testutils.CreateDatabases(t, conf.Security.UseHopsworksAPIKeys, dbs...)
	if err != nil {
		t.Fatalf("failed creating databases; error: %v ", err)
	}
	defer removeDatabases()

	routerCtx := server.CreateRouterContext()

	routerCtx.SetupRouter(handlers)

	err = routerCtx.StartRouter()
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer shutDownRouter(t, routerCtx)

	executer(tlsCtx)

	stats := dal.GetNativeBuffersStats()
	if stats.BuffersCount != stats.FreeBuffers {
		t.Fatalf("Number of free buffers do not match. Expecting: %d, Got: %d",
			stats.BuffersCount, stats.FreeBuffers)
	}
}

func shutDownRouter(t testing.TB, router server.Router) error {
	t.Helper()
	return router.StopRouter()
}

func PkTest(t *testing.T, tests map[string]api.PKTestInfo, isBinaryData bool, handlers *handlers.AllHandlers) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			dbs := []string{}
			dbs = append(dbs, testInfo.Db)

			WithDBs(t, dbs, handlers, func(tlsCtx testutils.TlsContext) {
				pkRESTTest(t, testInfo, tlsCtx, isBinaryData)
				pkGRPCTest(t, testInfo, tlsCtx, isBinaryData)
			})
		})
	}
}

func pkGRPCTest(t *testing.T, testInfo api.PKTestInfo, tlsCtx testutils.TlsContext, isBinaryData bool) {
	respCode, resp := sendGRPCPKReadRequest(t, tlsCtx, testInfo)

	if respCode == http.StatusOK {
		ValidateResGRPC(t, testInfo, resp, isBinaryData)
	}
}

func sendGRPCPKReadRequest(t *testing.T, tlsCtx testutils.TlsContext,
	testInfo api.PKTestInfo) (int, *api.PKReadResponseGRPC) {
	conf := config.GetAll()
	// Create gRPC client
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d",
		conf.GRPC.ServerIP,
		conf.GRPC.ServerPort),
		grpc.WithTransportCredentials(credentials.NewTLS(GetClientTLSConfig(t, tlsCtx))))
	defer conn.Close()

	if err != nil {
		t.Fatalf("Failed to connect to server %v", err)
	}
	client := api.NewRonDBRESTClient(conn)

	// Create Request
	pkReadParams := api.PKReadParams{}
	pkReadParams.DB = &testInfo.Db
	pkReadParams.Table = &testInfo.Table
	pkReadParams.Filters = testInfo.PkReq.Filters
	pkReadParams.OperationID = testInfo.PkReq.OperationID
	pkReadParams.ReadColumns = testInfo.PkReq.ReadColumns

	apiKey := testutils.HOPSWORKS_TEST_API_KEY
	reqProto := api.ConvertPKReadParams(&pkReadParams, &apiKey)

	expectedStatus := testInfo.HttpCode
	respCode := 200
	var errStr string
	respProto, err := client.PKRead(context.Background(), reqProto)
	if err != nil {
		respCode = GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Test failed. Expected: %d, Got: %d. Complete Error Message: %v ", expectedStatus, respCode, errStr)
	}

	if respCode != http.StatusOK && !strings.Contains(errStr, testInfo.ErrMsgContains) {
		t.Fatalf("Test failed. Error does not contain string: %s. Complete Error Message: %s", testInfo.ErrMsgContains, errStr)
	}

	if respCode == http.StatusOK {
		resp := api.ConvertPKReadResponseProto(respProto)
		return respCode, resp
	} else {
		return respCode, nil
	}
}

func GetStatusCodeFromError(t *testing.T, errGot error) int {
	errStr := fmt.Sprintf("%v", errGot)
	// error code is sandwiched b/w these two substrings
	subStr1 := "Error code: "

	if !strings.Contains(errStr, subStr1) {
		t.Fatalf("Invalid GRPC Error message: %s\n", errStr)
	}

	numStartIdx := strings.LastIndex(errStr, subStr1) + len(subStr1)
	numStr := errStr[numStartIdx : numStartIdx+3]
	errCode, err := strconv.Atoi(numStr)
	if err != nil {
		t.Fatalf("Invalid GRPC Error message. Unable to convert error code to int. Error msg: \"%s\". Error:%v ", errStr, err)
	}

	return errCode
}

func pkRESTTest(t *testing.T, testInfo api.PKTestInfo, tlsCtx testutils.TlsContext, isBinaryData bool) {
	url := NewPKReadURL(testInfo.Db, testInfo.Table)
	body, err := json.MarshalIndent(testInfo.PkReq, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}

	httpCode, res := SendHttpRequest(t, tlsCtx, config.PK_HTTP_VERB, url,
		string(body), testInfo.HttpCode, testInfo.ErrMsgContains)
	if httpCode == http.StatusOK {
		ValidateResHttp(t, testInfo, res, isBinaryData)
	}
}

func BatchTest(t *testing.T, tests map[string]api.BatchOperationTestInfo, isBinaryData bool,
	handlers *handlers.AllHandlers) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {

			// all databases used in this test
			dbNamesMap := map[string]bool{}
			dbNamesArr := []string{}
			for _, op := range testInfo.Operations {
				if _, ok := dbNamesMap[op.DB]; !ok {
					dbNamesMap[op.DB] = true
				}
			}

			for k := range dbNamesMap {
				dbNamesArr = append(dbNamesArr, k)
			}

			WithDBs(t, dbNamesArr, handlers, func(tlsCtx testutils.TlsContext) {
				batchRESTTest(t, testInfo, tlsCtx, isBinaryData)
				batchGRPCTest(t, testInfo, tlsCtx, isBinaryData)
			})
		})
	}
}

func batchGRPCTest(t *testing.T, testInfo api.BatchOperationTestInfo, tlsCtx testutils.TlsContext, isBinaryData bool) {
	httpCode, res := sendGRPCBatchRequest(t, tlsCtx, testInfo)
	if httpCode == http.StatusOK {
		validateBatchResponseGRPC(t, testInfo, res, isBinaryData)
	}
}

func sendGRPCBatchRequest(t *testing.T, tlsCtx testutils.TlsContext,
	testInfo api.BatchOperationTestInfo) (int, *api.BatchResponseGRPC) {
	conf := config.GetAll()
	// Create gRPC client
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d",
		conf.GRPC.ServerIP,
		conf.GRPC.ServerPort),
		grpc.WithTransportCredentials(credentials.NewTLS(GetClientTLSConfig(t, tlsCtx))))
	defer conn.Close()

	if err != nil {
		t.Fatalf("Failed to connect to server %v", err)
	}
	client := api.NewRonDBRESTClient(conn)

	// Create Request
	batchOpRequest := make([]*api.PKReadParams, len(testInfo.Operations))
	for i := 0; i < len(testInfo.Operations); i++ {
		op := testInfo.Operations[i]
		pkReadParams := api.PKReadParams{}
		pkReadParams.DB = &op.DB
		pkReadParams.Table = &op.Table
		pkReadParams.Filters = op.SubOperation.Body.Filters
		pkReadParams.OperationID = op.SubOperation.Body.OperationID
		pkReadParams.ReadColumns = op.SubOperation.Body.ReadColumns
		batchOpRequest[i] = &pkReadParams
	}

	apiKey := testutils.HOPSWORKS_TEST_API_KEY
	batchRequestProto := api.ConvertBatchOpRequest(batchOpRequest, &apiKey)

	expectedStatus := testInfo.HttpCode
	respCode := 200
	var errStr string
	respProto, err := client.Batch(context.Background(), batchRequestProto)
	if err != nil {
		respCode = GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Test failed. Expected: %d, Got: %d. Complete Error Message: %v ", expectedStatus, respCode, errStr)
	}

	if respCode != http.StatusOK && !strings.Contains(errStr, testInfo.ErrMsgContains) {
		t.Fatalf("Test failed. Error does not contain string: %s. Complete Error Message: %s", testInfo.ErrMsgContains, errStr)
	}

	if respCode == http.StatusOK {
		resp := api.ConvertBatchResponseProto(respProto)
		return respCode, resp
	} else {
		return respCode, nil
	}
}

func batchRESTTest(t *testing.T, testInfo api.BatchOperationTestInfo, tlsCtx testutils.TlsContext, isBinaryData bool) {
	//batch operation
	subOps := []api.BatchSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchOpRequest{Operations: &subOps}

	url := NewBatchReadURL()
	body, err := json.MarshalIndent(batch, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}
	httpCode, res := SendHttpRequest(t, tlsCtx, config.BATCH_HTTP_VERB, url,
		string(body), testInfo.HttpCode, testInfo.ErrMsgContains)
	if httpCode == http.StatusOK {
		validateBatchResponseHttp(t, testInfo, res, isBinaryData)
	}
}

func validateBatchResponseHttp(t testing.TB, testInfo api.BatchOperationTestInfo, resp string, isBinaryData bool) {
	t.Helper()
	validateBatchResponseOpIdsNCodeHttp(t, testInfo, resp)
	validateBatchResponseValuesHttp(t, testInfo, resp, isBinaryData)
}

func validateBatchResponseGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC, isBinaryData bool) {
	t.Helper()
	validateBatchResponseOpIdsNCodeGRPC(t, testInfo, resp)
	validateBatchResponseValuesGRPC(t, testInfo, resp, isBinaryData)
}

func validateBatchResponseOpIdsNCodeGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC) {
	if len(*resp.Result) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i, subResp := range *resp.Result {
		checkOpIDandStatus(t, testInfo.Operations[i], subResp.Body.OperationID,
			int(*subResp.Code))
	}
}

func checkOpIDandStatus(t testing.TB, testInfo api.BatchSubOperationTestInfo, opIDGot *string,
	statusGot int) {

	expctingOpID := testInfo.SubOperation.Body.OperationID
	expectingStatus := testInfo.HttpCode

	if expctingOpID != nil {
		if *expctingOpID != *opIDGot {
			t.Fatalf("Operation ID does not match. Expecting: %s, Got: %s. TestInfo: %v",
				*expctingOpID, *opIDGot, testInfo)
		}
	}

	if expectingStatus != statusGot {
		t.Fatalf("Return code does not match. Expecting: %d, Got: %d. TestInfo: %v",
			expectingStatus, statusGot, testInfo)
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
			int(*subResp.Code))
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

			err = compareDataWithDB(t, operation.DB, operation.Table, operation.SubOperation.Body.Filters,
				&key, val, isBinaryData)
			if err != nil {
				t.Fatalf("failed validating batched HTTP response; error: %v", err)
			}
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

			err = compareDataWithDB(t, operation.DB, operation.Table, operation.SubOperation.Body.Filters,
				&key, val, isBinaryData)
			if err != nil {
				t.Fatalf("failed validating batched gRPC response; error: %v", err)
			}
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
