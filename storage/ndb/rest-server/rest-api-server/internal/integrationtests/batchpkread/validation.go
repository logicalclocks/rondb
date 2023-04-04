package batchpkread

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/pkg/api"
)

func validateBatchResponseOpIdsNCodeGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC) {
	if len(*resp.Result) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i, subResp := range *resp.Result {
		checkOpIDandStatus(t, testInfo.Operations[i], subResp.Body.OperationID,
			int(*subResp.Code), subResp)
	}
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

func validateBatchResponseValuesGRPC(t testing.TB, testInfo api.BatchOperationTestInfo, resp *api.BatchResponseGRPC, isBinaryData bool) {
	for o := 0; o < len(testInfo.Operations); o++ {
		if *(*resp.Result)[o].Code != http.StatusOK {
			continue // data is null if the status is not OK
		}

		operation := testInfo.Operations[o]
		pkresponse := (*resp.Result)[o].Body
		for i := 0; i < len(operation.RespKVs); i++ {
			key := string(operation.RespKVs[i].(string))
			val, found := testclient.GetColumnDataFromGRPC(t, key, pkresponse)
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

			integrationtests.CompareDataWithDB(t, operation.DB, operation.Table, operation.SubOperation.Body.Filters,
				&key, val, isBinaryData)
		}
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
			val, found := testclient.GetColumnDataFromJson(t, key, pkresponse)
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}

			integrationtests.CompareDataWithDB(t, operation.DB, operation.Table, operation.SubOperation.Body.Filters,
				&key, val, isBinaryData)
		}
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
