package batchpkread

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchRESTTest(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	isBinaryData bool,
	validateData bool,
) {
	client := testutils.SetupHttpClient(t)
	batchRESTTestWithClient(t, client, testInfo, isBinaryData, validateData)
}

func batchRESTTestWithClient(
	t testing.TB,
	client *http.Client,
	testInfo api.BatchOperationTestInfo,
	isBinaryData bool,
	validateData bool,
) {
	httpCode, response := sendHttpBatchRequest(t, client, testInfo, isBinaryData)

	if log.IsTrace() {
		log.Tracef("Http Response %s\n", string(response))
	}

	if httpCode == http.StatusOK {
		validateBatchResponseHttp(t, testInfo, response, isBinaryData, validateData)
	}
}

func sendHttpBatchRequest(
	t testing.TB,
	client *http.Client,
	testInfo api.BatchOperationTestInfo,
	isBinaryData bool,
) (httpCode int, response []byte) {
	subOps := []api.BatchSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchOpRequest{Operations: &subOps}
	body, err := json.MarshalIndent(batch, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}
	url := testutils.NewBatchReadURL()
	httpCode, response = testclient.SendHttpRequestWithClient(
		t,
		client,
		config.BATCH_HTTP_VERB,
		url,
		string(body),
		testInfo.ErrMsgContains,
		testInfo.HttpCode[:]...,
	)
	return
}

func validateBatchResponseHttp(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	response []byte,
	isBinaryData bool,
	validateData bool,
) {
	t.Helper()

	validateBatchResponseOpIdsNCodeHttp(t, testInfo, response)
	if validateData {
		validateBatchResponseValuesHttp(t, testInfo, response, isBinaryData)
	}
}

func validateBatchResponseOpIdsNCodeHttp(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	resp []byte,
) {
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
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

func validateBatchResponseValuesHttp(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	resp []byte,
	isBinaryData bool,
) {
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch response. Error %v", err)
	}

	for o, operation := range testInfo.Operations {
		if *(*res.Result)[o].Code != http.StatusOK {
			continue // data is null if the status is not OK
		}

		pkresponse := *((*res.Result)[o].Body)
		parsedData := testclient.ParseColumnDataFromJson(t, pkresponse, isBinaryData)

		for _, keyIntf := range operation.RespKVs {
			key := string(keyIntf.(string))

			jsonVal, found := parsedData[key]
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}

			integrationtests.CompareDataWithDB(
				t,
				operation.DB,
				operation.Table,
				operation.SubOperation.Body.Filters,
				&key,
				jsonVal,
				isBinaryData,
			)
		}
	}
}
