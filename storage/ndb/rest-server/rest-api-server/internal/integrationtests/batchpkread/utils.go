package batchpkread

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func batchTest(t *testing.T, tests map[string]api.BatchOperationTestInfo, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			// This will mean that REST calls to Handler will be slightly faster
			batchRESTTest(t, testInfo, isBinaryData, true)
			batchGRPCTest(t, testInfo, isBinaryData, true)
		})
	}
}

func batchRESTTest(t *testing.T, testInfo api.BatchOperationTestInfo, isBinaryData bool, validateData bool) {
	httpCode, res := sendHttpBatchRequest(t, testInfo, isBinaryData)
	if httpCode == http.StatusOK {
		validateBatchResponseHttp(t, testInfo, res, isBinaryData, validateData)
	}
}

func batchGRPCTest(t *testing.T, testInfo api.BatchOperationTestInfo, isBinaryData bool, validateData bool) {
	httpCode, res := sendGRPCBatchRequest(t, testInfo)
	if httpCode == http.StatusOK {
		validateBatchResponseGRPC(t, testInfo, res, isBinaryData, validateData)
	}
}

func sendGRPCBatchRequest(t *testing.T, testInfo api.BatchOperationTestInfo) (int, *api.BatchResponseGRPC) {
	gRPCClient := api.NewRonDBRESTClient(testclient.GetGRPCConnction())

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
		respCode = testclient.GetStatusCodeFromError(t, err)
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
	httpCode, res = testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url,
		string(body), testInfo.ErrMsgContains, testInfo.HttpCode[:]...)
	return
}
