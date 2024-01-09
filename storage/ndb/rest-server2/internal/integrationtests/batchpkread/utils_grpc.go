package batchpkread

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"hopsworks.ai/rdrs2/internal/integrationtests"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchGRPCTest(t testing.TB, testInfo api.BatchOperationTestInfo, isBinaryData bool, validateData bool) {
	conn, err := testclient.InitGRPCConnction()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer conn.Close()
	batchGRPCTestWithConn(t, testInfo, isBinaryData, validateData, conn)
}

func batchGRPCTestWithConn(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	isBinaryData bool,
	validateData bool,
	conn *grpc.ClientConn,
) {
	httpCode, res := sendGRPCBatchRequest(t, testInfo, conn)
	if httpCode == http.StatusOK {
		validateBatchResponseGRPC(t, testInfo, res, isBinaryData, validateData)
	}
}

func sendGRPCBatchRequest(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	conn *grpc.ClientConn,
) (int, *api.BatchResponseGRPC) {
	gRPCClient := api.NewRonDBRESTClient(conn)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	respProto, err := gRPCClient.Batch(ctx, batchRequestProto)
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

func validateBatchResponseValuesGRPC(
	t testing.TB,
	testInfo api.BatchOperationTestInfo,
	resp *api.BatchResponseGRPC,
	isBinaryData bool,
) {
	for o, operation := range testInfo.Operations {
		if *(*resp.Result)[o].Code != http.StatusOK {
			continue // data is null if the status is not OK
		}

		pkresponse := (*resp.Result)[o].Body
		parsedData := testclient.ParseColumnDataFromGRPC(t, pkresponse, isBinaryData)

		for _, keyIntf := range operation.RespKVs {
			key := string(keyIntf.(string))

			grpcVal, found := parsedData[key]
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}

			integrationtests.CompareDataWithDB(
				t,
				operation.DB,
				operation.Table,
				operation.SubOperation.Body.Filters,
				&key,
				grpcVal,
				isBinaryData,
			)
		}
	}
}
