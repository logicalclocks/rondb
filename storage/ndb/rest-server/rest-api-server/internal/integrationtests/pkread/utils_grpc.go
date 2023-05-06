package pkread

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/pkg/api"
)

func pkGRPCTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	conn, err := testclient.InitGRPCConnction()
	if err != nil {
		t.Fatal(err.Error())
	}
	pkGRPCTestWithConn(t, testInfo, isBinaryData, validate, conn)
}

func pkGRPCTestWithConn(
	t testing.TB,
	testInfo api.PKTestInfo,
	isBinaryData bool,
	validate bool,
	grpcConn *grpc.ClientConn,
) {
	respCode, resp := handleGrpcRequest(t, testInfo, grpcConn)
	if respCode == http.StatusOK && validate {
		validateResGRPC(t, testInfo, resp, isBinaryData)
	}
}

func handleGrpcRequest(
	t testing.TB,
	testInfo api.PKTestInfo,
	grpcConn *grpc.ClientConn,
) (int, *api.PKReadResponseGRPC) {

	// Create Proto Request
	pkReadParams := api.PKReadParams{
		DB:          &testInfo.Db,
		Table:       &testInfo.Table,
		Filters:     testInfo.PkReq.Filters,
		OperationID: testInfo.PkReq.OperationID,
		ReadColumns: testInfo.PkReq.ReadColumns,
	}
	reqProto := api.ConvertPKReadParams(&pkReadParams)

	respCode, respProto := sendGrpcRequestAndCheckStatus(
		t,
		testInfo.HttpCode,
		testInfo.ErrMsgContains,
		reqProto,
		grpcConn,
	)

	// Parse Proto response
	if respCode == http.StatusOK {
		resp := api.ConvertPKReadResponseProto(respProto)
		return respCode, resp
	} else {
		return respCode, nil
	}
}

/*
Extracted this in case we want to reuse the Proto request struct and
avoid parsing the response, even though this does not make a noticeable
difference in performance tests.
*/
func sendGrpcRequestAndCheckStatus(
	t testing.TB,
	expectedStatus int,
	expectedErrMsg string,
	reqProto *api.PKReadRequestProto,
	grpcConn *grpc.ClientConn,
) (int, *api.PKReadResponseProto) {
	client := api.NewRonDBRESTClient(grpcConn)

	respCode := 200
	var errStr string
	respProto, err := client.PKRead(context.Background(), reqProto)
	if err != nil {
		respCode = testclient.GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Received unexpected status; Expected: %d, Got: %d; Complete Error Message: '%s'", expectedStatus, respCode, errStr)
	}

	if respCode != http.StatusOK && !strings.Contains(errStr, expectedErrMsg) {
		t.Fatalf("Received unexpected error message; It does not contain string: '%s'; Complete Error Message: '%s'", expectedErrMsg, errStr)
	}

	return respCode, respProto
}

func validateResGRPC(
	t testing.TB,
	testInfo api.PKTestInfo,
	resp *api.PKReadResponseGRPC,
	isBinaryData bool,
) {
	t.Helper()

	parsedData := testclient.ParseColumnDataFromGRPC(t, resp, isBinaryData)

	for _, keyIntf := range testInfo.RespKVs {
		key := string(keyIntf.(string))

		grpcVal, found := parsedData[key]
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		integrationtests.CompareDataWithDB(t,
			testInfo.Db,
			testInfo.Table,
			testInfo.PkReq.Filters,
			&key,
			grpcVal,
			isBinaryData,
		)
	}
}
