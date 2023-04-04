package pkread

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

func pkTestMultiple(t *testing.T, tests map[string]api.PKTestInfo, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			pkTest(t, testInfo, isBinaryData, true)
		})
	}
}

func pkTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	pkRESTTest(t, testInfo, isBinaryData, validate)
	pkGRPCTest(t, testInfo, isBinaryData, validate)
}

func pkGRPCTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	respCode, resp := sendGRPCPKReadRequest(t, testInfo)
	if respCode == http.StatusOK && validate {
		validateResGRPC(t, testInfo, resp, isBinaryData)
	}
}

func pkRESTTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	url := testutils.NewPKReadURL(testInfo.Db, testInfo.Table)
	body, err := json.MarshalIndent(testInfo.PkReq, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}

	httpCode, res := testclient.SendHttpRequest(
		t,
		config.PK_HTTP_VERB,
		url,
		string(body),
		testInfo.ErrMsgContains,
		testInfo.HttpCode,
	)
	if httpCode == http.StatusOK && validate {
		validateResHttp(t, testInfo, res, isBinaryData)
	}
}

func sendGRPCPKReadRequest(
	t testing.TB,
	testInfo api.PKTestInfo,
) (int, *api.PKReadResponseGRPC) {

	client := api.NewRonDBRESTClient(testclient.GetGRPCConnction())

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
		respCode = testclient.GetStatusCodeFromError(t, err)
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
