package batchpkread

import (
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchTestMultiple(t *testing.T, tests map[string]api.BatchOperationTestInfo, isBinaryData bool, validate bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			batchTest(t, testInfo, isBinaryData, validate)
		})
	}
}

func batchTest(t testing.TB, testInfo api.BatchOperationTestInfo, isBinaryData bool, validate bool) {
	if config.GetAll().REST.Enable {
		batchRESTTest(t, testInfo, isBinaryData, validate)
	}
	if config.GetAll().GRPC.Enable {
		batchGRPCTest(t, testInfo, isBinaryData, validate)
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
