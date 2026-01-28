/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026, 2026 Hopsworks AB
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

package batchwrite

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchWriteRESTTest(
	t testing.TB,
	testInfo api.BatchWriteOperationTestInfo,
) {
	client := testutils.SetupHttpClient(t)
	batchWriteRESTTestWithClient(t, client, testInfo)
}

func batchWriteRESTTestWithClient(
	t testing.TB,
	client *http.Client,
	testInfo api.BatchWriteOperationTestInfo,
) {
	httpCode, response := sendHttpBatchWriteRequest(t, client, testInfo)

	if log.IsTrace() {
		log.Tracef("Http Response %s\n", string(response))
	}

	if httpCode == http.StatusOK {
		validateBatchWriteResponseHttp(t, testInfo, response)
	}
}

func sendHttpBatchWriteRequest(
	t testing.TB,
	client *http.Client,
	testInfo api.BatchWriteOperationTestInfo,
) (httpCode int, response []byte) {
	subOps := []api.BatchWriteSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchWriteOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}
	url := testutils.NewBatchWriteURL()
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

func validateBatchWriteResponseHttp(
	t testing.TB,
	testInfo api.BatchWriteOperationTestInfo,
	response []byte,
) {
	t.Helper()

	validateBatchWriteResponseOpIdsNCodeHttp(t, testInfo, response)
}

func validateBatchWriteResponseOpIdsNCodeHttp(
	t testing.TB,
	testInfo api.BatchWriteOperationTestInfo,
	resp []byte,
) {
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch write response. Error %v", err)
	}

	if len(*res.Result) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i, subResp := range *res.Result {
		checkWriteOpIDandStatus(t, testInfo.Operations[i], subResp.Body.OperationID,
			int(*subResp.Code), subResp)
	}
}
