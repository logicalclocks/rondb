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

package batchpkread

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestReconnection1(t *testing.T) {
	reconnectionTest(t, 1, 500)
}

func TestReconnection2(t *testing.T) {
	reconnectionTest(t, 10, 10)
}

func reconnectionTest(t *testing.T, threads int, durationSec int) {
	log.Debugf("Starting Reconnection test with %d threads ", threads)
	tests := map[string]*api.BatchOperationTestInfo{
		"batch": { // bigger batch of numbers table
			HttpCode: []int{http.StatusOK, http.StatusInternalServerError},
			Operations: []api.BatchSubOperationTestInfo{
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB004 + "/int_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: testclient.NewReadColumns("col", 2),
							OperationID: testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB005 + "/bigint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: testclient.NewReadColumns("col", 2),
							OperationID: testclient.NewOperationID(64),
						},
					},
					Table:    "bigint_table",
					DB:       testdbs.DB005,
					HttpCode: []int{http.StatusOK},
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB006 + "/tinyint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     testclient.NewFiltersKVs("id0", -128, "id1", 0),
							ReadColumns: testclient.NewReadColumns("col", 2),
							OperationID: testclient.NewOperationID(64),
						},
					},
					Table:    "tinyint_table",
					DB:       testdbs.DB006,
					HttpCode: []int{http.StatusOK},
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
	}

	reconnectTestInt(t, threads, durationSec, tests)
}

func reconnectTestInt(t *testing.T, numThreads int, durationSec int,
	tests map[string]*api.BatchOperationTestInfo) {

	stop := false
	doneCh := make(chan int, numThreads)

	tests["batch"].HttpCode = []int{http.StatusOK, http.StatusInternalServerError}
	for i := 0; i < numThreads; i++ {
		go batchPKWorker(t, i, tests, &stop, &doneCh)
		go statWorker(t, &stop)
	}

	// do some work
	time.Sleep(1 * time.Second)

	// send multiple reconnection request.
	// only the first request should return OK
	for i := 0; i < 5; i++ {
		if i == 0 {
			//first reconnection request is supposed to succeed
			err := dal.Reconnect()
			if err != nil {
				t.Fatalf("Reconnection request failed %v ", err)
			}
		} else {
			//subsequent reconnection requests are supposed to fail
			err := dal.Reconnect()
			if err == nil {
				t.Fatalf("was expection reconnection request to fail")
			}

			if !strings.Contains(err.Message, common.ERROR_036()) {
				t.Fatalf("Unexpected error message . Expecting: %s, Got: %s ", common.ERROR_036(), err.Message)
			}
		}
	}

	// Stop after some time
	time.Sleep(time.Duration(durationSec) * time.Second)
	stop = true
	opCount := 0
	for i := 0; i < numThreads; i++ {
		c := <-doneCh
		opCount += c
	}

	//do some synchronous work. no error expected this time
	tests["batch"].HttpCode = []int{http.StatusOK}
	batchPKWorker(t, 0, tests, &stop, &doneCh)

	log.Infof("Total Ops performed: %d\n", opCount)
}

func batchPKWorker(
	t *testing.T,
	id int,
	tests map[string]*api.BatchOperationTestInfo,
	stop *bool,
	done *chan int,
) {
	httpClient := testutils.SetupHttpClient(t)
	grpcConn, err := testclient.InitGRPCConnction()
	if err != nil {
		// Cannot fail a test case in a go-routine
		t.Log(err.Error())
		return
	}

	opCount := 0
	// need to do this if an operation fails
	defer func(t *testing.T, opCount *int) {
		*done <- *opCount
	}(t, &opCount)

	for {
		for _, testInfo := range tests {
			batchRESTTestWithClient(t, httpClient, *testInfo, false, false)
			batchGRPCTestWithConn(t, *testInfo, false, false, grpcConn)
		}
		opCount++

		time.Sleep(100 * time.Millisecond)

		if *stop {
			return
		}
	}
}

func statWorker(t *testing.T, stop *bool) {
	httpClient := testutils.SetupHttpClient(t)

	for {
		stat(t, httpClient)
		time.Sleep(100 * time.Millisecond)

		if *stop {
			return
		}
	}
}

func stat(t *testing.T, client *http.Client) {
	body := ""
	url := testutils.NewStatURL()
	_, respBody := testclient.SendHttpRequestWithClient(t, client, config.STAT_HTTP_VERB, url, string(body),
		"", http.StatusOK)

	var stats api.StatResponse
	err := json.Unmarshal([]byte(respBody), &stats)
	if err != nil {
		t.Fatalf("%v", err)
	}
}
