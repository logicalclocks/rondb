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
	"net/http"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

func TestUnloadSchema(t *testing.T) {
	id := "1"
	tests := map[string]api.BatchOperationTestInfo{
		"batch": { // bigger batch of numbers table
			HttpCode: []int{http.StatusOK, http.StatusNotFound},
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
				{ // operation on a DB that will change when the test is running
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB025 + "/table_1/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     testclient.NewFiltersKVs("id0", "1"),
							OperationID: &id,
						},
					},
					Table:    "table_1",
					DB:       testdbs.DB025,
					HttpCode: []int{http.StatusOK, http.StatusNotFound},
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
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB007 + "/smallint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     testclient.NewFiltersKVs("id0", 32767, "id1", 65535),
							ReadColumns: testclient.NewReadColumns("col", 2),
							OperationID: testclient.NewOperationID(64),
						},
					},
					Table:    "smallint_table",
					DB:       testdbs.DB007,
					HttpCode: []int{http.StatusOK},
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB007 + "/smallint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     testclient.NewFiltersKVs("id0", 1, "id1", 1),
							ReadColumns: testclient.NewReadColumns("col", 2),
							OperationID: testclient.NewOperationID(64),
						},
					},
					Table:    "smallint_table",
					DB:       testdbs.DB007,
					HttpCode: []int{http.StatusOK},
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
	}

	// Reset the database
	err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme)
	if err != nil {
		t.Fatalf("failed to re-create database. Error: %v", err)
	}

	// start the worker threads
	numThreads := int(20)
	stop := false
	doneCh := make(chan int, numThreads)

	for i := 0; i < numThreads; i++ {
		go somework(t, i, tests, &stop, &doneCh)
	}
	time.Sleep(2 * time.Second)

	//change the schema
	//dal.SetOpRetryProps(3, 400)
	log.Info("Changing the schema for the test")
	err = testutils.RunQueriesOnDataCluster(testdbs.DB025UpdateScheme)
	if err != nil {
		t.Fatalf("failed to re-create database. Error: %v", err)
	}
	log.Info("Changed the schema for the test")

	// Stop after some time
	time.Sleep(2 * time.Second)
	stop = true
	opCount := 0
	for i := 0; i < numThreads; i++ {
		c := <-doneCh
		opCount += c
	}

	log.Infof("Total Ops: %d\n", opCount)
}

func somework(t *testing.T, id int, tests map[string]api.BatchOperationTestInfo, stop *bool, done *chan int) {
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
			if config.GetAll().REST.Enable {
				batchRESTTestWithClient(t, httpClient, testInfo, false, false)
			}
			if config.GetAll().GRPC.Enable {
				batchGRPCTestWithConn(t, testInfo, false, false, grpcConn)
			}
		}
		opCount++

		if *stop {
			return
		}
	}
}
