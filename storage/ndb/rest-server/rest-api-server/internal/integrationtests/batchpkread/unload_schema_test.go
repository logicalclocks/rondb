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

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
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
							Filters:     integrationtests.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: integrationtests.NewReadColumns("col", 2),
							OperationID: integrationtests.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{ // operation on a DB that will change when the test is running
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB025 + "/table_1/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     integrationtests.NewFiltersKVs("id0", "1"),
							OperationID: &id,
						},
					},
					Table:    "table_1",
					DB:       testdbs.DB025,
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB005 + "/bigint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     integrationtests.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: integrationtests.NewReadColumns("col", 2),
							OperationID: integrationtests.NewOperationID(64),
						},
					},
					Table:    "bigint_table",
					DB:       testdbs.DB005,
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB006 + "/tinyint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     integrationtests.NewFiltersKVs("id0", -128, "id1", 0),
							ReadColumns: integrationtests.NewReadColumns("col", 2),
							OperationID: integrationtests.NewOperationID(64),
						},
					},
					Table:    "tinyint_table",
					DB:       testdbs.DB006,
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB007 + "/smallint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     integrationtests.NewFiltersKVs("id0", 32767, "id1", 65535),
							ReadColumns: integrationtests.NewReadColumns("col", 2),
							OperationID: integrationtests.NewOperationID(64),
						},
					},
					Table:    "smallint_table",
					DB:       testdbs.DB007,
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string(testdbs.DB007 + "/smallint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     integrationtests.NewFiltersKVs("id0", 1, "id1", 1),
							ReadColumns: integrationtests.NewReadColumns("col", 2),
							OperationID: integrationtests.NewOperationID(64),
						},
					},
					Table:    "smallint_table",
					DB:       testdbs.DB007,
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
	}

	// Reset the database
	err := testutils.RunQueries(testdbs.DB025Scheme)
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
	err = testutils.RunQueries(testdbs.DB025UpdateScheme)
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
	opCount := 0
	// need to do this if an operation fails
	defer func(t *testing.T, opCount *int) {
		*done <- *opCount
	}(t, &opCount)

	for {
		for _, testInfo := range tests {
			integrationtests.BatchRESTTest(t, testInfo, false /*is binary*/, false /*validate data*/)
			integrationtests.BatchGRPCTest(t, testInfo, false /*is binary*/, false /*validate data*/)
		}
		opCount++

		if *stop {
			return
		}
	}
}
