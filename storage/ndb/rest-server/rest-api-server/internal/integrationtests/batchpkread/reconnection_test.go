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
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestReconnection(t *testing.T) {
	tests := map[string]api.BatchOperationTestInfo{
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

	numThreads := int(1)
	stop := false
	doneCh := make(chan int, numThreads)

	for i := 0; i < numThreads; i++ {
		go somework(t, i, tests, &stop, &doneCh)
	}
	time.Sleep(2 * time.Second)

	dal.Reconnect()

	log.Info("Reconnection request sent")

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
