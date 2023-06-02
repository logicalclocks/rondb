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

package batchfeaturestore

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"

	"hopsworks.ai/rdrs/internal/integrationtests"
	fshelper "hopsworks.ai/rdrs/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs/internal/log"
)

/*
	 The number of parallel client go-routines spawned in RunParallel()
	 can be influenced by setting runtime.GOMAXPROCS(). It defaults to the
	 number of CPUs.

	 This test can be run as follows:

		 go test \
			 -test.bench BenchmarkSimple \
			 -test.run=thisexpressionwontmatchanytest \
			 -cpu 1,2,4,8 \
			 -benchmem \
			 -benchtime=100x \ 		// 100 times
			 -benchtime=10s \ 		// 10 sec
			 ./internal/integrationtests/pkread/
*/

const totalNumRequest = 100000

func Benchmark(b *testing.B) {
	for _, n := range []int{10, 50, 100} {
		run(b, "fsdb001", "sample_1", 1, n)
	}
}

func Benchmark_join(b *testing.B) {
	for _, n := range []int{5, 25, 50} {
		run(b, "fsdb001", "sample_1n2", 1, n)
	}
}

func getSampleData(n int, fsName string, fvName string, fvVersion int) ([][]interface{}, []string, []string, error) {
	switch fmt.Sprintf("%s|%s|%d", fsName, fvName, fvVersion) {
	case "fsdb001|sample_1|1":
		return fshelper.GetNSampleData("fsdb001", "sample_1_1", n)
	case "fsdb001|sample_3|1":
		return fshelper.GetNSampleData("fsdb001", "sample_3_1", n)
	case "fsdb001|sample_1n2|1":
		return fshelper.GetNSampleDataWithJoin(n, "fsdb001", "sample_1_1", "fsdb001", "sample_2_1", "fg2_")
	default:
		panic("No sample data for given feature view")
	}
}

func run(b *testing.B, fsName string, fvName string, fvVersion int, batchSize int) {
	const nReq = 100

	log.Infof("Test config: fs: %s fv: %s version: %d batch_size: %d", fsName, fvName, fvVersion, batchSize)
	var metadata, err = feature_store.GetFeatureViewMetadata(fsName, fvName, fvVersion)
	if err != nil {
		panic("Cannot get metadata.")
	}
	log.Infof(`
	Number of tables: %d 
	Batch size in robdb request: %d 
	Number of columns: %d`,
		len(metadata.FeatureGroupFeatures), len(metadata.FeatureGroupFeatures)*batchSize, metadata.NumOfFeatures)

	var fsReqs = make([]string, 0, nReq)
	for i := 0; i < nReq; i++ {
		rows, pks, cols, err := getSampleData(batchSize, fsName, fvName, fvVersion)
		if err != nil {
			panic("Failed to get sample data: " + err.Error())
		}
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&rows, &pks, &cols),
			nil,
			nil,
		)
		reqBody := fmt.Sprintf("%s", fsReq)
		fsReqs = append(fsReqs, reqBody)

	}
	integrationtests.RunRestTemplate(b, config.FEATURE_STORE_HTTP_VERB, testutils.NewBatchFeatureStoreURL(), &fsReqs, totalNumRequest)
}

// Include this benchmark for comparison
func BenchmarkBatchPkRead(b *testing.B) {
	var batchSize = 100
	table := "table_1"
	col := "id0"
	var reqs = make([]string, 0)
	for i := 0; i < 100; i++ {
		numRows := testdbs.BENCH_DB_NUM_ROWS
		operations := []api.BatchSubOperationTestInfo{}
		for i := 0; i < batchSize; i++ {
			// We will set the pk to filter later
			operations = append(operations, createSubOperation(b, table, testdbs.Benchmark, "", http.StatusOK))
		}
		batchTestInfo := api.BatchOperationTestInfo{
			HttpCode:       []int{http.StatusOK},
			Operations:     operations,
			ErrMsgContains: "",
		}
		for _, op := range batchTestInfo.Operations {
			op.SubOperation.Body.Filters = testclient.NewFilter(&col, rand.Intn(numRows))
		}
		subOps := []api.BatchSubOp{}
		for _, op := range batchTestInfo.Operations {
			subOps = append(subOps, op.SubOperation)
		}
		batch := api.BatchOpRequest{Operations: &subOps}
		body, err := json.MarshalIndent(batch, "", "\t")
		if err != nil {
			b.Fatalf("Failed to marshall test request %v", err)
		}
		reqs = append(reqs, string(body))
	}
	integrationtests.RunRestTemplate(b, config.BATCH_HTTP_VERB, testutils.NewBatchReadURL(), &reqs, totalNumRequest)
}

func createSubOperation(t testing.TB, table string, database string, pk string, expectedStatus int) api.BatchSubOperationTestInfo {
	respKVs := []interface{}{"col0"}
	return api.BatchSubOperationTestInfo{
		SubOperation: api.BatchSubOp{
			Method:      &[]string{config.PK_HTTP_VERB}[0],
			RelativeURL: &[]string{string(database + "/" + table + "/" + config.PK_DB_OPERATION)}[0],
			Body: &api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", pk),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
		},
		Table:    table,
		DB:       database,
		HttpCode: []int{expectedStatus},
		RespKVs:  respKVs,
	}
}
