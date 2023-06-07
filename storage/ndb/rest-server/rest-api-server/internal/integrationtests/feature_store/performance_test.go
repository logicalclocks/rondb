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

package feature_store

import (
	"fmt"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/resources/testdbs"
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
			 ./internal/integrationtests/feature_store/
*/

const totalNumRequest = 100000

func Benchmark(b *testing.B) {
	run(b, testdbs.FSDB001, "sample_1", 1)
}

func Benchmark_join(b *testing.B) {
	run(b, testdbs.FSDB001, "sample_1n2", 1)
}

const nrows = 100

func getSampleData(fsName string, fvName string, fvVersion int) ([][]interface{}, []string, []string, error) {
	switch fmt.Sprintf("%s|%s|%d", fsName, fvName, fvVersion) {
	case "fsdb001|sample_1|1":
		return GetNSampleData(testdbs.FSDB001, "sample_1_1", nrows)
	case "fsdb001|sample_3|1":
		return GetNSampleData(testdbs.FSDB001, "sample_3_1", nrows)
	case "fsdb001|sample_1n2|1":
		return GetNSampleDataWithJoin(nrows, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	default:
		return nil, nil, nil, nil
	}
}

func run(b *testing.B, fsName string, fvName string, fvVersion int) {
	log.Infof("Test config: fs: %s fv: %s version: %d", fsName, fvName, fvVersion)
	var metadata, err = feature_store.GetFeatureViewMetadata(fsName, fvName, fvVersion)
	if err != nil {
		panic("Cannot get metadata.")
	}
	log.Infof(`
	Number of tables: %d 
	Batch size in robdb request: %d 
	Number of columns: %d`,
		len(metadata.FeatureGroupFeatures), len(metadata.FeatureGroupFeatures), metadata.NumOfFeatures)

	var fsReqs = make([]string, 0, nrows)

	rows, pks, cols, _ := getSampleData(fsName, fvName, fvVersion)
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		reqBody := fsReq.String()
		fsReqs = append(fsReqs, reqBody)
	}

	integrationtests.RunRestTemplate(b, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(), &fsReqs, totalNumRequest)

}
