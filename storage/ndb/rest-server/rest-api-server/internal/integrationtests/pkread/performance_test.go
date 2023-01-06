/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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

package pkread

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

/*
	go test \
		-test.bench BenchmarkSimple \
		-test.run=thisexpressionwontmatchanytest \
		-cpu 1,2,4,8 \
		-benchmem \
		-benchtime=100x \ 		// 100 times
		-benchtime=10s \ 		// 10 sec
		./internal/router/handler/pkread/
*/
func BenchmarkSimple(b *testing.B) {
	numOps := b.N
	b.Logf("numOps: %d", numOps)

	table := "table_1"
	maxRows := testdbs.BENCH_DB_NUM_ROWS
	opCount := 0
	threadId := 0
	integrationtests.WithDBs(b, []string{testdbs.Benchmark}, func(tc testutils.TlsContext) {
		b.ResetTimer()
		start := time.Now()

		b.RunParallel(func(bp *testing.PB) {
			url := integrationtests.NewPKReadURL(testdbs.Benchmark, table)
			operationId := fmt.Sprintf("operation_%d", threadId)
			threadId++

			opCount++
			reqBody := createReq(b, maxRows, opCount, operationId)

			for bp.Next() {
				integrationtests.SendHttpRequest(b, tc, config.PK_HTTP_VERB, url, reqBody, http.StatusOK, "")
			}
		})
		b.StopTimer()

		opsPerSecond := float64(numOps) / time.Since(start).Seconds()
		nanoSecondsPerOp := float64(time.Since(start).Nanoseconds()) / float64(numOps)
		b.Logf("Throughput: %f operations/second", opsPerSecond)
		b.Logf("Latency: 	%f nanoseconds/operation", nanoSecondsPerOp)
	})
}

func createReq(b *testing.B, maxRows, opCount int, operationId string) string {
	rowId := opCount % maxRows
	col := "id0"
	param := api.PKReadBody{
		Filters:     integrationtests.NewFilter(&col, rowId),
		ReadColumns: integrationtests.NewReadColumns("col_", 1),
		OperationID: &operationId,
	}
	body, err := json.Marshal(param)
	if err != nil {
		b.Fatalf("failed marshaling body; error: %v", err)
	}
	return string(body)
}

func BenchmarkMT(b *testing.B) {
	numOps := b.N
	b.Logf("numOps: %d", numOps)

	table := "table_1"
	maxRows := 1000

	numThreads := 1 // this is for experimentation
	donePerThread := make([]chan bool, numThreads)
	for i := 0; i < numThreads; i++ {
		donePerThread[i] = make(chan bool)
	}

	sharedLoad := make(chan int, numOps)
	for operationId := 0; operationId < numOps; operationId++ {
		sharedLoad <- operationId
	}

	integrationtests.WithDBs(b, []string{testdbs.Benchmark}, func(tc testutils.TlsContext) {
		b.ResetTimer()
		start := time.Now()

		for _, done := range donePerThread {
			go runner(b, tc, testdbs.Benchmark, table, maxRows, sharedLoad, done)
		}

		for _, done := range donePerThread {
			<-done
		}

		b.StopTimer()
		opsPerSecond := float64(numOps) / time.Since(start).Seconds()
		nanoSecondsPerOp := float64(time.Since(start).Nanoseconds()) / float64(b.N)
		b.Logf("Throughput: %f operations/second", opsPerSecond)
		b.Logf("Latency: 	%f nanoseconds/operation", nanoSecondsPerOp)
	})
}

func runner(b *testing.B, tc testutils.TlsContext, db string, table string, maxRowID int, load chan int, done chan bool) {
	for {
		select {
		case opId := <-load:
			rowId := opId % maxRowID
			url := integrationtests.NewPKReadURL(db, table)
			col := "id0"
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilter(&col, rowId),
				ReadColumns: integrationtests.NewReadColumns("col_", 1),
				OperationID: integrationtests.NewOperationID(5),
			}
			body, _ := json.Marshal(param)

			integrationtests.SendHttpRequest(b, tc, config.PK_HTTP_VERB, url, string(body), http.StatusOK, "")
		default:
			done <- true
		}
	}
}
