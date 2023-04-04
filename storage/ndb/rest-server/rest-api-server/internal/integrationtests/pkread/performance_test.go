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

package pkread

import (
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

/*
	IMPORTANT: This will every request against both the REST & the gRPC server.
		Check pkTest() to comment out either of the server requests.

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
	// Number of total operations
	numOps := b.N
	b.Logf("numOps: %d", numOps)

	table := "table_1"
	maxRows := testdbs.BENCH_DB_NUM_ROWS
	threadId := 0

	b.ResetTimer()
	start := time.Now()

	/*
		With 10-core CPU, this will run 10 Go-routines.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {
		col := "id0"

		// Every go-routine will always query the same row
		operationId := fmt.Sprintf("operation_%d", threadId)
		threadId++

		b.Logf("threadId: %d", threadId)

		validateColumns := []interface{}{"col_0"}
		testInfo := api.PKTestInfo{
			PkReq: api.PKReadBody{
				// Fill out Filters later
				ReadColumns: testclient.NewReadColumns("col_", 1),
				OperationID: &operationId,
			},
			Table:          table,
			Db:             testdbs.Benchmark,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		}

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {
			// Every request queries a random row
			filter := testclient.NewFilter(&col, rand.Intn(maxRows))
			testInfo.PkReq.Filters = filter

			pkTest(b, testInfo, false, false)
		}
	})
	b.StopTimer()

	opsPerSecond := float64(numOps) / time.Since(start).Seconds()
	nanoSecondsPerOp := float64(time.Since(start).Nanoseconds()) / float64(numOps)
	b.Logf("Throughput: %f operations/second", opsPerSecond)
	b.Logf("Latency: 	%f nanoseconds/operation", nanoSecondsPerOp)
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

	b.ResetTimer()
	start := time.Now()

	for _, done := range donePerThread {
		go runner(b, testdbs.Benchmark, table, maxRows, sharedLoad, done)
	}

	for _, done := range donePerThread {
		<-done
	}

	b.StopTimer()
	opsPerSecond := float64(numOps) / time.Since(start).Seconds()
	nanoSecondsPerOp := float64(time.Since(start).Nanoseconds()) / float64(b.N)
	b.Logf("Throughput: %f operations/second", opsPerSecond)
	b.Logf("Latency: 	%f nanoseconds/operation", nanoSecondsPerOp)
}

func runner(b *testing.B, db string, table string, maxRowID int, load chan int, done chan bool) {
	testInfo := api.PKTestInfo{}
	for {
		select {
		case opId := <-load:
			rowId := opId % maxRowID
			col := "id0"
			validateColumns := []interface{}{"col_0"}
			testInfo = api.PKTestInfo{
				PkReq: api.PKReadBody{
					Filters:     testclient.NewFilter(&col, rowId),
					ReadColumns: testclient.NewReadColumns("col_", 1),
					OperationID: testclient.NewOperationID(5),
				},
				Table:          table,
				Db:             db,
				HttpCode:       http.StatusOK,
				ErrMsgContains: "",
				RespKVs:        validateColumns,
			}
			pkTest(b, testInfo, false, false)
		default:
			done <- true
		}
	}
}
