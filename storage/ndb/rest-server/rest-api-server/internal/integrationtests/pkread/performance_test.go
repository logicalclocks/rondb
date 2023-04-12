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
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

/*
The number of parallel client go-routines spawned in RunParallel()
can be influenced by setting runtime.GOMAXPROCS(). It defaults to the
number of CPUs.

This tends to deliver best results for pkread:

	`runtime.GOMAXPROCS(runtime.NumCPU() * 2)`

	go test \
		-test.bench BenchmarkSimple \
		-test.run=thisexpressionwontmatchanytest \
		-cpu 1,2,4,8 \
		-benchmem \
		-benchtime=100x \ 		// 100 times
		-benchtime=10s \ 		// 10 sec
		./internal/integrationtests/pkread/
*/
func BenchmarkSimple(b *testing.B) {
	// Number of total requests
	numRequests := b.N

	/*
		IMPORTANT: This benchmark will run requests against EITHER the REST or
		the gRPC server, depending on this flag.
	*/
	runAgainstGrpcServer := true

	table := "table_1"
	maxRows := testdbs.BENCH_DB_NUM_ROWS
	threadId := 0

	latenciesChannel := make(chan time.Duration, numRequests)

	b.ResetTimer()
	start := time.Now()

	/*
		Assuming GOMAXPROCS is not set, a 10-core CPU
		will run 10 Go-routines here.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {
		col := "id0"

		// Every go-routine will always use the same operation id
		operationId := fmt.Sprintf("operation_%d", threadId)
		threadId++

		validateColumns := []interface{}{"col0"}
		testInfo := api.PKTestInfo{
			PkReq: api.PKReadBody{
				// Fill out Filters later
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: &operationId,
			},
			Table:          table,
			Db:             testdbs.Benchmark,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		}

		// One connection per go-routine
		var err error
		var grpcConn *grpc.ClientConn
		if runAgainstGrpcServer {
			conf := config.GetAll()
			grpcConn, err = testutils.CreateGrpcConn(conf.Security.APIKey.UseHopsworksAPIKeys, conf.Security.TLS.EnableTLS)
			if err != nil {
				b.Fatal(err.Error())
			}
		}

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {
			// Every request queries a random row
			filter := testclient.NewFilter(&col, rand.Intn(maxRows))
			testInfo.PkReq.Filters = filter

			requestStartTime := time.Now()
			if runAgainstGrpcServer {
				pkGRPCTestWithConn(b, testInfo, false, false, grpcConn)
			} else {
				pkRESTTest(b, testInfo, false, false)
			}
			latenciesChannel <- time.Since(requestStartTime)
		}
	})
	b.StopTimer()

	requestsPerSecond := float64(numRequests) / time.Since(start).Seconds()

	latencies := make([]time.Duration, numRequests)
	for i := 0; i < numRequests; i++ {
		latencies[i] = <-latenciesChannel
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[int(float64(numRequests)*0.5)]
	p99 := latencies[int(float64(numRequests)*0.99)]

	b.Logf("Number of requests:         %d", numRequests)
	b.Logf("Number of threads:          %d", threadId)
	b.Logf("Throughput:                 %f pk lookups/second", requestsPerSecond)
	b.Logf("50th percentile latency:    %v μs", p50.Microseconds())
	b.Logf("99th percentile latency:    %v μs", p99.Microseconds())
	b.Log("-------------------------------------------------")
}
