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
	The number of parallel client go-routines spawned can be influenced by setting
	runtime.GOMAXPROCS(). It defaults to the number of CPUs.

	The higher batch size, the higher the GOMAXPROCS can be set to deliver best results.

	go test \
		-test.bench BenchmarkSimple \
		-test.run=thisexpressionwontmatchanytest \
		-cpu 1,2,4,8 \
		-benchmem \
		-benchtime=100x \ 		// 100 times
		-benchtime=10s \ 		// 10 sec
		./internal/integrationtests/batchpkread/
*/
func BenchmarkSimple(b *testing.B) {
	// Number of total operations
	numOps := b.N
	b.Logf("numOps: %d", numOps)

	/*
		IMPORTANT: This benchmark will run requests against EITHER the REST or
		the gRPC server, depending on this flag.
	*/
	runAgainstGrpcServer := true

	const batchSize = 100

	table := "table_1"
	maxRows := testdbs.BENCH_DB_NUM_ROWS
	threadId := 0

	// Initialize the slice for storing the latencies.
	latencies := make([]time.Duration, numOps)
	latenciesChannel := make(chan time.Duration, numOps)

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

		// Every go-routine will always query the same row
		threadId++
		b.Logf("threadId: %d", threadId)

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

		// One connection per go-routine
		var err error
		var grpcConn *grpc.ClientConn
		if runAgainstGrpcServer {
			conf := config.GetAll()
			grpcConn, err = testutils.CreateGrpcConn(conf.Security.UseHopsworksAPIKeys, conf.Security.EnableTLS)
			if err != nil {
				b.Fatal(err.Error())
			}
		}

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {
			// Every request queries a random rows
			for _, op := range batchTestInfo.Operations {
				op.SubOperation.Body.Filters = testclient.NewFilter(&col, rand.Intn(maxRows))
			}

			requestStartTime := time.Now()
			if runAgainstGrpcServer {
				batchGRPCTestWithConn(b, batchTestInfo, false, false, grpcConn)
			} else {
				batchRESTTest(b, batchTestInfo, false, false)
			}
			latenciesChannel <- time.Since(requestStartTime)
		}
	})
	b.StopTimer()

	numTotalOps := numOps * batchSize
	b.Logf("Batch size: %d", batchSize)
	b.Logf("Number total pk lookups: %d", numTotalOps)

	opsPerSecond := float64(numTotalOps) / time.Since(start).Seconds()
	b.Logf("Throughput: %f pk lookups/second", opsPerSecond)

	for i := 0; i < numOps; i++ {
		latencies[i] = <-latenciesChannel
	}

	// Calculate the 50th and 99th percentile latencies.
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[int(float64(numOps)*0.5)]
	p99 := latencies[int(float64(numOps)*0.99)]
	b.Logf("50th percentile latency: %vms\n", p50.Milliseconds())
	b.Logf("99th percentile latency: %vms\n", p99.Milliseconds())
}
