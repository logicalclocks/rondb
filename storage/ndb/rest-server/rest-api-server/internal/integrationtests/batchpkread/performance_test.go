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
	"encoding/base64"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
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

The higher the batch size, the higher the GOMAXPROCS can be set to deliver best results.

This tests can be run as follows:

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
	// Number of total requests
	numRequests := b.N
	const batchSize = 100

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

		// Every go-routine will always query the same row
		threadId++

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

	numTotalPkLookups := numRequests * batchSize
	PkLookupsPerSecond := float64(numTotalPkLookups) / time.Since(start).Seconds()

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
	b.Logf("Batch size (per requests):  %d", batchSize)
	b.Logf("Number of threads:          %d", threadId)
	b.Logf("Throughput:                 %f pk lookups/second", PkLookupsPerSecond)
	b.Logf("50th percentile latency:    %v ms", p50.Milliseconds())
	b.Logf("99th percentile latency:    %v ms", p99.Milliseconds())
	b.Log("-------------------------------------------------")
}

func BenchmarkBinary(b *testing.B) {
	// Number of total requests
	numRequests := b.N
	const batchSize = 100

	/*
		IMPORTANT: This benchmark will run requests against EITHER the REST or
		the gRPC server, depending on this flag.
	*/
	runAgainstGrpcServer := true

	table := "table_2"
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

		// Every go-routine will always query the same row
		threadId++

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
			rowIdByte := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(rand.Intn(maxRows))))

			// Every request queries a random rows
			for _, op := range batchTestInfo.Operations {
				op.SubOperation.Body.Filters = testclient.NewFilter(&col, rowIdByte)
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

	numTotalPkLookups := numRequests * batchSize
	PkLookupsPerSecond := float64(numTotalPkLookups) / time.Since(start).Seconds()

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
	b.Logf("Batch size (per requests):  %d", batchSize)
	b.Logf("Number of threads:          %d", threadId)
	b.Logf("Throughput:                 %f pk lookups/second", PkLookupsPerSecond)
	b.Logf("50th percentile latency:    %v ms", p50.Milliseconds())
	b.Logf("99th percentile latency:    %v ms", p99.Milliseconds())
	b.Log("-------------------------------------------------")
}
