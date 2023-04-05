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

package ping

import (
	"sort"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
)

/*
	The number of parallel client go-routines spawned can be influenced by setting
	runtime.GOMAXPROCS(). It defaults to the number of CPUs.

	go test \
		-test.bench BenchmarkSimple \
		-test.run=thisexpressionwontmatchanytest \
		-cpu 1,2,4,8 \
		-benchmem \
		-benchtime=100x \ 		// 100 times
		-benchtime=10s \ 		// 10 sec
		./internal/integrationtests/ping/
*/
func BenchmarkSimple(b *testing.B) {
	// Number of total operations
	numOps := b.N

	runAgainstGrpcServer := true

	threadId := 0

	latenciesChannel := make(chan time.Duration, numOps)

	b.ResetTimer()
	start := time.Now()

	/*
		With 10-core CPU, this will run 10 Go-routines.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {
		threadId++

		// One connection per go-routine
		conf := config.GetAll()
		grpcConn, err := testutils.CreateGrpcConn(conf.Security.UseHopsworksAPIKeys, conf.Security.EnableTLS)
		if err != nil {
			b.Fatal(err.Error())
		}

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {
			requestStartTime := time.Now()
			if runAgainstGrpcServer {
				sendGrpcPingRequestWithConnection(b, grpcConn)
			} else {
				sendRestPingRequest(b)
			}
			latenciesChannel <- time.Since(requestStartTime)
		}
	})
	b.StopTimer()

	opsPerSecond := float64(numOps) / time.Since(start).Seconds()

	latencies := make([]time.Duration, numOps)
	for i := 0; i < numOps; i++ {
		latencies[i] = <-latenciesChannel
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[int(float64(numOps)*0.5)]
	p99 := latencies[int(float64(numOps)*0.99)]

	b.Logf("Number of operations:       %d", numOps)
	b.Logf("Number of threads:          %d", threadId)
	b.Logf("Throughput:                 %f ops/second", opsPerSecond)
	b.Logf("50th percentile latency:    %v μs", p50.Microseconds())
	b.Logf("99th percentile latency:    %v μs", p99.Microseconds())
	b.Log("-------------------------------------------------")
}
