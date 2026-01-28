/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package ping

import (
	"net/http"
	"runtime"
	"sort"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/testutils"
)

func BenchmarkSimple(b *testing.B) {
	// Number of total requests
	numRequests := b.N

	threadId := 0

	latenciesChannel := make(chan time.Duration, numRequests)

	b.ResetTimer()
	start := time.Now()
	runtime.GOMAXPROCS(16)

	/*
		With 10-core CPU, this will run 10 Go-routines.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {
		threadId++

		// One connection per go-routine
		var httpClient *http.Client
		httpClient = testutils.SetupHttpClient(b)

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {
			requestStartTime := time.Now()
			sendRestPingRequestWithClient(b, httpClient)
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
	b.Logf("Throughput:                 %f requests/second", requestsPerSecond)
	b.Logf("50th percentile latency:    %v μs", p50.Microseconds())
	b.Logf("99th percentile latency:    %v μs", p99.Microseconds())
	b.Log("-------------------------------------------------")
}
