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

package integrationtests

import (
	"math/rand"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
)

func RunRestTemplate(b *testing.B, method, url string, reqs *[]string, numRequests int) {
	var nReq = len(*reqs)
	maxProcs := runtime.GOMAXPROCS(0) // Calling it with "0" does not change anything, it only returns current value for GOMAXPROCS
	log.Infof("GOMAXPROCS: %d", maxProcs)
	numRequests = b.N

	latenciesChannel := make(chan time.Duration, numRequests)
	var threadId int32 = 0
	var numError int32 = 0
	conf := config.GetAll()
	log.Infof("Starting benchmark test in parallel")
	b.ResetTimer()
	start := time.Now()

	b.RunParallel(func(bp *testing.PB) {
		atomic.AddInt32(&threadId, 1)
		var httpClient *http.Client = testutils.SetupHttpClient(b)

		for bp.Next() {
			var req, _ = http.NewRequest(
				method,
				url,
				strings.NewReader((*reqs)[rand.Intn(nReq)]))
			req.Header.Set("Content-Type", "application/json")
			if conf.Security.APIKey.UseHopsworksAPIKeys {
				req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
			}
			requestStartTime := time.Now()
			resp, err := httpClient.Do(req)
			latenciesChannel <- time.Since(requestStartTime)
			if resp == nil || resp.StatusCode != http.StatusOK {
				atomic.AddInt32(&numError, 1)
				log.Infof("Error status code: %d", resp.StatusCode)
				if err != nil {
					log.Info(err.Error())
				} else {
					var respStr []byte = make([]byte, 1000)
					resp.Body.Read(respStr)
					log.Info(string(respStr))
				}
				break
			}
			resp.Body.Close()
		}
		httpClient.CloseIdleConnections()
	})

	b.StopTimer()
	log.Info("Finished all the tests.")
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
	b.Logf("Number of error:            %d", numError)
	b.Logf("Number of threads:          %d", threadId)
	b.Logf("Throughput:                 %f pk lookups/second", requestsPerSecond)
	b.Logf("50th percentile latency:    %v μs", p50.Microseconds())
	b.Logf("99th percentile latency:    %v μs", p99.Microseconds())
	b.Log("-------------------------------------------------")
}
