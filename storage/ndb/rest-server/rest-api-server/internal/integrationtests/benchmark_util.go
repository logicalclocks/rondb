/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023,2025 Hopsworks AB
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
	"encoding/hex"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	"golang.org/x/exp/rand"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
)

type RandomRequest interface {
	GetRandomRequest(b *testing.B) (string, string, string)
	GetBatchSize() int
}

func WrapperBenchmark(b *testing.B, req RandomRequest) {
	fmt.Printf("Running Benchmark %#v\n", req)

	// Number of total requests
	numRequests := b.N

	latenciesChannel := make(chan time.Duration, numRequests)

	b.ResetTimer()

	start := time.Now()
	last := time.Now()
	var ops atomic.Uint64
	runtime.GOMAXPROCS(20)
	threadId := 0
	batchSize := req.GetBatchSize()

	/*
		Assuming GOMAXPROCS is not set, a 10-core CPU
		will run 10 Go-routines here.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {

		// Every go-routine will always query the same row
		threadId++

		// create a request
		verb, url, body := req.GetRandomRequest(b)

		// One http connection per go-routine
		var httpClient *http.Client
		httpClient = testutils.SetupHttpClient(b)

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {

			requestStartTime := time.Now()

			testclient.SendHttpRequestWithClient(b, httpClient, verb, url, body, "", http.StatusOK)

			latenciesChannel <- time.Since(requestStartTime)
			count := ops.Add(1)
			duration := uint64(20000)
			if count%duration == 0 {
				tempTotalPkLookups := duration * uint64(batchSize)
				tempPkLookupsPerSecond := float64(tempTotalPkLookups) / time.Since(last).Seconds()
				tempTotalRESTLookupsPerSecond := float64(duration) / time.Since(last).Seconds()
				b.Logf("Throughput: %f PK op/second, %f REST op/second", tempPkLookupsPerSecond, tempTotalRESTLookupsPerSecond)
				last = time.Now()
			}
		}
	})
	b.StopTimer()

	numTotalLookups := numRequests * batchSize
	pkOpsPerSecond := float64(numTotalLookups) / time.Since(start).Seconds()
	opsPerSecond := float64(numRequests) / time.Since(start).Seconds()

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
	b.Logf("Throughput:                 %f PK ops/second", pkOpsPerSecond)
	b.Logf("Throughput:                 %f REST ops/second", opsPerSecond)
	b.Logf("50th percentile latency:    %v us", p50.Microseconds())
	b.Logf("99th percentile latency:    %v us", p99.Microseconds())
	b.Log("--------------------------------------------------------------------------------")
}

// generate additional data in the fsdb002.sample_complex_type_512_1
func generateRandomComplexData(rows int, cols int) {

	schemaStr := `[
       "null",
       {
         "type": "array",
         "items": ["null", "long"]
       }
     ]`

	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		log.Fatalf("Failed to create codec: %v", err)
	}

	//random arrays
	rand.Seed(uint64(time.Now().UnixNano()))
	for i := 0; i < rows; i++ {
		testArray := map[string]interface{}{
			"array": generateArray(cols),
		}

		encodedData, err := codec.BinaryFromNative(nil, testArray)
		if err != nil {
			log.Fatalf("Failed to encode data: %v", err)
		}

		// Convert the encoded data to hex
		hexData := hex.EncodeToString(encodedData)

		// id := rand.Int63()
		id, err := uuid.NewRandom()
		if err != nil {
			fmt.Printf("Failed to generate UUID: %v\n", err)
			return
		}
		err = testutils.RunQueriesOnDataCluster(fmt.Sprintf("INSERT INTO fsdb002.sample_complex_type_512_1 VALUES ( \"%s\", 0x%s );\n", id.String(), hexData))
		if err != nil {
			fmt.Printf("Failed to insert data %v\n", err)
		}
	}
}

func generateArray(n int) []interface{} {
	rand.Seed(uint64(time.Now().UnixNano()))
	array := make([]interface{}, n)

	for i := 0; i < n; i++ {
		if rand.Intn(2) == 0 {
			array[i] = nil // Randomly insert null
		} else {
			array[i] = map[string]interface{}{"long": rand.Int63()}
		}
	}

	return array
}
