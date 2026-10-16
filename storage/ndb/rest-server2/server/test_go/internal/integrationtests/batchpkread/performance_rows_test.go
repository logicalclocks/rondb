/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/spillfixture"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144 benchmarks for the batch response writer. A batch no longer
// gets one 10 MiB response buffer: several sub-responses pack into one
// RespBufferBytes buffer (512 KiB on 26.02, 1 MiB on 26.10), a sub-response that does not fit the room left continues
// in the next buffer of the chain, and a blob larger than that room spills
// into continuation buffers of its own. Sizes come from the limits rdrs2
// reports (on a binary without rdrs_limits, from RDRS_LIMITS_OVERRIDE; see
// GetLimits) so the same benchmark compares the old and the new binary on one
// cluster; rows are prepared once, before the timer. The batch and every
// sub-operation must answer 200.

// runBatchBenchmark sends b.N batches, one http.Client per go-routine,
// each go-routine on its own copy of the batch from newBatch so that
// beforeRequest (when not nil) may rewrite the filters without racing.
// Throughput and latency are reported like BenchmarkSimple. A failed
// request fails the benchmark through testutils.ParallelTB instead of
// hanging it.
func runBatchBenchmark(
	b *testing.B,
	newBatch func() api.BatchOperationTestInfo,
	beforeRequest func(api.BatchOperationTestInfo),
) {
	batchSize := len(newBatch().Operations)
	var threads atomic.Int32
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)
	ptb := testutils.ParallelTB{TB: b}

	b.ResetTimer()
	start := time.Now()

	// RunParallel starts GOMAXPROCS go-routines that share the b.N requests.
	b.RunParallel(func(bp *testing.PB) {
		threads.Add(1)
		batchTestInfo := newBatch()
		// One connection per go-routine
		httpClient := testutils.SetupHttpClient(ptb)
		for bp.Next() {
			if beforeRequest != nil {
				beforeRequest(batchTestInfo)
			}
			requestStartTime := time.Now()
			batchRESTTestWithClient(ptb, httpClient, batchTestInfo, false, false)
			latency := time.Since(requestStartTime)
			mu.Lock()
			latencies = append(latencies, latency)
			mu.Unlock()
		}
	})
	b.StopTimer()
	if b.Failed() {
		return
	}
	numRequests := len(latencies)

	numTotalPkLookups := numRequests * batchSize
	PkLookupsPerSecond := float64(numTotalPkLookups) / time.Since(start).Seconds()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[int(float64(numRequests)*0.5)]
	p99 := latencies[int(float64(numRequests)*0.99)]

	b.Logf("Number of requests:         %d", numRequests)
	b.Logf("Batch size (per requests):  %d", batchSize)
	b.Logf("Number of threads:          %d", threads.Load())
	b.Logf("Throughput:                 %f pk lookups/second", PkLookupsPerSecond)
	b.Logf("50th percentile latency:    %v us", p50.Microseconds())
	b.Logf("99th percentile latency:    %v us", p99.Microseconds())
	b.Log("-------------------------------------------------")
}

// BenchmarkMaxOperations sends batches of exactly MaxOperationsPerBatchRequest
// reads of the benchmark table, the most sub-requests one request buffer
// has to parse and the most sub-responses one chain has to pack. The
// rows are random, as in BenchmarkSimple, whose 200 operations are the
// comparison point below the limit.
func BenchmarkMaxOperations(b *testing.B) {
	batchSize := int(testutils.GetLimits(b).MaxOperationsPerBatchRequest)
	table := "table_1"
	numRows := testdbs.BENCH_DB_NUM_ROWS
	col := "id0"

	runBatchBenchmark(b, func() api.BatchOperationTestInfo {
		operations := []api.BatchSubOperationTestInfo{}
		for i := 0; i < batchSize; i++ {
			// We will set the pk to filter later
			operations = append(operations, createSubOperation(b, table, testdbs.Benchmark, "", http.StatusOK))
		}
		return api.BatchOperationTestInfo{
			HttpCode:       []int{http.StatusOK},
			Operations:     operations,
			ErrMsgContains: "",
		}
	}, func(batchTestInfo api.BatchOperationTestInfo) {
		// Every request queries a random rows
		for _, op := range batchTestInfo.Operations {
			op.SubOperation.Body.Filters = testclient.NewFilter(&col, rand.Intn(numRows))
		}
	})
}

// BenchmarkWidestRows16 reads the widest row sixteen times in one batch
// with no columns named. Each sub-response is the largest fixed part the
// writer produces, so a few of them fill a buffer and the chain grows
// across several buffers within one batch: this is the packing and
// chaining path with no blob involved.
func BenchmarkWidestRows16(b *testing.B) {
	spec := testutils.WidestRowSpec(testutils.GetLimits(b))
	const table = "widest_row_bench"
	testutils.CreateWideRowTable(b, testdbs.DB013, table, spec, map[int]byte{1: 'a'})

	runBatchBenchmark(b, func() api.BatchOperationTestInfo {
		operations := make([]api.BatchSubOperationTestInfo, 16)
		for i := range operations {
			operations[i] = api.BatchSubOperationTestInfo{
				SubOperation: api.BatchSubOp{
					Method:      &[]string{config.PK_HTTP_VERB}[0],
					RelativeURL: &[]string{string(testdbs.DB013 + "/" + table + "/" + config.PK_DB_OPERATION)}[0],
					Body: &api.PKReadBody{
						Filters:     testclient.NewFiltersKVs("id", 1),
						OperationID: testclient.NewOperationID(5),
					},
				},
				Table:    table,
				DB:       testdbs.DB013,
				HttpCode: []int{http.StatusOK},
			}
		}
		return api.BatchOperationTestInfo{
			HttpCode:   []int{http.StatusOK},
			Operations: operations,
		}
	}, nil)
}

// BenchmarkBlobSpillInBatch alternates four plain int rows with four reads
// of a blob one byte larger than a response buffer. Every blob spills into
// a continuation buffer of its own while the plain rows before and after
// it pack into the shared buffers, so one batch exercises both the packing
// and the spill path and the writer's switching between them.
func BenchmarkBlobSpillInBatch(b *testing.B) {
	spillfixture.SetupSpillTable(b)
	size := int(testutils.GetLimits(b).RespBufferBytes)
	testutils.SetBlob(b, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID,
		testutils.DeterministicBytes(size+1, 25))

	runBatchBenchmark(b, func() api.BatchOperationTestInfo {
		operations := []api.BatchSubOperationTestInfo{}
		for i := 0; i < 4; i++ {
			operations = append(operations, intTableOps(1)[0], api.BatchSubOperationTestInfo{
				SubOperation: spillOp(b, []string{"b"}),
				Table:        spillfixture.Table,
				DB:           spillfixture.DB,
				HttpCode:     []int{http.StatusOK},
			})
		}
		return api.BatchOperationTestInfo{
			HttpCode:   []int{http.StatusOK},
			Operations: operations,
		}
	}, nil)
}
