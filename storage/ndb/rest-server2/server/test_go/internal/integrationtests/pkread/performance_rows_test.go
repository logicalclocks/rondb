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

package pkread

import (
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/integrationtests/spillfixture"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144 benchmarks for the single pk-read response writer. The response
// buffer shrank from 10 MiB to RespBufferBytes (512 KiB on 26.02, 1 MiB on
// 26.10) and a value that does not fit the
// room left spills into continuation buffers, which the JSON writer then
// reads as a segment list. Every size below is taken from the limits rdrs2
// reports (on a binary without rdrs_limits, from RDRS_LIMITS_OVERRIDE; see
// GetLimits) so the same benchmark compares the old and the new binary on one
// cluster; the row is prepared once, before the timer, and the benchmark
// measures read throughput and latency only. Every response must be 200.

const fourMiB = 4 * 1024 * 1024

// runPKReadBenchmark reads testInfo b.N times, one http.Client per
// go-routine, and reports throughput and latency like BenchmarkSimple. A
// failed request fails the benchmark through testutils.ParallelTB instead
// of hanging it.
func runPKReadBenchmark(b *testing.B, testInfo api.PKTestInfo) {
	var threads atomic.Int32
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)
	ptb := testutils.ParallelTB{TB: b}

	b.ResetTimer()
	start := time.Now()

	// RunParallel starts GOMAXPROCS go-routines that share the b.N requests.
	b.RunParallel(func(bp *testing.PB) {
		threads.Add(1)
		// One connection per go-routine
		httpClient := testutils.SetupHttpClient(ptb)
		for bp.Next() {
			requestStartTime := time.Now()
			pkRESTTestWithClient(ptb, httpClient, testInfo, false, false)
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

	requestsPerSecond := float64(numRequests) / time.Since(start).Seconds()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[int(float64(numRequests)*0.5)]
	p99 := latencies[int(float64(numRequests)*0.99)]

	b.Logf("Number of requests:         %d", numRequests)
	b.Logf("Number of threads:          %d", threads.Load())
	b.Logf("Throughput:                 %f pk lookups/second", requestsPerSecond)
	b.Logf("50th percentile latency:    %v μs", p50.Microseconds())
	b.Logf("99th percentile latency:    %v μs", p99.Microseconds())
	b.Log("-------------------------------------------------")
}

// spillTableRead is a pk-read of the spill fixture's single row asking for
// the given columns only, expected to succeed.
func spillTableRead(cols []string) api.PKTestInfo {
	readCols := make([]api.ReadColumn, len(cols))
	for i := range cols {
		c := cols[i]
		readCols[i] = api.ReadColumn{Column: &c}
	}
	return api.PKTestInfo{
		PkReq: api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id", spillfixture.RowID),
			ReadColumns: &readCols,
			OperationID: testclient.NewOperationID(3),
		},
		Table:    spillfixture.Table,
		Db:       spillfixture.DB,
		HttpCode: http.StatusOK,
	}
}

// BenchmarkWidestRow reads the widest row the limits allow with no columns
// named, so the response has the most column entries the writer ever
// produces (unescaped, so not the largest byte size). A printable fill keeps JSON escaping out of the picture: this
// is the cost of writing every column entry, not of escaping.
func BenchmarkWidestRow(b *testing.B) {
	spec := testutils.WidestRowSpec(testutils.GetLimits(b))
	const table = "widest_row_bench"
	testutils.CreateWideRowTable(b, testdbs.DB013, table, spec, map[int]byte{1: 'a'})

	runPKReadBenchmark(b, api.PKTestInfo{
		PkReq: api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id", 1),
			OperationID: testclient.NewOperationID(3),
		},
		Table:    table,
		Db:       testdbs.DB013,
		HttpCode: http.StatusOK,
	})
}

// BenchmarkBlobHalfBuffer reads a blob of half a response buffer: it fits
// the home buffer, so this is the no-spill baseline the spill benchmarks
// are compared against.
func BenchmarkBlobHalfBuffer(b *testing.B) {
	spillfixture.SetupSpillTable(b)
	size := int(testutils.GetLimits(b).RespBufferBytes)
	testutils.SetBlob(b, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID,
		testutils.DeterministicBytes(size/2, 21))

	runPKReadBenchmark(b, spillTableRead([]string{"b"}))
}

// BenchmarkBlobSpill reads a blob one byte larger than a response buffer:
// it never fits the home buffer and takes exactly one continuation buffer,
// the cheapest spill there is. The difference to BenchmarkBlobHalfBuffer is
// the price of taking a second buffer and reading the value as segments.
func BenchmarkBlobSpill(b *testing.B) {
	spillfixture.SetupSpillTable(b)
	size := int(testutils.GetLimits(b).RespBufferBytes)
	testutils.SetBlob(b, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID,
		testutils.DeterministicBytes(size+1, 22))

	runPKReadBenchmark(b, spillTableRead([]string{"b"}))
}

// BenchmarkBlob4MiB reads a blob that spans many continuation buffers. The
// old binary wrote it into one 10 MiB buffer; the new one chains
// 4 MiB / RespBufferBytes buffers (eight on 26.02, four on 26.10) and
// base64-encodes across their boundaries.
func BenchmarkBlob4MiB(b *testing.B) {
	spillfixture.SetupSpillTable(b)
	limits := testutils.GetLimits(b)
	needed := (int64(fourMiB)+limits.RespBufferBytes-1)/limits.RespBufferBytes + 2
	if needed > limits.MaxBuffers {
		b.Skipf("a 4 MiB blob needs %d buffers, pool cap is %d", needed, limits.MaxBuffers)
	}
	if limit := limits.MaxBlobTextValueBytes; limit != 0 && fourMiB > limit {
		b.Skipf("a 4 MiB blob is over MaxBlobTextValueBytes (%d); raise it on the benchmarked server", limit)
	}
	testutils.SetBlob(b, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID,
		testutils.DeterministicBytes(fourMiB, 23))

	runPKReadBenchmark(b, spillTableRead([]string{"b"}))
}

// BenchmarkTextHalfBuffer reads half a buffer of printable LONGTEXT: TEXT
// goes through charset conversion and JSON escaping before the spill
// decision, and printable latin1 grows through neither, so this isolates
// the conversion and escape pass from any spill.
func BenchmarkTextHalfBuffer(b *testing.B) {
	spillfixture.SetupSpillTable(b)
	size := int(testutils.GetLimits(b).RespBufferBytes)
	testutils.SetBlob(b, spillfixture.DB, spillfixture.Table, "t", spillfixture.RowID,
		testutils.PrintableBytes(size/2, 24))

	runPKReadBenchmark(b, spillTableRead([]string{"t"}))
}
