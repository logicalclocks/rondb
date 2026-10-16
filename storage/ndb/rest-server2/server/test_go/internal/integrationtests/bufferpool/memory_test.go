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

package bufferpool

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
)

// RONDB-1144 memory regression. The reason for the whole change: a pod
// with a 500 MiB limit was OOM-killed within seconds of a wide-row batch
// load, because every in-flight HTTP request held 12 MiB of pool buffers
// that were never returned to the OS. This test drives the dedicated
// rdrs2 of rdrs2-golang_memory_regression (MaxBuffers=256,
// PreAllocatedBuffers=8, BufferShrinkAfterS=30, NumThreads=8) with
// widest-row batches from two workers for a minute, samples RssAnon from
// /proc/<pid>/status every few seconds, and asserts two things:
//   - no creep: once warm (10 s in), no sample exceeds the warm value by
//     more than 10 % plus 32 MiB;
//   - a bound derived from the computed sizes: the final sample is at most
//     the idle RSS plus MaxBuffers pairs of buffers plus 64 MiB of slack
//     for malloc's per-thread arenas (uncapped), stacks and the NDB API.
// Linux only (it needs /proc); the MTR test sources include/linux.inc so
// the Go test is never reached elsewhere, and it skips on its own too.

const (
	memWorkers       = 2
	memSampleEvery   = 5 * time.Second
	memWarmAfter     = 10 * time.Second
	memCreepFraction = 0.10
	memCreepSlackKB  = 32 * 1024
	memBoundSlackKB  = 64 * 1024
)

type rssSample struct {
	at    time.Duration
	rssKB int64
}

// rssAnonKB reads RssAnon of pid from /proc. The kernel reports it in kB.
func rssAnonKB(t *testing.T, pid int) int64 {
	t.Helper()
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatalf("open /proc/%d/status: %v", pid, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "RssAnon:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatalf("RssAnon line %q: %v", line, err)
		}
		return kb
	}
	t.Fatalf("no RssAnon in /proc/%d/status", pid)
	return 0
}

func readPid(t *testing.T, pidfile string) int {
	t.Helper()
	raw, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatalf("read pid file %q: %v", pidfile, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("pid file %q holds %q", pidfile, raw)
	}
	return pid
}

// wideBatchOps is how many widest-row operations one batch carries so
// that both workers together stay inside the pool cap with room to spare.
// An operation that names no columns reserves the room for every column
// name of the table, so floor(ReqBufferBytes / MinReqBufferBytes) of them
// share one request buffer; the responses are far smaller than that and
// never the limiting side.
func wideBatchOps(limits testutils.Limits, prealloc int64) int {
	perReqBuffer := limits.ReqBufferBytes / limits.MinReqBufferBytes
	if perReqBuffer < 1 {
		perReqBuffer = 1
	}
	// Half the cap above the preallocation, split across the workers.
	buffersPerWorker := (limits.MaxBuffers - prealloc) / 2 / memWorkers
	ops := buffersPerWorker * perReqBuffer
	if ops > limits.MaxOperationsPerBatchRequest {
		ops = limits.MaxOperationsPerBatchRequest
	}
	if ops < 1 {
		ops = 1
	}
	return int(ops)
}

func TestMemoryBoundedUnderWideRowBatchLoad(t *testing.T) {
	if os.Getenv("RDRS_MEM_TEST") != "1" {
		t.Skip("set RDRS_MEM_TEST=1 (driven by rdrs2-golang_memory_regression.test)")
	}
	if runtime.GOOS != "linux" {
		t.Skip("needs /proc/<pid>/status")
	}
	limits := testutils.GetLimits(t)
	if want := envInt(t, "RDRS_MEM_MAX_BUFFERS"); limits.MaxBuffers != want {
		t.Fatalf("MaxBuffers override not applied: rdrs_limits=%d cnf=%d", limits.MaxBuffers, want)
	}
	prealloc := envInt(t, "RDRS_MEM_PREALLOCATED")
	pid := readPid(t, os.Getenv("RDRS_MEM_PIDFILE"))
	duration := 60 * time.Second
	if s := os.Getenv("RDRS_MEM_SECONDS"); s != "" {
		duration = time.Duration(envInt(t, "RDRS_MEM_SECONDS")) * time.Second
	}

	// A printable fill: no JSON escape growth, so the response of a widest
	// row is about its row size and a batch needs tens of buffers, not
	// hundreds.
	spec := testutils.WidestRowSpec(limits)
	const table = "wide"
	testutils.CreateWideRowTable(t, poolDB(), table, spec, map[int]byte{1: 'a'})

	ops := wideBatchOps(limits, prealloc)
	op := fmt.Sprintf(`{"method":"POST","relative-url":"%s/%s/%s","body":{"filters":[{"column":"id","value":1}]}}`,
		poolDB(), table, config.PK_DB_OPERATION)
	body := `{"operations":[` + strings.Repeat(op+",", ops-1) + op + `]}`
	url := testutils.NewBatchReadURL()

	// Idle RSS before any of our load, then one batch checked in full so a
	// broken response is reported as such and not as a memory number.
	rss0 := rssAnonKB(t, pid)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, body, "", http.StatusOK)

	var (
		requests int64
		failures int64
		firstErr atomic.Value
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// One client shared by the workers (http.Client is safe for concurrent
	// use); built here so a TLS setup failure is reported on the test
	// goroutine.
	client := testutils.SetupHttpClient(t)
	for w := 0; w < memWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
				if err != nil {
					firstErr.CompareAndSwap(nil, err.Error())
					atomic.AddInt64(&failures, 1)
					return
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				atomic.AddInt64(&requests, 1)
				if err != nil {
					firstErr.CompareAndSwap(nil, err.Error())
					atomic.AddInt64(&failures, 1)
					continue
				}
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					firstErr.CompareAndSwap(nil, fmt.Sprintf("status %d: %.200s", resp.StatusCode, data))
					atomic.AddInt64(&failures, 1)
				}
			}
		}()
	}

	// Sample while the workers run.
	start := time.Now()
	var samples []rssSample
	ticker := time.NewTicker(memSampleEvery)
	for time.Since(start) < duration {
		<-ticker.C
		samples = append(samples, rssSample{at: time.Since(start), rssKB: rssAnonKB(t, pid)})
	}
	ticker.Stop()
	close(stop)
	wg.Wait()
	final := rssAnonKB(t, pid)

	pairKB := (limits.ReqBufferBytes + limits.RespBufferBytes) / 1024
	boundKB := rss0 + limits.MaxBuffers*pairKB + memBoundSlackKB
	warmKB := int64(-1)
	for _, s := range samples {
		if s.at >= memWarmAfter {
			warmKB = s.rssKB
			break
		}
	}

	var report strings.Builder
	fmt.Fprintf(&report, "ops per batch %d, workers %d, requests %d, failures %d\n",
		ops, memWorkers, atomic.LoadInt64(&requests), atomic.LoadInt64(&failures))
	fmt.Fprintf(&report, "idle RssAnon %d kB, warm %d kB, final %d kB, bound %d kB "+
		"(idle + %d x %d kB + %d kB)\n", rss0, warmKB, final, boundKB,
		limits.MaxBuffers, pairKB, int64(memBoundSlackKB))
	for _, s := range samples {
		fmt.Fprintf(&report, "%6.1fs %d kB\n", s.at.Seconds(), s.rssKB)
	}
	if path := os.Getenv("RDRS_MEM_REPORT"); path != "" {
		if err := os.WriteFile(path, []byte(report.String()), 0o644); err != nil {
			t.Logf("could not write report %s: %v", path, err)
		}
	}
	t.Log(report.String())

	if f := atomic.LoadInt64(&failures); f != 0 {
		t.Fatalf("%d of %d requests failed under load; first: %v", f, atomic.LoadInt64(&requests), firstErr.Load())
	}
	if n := atomic.LoadInt64(&requests); n < 2*memWorkers {
		t.Fatalf("too few requests completed (%d) for the load to mean anything", n)
	}
	if warmKB < 0 {
		t.Fatalf("no sample after %s; duration %s too short", memWarmAfter, duration)
	}
	creepLimit := int64(float64(warmKB)*(1+memCreepFraction)) + memCreepSlackKB
	for _, s := range samples {
		if s.at >= memWarmAfter && s.rssKB > creepLimit {
			t.Fatalf("RssAnon crept: %d kB at %.0fs, warm %d kB, limit %d kB", s.rssKB, s.at.Seconds(), warmKB, creepLimit)
		}
	}
	if final > boundKB {
		t.Fatalf("RssAnon %d kB exceeds the derived bound %d kB", final, boundKB)
	}
}
