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
package testutils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// RONDB-1144: read the server's Prometheus text at /metrics. Tests take the
// limits rdrs2 derived (rdrs_limits) and the buffer pool state
// (rdrs_buffer_pool, rdrs_buffer_pool_events) from here, never from the Go
// config mirror, so the same test runs on a branch with other NDB limits.

// Sample is one line of a metric family: its label set and value.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// Metrics maps a family name to its samples.
type Metrics map[string][]Sample

var (
	sampleRe = regexp.MustCompile(`^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)`)
	labelRe  = regexp.MustCompile(`([A-Za-z0-9_]+)="((?:[^"\\]|\\.)*)"`)
)

// parseMetricsText parses Prometheus text exposition format. Comment and
// blank lines are skipped; a line that is not a sample is ignored.
func parseMetricsText(text string) Metrics {
	m := Metrics{}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		match := sampleRe.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		value, err := strconv.ParseFloat(match[3], 64)
		if err != nil {
			continue
		}
		labels := map[string]string{}
		for _, lm := range labelRe.FindAllStringSubmatch(match[2], -1) {
			labels[lm[1]] = lm[2]
		}
		m[match[1]] = append(m[match[1]], Sample{Labels: labels, Value: value})
	}
	return m
}

// ScrapeMetrics fetches /metrics of the configured server. The route has no
// API version prefix and needs no API key.
func ScrapeMetrics(t testing.TB) Metrics {
	t.Helper()
	client := SetupHttpClient(t)
	resp, err := client.Get(NewMetricsURL())
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /metrics failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d, body %s", resp.StatusCode, string(body))
	}
	return parseMetricsText(string(body))
}

// Has reports whether family has a sample carrying every given label.
func (m Metrics) Has(family string, labels map[string]string) bool {
	_, ok := m.find(family, labels)
	return ok
}

// Get returns the value of the sample of family carrying every given label,
// rounded to an integer; it fails the test when there is none.
func (m Metrics) Get(t testing.TB, family string, labels map[string]string) int64 {
	t.Helper()
	v, ok := m.find(family, labels)
	if !ok {
		t.Fatalf("metric %s%v not found", family, labels)
	}
	return int64(math.Round(v))
}

func (m Metrics) find(family string, labels map[string]string) (float64, bool) {
	for _, s := range m[family] {
		matches := true
		for k, v := range labels {
			if s.Labels[k] != v {
				matches = false
				break
			}
		}
		if matches {
			return s.Value, true
		}
	}
	return 0, false
}

// Pool label values of rdrs_buffer_pool and rdrs_buffer_pool_events.
const (
	PoolRequest  = "request"
	PoolResponse = "response"
)

// PoolStats is the state of one pool, in buffers.
type PoolStats struct {
	InUse, Free, Peak, Allocated int64
}

// PoolSnapshot is both pools plus their cumulative event counters
// ([pool][event], events: rejected, retry, shrunk, allocated).
type PoolSnapshot struct {
	Req, Resp PoolStats
	Events    map[string]map[string]int64
}

func poolStats(t testing.TB, m Metrics, pool string) PoolStats {
	t.Helper()
	get := func(state string) int64 {
		return m.Get(t, "rdrs_buffer_pool", map[string]string{"pool": pool, "state": state})
	}
	return PoolStats{InUse: get("in_use"), Free: get("free"), Peak: get("peak"), Allocated: get("allocated")}
}

// TakePoolSnapshot scrapes /metrics and returns the buffer pool state.
func TakePoolSnapshot(t testing.TB) PoolSnapshot {
	t.Helper()
	m := ScrapeMetrics(t)
	snap := PoolSnapshot{
		Req:    poolStats(t, m, PoolRequest),
		Resp:   poolStats(t, m, PoolResponse),
		Events: map[string]map[string]int64{PoolRequest: {}, PoolResponse: {}},
	}
	for _, s := range m["rdrs_buffer_pool_events"] {
		pool, event := s.Labels["pool"], s.Labels["event"]
		if _, ok := snap.Events[pool]; ok {
			snap.Events[pool][event] = int64(math.Round(s.Value))
		}
	}
	return snap
}

// EventDelta is how much the counter for pool/event moved between two
// snapshots.
func EventDelta(before, after PoolSnapshot, pool, event string) int64 {
	return after.Events[pool][event] - before.Events[pool][event]
}

// WaitPool polls the pool state every 500 ms until cond holds or timeout
// passes, calling between (when not nil) before each poll. It returns the
// last snapshot and fails the test on timeout.
func WaitPool(t testing.TB, timeout time.Duration, cond func(PoolSnapshot) bool, between func()) PoolSnapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if between != nil {
			between()
		}
		snap := TakePoolSnapshot(t)
		if cond(snap) {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool state did not reach the expected condition within %v; last: %+v", timeout, snap)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// AssertPoolBalanced waits up to two seconds for in_use of both pools to
// return to what it was in before. Buffers are returned after the HTTP
// reply is queued, so a scrape immediately after a response can still see
// them out.
func AssertPoolBalanced(t testing.TB, before PoolSnapshot) {
	t.Helper()
	WaitPool(t, 2*time.Second, func(s PoolSnapshot) bool {
		return s.Req.InUse == before.Req.InUse && s.Resp.InUse == before.Resp.InUse
	}, nil)
}

// Limits is what rdrs2 derived from its build, configuration and memory
// limit, as exported by the rdrs_limits family.
type Limits struct {
	MaxColumns                   int64
	MaxRowBytes                  int64
	ReqBufferBytes               int64
	RespBufferBytes              int64
	MinReqBufferBytes            int64
	MinRespBufferBytes           int64
	MaxOperationsPerBatchRequest int64
	MaxOperationsPerNdbExecute   int64
	MaxBlobTextValueBytes        int64
	MaxBuffers                   int64
	MemoryLimitBytes             int64
}

// GetLimits reads rdrs_limits; every field must be present. A server that
// predates the family (an rdrs2 without RONDB-1144, benchmarked side by
// side with a current one) has no limits to report; for that case only,
// RDRS_LIMITS_OVERRIDE carries the Limits struct as JSON and is used when
// the family is absent. It never overrides a server that does report.
func GetLimits(t testing.TB) Limits {
	t.Helper()
	m := ScrapeMetrics(t)
	if !m.Has("rdrs_limits", map[string]string{"name": "max_columns"}) {
		if raw := os.Getenv("RDRS_LIMITS_OVERRIDE"); raw != "" {
			var l Limits
			if err := json.Unmarshal([]byte(raw), &l); err != nil {
				t.Fatalf("RDRS_LIMITS_OVERRIDE: %v", err)
			}
			return l
		}
	}
	get := func(name string) int64 {
		return m.Get(t, "rdrs_limits", map[string]string{"name": name})
	}
	return Limits{
		MaxColumns:                   get("max_columns"),
		MaxRowBytes:                  get("max_row_bytes"),
		ReqBufferBytes:               get("req_buffer_bytes"),
		RespBufferBytes:              get("resp_buffer_bytes"),
		MinReqBufferBytes:            get("min_req_buffer_bytes"),
		MinRespBufferBytes:           get("min_resp_buffer_bytes"),
		MaxOperationsPerBatchRequest: get("max_operations_per_batch_request"),
		MaxOperationsPerNdbExecute:   get("max_operations_per_ndb_execute"),
		MaxBlobTextValueBytes:        get("max_blob_text_value_bytes"),
		MaxBuffers:                   get("max_buffers"),
		MemoryLimitBytes:             get("memory_limit_bytes"),
	}
}

func (s PoolSnapshot) String() string {
	return fmt.Sprintf("request %+v response %+v events %v", s.Req, s.Resp, s.Events)
}
