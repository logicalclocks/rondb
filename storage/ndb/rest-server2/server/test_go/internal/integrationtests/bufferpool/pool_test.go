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

// Package bufferpool drives the RONDB-1144 buffer pool tests against the
// dedicated rdrs2 of mysql-test/suite/rdrs2-golang/t/rdrs2-golang_buffer_pool
// (MaxBuffers=8, PreAllocatedBuffers=2, BufferShrinkAfterS=5, no auth).
// It has no TestMain and skips unless that test sets RDRS_POOL_TEST=1, so
// the generic gotest pass never runs it against the wrong server.
package bufferpool

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

const exhaustedMsg = "buffer pool exhausted"

func envInt(t *testing.T, name string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil {
		t.Fatalf("%s is not set to a number: %q", name, os.Getenv(name))
	}
	return v
}

// requirePoolServer skips outside the MTR driver and checks that the server
// really runs with the pool settings the .cnf asked for.
func requirePoolServer(t *testing.T) testutils.Limits {
	t.Helper()
	if os.Getenv("RDRS_POOL_TEST") != "1" {
		t.Skip("set RDRS_POOL_TEST=1 (driven by rdrs2-golang_buffer_pool.test)")
	}
	limits := testutils.GetLimits(t)
	if want := envInt(t, "RDRS_POOL_MAX_BUFFERS"); limits.MaxBuffers != want {
		t.Fatalf("MaxBuffers override not applied: rdrs_limits=%d cnf=%d", limits.MaxBuffers, want)
	}
	return limits
}

func poolDB() string {
	if db := os.Getenv("POOL_DB"); db != "" {
		return db
	}
	return "pool_db"
}

// opJSON is one batch sub-operation reading pool_db.small id=1. Without
// readColumns the operation request reserves room for every column name of
// the table, the largest a request can be; with them it is a few hundred
// bytes.
func opJSON(withColumns bool) string {
	cols := ""
	if withColumns {
		cols = `,"readColumns":[{"column":"val"}]`
	}
	return fmt.Sprintf(`{"method":"POST","relative-url":"%s/small/%s","body":{"filters":[{"column":"id","value":1}]%s}}`,
		poolDB(), config.PK_DB_OPERATION, cols)
}

func batchBody(n int, withColumns bool) string {
	ops := make([]string, n)
	for i := range ops {
		ops[i] = opJSON(withColumns)
	}
	return `{"operations":[` + strings.Join(ops, ",") + `]}`
}

func sendBatch(t *testing.T, body string, expectedMsg string, codes ...int) int {
	t.Helper()
	code, _ := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, testutils.NewBatchReadURL(), body, expectedMsg, codes...)
	return code
}

func smallReadOK(t *testing.T) {
	t.Helper()
	url := testutils.NewPKReadURL(poolDB(), "small")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, `{"filters":[{"column":"id","value":1}]}`, "", http.StatusOK)
}

// A batch of the maximum number of operations that name no columns needs
// far more request buffers than the cap: the server answers 503 naming the
// pool, counts the rejection, keeps the pool within the cap, returns every
// buffer it took, and serves the next small read.
func TestPoolExhaustedReturns503(t *testing.T) {
	limits := requirePoolServer(t)
	before := testutils.TakePoolSnapshot(t)

	sendBatch(t, batchBody(int(limits.MaxOperationsPerBatchRequest), false), exhaustedMsg,
		http.StatusServiceUnavailable)

	after := testutils.TakePoolSnapshot(t)
	if d := testutils.EventDelta(before, after, testutils.PoolRequest, "rejected") +
		testutils.EventDelta(before, after, testutils.PoolResponse, "rejected"); d < 1 {
		t.Fatalf("rejected counter did not move: before %s after %s", before, after)
	}
	if after.Req.Allocated > limits.MaxBuffers || after.Resp.Allocated > limits.MaxBuffers {
		t.Fatalf("pool grew past MaxBuffers %d: %s", limits.MaxBuffers, after)
	}
	testutils.AssertPoolBalanced(t, before)
	smallReadOK(t)
}

// Grow a pool above PreAllocatedBuffers with batches of increasing size
// (accepting 503 once the cap is hit), then watch it shrink back after the
// idle period while small reads keep flowing.
func TestPoolShrinksAfterIdle(t *testing.T) {
	limits := requirePoolServer(t)
	prealloc := envInt(t, "RDRS_POOL_PREALLOCATED")
	shrinkAfter := envInt(t, "RDRS_POOL_SHRINK_AFTER_S")

	grown := false
	for n := 4; n <= int(limits.MaxOperationsPerBatchRequest); n *= 2 {
		sendBatch(t, batchBody(n, false), "", http.StatusOK, http.StatusServiceUnavailable)
		snap := testutils.TakePoolSnapshot(t)
		if snap.Req.Allocated > prealloc {
			grown = true
			break
		}
	}
	if !grown {
		t.Fatalf("could not grow the request pool above PreAllocatedBuffers=%d", prealloc)
	}

	before := testutils.TakePoolSnapshot(t)
	timeout := time.Duration(3*shrinkAfter+10) * time.Second
	after := testutils.WaitPool(t, timeout, func(s testutils.PoolSnapshot) bool {
		return s.Req.Allocated == prealloc && s.Resp.Allocated <= prealloc
	}, func() { smallReadOK(t) })
	if d := testutils.EventDelta(before, after, testutils.PoolRequest, "shrunk"); d < 1 {
		t.Fatalf("shrunk counter did not move: before %s after %s", before, after)
	}
	if after.Req.InUse != 0 || after.Req.Free != after.Req.Allocated {
		t.Fatalf("pool not idle after shrink: %s", after)
	}
	if limits.MaxBuffers < prealloc {
		t.Fatalf("limits inconsistent: MaxBuffers %d < PreAllocatedBuffers %d", limits.MaxBuffers, prealloc)
	}
}

// With StartReserveOverride far below the computed reserve, a batch of
// widest rows lets a response start near the end of a buffer and overflow
// it; the server rewrites that response in a fresh buffer (rule 5), counts
// a retry, and every value is still correct.
func TestRetryWithLoweredStartReserve(t *testing.T) {
	limits := requirePoolServer(t)
	if _, ok := os.LookupEnv("RDRS_POOL_START_RESERVE"); !ok {
		t.Skip("RDRS_POOL_START_RESERVE not set")
	}
	spec := testutils.WidestRowSpec(limits)
	const table = "widest"
	testutils.CreateWideRowTable(t, poolDB(), table, spec, map[int]byte{1: 1})

	before := testutils.TakePoolSnapshot(t)
	// Enough widest responses to walk past the end of two buffers. Each
	// response carries per column a 16-byte column table entry, the escaped
	// value and up to 8 bytes of terminator and alignment, and every fill
	// byte escapes to six (\u0001), so its size is
	// known closely; MinRespBufferBytes would be the worst case for the
	// limits, not for this row, and on 26.10 the creatable widest row is
	// much narrower than that worst case.
	perOp := int64(spec.NCols) * (16 + 8 + 6*int64(spec.Width))
	n := int(2*limits.RespBufferBytes/perOp) + 2
	ops := make([]string, n)
	for i := range ops {
		ops[i] = fmt.Sprintf(`{"method":"POST","relative-url":"%s/%s/%s","body":{"filters":[{"column":"id","value":1}]}}`,
			poolDB(), table, config.PK_DB_OPERATION)
	}
	body := `{"operations":[` + strings.Join(ops, ",") + `]}`
	_, resp := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, testutils.NewBatchReadURL(), body, "", http.StatusOK)
	var res api.BatchResponseJSON
	if err := json.Unmarshal(resp, &res); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if res.Result == nil || len(*res.Result) != n {
		t.Fatalf("expected %d sub-responses", n)
	}
	want := testutils.ExpectedWideValue(spec, 1)
	for i, sub := range *res.Result {
		if sub.Code == nil || *sub.Code != http.StatusOK || sub.Body.Data == nil {
			t.Fatalf("sub-operation %d failed: %v", i, sub.Code)
		}
		if len(*sub.Body.Data) != spec.NCols+1 {
			t.Fatalf("sub-operation %d: %d columns", i, len(*sub.Body.Data))
		}
		for c := 0; c < spec.NCols; c++ {
			raw := (*sub.Body.Data)[spec.ColumnName(c)]
			var s string
			if raw == nil || json.Unmarshal(*raw, &s) != nil || s != want {
				t.Fatalf("sub-operation %d column %d differs", i, c)
			}
		}
	}
	after := testutils.TakePoolSnapshot(t)
	if d := testutils.EventDelta(before, after, testutils.PoolResponse, "retry"); d < 1 {
		t.Fatalf("expected at least one retry with the lowered reserve: %s -> %s", before, after)
	}
	testutils.AssertPoolBalanced(t, before)
}

// Malformed batches after a valid operation: whatever was taken for the
// operations before the error is returned. The status of a malformed body
// is the parser's business (today 400 or, for some simdjson errors, 500);
// this test only cares that no buffer leaks.
func TestBatchParseErrorAfterValidOpReleasesBuffers(t *testing.T) {
	requirePoolServer(t)
	before := testutils.TakePoolSnapshot(t)

	// Value missing after the colon.
	broken := `{"operations":[` + opJSON(true) + `,{"method":"POST","relative-url":"` + poolDB() +
		`/small/` + config.PK_DB_OPERATION + `","body":{"filters":[{"column":"id","value":}]}}]}`
	sendBatch(t, broken, "", http.StatusBadRequest, http.StatusInternalServerError)
	testutils.AssertPoolBalanced(t, before)

	// Well-formed JSON, unknown column: rejected by the server either at
	// parse time or per operation.
	unknown := `{"operations":[` + opJSON(true) + `,{"method":"POST","relative-url":"` + poolDB() +
		`/small/` + config.PK_DB_OPERATION + `","body":{"filters":[{"column":"no_such_col","value":1}]}}]}`
	sendBatch(t, unknown, "", http.StatusOK, http.StatusBadRequest)
	testutils.AssertPoolBalanced(t, before)

	// Body cut off after the first operation.
	truncated := `{"operations":[` + opJSON(true) + `,{"method":"POST","relative-url":"` + poolDB()
	sendBatch(t, truncated, "", http.StatusBadRequest, http.StatusInternalServerError)
	testutils.AssertPoolBalanced(t, before)
}
