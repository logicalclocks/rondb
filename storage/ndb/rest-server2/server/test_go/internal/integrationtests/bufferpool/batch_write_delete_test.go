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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

// RONDB-1144 on 26.10: batch pk-write and batch pk-delete place their
// operations in buffer chains like batch pk-read does, and their parsers
// bound the batch the same way. These run on the pool server of
// rdrs2-golang_buffer_pool (no auth, MaxBuffers=8) against pool_db.small
// (id INT PRIMARY KEY, val VARCHAR(32) NOT NULL); the fixture row id=1 is
// left alone, the tests use ids from 1000 up.

const batchWriteFirstID = 1000

func writeOpJSON(id int) string {
	return fmt.Sprintf(`{"method":"POST","relative-url":"%s/small/%s","body":{"filters":[{"column":"id","value":%d}],"writeColumns":[{"column":"val","value":"w%d"}]}}`,
		poolDB(), config.PK_WRITE_OPERATION, id, id)
}

func deleteOpJSON(id int) string {
	return fmt.Sprintf(`{"method":"POST","relative-url":"%s/small/%s","body":{"filters":[{"column":"id","value":%d}]}}`,
		poolDB(), config.PK_DELETE_OPERATION, id)
}

func opsBody(n int, op func(int) string) string {
	ops := make([]string, n)
	for i := range ops {
		ops[i] = op(batchWriteFirstID + i)
	}
	return `{"operations":[` + strings.Join(ops, ",") + `]}`
}

// sendBatchTo sends body to url with the given verb (batch delete is served
// on DELETE) and, on 200, checks that every sub-operation answered 200.
func sendBatchTo(t *testing.T, verb, url, body string, expectedMsg string, codes ...int) {
	t.Helper()
	code, resp := testclient.SendHttpRequest(t, verb, url, body, expectedMsg, codes...)
	if code != http.StatusOK {
		return
	}
	var res api.BatchResponseJSON
	if err := json.Unmarshal(resp, &res); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if res.Result == nil {
		t.Fatalf("no result in batch response")
	}
	for i, sub := range *res.Result {
		if sub.Code == nil || *sub.Code != http.StatusOK {
			msg := ""
			if sub.Message != nil {
				msg = *sub.Message
			}
			code := int32(-1)
			if sub.Code != nil {
				code = *sub.Code
			}
			t.Fatalf("sub-operation %d of %s: code %d %q", i, url, code, msg)
		}
	}
}

// A full batch of writes, then a full batch of deletes of the same rows:
// every request and response buffer the chains took is back in the pool
// after each, and the rows really were written in between.
func TestBatchWriteAndDeleteReleaseBuffers(t *testing.T) {
	limits := requirePoolServer(t)
	n := int(limits.MaxOperationsPerBatchRequest)
	before := testutils.TakePoolSnapshot(t)
	t.Cleanup(func() {
		// Whatever a failed run left behind; after a passing run every row
		// is already gone and the sub-operations answer 404, which is fine.
		testclient.SendHttpRequest(t, config.BATCH_DELETE_HTTP_VERB, testutils.NewBatchDeleteURL(),
			opsBody(n, deleteOpJSON), "", http.StatusOK)
	})

	sendBatchTo(t, config.BATCH_HTTP_VERB, testutils.NewBatchWriteURL(), opsBody(n, writeOpJSON), "", http.StatusOK)
	testutils.AssertPoolBalanced(t, before)

	// One of the written rows, read back through the read path.
	lastID := batchWriteFirstID + n - 1
	_, resp := testclient.SendHttpRequest(t, config.PK_HTTP_VERB, testutils.NewPKReadURL(poolDB(), "small"),
		fmt.Sprintf(`{"filters":[{"column":"id","value":%d}]}`, lastID), "", http.StatusOK)
	var read api.PKReadResponseJSON
	if err := json.Unmarshal(resp, &read); err != nil || read.Data == nil {
		t.Fatalf("read back: %v %s", err, resp)
	}
	var val string
	if raw := (*read.Data)["val"]; raw == nil || json.Unmarshal(*raw, &val) != nil || val != fmt.Sprintf("w%d", lastID) {
		t.Fatalf("row %d: val = %s", lastID, resp)
	}

	sendBatchTo(t, config.BATCH_DELETE_HTTP_VERB, testutils.NewBatchDeleteURL(), opsBody(n, deleteOpJSON), "", http.StatusOK)
	testutils.AssertPoolBalanced(t, before)
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, testutils.NewPKReadURL(poolDB(), "small"),
		fmt.Sprintf(`{"filters":[{"column":"id","value":%d}]}`, lastID), "", http.StatusNotFound)
}

// One operation past MaxOperationsPerBatchRequest: 413 from the parser, before
// any buffer is taken, for writes and for deletes alike.
func TestBatchWriteOverOperationLimit(t *testing.T) {
	limits := requirePoolServer(t)
	before := testutils.TakePoolSnapshot(t)
	sendBatchTo(t, config.BATCH_HTTP_VERB, testutils.NewBatchWriteURL(), opsBody(int(limits.MaxOperationsPerBatchRequest)+1, writeOpJSON),
		"exceeds MaxOperationsPerBatchRequest", http.StatusRequestEntityTooLarge)
	after := testutils.TakePoolSnapshot(t)
	if after.Req.Allocated != before.Req.Allocated || after.Resp.Allocated != before.Resp.Allocated {
		t.Fatalf("a rejected batch took buffers: %s -> %s", before, after)
	}
	testutils.AssertPoolBalanced(t, before)
}

func TestBatchDeleteOverOperationLimit(t *testing.T) {
	limits := requirePoolServer(t)
	before := testutils.TakePoolSnapshot(t)
	sendBatchTo(t, config.BATCH_DELETE_HTTP_VERB, testutils.NewBatchDeleteURL(), opsBody(int(limits.MaxOperationsPerBatchRequest)+1, deleteOpJSON),
		"exceeds MaxOperationsPerBatchRequest", http.StatusRequestEntityTooLarge)
	after := testutils.TakePoolSnapshot(t)
	if after.Req.Allocated != before.Req.Allocated || after.Resp.Allocated != before.Resp.Allocated {
		t.Fatalf("a rejected batch took buffers: %s -> %s", before, after)
	}
	testutils.AssertPoolBalanced(t, before)
}
