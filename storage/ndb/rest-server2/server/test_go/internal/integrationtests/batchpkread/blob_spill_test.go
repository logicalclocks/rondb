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
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/spillfixture"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144: a spilling blob inside a batch. The responses before and
// after it are plain rows packed into the same buffers; the blob's
// continuation buffers are appended to the chain between them.

func plainIntOp() api.BatchSubOp {
	return api.BatchSubOp{
		Method:      &[]string{config.PK_HTTP_VERB}[0],
		RelativeURL: &[]string{string(testdbs.DB004 + "/int_table/" + config.PK_DB_OPERATION)}[0],
		Body: &api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
			ReadColumns: testclient.NewReadColumns("col", 2),
		},
	}
}

func spillOp(t testing.TB, cols []string) api.BatchSubOp {
	var body api.PKReadBody
	if err := json.Unmarshal([]byte(spillfixture.ReadBody(t, cols)), &body); err != nil {
		t.Fatal(err)
	}
	return api.BatchSubOp{
		Method:      &[]string{config.PK_HTTP_VERB}[0],
		RelativeURL: &[]string{string(spillfixture.DB + "/" + spillfixture.Table + "/" + config.PK_DB_OPERATION)}[0],
		Body:        &body,
	}
}

func sendOps(t *testing.T, ops []api.BatchSubOp) api.BatchResponseJSON {
	t.Helper()
	batch := api.BatchOpRequest{Operations: &ops}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	_, resp := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, testutils.NewBatchReadURL(), string(body), "", http.StatusOK)
	var res api.BatchResponseJSON
	if err := json.Unmarshal(resp, &res); err != nil {
		t.Fatalf("unmarshal batch response: %v", err)
	}
	if res.Result == nil || len(*res.Result) != len(ops) {
		t.Fatalf("expected %d sub-responses", len(ops))
	}
	for i, sub := range *res.Result {
		if sub.Code == nil || *sub.Code != http.StatusOK {
			t.Fatalf("sub-operation %d: code %v", i, sub.Code)
		}
	}
	return res
}

func TestBatchBlobSpillBetweenPlainRows(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := int(testutils.GetLimits(t).RespBufferBytes)
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(size+1, 11)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want)

	res := sendOps(t, []api.BatchSubOp{plainIntOp(), spillOp(t, []string{"b", "v"}), plainIntOp()})
	for _, i := range []int{0, 2} {
		data := (*res.Result)[i].Body.Data
		if data == nil || (*data)["col0"] == nil || (*data)["col1"] == nil {
			t.Fatalf("plain sub-operation %d lost its columns", i)
		}
	}
	blobData := (*res.Result)[1].Body.Data
	if blobData == nil {
		t.Fatalf("blob sub-operation has no data")
	}
	spillfixture.ExpectBlob(t, *blobData, "b", want)
	if v, _ := spillfixture.Value(t, *blobData, "v"); v != spillfixture.AfterBlob {
		t.Fatalf("v: got %q", v)
	}
	testutils.AssertPoolBalanced(t, before)
}

func TestBatchFourSpillingOps(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := int(testutils.GetLimits(t).RespBufferBytes)
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(size+1, 12)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want)

	ops := make([]api.BatchSubOp, 4)
	for i := range ops {
		ops[i] = spillOp(t, []string{"b"})
	}
	res := sendOps(t, ops)
	for i := range ops {
		data := (*res.Result)[i].Body.Data
		if data == nil {
			t.Fatalf("sub-operation %d has no data", i)
		}
		spillfixture.ExpectBlob(t, *data, "b", want)
	}
	testutils.AssertPoolBalanced(t, before)
}
