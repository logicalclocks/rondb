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
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144: the widest row the limits allow, every column CHAR or
// VARCHAR full of control characters (six bytes each once escaped), read
// with no columns named. Its fixed part is the largest a response can have
// and must fit the home buffer without a retry. The spec comes from the
// limits rdrs2 reports: 511 columns on 26.02, 4095 on 26.10. The row's
// byte size is bounded by NDB's part caps on 26.10 and by MaxRowBytes on
// 26.02 (see WidestRowSpec), so the guard here is on the column count and
// a usable width, not on bytes.
func TestWidestRowFitsHomeBuffer(t *testing.T) {
	limits := testutils.GetLimits(t)
	spec := testutils.WidestRowSpec(limits)
	if spec.NCols != int(limits.MaxColumns)-1 || spec.Width < 4 {
		t.Fatalf("spec %+v does not use every column or is narrower than 4: limits %+v", spec, limits)
	}
	const table = "widest_row"
	testutils.CreateWideRowTable(t, testdbs.DB013, table, spec, map[int]byte{1: 1})

	before := testutils.TakePoolSnapshot(t)
	body := api.PKReadBody{
		Filters:     testclient.NewFiltersKVs("id", 1),
		OperationID: testclient.NewOperationID(3),
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	url := testutils.NewPKReadURL(testdbs.DB013, table)
	_, resp := testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(bodyJSON), "", http.StatusOK)
	var pk api.PKReadResponseJSON
	if err := json.Unmarshal(resp, &pk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if pk.Data == nil || len(*pk.Data) != spec.NCols+1 {
		got := 0
		if pk.Data != nil {
			got = len(*pk.Data)
		}
		t.Fatalf("expected %d columns, got %d", spec.NCols+1, got)
	}
	want := testutils.ExpectedWideValue(spec, 1)
	for i := 0; i < spec.NCols; i++ {
		raw := (*pk.Data)[spec.ColumnName(i)]
		if raw == nil {
			t.Fatalf("column %s missing", spec.ColumnName(i))
		}
		var s string
		if err := json.Unmarshal(*raw, &s); err != nil {
			t.Fatalf("column %s: %v", spec.ColumnName(i), err)
		}
		if s != want {
			t.Fatalf("column %s differs (got %d bytes)", spec.ColumnName(i), len(s))
		}
	}
	after := testutils.TakePoolSnapshot(t)
	if d := testutils.EventDelta(before, after, testutils.PoolResponse, "retry"); d != 0 {
		t.Fatalf("the widest row needed %d retries; the start reserve is too small", d)
	}
	testutils.AssertPoolBalanced(t, before)
}
