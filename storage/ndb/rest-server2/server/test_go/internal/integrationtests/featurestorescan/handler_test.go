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
package featurestorescan

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
	"hopsworks.ai/rdrs2/version"
)

/*
 * Feature views used below (seeded by the Go test fixtures):
 *   fsdb002 / sample_1n2 v1  : sample_1_1 (root, pk id1) joined with
 *                              sample_2_1 (prefix fg2_) on id1  -> star schema
 *   fsdb001 / sample_3 v1    : single FG with composite pk (id1, id2)
 *   fsdb002 / sample_complex_type v1 : single FG with Avro-encoded columns
 *   fsdb001 / sample_1n1_self v1 : sample_1_1 joined with itself (prefix fg1_)
 */

const (
	fsStar    = testdbs.FSDB002
	fvStar    = "sample_1n2"
	fvVersion = 1
)

// ---- primary-key fast path ----

func Test_FastPath_PkEquality_MatchesBatchFeatureStore(t *testing.T) {
	for _, row := range sampleRows(t, 3) {
		req := NewScanRequest(fsStar, fvStar, fvVersion).
			With("filters", Cmp("id1", "EQ", row.Id1))
		got := GetScanResponse(t, req)
		ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries([]int64{row.Id1}))
		if got.Rows != 1 {
			t.Fatalf("expected 1 row for id1=%d, got %d", row.Id1, got.Rows)
		}
		ExpectSameVectors(t, got, ref)
	}
}

func Test_FastPath_AbsentKey_NoRow(t *testing.T) {
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", Cmp("id1", "EQ", 987654321)).
		With("metadataOptions", map[string]bool{"featureName": true, "featureType": true})
	got := GetScanResponse(t, req)
	if got.Rows != 0 || len(got.Features) != 0 || len(got.Status) != 0 {
		t.Fatalf("absent key must produce no row, got %s", got.Features)
	}
	if len(got.Metadata) == 0 {
		t.Errorf("metadata must be emitted even for an empty result when requested")
	}
}

func Test_FastPath_Or_DedupAndBranchOrder(t *testing.T) {
	rows := sampleRows(t, 2)
	a, b := rows[0].Id1, rows[1].Id1
	// a OR b OR a(duplicate) OR a as a string OR a as a float OR absent,
	// as a nested binary tree: every spelling of a key is one branch
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", OrOf(Cmp("id1", "EQ", a), Cmp("id1", "EQ", b),
			Cmp("id1", "EQ", a), Cmp("id1", "EQ", fmt.Sprintf("%d", a)),
			Cmp("id1", "EQ", float64(a)), Cmp("id1", "EQ", 987654321)))
	got := GetScanResponse(t, req)
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries([]int64{a, b}))
	if got.Rows != 2 {
		t.Fatalf("expected 2 rows (dedup + absent dropped), got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)
}

func Test_FastPath_ResidualPredicate(t *testing.T) {
	row := sampleRows(t, 1)[0]
	// residual holds
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", AndOf(Cmp("id1", "EQ", row.Id1), Cmp("data1", "EQ", row.Data1)))
	got := GetScanResponse(t, req)
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries([]int64{row.Id1}))
	if got.Rows != 1 {
		t.Fatalf("residual that holds must keep the row, got %d rows", got.Rows)
	}
	ExpectSameVectors(t, got, ref)

	// residual fails: row is dropped, not reported as MISSING
	req = NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", AndOf(Cmp("id1", "EQ", row.Id1), Cmp("data1", "EQ", row.Data1+1)))
	got = GetScanResponse(t, req)
	if got.Rows != 0 {
		t.Fatalf("residual that fails must drop the row, got %d rows: %s", got.Rows, got.Features)
	}

	// residual with ISNOTNULL / range, PK equality as the right operand
	req = NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", AndOf(IsNotNull("data1"), Cmp("data1", "LE", row.Data1), Cmp("id1", "EQ", row.Id1)))
	got = GetScanResponse(t, req)
	if got.Rows != 1 {
		t.Fatalf("expected 1 row, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)
}

func Test_SameKeyDifferentResiduals_TakesScanPath(t *testing.T) {
	row := sampleRows(t, 1)[0]
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries([]int64{row.Id1}))
	// (id = k AND data1 = v+1) OR (id = k AND data1 = v): the second branch
	// holds, so the row must be returned even though the first branch does
	// not. Two branches on one key with residuals are served by the scan.
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", OrOf(
			AndOf(Cmp("id1", "EQ", row.Id1), Cmp("data1", "EQ", row.Data1+1)),
			AndOf(Cmp("id1", "EQ", row.Id1), Cmp("data1", "EQ", row.Data1)))).
		With("limit", 10)
	got := GetScanResponse(t, req)
	if got.Rows != 1 {
		t.Fatalf("expected 1 row, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)

	// bare key OR (same key AND failing residual): the bare key wins
	req = NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", OrOf(
			AndOf(Cmp("id1", "EQ", row.Id1), Cmp("data1", "EQ", row.Data1+1)),
			Cmp("id1", "EQ", row.Id1))).
		With("limit", 10)
	got = GetScanResponse(t, req)
	if got.Rows != 1 {
		t.Fatalf("expected 1 row, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)
}

func Test_FastPath_Limit_TruncatesInBranchOrder(t *testing.T) {
	rows := sampleRows(t, 3)
	ids := []int64{rows[0].Id1, rows[1].Id1, rows[2].Id1}
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", OrOf(Cmp("id1", "EQ", ids[0]), Cmp("id1", "EQ", ids[1]), Cmp("id1", "EQ", ids[2]))).
		With("limit", 2)
	got := GetScanResponse(t, req)
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries(ids[:2]))
	if got.Rows != 2 {
		t.Fatalf("limit 2 over 3 branches must return 2 rows, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)
}

// An OR chain of key equalities holds at most 32 keys, because logic
// nodes are binary and the filter parser allows 32 levels of nesting.
func Test_FastPath_OrChainBound(t *testing.T) {
	chain := func(n int) ScanRequest {
		var leaves []map[string]interface{}
		for i := 0; i < n; i++ {
			leaves = append(leaves, Cmp("id1", "EQ", 900000000+i))
		}
		return NewScanRequest(fsStar, fvStar, fvVersion).With("filters", OrOf(leaves...))
	}
	if got := GetScanResponse(t, chain(32)); got.Rows != 0 {
		t.Errorf("32 absent keys must give no rows, got %d", got.Rows)
	}
	GetScanResponseWithDetail(t, chain(33), "nesting too deep", http.StatusBadRequest)
}

func Test_CompositePk(t *testing.T) {
	const fs, fv = testdbs.FSDB001, "sample_3"
	var id1 int64
	var id2 string
	queryRows(t, "SELECT id1, id2 FROM fsdb001.sample_3_1 ORDER BY RAND() LIMIT 1",
		func(rows interface {
			Scan(dest ...interface{}) error
		}) error {
			return rows.Scan(&id1, &id2)
		})
	ref := GetBatchReference(t, fs, fv, 1, []string{"id1", "id2"},
		[][]interface{}{{[]byte(fmt.Sprintf("%d", id1)), []byte(fmt.Sprintf("%q", id2))}})

	// full key: fast path
	got := GetScanResponse(t, NewScanRequest(fs, fv, 1).
		With("filters", AndOf(Cmp("id1", "EQ", id1), Cmp("id2", "EQ", id2))))
	if got.Rows != 1 {
		t.Fatalf("expected 1 row, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)

	// full key nested under another AND, with a residual: still fast path
	got = GetScanResponse(t, NewScanRequest(fs, fv, 1).
		With("filters", Logic("AND", Logic("AND", Cmp("id1", "EQ", id1), Cmp("id2", "EQ", id2)), IsNotNull("id1"))))
	if got.Rows != 1 {
		t.Fatalf("expected 1 row, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)

	// partial key: scan path, same result
	got = GetScanResponse(t, NewScanRequest(fs, fv, 1).
		With("filters", Cmp("id1", "EQ", id1)).With("limit", 10))
	if got.Rows != 1 {
		t.Fatalf("expected 1 row for the partial key, got %d", got.Rows)
	}
	ExpectSameVectors(t, got, ref)
}

// ---- scan path ----

func Test_ScanPath_NonPkFilter_MatchesBatchFeatureStore(t *testing.T) {
	row := sampleRows(t, 1)[0]
	ids := idsWhere(t, fmt.Sprintf("data1 = %d", row.Data1))
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", Cmp("data1", "EQ", row.Data1)).
		With("limit", 1000)
	got := GetScanResponse(t, req)
	if got.Rows != len(ids) {
		t.Fatalf("expected %d rows with data1=%d, got %d", len(ids), row.Data1, got.Rows)
	}
	if got.Warning == "" {
		t.Errorf("a filter-only request must carry the table-scan warning")
	}
	SortByFirstFeature(got.Features)
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries(ids))
	// statuses were sorted along with the vectors only if we re-align them
	got.Status = nil
	delete(got.rawKeys, "status")
	ExpectSameVectors(t, got, ref)
}

func Test_ScanPath_LogicFilters(t *testing.T) {
	rows := sampleRows(t, 2)
	lo, hi := rows[0].Data1, rows[1].Data1
	if lo > hi {
		lo, hi = hi, lo
	}
	ids := idsWhere(t, fmt.Sprintf("data1 >= %d AND data1 <= %d", lo, hi))
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", AndOf(Cmp("data1", "GE", lo), Cmp("data1", "LE", hi))).
		With("limit", 1000)
	got := GetScanResponse(t, req)
	if got.Rows != len(ids) {
		t.Fatalf("expected %d rows in [%d,%d], got %d", len(ids), lo, hi, got.Rows)
	}
	SortByFirstFeature(got.Features)
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries(ids))
	got.Status = nil
	delete(got.rawKeys, "status")
	ExpectSameVectors(t, got, ref)
}

func Test_ScanPath_FullScan_LimitAndWarning(t *testing.T) {
	total := len(idsWhere(t, "1=1"))
	req := NewScanRequest(fsStar, fvStar, fvVersion).With("limit", 3)
	got := GetScanResponse(t, req)
	if got.Rows != 3 {
		t.Fatalf("expected 3 rows, got %d", got.Rows)
	}
	if got.Warning == "" {
		t.Errorf("a filter-less request must carry a warning")
	}
	// Without limit the default (>= table size here) returns every row
	got = GetScanResponse(t, NewScanRequest(fsStar, fvStar, fvVersion))
	if got.Rows != total {
		t.Fatalf("expected all %d rows, got %d", total, got.Rows)
	}
	for i, s := range got.Status {
		if s != api.FEATURE_STATUS_COMPLETE && s != api.FEATURE_STATUS_MISSING {
			t.Errorf("row %d has status %s", i, s)
		}
	}
}

// URL of the feature_store_scan endpoint of another mtr rdrs2 instance, named
// by the environment variable holding its config file (see
// suite/rdrs2-golang/my.cnf). Skips outside mtr.
func scanURLOfInstance(t *testing.T, envVar string) string {
	t.Helper()
	cfgPath := os.Getenv(envVar)
	if cfgPath == "" {
		t.Skipf("%s not set (run via mtr --suite=rdrs2-golang)", envVar)
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", cfgPath, err)
	}
	var conf config.AllConfigs
	if err := json.Unmarshal(raw, &conf); err != nil {
		t.Fatalf("cannot parse %s: %v", cfgPath, err)
	}
	if conf.Security.TLS.EnableTLS {
		t.Fatalf("%s must name a plain-HTTP instance", envVar)
	}
	return fmt.Sprintf("http://%s/%s/%s",
		net.JoinHostPort(testutils.ConnectHost(conf.REST.ServerIP),
			strconv.Itoa(int(conf.REST.ServerPort))),
		version.API_VERSION, config.FEATURE_STORE_SCAN_OPERATION)
}

const tableScanRejected = "scan the whole root feature group"

// Every request shape that reads the whole root table, plus two that do not.
func tableScanCases(t *testing.T, indexName string) (tableScans, notTableScans []ScanRequest) {
	t.Helper()
	row := sampleRows(t, 1)[0]
	tableScans = []ScanRequest{
		NewScanRequest(fsStar, fvStar, fvVersion).With("limit", 1),
		NewScanRequest(fsStar, fvStar, fvVersion).With("filters", Cmp("data1", "EQ", row.Data1)),
		NewScanRequest(fsStar, fvStar, fvVersion).With("filters", Cmp("id1", "GE", row.Id1)),
		NewScanRequest(fsStar, fvStar, fvVersion).With("index", map[string]interface{}{
			"name": indexName, "key_columns": []string{"data1"},
			"ranges": []interface{}{map[string]interface{}{}}}),
	}
	notTableScans = []ScanRequest{
		NewScanRequest(fsStar, fvStar, fvVersion).With("index", map[string]interface{}{
			"name": indexName, "key_columns": []string{"data1"},
			"ranges": []interface{}{map[string]interface{}{
				"lower": map[string]interface{}{"values": []interface{}{row.Data1}, "inclusive": true},
				"upper": map[string]interface{}{"values": []interface{}{row.Data1}, "inclusive": true}}}}),
		NewScanRequest(fsStar, fvStar, fvVersion).With("filters", Cmp("id1", "EQ", row.Id1)),
	}
	return tableScans, notTableScans
}

func withAllowTableScan(req ScanRequest) ScanRequest {
	return req.With("options", map[string]bool{"allowTableScan": true})
}

// FeatureStoreScanTableScanPolicy=request (the plain-HTTP mtr instance): a
// table scan runs only when the request sets options.allowTableScan, and then
// still carries the warning. The TLS instance the rest of this package uses
// has policy "allow", so it cannot show this.
func Test_TableScan_PolicyRequest(t *testing.T) {
	url := scanURLOfInstance(t, "RDRS_CONFIG_FILE_NO_TLS")
	execSQL(t, "ALTER TABLE fsdb002.sample_1_1 ADD INDEX idx_fss_policy (data1)")
	t.Cleanup(func() { execSQL(t, "ALTER TABLE fsdb002.sample_1_1 DROP INDEX idx_fss_policy") })
	tableScans, notTableScans := tableScanCases(t, "idx_fss_policy")

	for i, req := range tableScans {
		t.Run(fmt.Sprintf("table scan %d without consent", i), func(t *testing.T) {
			testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, url,
				req.String(), "allowTableScan", http.StatusBadRequest)
		})
		t.Run(fmt.Sprintf("table scan %d with consent", i), func(t *testing.T) {
			_, body := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, url,
				withAllowTableScan(req).String(), "", http.StatusOK)
			var resp ScanResponse
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatalf("Unmarshal failed %s; body: %s", err, body)
			}
			if resp.Warning == "" {
				t.Errorf("a consented table scan must still carry the warning")
			}
		})
	}
	for i, req := range notTableScans {
		t.Run(fmt.Sprintf("not a table scan %d", i), func(t *testing.T) {
			testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, url,
				req.String(), "", http.StatusOK)
		})
	}
}

// FeatureStoreScanTableScanPolicy=reject, the default (the no-auth mtr
// instance): a table scan is refused even when the request consents.
func Test_TableScan_PolicyReject(t *testing.T) {
	url := scanURLOfInstance(t, "RDRS_CONFIG_FILE_NOAUTH")
	execSQL(t, "ALTER TABLE fsdb002.sample_1_1 ADD INDEX idx_fss_reject (data1)")
	t.Cleanup(func() { execSQL(t, "ALTER TABLE fsdb002.sample_1_1 DROP INDEX idx_fss_reject") })
	tableScans, notTableScans := tableScanCases(t, "idx_fss_reject")

	for i, req := range tableScans {
		for _, c := range []struct {
			name string
			req  ScanRequest
		}{{"without consent", req}, {"with consent", withAllowTableScan(req)}} {
			t.Run(fmt.Sprintf("table scan %d %s", i, c.name), func(t *testing.T) {
				testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, url,
					c.req.String(), tableScanRejected, http.StatusBadRequest)
			})
		}
	}
	for i, req := range notTableScans {
		t.Run(fmt.Sprintf("not a table scan %d", i), func(t *testing.T) {
			testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, url,
				req.String(), "", http.StatusOK)
		})
	}
}

func Test_ScanPath_OrderedIndexScan(t *testing.T) {
	execSQL(t, "ALTER TABLE fsdb002.sample_1_1 ADD INDEX idx_fss_data1 (data1)")
	t.Cleanup(func() { execSQL(t, "ALTER TABLE fsdb002.sample_1_1 DROP INDEX idx_fss_data1") })

	const lower = 10
	req := NewScanRequest(fsStar, fvStar, fvVersion).
		With("index", map[string]interface{}{
			"name":        "idx_fss_data1",
			"key_columns": []string{"data1"},
			"ranges": []interface{}{map[string]interface{}{
				"lower": map[string]interface{}{"values": []interface{}{lower}, "inclusive": true},
			}},
			"order": "desc",
		}).
		With("limit", 5)
	got := GetScanResponse(t, req)
	if got.Rows != 5 {
		t.Fatalf("expected 5 rows, got %d", got.Rows)
	}
	if got.Warning != "" {
		t.Errorf("a bounded index range is not a table scan; got warning %q", got.Warning)
	}
	prev := float64(1 << 62)
	var ids []int64
	for i, f := range got.Features {
		data1 := f[2].(float64)
		if data1 < lower {
			t.Errorf("row %d: data1 %v below the range bound", i, data1)
		}
		if data1 > prev {
			t.Errorf("row %d: data1 %v breaks descending order (prev %v)", i, data1, prev)
		}
		prev = data1
		ids = append(ids, int64(f[0].(float64)))
	}
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries(ids))
	ExpectSameVectors(t, got, ref)

	// An index block disables the fast path even with a PK equality filter
	row := sampleRows(t, 1)[0]
	req = NewScanRequest(fsStar, fvStar, fvVersion).
		With("index", map[string]interface{}{
			"name":        "idx_fss_data1",
			"key_columns": []string{"data1"},
			"ranges": []interface{}{map[string]interface{}{
				"lower": map[string]interface{}{"values": []interface{}{row.Data1}, "inclusive": true},
				"upper": map[string]interface{}{"values": []interface{}{row.Data1}, "inclusive": true},
			}},
		}).
		With("filters", Cmp("id1", "EQ", row.Id1)).
		With("limit", 10)
	got = GetScanResponse(t, req)
	ref = GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries([]int64{row.Id1}))
	ExpectSameVectors(t, got, ref)
}

func Test_ComplexTypes_BothPaths(t *testing.T) {
	const fs, fv = testdbs.FSDB002, "sample_complex_type"
	var id int64
	queryRows(t, "SELECT id1 FROM fsdb002.sample_complex_type_1 ORDER BY RAND() LIMIT 1",
		func(rows interface {
			Scan(dest ...interface{}) error
		}) error {
			return rows.Scan(&id)
		})
	ref := GetBatchReference(t, fs, fv, 1, []string{"id1"}, idEntries([]int64{id}))

	got := GetScanResponse(t, NewScanRequest(fs, fv, 1).With("filters", Cmp("id1", "EQ", id)))
	ExpectSameVectors(t, got, ref)

	// GE+LE on the key is not an equality, so this is the scan path
	got = GetScanResponse(t, NewScanRequest(fs, fv, 1).
		With("filters", AndOf(Cmp("id1", "GE", id), Cmp("id1", "LE", id))))
	ExpectSameVectors(t, got, ref)

	// decoded values are JSON, not base64 strings
	for i, f := range ref.Features[0] {
		if _, isString := f.(string); isString && i > 1 {
			t.Errorf("feature %d of the reference is a string; complex columns should be decoded", i)
		}
	}
}

func Test_SelfJoinView(t *testing.T) {
	const fs, fv = testdbs.FSDB001, "sample_1n1_self"
	row := sampleRowsIn(t, fs, 1)[0]
	ref := GetBatchReference(t, fs, fv, 1, []string{"id1"}, idEntries([]int64{row.Id1}))
	got := GetScanResponse(t, NewScanRequest(fs, fv, 1).With("filters", Cmp("id1", "EQ", row.Id1)))
	ExpectSameVectors(t, got, ref)
	got = GetScanResponse(t, NewScanRequest(fs, fv, 1).With("filters", Cmp("data1", "EQ", row.Data1)).With("limit", 100))
	SortByFirstFeature(got.Features)
	got.Status = nil
	delete(got.rawKeys, "status")
	ids := idsWhereIn(t, fs, fmt.Sprintf("data1 = %d", row.Data1))
	ExpectSameVectors(t, got, GetBatchReference(t, fs, fv, 1, []string{"id1"}, idEntries(ids)))
}

// ---- options and metadata ----

func Test_MetadataAndOptions(t *testing.T) {
	row := sampleRows(t, 1)[0]
	base := func() ScanRequest {
		return NewScanRequest(fsStar, fvStar, fvVersion).With("filters", Cmp("id1", "EQ", row.Id1))
	}

	got := GetScanResponse(t, base().With("metadataOptions", map[string]bool{"featureName": true, "featureType": true}))
	ref := GetBatchReference(t, fsStar, fvStar, fvVersion, []string{"id1"}, idEntries([]int64{row.Id1}))
	if len(got.Metadata) != len(ref.Features[0]) {
		t.Fatalf("metadata has %d entries for %d features", len(got.Metadata), len(ref.Features[0]))
	}
	names := map[string]bool{}
	for _, m := range got.Metadata {
		if m.Name == nil || m.Type == nil {
			t.Fatalf("metadata entry with null fields although requested: %s", m)
		}
		names[*m.Name] = true
	}
	for _, expected := range []string{"id1", "ts", "data1", "data2", "fg2_id1", "fg2_ts", "fg2_data1", "fg2_data2"} {
		if !names[expected] {
			t.Errorf("feature %s missing from metadata", expected)
		}
	}

	// default: metadata is an empty array, as /feature_store
	got = GetScanResponse(t, base())
	if !got.HasKey("metadata") || len(got.Metadata) != 0 {
		t.Errorf("metadata must be an empty array by default, got %s", canonical(t, got.Metadata))
	}

	// both options false: also an empty array
	got = GetScanResponse(t, base().With("metadataOptions", map[string]bool{"featureName": false, "featureType": false}))
	if len(got.Metadata) != 0 {
		t.Errorf("metadata must be an empty array when both options are false, got %s", canonical(t, got.Metadata))
	}

	// one option set: one entry per feature, the other field null
	got = GetScanResponse(t, base().With("metadataOptions", map[string]bool{"featureType": true}))
	if len(got.Metadata) != len(ref.Features[0]) {
		t.Fatalf("metadata has %d entries for %d features", len(got.Metadata), len(ref.Features[0]))
	}
	for _, m := range got.Metadata {
		if m.Name != nil || m.Type == nil {
			t.Errorf("featureType alone must populate only the type, got %s", m)
		}
	}
	if !got.HasKey("status") || got.HasKey("detailedStatus") {
		t.Errorf("status must be present and detailedStatus absent by default")
	}

	got = GetScanResponse(t, base().With("options", map[string]bool{"includeStatus": false}))
	if got.HasKey("status") {
		t.Errorf("includeStatus=false must omit the status array")
	}

	got = GetScanResponse(t, base().With("options", map[string]bool{"includeDetailedStatus": true}))
	if !got.HasKey("status") || len(got.DetailedStatus) != 1 || len(got.DetailedStatus[0]) != 2 {
		t.Errorf("includeDetailedStatus must give one entry per feature group: %s", canonical(t, got.DetailedStatus))
	}

	// scan path: the root feature group is read by the scan, not the batch,
	// but must still get its entry, in the same position and with the same
	// status the fast path reports for the same row. Most root rows of this
	// fixture have no sample_2_1 row, so the joined entry is often 404.
	got = GetScanResponse(t, NewScanRequest(fsStar, fvStar, fvVersion).
		With("filters", Cmp("data1", "EQ", row.Data1)).
		With("limit", 1000).
		With("options", map[string]bool{"includeDetailedStatus": true}))
	if got.Rows < 1 || len(got.DetailedStatus) != got.Rows {
		t.Fatalf("scan path must give detailedStatus for each of %d rows: %s", got.Rows, canonical(t, got.DetailedStatus))
	}
	for r, scanned := range got.DetailedStatus {
		if len(scanned) != 2 {
			t.Errorf("row %d: %d detailedStatus entries, want one per feature group", r, len(scanned))
		}
		id1 := int64(got.Features[r][0].(float64))
		fast := GetScanResponse(t, NewScanRequest(fsStar, fvStar, fvVersion).
			With("filters", Cmp("id1", "EQ", id1)).
			With("options", map[string]bool{"includeDetailedStatus": true}))
		if fast.Rows != 1 {
			t.Fatalf("fast path returned %d rows for id1=%d", fast.Rows, id1)
		}
		if !equalJSON(t, scanned, fast.DetailedStatus[0]) {
			t.Errorf("row %d (id1=%d): scan path detailedStatus %s, fast path %s",
				r, id1, canonical(t, scanned), canonical(t, fast.DetailedStatus[0]))
		}
	}
}

// ---- errors ----

func Test_Errors(t *testing.T) {
	cases := []struct {
		name    string
		req     ScanRequest
		status  int
		message string
	}{
		{"joined fg column", NewScanRequest(fsStar, fvStar, fvVersion).With("filters", Cmp("fg2_data1", "EQ", "x")),
			http.StatusBadRequest, "root feature group"},
		{"unknown column", NewScanRequest(fsStar, fvStar, fvVersion).With("filters", Cmp("nope", "EQ", 1)),
			http.StatusBadRequest, "not a feature of the feature view"},
		{"complex column", NewScanRequest(testdbs.FSDB002, "sample_complex_type", 1).With("filters", IsNotNull("array")),
			http.StatusBadRequest, "complex"},
		{"entries not accepted", NewScanRequest(fsStar, fvStar, fvVersion).With("entries", []interface{}{}),
			http.StatusBadRequest, "entries"},
		{"passedFeatures not accepted", NewScanRequest(fsStar, fvStar, fvVersion).With("passedFeatures", []interface{}{}),
			http.StatusBadRequest, "passedFeatures"},
		{"readColumns not accepted", NewScanRequest(fsStar, fvStar, fvVersion).With("readColumns", []interface{}{}),
			http.StatusBadRequest, "readColumns"},
		{"limit above max", NewScanRequest(fsStar, fvStar, fvVersion).With("limit", 100000),
			http.StatusBadRequest, "Limit exceeded"},
		{"negative limit", NewScanRequest(fsStar, fvStar, fvVersion).With("limit", -1),
			http.StatusBadRequest, "limit"},
		{"unknown option", NewScanRequest(fsStar, fvStar, fvVersion).With("options", map[string]bool{"foo": true}),
			http.StatusBadRequest, "foo"},
		{"allowTableScan not a bool", NewScanRequest(fsStar, fvStar, fvVersion).With("options", map[string]interface{}{"allowTableScan": "yes"}),
			http.StatusBadRequest, ""},
		{"unknown metadata option", NewScanRequest(fsStar, fvStar, fvVersion).With("metadataOptions", map[string]bool{"foo": true}),
			http.StatusBadRequest, "foo"},
		{"missing feature view name", ScanRequest{"featureStoreName": fsStar, "featureViewVersion": 1},
			http.StatusBadRequest, "featureViewName"},
		{"feature view not found", NewScanRequest(fsStar, "does_not_exist", 1),
			http.StatusNotFound, "Feature view does not exist"},
		{"feature store not found", NewScanRequest("does_not_exist", fvStar, 1),
			http.StatusNotFound, "Feature store does not exist"},
		{"bad filter op", NewScanRequest(fsStar, fvStar, fvVersion).With("filters", map[string]interface{}{"op": "XOR"}),
			http.StatusBadRequest, ""},
		{"index on joined fg column", NewScanRequest(fsStar, fvStar, fvVersion).With("index", map[string]interface{}{
			"name": "idx", "key_columns": []string{"fg2_data1"},
			"ranges": []interface{}{map[string]interface{}{
				"lower": map[string]interface{}{"values": []interface{}{"x"}, "inclusive": true}}}}),
			http.StatusBadRequest, "root feature group"},
		{"index without ranges", NewScanRequest(fsStar, fvStar, fvVersion).With("index", map[string]interface{}{
			"name": "idx", "key_columns": []string{"data1"}}),
			http.StatusBadRequest, "Missing index ranges"},
		// sample_1n3_joinoncol joins sample_3 on the root's data1 column, not
		// its primary key: the joined rows cannot be found from a root row
		{"not star schema", NewScanRequest(testdbs.FSDB001, "sample_1n3_joinoncol", 1).With("filters", Cmp("id1", "EQ", 1)),
			http.StatusBadRequest, "star schema"},
		// fv_spine_group's root (join index 0) is the spine feature group
		{"root fg not online", NewScanRequest(testdbs.FSDB004, "fv_spine_group", 1).With("filters", Cmp("id", "EQ", 1)),
			http.StatusBadRequest, "online"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			GetScanResponseWithDetail(t, c.req, c.message, c.status)
		})
	}
}
