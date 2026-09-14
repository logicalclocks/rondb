/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package data

import (
	"strings"
	"testing"
)

func TestEdgeRowCounts(t *testing.T) {
	perEntity := map[int64]int{}
	for _, r := range EdgeHistRows() {
		perEntity[r.EntityID]++
	}
	want := map[int64]int{EdgeHistMixedNull: 4, EdgeHistAllNull: 3, EdgeHistExact: 3, EdgeHistRounding: 5, EdgeHistDates: 5}
	for e, n := range want {
		if perEntity[e] != n {
			t.Errorf("edge_hist entity %d: %d rows, want %d", e, perEntity[e], n)
		}
	}
	if perEntity[EdgeHistEmpty] != 0 {
		t.Error("edge_hist entity 6 must have no rows")
	}
	if len(EdgeBigRows()) != 9 || len(EdgeStrRows()) != 19 || len(EdgeTsRows()) != 6 ||
		len(EdgeSeqRows()) != 11 || len(EdgeChildRows()) != 4 || len(EdgeParentRows()) != 6 {
		t.Errorf("edge row counts: big=%d str=%d ts=%d seq=%d child=%d parent=%d",
			len(EdgeBigRows()), len(EdgeStrRows()), len(EdgeTsRows()), len(EdgeSeqRows()),
			len(EdgeChildRows()), len(EdgeParentRows()))
	}
}

func TestEdgeBigSums(t *testing.T) {
	sums := map[int64]int64{}
	for _, r := range EdgeBigRows() {
		if r.EntityID == EdgeBigOverflow {
			continue // deliberate overflow probe
		}
		sums[r.EntityID] += r.BigVal
	}
	if sums[EdgeBigSafe] != (int64(1)<<54)-1 {
		t.Errorf("safe group SUM = %d, want 2^54-1", sums[EdgeBigSafe])
	}
	if sums[EdgeBigLimits] != 0 {
		t.Errorf("limits group SUM = %d, want 0", sums[EdgeBigLimits])
	}
}

func TestEdgeStringEscaping(t *testing.T) {
	cases := map[string]string{
		"O'Brien":      `'O''Brien'`,
		`back\slash`:   `'back\\slash'`,
		"tab\there":    `'tab\there'`,
		"line1\nline2": `'line1\nline2'`,
		"":             `''`,
		"日本語":          `'日本語'`,
	}
	for in, want := range cases {
		if got := SQLString(in); got != want {
			t.Errorf("SQLString(%q) = %s, want %s", in, got, want)
		}
	}
	// The MTR tuples carry the escaped forms.
	var tuples []string
	for _, tg := range EdgeTables() {
		if tg.Table == TEdgeStr {
			tg.Rows(NewScale(0.01), 1, func(tu string) { tuples = append(tuples, tu) })
		}
	}
	joined := strings.Join(tuples, "\n")
	for _, want := range []string{`('O''Brien', `, `('backslash', 'back\\slash', `, `('tab', 'tab\there', `, `('newline', 'line1\nline2', `, `('NULL', NULL, `, `('', 'empty key', `} {
		if !strings.Contains(joined, want) {
			t.Errorf("edge_str tuples miss %q", want)
		}
	}
}

func TestEdgeStrKeysUniqueUnderCollation(t *testing.T) {
	// utf8mb4_0900_ai_ci: keys that differ only by case or accent collide.
	seen := map[string]string{}
	fold := func(k string) string {
		k = strings.ToLower(k)
		k = strings.NewReplacer("å", "a", "ä", "a", "ö", "o", "é", "e").Replace(k)
		return k
	}
	for _, r := range EdgeStrRows() {
		if prev, ok := seen[fold(r.SKey)]; ok {
			t.Errorf("keys %q and %q collide under an accent/case-insensitive collation", prev, r.SKey)
		}
		seen[fold(r.SKey)] = r.SKey
	}
}

func TestEdgeTsTicks(t *testing.T) {
	for _, r := range EdgeTsRows() {
		if r.Seq != 2 {
			continue
		}
		if FormatTimestamp(r.Ts0) != EdgeTsCutoffString || FormatTimestamp3(r.Ts3) != EdgeTsCutoffString+".000" ||
			FormatTimestamp6(r.Ts6) != EdgeTsCutoffString+".000000" {
			t.Errorf("cutoff row renders as %s / %s / %s", FormatTimestamp(r.Ts0), FormatTimestamp3(r.Ts3), FormatTimestamp6(r.Ts6))
		}
	}
	rows := EdgeTsRows()
	if FormatTimestamp3(rows[0].Ts3) != "2026-05-01 11:59:59.999" || FormatTimestamp6(rows[2].Ts6) != "2026-05-01 12:00:00.000001" {
		t.Errorf("tick rows: %s / %s", FormatTimestamp3(rows[0].Ts3), FormatTimestamp6(rows[2].Ts6))
	}
}

func TestEdgeSeqReversed(t *testing.T) {
	var rev []EdgeSeqRow
	for _, r := range EdgeSeqRows() {
		if r.EntityID == EdgeSeqReversed {
			rev = append(rev, r)
		}
	}
	if len(rev) != 3 || !rev[0].EventTime.After(rev[2].EventTime) || rev[0].SequenceNo >= rev[2].SequenceNo {
		t.Errorf("reversed entity must have the newest event_time at the lowest sequence_no: %+v", rev)
	}
}

func TestEdgeChecksumsAndSchema(t *testing.T) {
	counts := map[string]int64{}
	for _, c := range EdgeChecksums() {
		counts[c.Table] = c.Count
	}
	want := map[string]int64{TEdgeHist: 20, TEdgeBig: 9, TEdgeStr: 19, TEdgeTs: 6, TEdgeSeq: 11, TEdgeChild: 4, TEdgeParent: 6}
	for table, n := range want {
		if counts[table] != n {
			t.Errorf("%s checksum count = %d, want %d", table, counts[table], n)
		}
	}
	// Every edge table has a generator, a checksum and a schema entry, and
	// the Checksums() list used by both loaders includes them.
	all := map[string]bool{}
	for _, c := range Checksums(NewScale(0.01), true) {
		all[c.Table] = true
	}
	fgs := SchemaMap()
	for _, tg := range EdgeTables() {
		if !tg.Fixed || tg.Select != nil {
			t.Errorf("%s must be a fixed table without a Select renderer", tg.Table)
		}
		if !all[tg.Table] {
			t.Errorf("%s missing from Checksums()", tg.Table)
		}
		if _, ok := fgs[tg.Table]; !ok {
			t.Errorf("%s missing from Schema()", tg.Table)
		}
		n := 0
		tg.Rows(NewScale(0.01), 1, func(string) { n++ })
		if int64(n) != counts[tg.Table] {
			t.Errorf("%s: %d tuples, checksum count %d", tg.Table, n, counts[tg.Table])
		}
	}
	if ts := fgs[TEdgeTs]; ts.Features[4].OnlineType != "timestamp(6)" {
		t.Errorf("edge_ts_1.ts6 online type = %q", ts.Features[4].OnlineType)
	}
}

func TestRenderMTRFixedTables(t *testing.T) {
	inc, err := RenderDataInc(MTROptions{DB: "test", Scale: NewScale(0.01), HashTwin: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"INSERT INTO `test`.`edge_str_1` (`s_key`, `s_val`, `s_note`, `n`) VALUES\n('O''Brien', ",
		"INSERT INTO `test`.`edge_child_1` (`ck1`, `ck2`, `label`, `payload`, `weight`) VALUES\n(1, 2, 'A', UNHEX('0A'), 12)",
		"if ($fs_check != '20,",
	} {
		if !strings.Contains(inc, want) {
			t.Errorf("data include misses %q", want)
		}
	}
}
