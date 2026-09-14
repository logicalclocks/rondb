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

// Mandatory edge fixtures (data_model.md §11, review A2/A4/A5): a small
// FIXED data set loaded alongside the bulk tables at every scale factor.
// Rows are literal tuples shared by the MTR include and the bench loader
// (both emit INSERT ... VALUES from the same Go values), so the two paths
// cannot drift; the checksums pin the content like the bulk tables.
//
// Entity ids are named constants so tests pick fixtures by name.

import (
	"fmt"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// Edge table names.
const (
	TEdgeHist   = "edge_hist_1"   // nullable type mix, COUNT(*) vs COUNT(col), null propagation
	TEdgeBig    = "edge_big_1"    // exact BIGINT around 2^53 and at the signed limits, large DECIMAL(18,2)
	TEdgeStr    = "edge_str_1"    // quoting, literal NULL, empty, case/accent, UTF-8, 100-char boundary, escapes
	TEdgeTs     = "edge_ts_1"     // TIMESTAMP(0)/(3)/(6) rows at a window cutoff and one tick around it
	TEdgeSeq    = "edge_seq_1"    // collect FG with an explicit sequence_no order column
	TEdgeParent = "edge_parent_1" // snowflake parent with two FK columns (+ complex projection)
	TEdgeChild  = "edge_child_1"  // snowflake child with a composite PK (+ binary projection)
)

// edge_hist_1 entities.
const (
	EdgeHistMixedNull int64 = 1 // 4 rows, NULLs scattered over the nullable columns
	EdgeHistAllNull   int64 = 2 // 3 rows, every nullable column NULL
	EdgeHistExact     int64 = 3 // 3 rows, no NULLs, exactly representable floats
	EdgeHistRounding  int64 = 4 // 5 rows, rounding-sensitive finite floats
	EdgeHistDates     int64 = 5 // 5 rows, DATE spread incl. leap days and the DATE maximum
	EdgeHistEmpty     int64 = 6 // no rows
)

// edge_big_1 entities.
const (
	EdgeBigSafe     int64 = 1 // values around 2^53; SUM = 2^54 - 1, fits BIGINT
	EdgeBigLimits   int64 = 2 // ±(2^63 - 1) and 0; SUM = 0, MIN/MAX at the limits
	EdgeBigOverflow int64 = 3 // 2^63 - 1 and 1: a deliberate SUM overflow probe (not a required success case)
)

// edge_ts_1: the fixed window cutoff and its entities.
var EdgeTsCutoff = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

const (
	EdgeTsCutoffString       = "2026-05-01 12:00:00"
	EdgeTsEntityA      int64 = 1 // 3 rows: one tick before, at, one tick after the cutoff
	EdgeTsEntityB      int64 = 2 // same, for batch queries
	EdgeTsEmpty        int64 = 3 // no rows
)

// edge_seq_1 entities (collect with sequence_no as the order column).
const (
	EdgeSeqSeven    int64 = 1 // 7 rows, sequence_no 1..7, event_time monotonic
	EdgeSeqOne      int64 = 2 // 1 row
	EdgeSeqEmpty    int64 = 3 // no rows (entity miss)
	EdgeSeqReversed int64 = 4 // 3 rows whose event_time order is the REVERSE of sequence_no
)

// edge_parent_1 entities and their hop to edge_child_1 (ck1, ck2).
const (
	EdgeParentMatch12  int64 = 1 // (1,2) -> child "A"
	EdgeParentMatch21  int64 = 2 // (2,1) -> child "B"; the swapped binding (1,2) selects "A"
	EdgeParentDangling int64 = 3 // (2,2): no child row
	EdgeParentNullHop  int64 = 4 // (NULL,1)
	EdgeParentMatch33  int64 = 5 // (3,3) -> child "C" (NULL payload)
	EdgeParentMatch55  int64 = 6 // (5,5) -> child "D"
)

// Optional values used by the fixed rows.
type OptF64 struct {
	V     float64
	Valid bool
}
type OptStr struct {
	V     string
	Valid bool
}
type OptCents struct {
	V     int64
	Valid bool
}
type OptDate struct {
	V     string // YYYY-MM-DD
	Valid bool
}

func optF(v float64) OptF64 { return OptF64{V: v, Valid: true} }
func optS(v string) OptStr  { return OptStr{V: v, Valid: true} }
func optD(v string) OptDate { return OptDate{V: v, Valid: true} }
func optDC(v int64) OptCents {
	return OptCents{V: v, Valid: true}
}

func sqlF64(v OptF64) string {
	if !v.Valid {
		return "NULL"
	}
	return SQLDouble(v.V)
}

func sqlStr(v OptStr) string {
	if !v.Valid {
		return "NULL"
	}
	return SQLString(v.V)
}

func sqlDate(v OptDate) string {
	if !v.Valid {
		return "NULL"
	}
	return "'" + v.V + "'"
}

func sqlCents(v OptCents) string {
	if !v.Valid {
		return "NULL"
	}
	return FormatCents(v.V)
}

// FormatTimestamp6 renders t as 'YYYY-MM-DD HH:MM:SS.ffffff' (UTC).
func FormatTimestamp6(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

// ---- edge_hist_1 ------------------------------------------------------------

// EdgeHistRow is one edge_hist_1 row.
type EdgeHistRow struct {
	EntityID  int64
	EventTime time.Time
	FFloat    OptF64
	FDouble   OptF64
	DDate     OptDate
	I1, I2    OptInt
	I3        OptInt
	DecVal    OptCents
	SVal      OptStr
	BigVal    int64
}

func edgeHistTime(entity int64, k int) time.Time {
	return FSNow.Add(-time.Duration(entity*100+int64(k)) * time.Hour)
}

// EdgeHistRows returns the fixed edge_hist_1 rows.
func EdgeHistRows() []EdgeHistRow {
	var rows []EdgeHistRow
	add := func(e int64, k int, ff, fd OptF64, dd OptDate, i1, i2, i3 OptInt, dec OptCents, sv OptStr, big int64) {
		rows = append(rows, EdgeHistRow{EntityID: e, EventTime: edgeHistTime(e, k), FFloat: ff, FDouble: fd,
			DDate: dd, I1: i1, I2: i2, I3: i3, DecVal: dec, SVal: sv, BigVal: big})
	}
	// Mixed NULL group: every nullable column is NULL in at least one row
	// and non-NULL in at least one row; GREATEST(i1,i2,i3) is NULL whenever
	// any operand is NULL (rows 1, 2, 4) and 25 in row 3.
	add(EdgeHistMixedNull, 1, optF(1.5), optF(0.25), optD("2026-01-15"), some(10), null, some(30), optDC(1050), optS("alpha"), 1)
	add(EdgeHistMixedNull, 2, optF(2.5), optF(0.5), optD("2026-02-15"), null, some(20), null, OptCents{}, OptStr{}, 2)
	add(EdgeHistMixedNull, 3, OptF64{}, optF(0.75), OptDate{}, some(5), some(15), some(25), optDC(-325), optS("Beta"), 3)
	add(EdgeHistMixedNull, 4, OptF64{}, OptF64{}, OptDate{}, null, null, null, OptCents{}, OptStr{}, 4)
	// All-NULL group.
	for k := 1; k <= 3; k++ {
		add(EdgeHistAllNull, k, OptF64{}, OptF64{}, OptDate{}, null, null, null, OptCents{}, OptStr{}, 0)
	}
	// Exact group: dyadic floats, leap days, negative decimal.
	add(EdgeHistExact, 1, optF(0.5), optF(1024.5), optD("2024-02-29"), some(1), some(2), some(3), optDC(101), optS("x"), 100)
	add(EdgeHistExact, 2, optF(1.25), optF(-0.125), optD("2000-02-29"), some(4), some(5), some(6), optDC(202), optS("y"), 200)
	add(EdgeHistExact, 3, optF(2.0), optF(3.75), optD("1970-01-01"), some(7), some(8), some(9), optDC(303), optS("z"), 300)
	// Rounding-sensitive group: non-dyadic finite values (FLOAT rounds to
	// binary32, SUM order matters, AVG is inexact).
	add(EdgeHistRounding, 1, optF(0.1), optF(0.1), optD("2026-03-01"), some(1), some(1), some(1), optDC(10), optS("r1"), 1)
	add(EdgeHistRounding, 2, optF(0.2), optF(0.2), optD("2026-03-02"), some(2), some(2), some(2), optDC(20), optS("r2"), 2)
	add(EdgeHistRounding, 3, optF(0.3), optF(0.3), optD("2026-03-03"), some(3), some(3), some(3), optDC(30), optS("r3"), 3)
	add(EdgeHistRounding, 4, optF(1e-7), optF(1e-7), optD("2026-03-04"), some(4), some(4), some(4), optDC(0), optS("r4"), 4)
	add(EdgeHistRounding, 5, optF(123456.789), optF(123456.789), optD("2026-03-05"), some(5), some(5), some(5), optDC(12345679), optS("r5"), 5)
	// DATE spread.
	add(EdgeHistDates, 1, optF(1), optF(1), optD("1970-01-01"), some(1), some(1), some(1), optDC(100), optS("d1"), 1)
	add(EdgeHistDates, 2, optF(1), optF(1), optD("1999-12-31"), some(1), some(1), some(1), optDC(100), optS("d2"), 2)
	add(EdgeHistDates, 3, optF(1), optF(1), optD("2000-01-01"), some(1), some(1), some(1), optDC(100), optS("d3"), 3)
	add(EdgeHistDates, 4, optF(1), optF(1), optD("2024-02-29"), some(1), some(1), some(1), optDC(100), optS("d4"), 4)
	add(EdgeHistDates, 5, optF(1), optF(1), optD("9999-12-31"), some(1), some(1), some(1), optDC(100), optS("d5"), 5)
	return rows
}

func (r EdgeHistRow) tuple() string {
	return tuple(itoa(r.EntityID), SQLString(FormatTimestamp(r.EventTime)), sqlF64(r.FFloat), sqlF64(r.FDouble),
		sqlDate(r.DDate), SQLOptInt(r.I1), SQLOptInt(r.I2), SQLOptInt(r.I3), sqlCents(r.DecVal), sqlStr(r.SVal),
		itoa(r.BigVal))
}

// ---- edge_big_1 ----------------------------------------------------------------

// EdgeBigRow is one edge_big_1 row.
type EdgeBigRow struct {
	EntityID int64
	Seq      int
	BigVal   int64
	DecBig   OptCents
	FExact   float64
}

const twoPow53 = int64(1) << 53

// EdgeBigRows returns the fixed edge_big_1 rows.
func EdgeBigRows() []EdgeBigRow {
	return []EdgeBigRow{
		// Safe large values: SUM(big_val) = 2^54 - 1.
		{EdgeBigSafe, 1, twoPow53 - 1, optDC(12345678901234567), 0.5},
		{EdgeBigSafe, 2, twoPow53, optDC(12345678901234567), 0.25},
		{EdgeBigSafe, 3, twoPow53 + 1, optDC(-100), 1e15},
		{EdgeBigSafe, 4, -(twoPow53 + 1), OptCents{}, 1024},
		// Signed limits, non-overflowing SUM (= 0).
		{EdgeBigLimits, 1, 9223372036854775807, optDC(99999999999999999), 2.5},
		{EdgeBigLimits, 2, -9223372036854775807, optDC(-99999999999999999), -2.5},
		{EdgeBigLimits, 3, 0, optDC(0), 0},
		// Deliberate overflow probe: SUM(big_val) exceeds BIGINT.
		{EdgeBigOverflow, 1, 9223372036854775807, optDC(1), 1},
		{EdgeBigOverflow, 2, 1, optDC(1), 1},
	}
}

func (r EdgeBigRow) tuple() string {
	return tuple(itoa(r.EntityID), itoa(int64(r.Seq)), itoa(r.BigVal), sqlCents(r.DecBig), SQLDouble(r.FExact))
}

// ---- edge_str_1 ----------------------------------------------------------------

// EdgeStrRow is one edge_str_1 row.  The key column is unique under the
// server collation (utf8mb4_0900_ai_ci): case and accent pairs therefore
// live in the value column under distinct keys.
type EdgeStrRow struct {
	SKey  string
	SVal  OptStr
	SNote string
	N     int
}

// EdgeStrRows returns the fixed edge_str_1 rows.
func EdgeStrRows() []EdgeStrRow {
	// 100-character boundary keys.  The two-byte key uses Greek lambda, not
	// an accented Latin letter: under utf8mb4_0900_ai_ci "ä" equals "a", so
	// 100 × "ä" would collide with 100 × "a" as a duplicate primary key.
	hundredA := ""
	hundredLambda := ""
	for i := 0; i < 100; i++ {
		hundredA += "a"
		hundredLambda += "λ"
	}
	return []EdgeStrRow{
		{"O'Brien", optS("apostrophe in key"), "quote", 1},
		{"NULL", OptStr{}, "key is the literal string NULL; value is SQL NULL", 2},
		{"null-string", optS("NULL"), "value is the literal string NULL", 3},
		{"", optS("empty key"), "empty", 4},
		{"empty-value", optS(""), "empty string value, not NULL", 5},
		{"case-lower", optS("case"), "case pair", 6},
		{"case-upper", optS("CASE"), "case pair", 7},
		{"accent-plain", optS("Asa"), "accent pair", 8},
		{"accent-ring", optS("Åsa"), "accent pair", 9},
		{"日本語", optS("multibyte key"), "utf8 3-byte", 10},
		{"emoji-value", optS("smile 😀"), "utf8 4-byte value", 11},
		{hundredA, optS("100 ascii chars"), "boundary", 12},
		{hundredLambda, optS("100 two-byte chars"), "boundary", 13},
		{"backslash", optS(`back\slash`), "escape", 14},
		{"tab", optS("tab\there"), "escape", 15},
		{"newline", optS("line1\nline2"), "escape", 16},
		{"quote-value", optS("it's"), "quote in value", 17},
		{"in-list-a", optS("a"), "IN list", 18},
		{"in-list-b", optS("b"), "IN list", 19},
	}
}

func (r EdgeStrRow) tuple() string {
	return tuple(SQLString(r.SKey), sqlStr(r.SVal), SQLString(r.SNote), itoa(int64(r.N)))
}

// ---- edge_ts_1 -----------------------------------------------------------------

// EdgeTsRow is one edge_ts_1 row.
type EdgeTsRow struct {
	EntityID int64
	Seq      int
	Ts0      time.Time // second precision
	Ts3      time.Time // millisecond precision
	Ts6      time.Time // microsecond precision
}

// EdgeTsRows returns the fixed edge_ts_1 rows: for each entity one row one
// tick before the cutoff, one exactly at it and one tick after, per
// precision.
func EdgeTsRows() []EdgeTsRow {
	var rows []EdgeTsRow
	for _, e := range []int64{EdgeTsEntityA, EdgeTsEntityB} {
		rows = append(rows,
			EdgeTsRow{e, 1, EdgeTsCutoff.Add(-time.Second), EdgeTsCutoff.Add(-time.Millisecond), EdgeTsCutoff.Add(-time.Microsecond)},
			EdgeTsRow{e, 2, EdgeTsCutoff, EdgeTsCutoff, EdgeTsCutoff},
			EdgeTsRow{e, 3, EdgeTsCutoff.Add(time.Second), EdgeTsCutoff.Add(time.Millisecond), EdgeTsCutoff.Add(time.Microsecond)},
		)
	}
	return rows
}

func (r EdgeTsRow) tuple() string {
	return tuple(itoa(r.EntityID), itoa(int64(r.Seq)), SQLString(FormatTimestamp(r.Ts0)),
		SQLString(FormatTimestamp3(r.Ts3)), SQLString(FormatTimestamp6(r.Ts6)))
}

// ---- edge_seq_1 ----------------------------------------------------------------

// EdgeSeqRow is one edge_seq_1 row.
type EdgeSeqRow struct {
	EntityID   int64
	SequenceNo int64
	EventTime  time.Time
	Payload    OptStr
	Amount     int
}

// EdgeSeqRows returns the fixed edge_seq_1 rows.
func EdgeSeqRows() []EdgeSeqRow {
	var rows []EdgeSeqRow
	for i := int64(1); i <= 7; i++ {
		rows = append(rows, EdgeSeqRow{EdgeSeqSeven, i, FSNow.Add(-time.Duration(8-i) * time.Hour),
			optS(fmt.Sprintf("p%d", i)), int(i * 10)})
	}
	rows = append(rows, EdgeSeqRow{EdgeSeqOne, 1, FSNow.Add(-time.Hour), optS("single"), 5})
	// Reversed: the newest event_time has the LOWEST sequence_no, so the
	// order column and the event time disagree.
	rows = append(rows,
		EdgeSeqRow{EdgeSeqReversed, 1, FSNow.Add(-time.Hour), OptStr{}, 1},
		EdgeSeqRow{EdgeSeqReversed, 2, FSNow.Add(-2 * time.Hour), optS("mid"), 2},
		EdgeSeqRow{EdgeSeqReversed, 3, FSNow.Add(-3 * time.Hour), optS("old"), 3},
	)
	return rows
}

func (r EdgeSeqRow) tuple() string {
	return tuple(itoa(r.EntityID), itoa(r.SequenceNo), SQLString(FormatTimestamp(r.EventTime)), sqlStr(r.Payload),
		itoa(int64(r.Amount)))
}

// ---- edge_parent_1 / edge_child_1 ---------------------------------------------

// EdgeParentRow is one edge_parent_1 row.
type EdgeParentRow struct {
	ParentID int64
	Ck1, Ck2 OptInt
	Name     string
	TagsHex  OptStr // VARBINARY as hex, NULL allowed
}

// EdgeChildRow is one edge_child_1 row.
type EdgeChildRow struct {
	Ck1, Ck2   int64
	Label      string
	PayloadHex OptStr // VARBINARY as hex, NULL allowed
	Weight     int
}

// EdgeParentRows returns the fixed edge_parent_1 rows.
func EdgeParentRows() []EdgeParentRow {
	return []EdgeParentRow{
		{EdgeParentMatch12, some(1), some(2), "p-12", optS("0102")},
		{EdgeParentMatch21, some(2), some(1), "p-21", optS("0201")},
		{EdgeParentDangling, some(2), some(2), "p-22-dangling", OptStr{}},
		{EdgeParentNullHop, null, some(1), "p-null-1", optS("00")},
		{EdgeParentMatch33, some(3), some(3), "p-33", optS("DEADBEEF")},
		{EdgeParentMatch55, some(5), some(5), "p-55", optS("FF")},
	}
}

// EdgeChildRows returns the fixed edge_child_1 rows.
func EdgeChildRows() []EdgeChildRow {
	return []EdgeChildRow{
		{1, 2, "A", optS("0A"), 12},
		{2, 1, "B", optS("0B"), 21},
		{3, 3, "C", OptStr{}, 33},
		{5, 5, "D", optS("0D0D"), 55},
	}
}

func sqlHex(v OptStr) string {
	if !v.Valid {
		return "NULL"
	}
	return "UNHEX('" + v.V + "')"
}

func (r EdgeParentRow) tuple() string {
	return tuple(itoa(r.ParentID), SQLOptInt(r.Ck1), SQLOptInt(r.Ck2), SQLString(r.Name), sqlHex(r.TagsHex))
}

func (r EdgeChildRow) tuple() string {
	return tuple(itoa(r.Ck1), itoa(r.Ck2), SQLString(r.Label), sqlHex(r.PayloadHex), itoa(int64(r.Weight)))
}

// ---- schema, generators, checksums ---------------------------------------------

// EdgeSchema returns the edge feature groups (load order: child before parent).
func EdgeSchema() []spec.FeatureGroup {
	return []spec.FeatureGroup{
		{
			Name: "edge_hist", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			Features: []spec.Feature{
				{Name: "entity_id", Type: "bigint", Primary: true},
				{Name: "event_time", Type: "timestamp", Primary: true},
				{Name: "f_float", Type: "float"},
				{Name: "f_double", Type: "double"},
				{Name: "d_date", Type: "date"},
				{Name: "i1", Type: "int"},
				{Name: "i2", Type: "int"},
				{Name: "i3", Type: "int"},
				{Name: "dec_val", Type: "decimal(18,2)"},
				{Name: "s_val", Type: "string"},
				{Name: "big_val", Type: "bigint"},
			},
		},
		{
			Name: "edge_big", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "entity_id", Type: "bigint", Primary: true},
				{Name: "seq", Type: "int", Primary: true},
				{Name: "big_val", Type: "bigint"},
				{Name: "dec_big", Type: "decimal(18,2)"},
				{Name: "f_exact", Type: "double"},
			},
		},
		{
			Name: "edge_str", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "s_key", Type: "string", Primary: true},
				{Name: "s_val", Type: "string"},
				{Name: "s_note", Type: "string"},
				{Name: "n", Type: "int"},
			},
		},
		{
			Name: "edge_ts", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "entity_id", Type: "bigint", Primary: true},
				{Name: "seq", Type: "int", Primary: true},
				{Name: "ts0", Type: "timestamp"},
				{Name: "ts3", Type: "timestamp", OnlineType: "timestamp(3)"},
				{Name: "ts6", Type: "timestamp", OnlineType: "timestamp(6)"},
			},
		},
		{
			Name: "edge_seq", Version: 1, FeaturestoreID: 1, EventTime: "event_time",
			Features: []spec.Feature{
				{Name: "entity_id", Type: "bigint", Primary: true},
				{Name: "sequence_no", Type: "bigint", Primary: true},
				{Name: "event_time", Type: "timestamp"},
				{Name: "payload", Type: "string"},
				{Name: "amount", Type: "int"},
			},
		},
		{
			Name: "edge_child", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "ck1", Type: "int", Primary: true},
				{Name: "ck2", Type: "int", Primary: true},
				{Name: "label", Type: "string"},
				{Name: "payload", Type: "binary", Complex: true},
				{Name: "weight", Type: "int"},
			},
		},
		{
			Name: "edge_parent", Version: 1, FeaturestoreID: 1,
			Features: []spec.Feature{
				{Name: "parent_id", Type: "bigint", Primary: true},
				{Name: "ck1", Type: "int"},
				{Name: "ck2", Type: "int"},
				{Name: "name", Type: "string"},
				{Name: "tags", Type: "array<string>", Complex: true},
			},
		},
	}
}

// fixedTable builds a TableGen for a fixed-row table: one pseudo entity
// whose Rows() emits every tuple; Select is nil (the include renders
// INSERT ... VALUES from the same tuples).
func fixedTable(table string, fg spec.FeatureGroup, tuples func() []string) TableGen {
	var cols []string
	for _, ft := range fg.Features {
		cols = append(cols, ft.Name)
	}
	return TableGen{
		Table: table, FG: fg, Columns: cols, Mean: 1, Fixed: true,
		Entities: func(sc Scale) int64 { return 1 },
		Rows: func(sc Scale, e int64, emit func(string)) {
			for _, t := range tuples() {
				emit(t)
			}
		},
	}
}

// EdgeTables returns the edge table generators in load order.
func EdgeTables() []TableGen {
	fgs := map[string]spec.FeatureGroup{}
	for _, fg := range EdgeSchema() {
		fgs[fg.TableName()] = fg
	}
	return []TableGen{
		fixedTable(TEdgeHist, fgs[TEdgeHist], func() []string {
			var t []string
			for _, r := range EdgeHistRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
		fixedTable(TEdgeBig, fgs[TEdgeBig], func() []string {
			var t []string
			for _, r := range EdgeBigRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
		fixedTable(TEdgeStr, fgs[TEdgeStr], func() []string {
			var t []string
			for _, r := range EdgeStrRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
		fixedTable(TEdgeTs, fgs[TEdgeTs], func() []string {
			var t []string
			for _, r := range EdgeTsRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
		fixedTable(TEdgeSeq, fgs[TEdgeSeq], func() []string {
			var t []string
			for _, r := range EdgeSeqRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
		fixedTable(TEdgeChild, fgs[TEdgeChild], func() []string {
			var t []string
			for _, r := range EdgeChildRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
		fixedTable(TEdgeParent, fgs[TEdgeParent], func() []string {
			var t []string
			for _, r := range EdgeParentRows() {
				t = append(t, r.tuple())
			}
			return t
		}),
	}
}

// EdgeChecksums computes the checksums of the edge tables from the fixed rows.
func EdgeChecksums() []Checksum {
	hist := Checksum{Table: TEdgeHist, SumExpr: "big_val", CntExpr: "i1"}
	for _, r := range EdgeHistRows() {
		hist.Count++
		hist.Sum += r.BigVal
		if r.I1.Valid {
			hist.CntValue++
		}
	}
	big := Checksum{Table: TEdgeBig, SumExpr: "seq", CntExpr: "dec_big"}
	for _, r := range EdgeBigRows() {
		big.Count++
		big.Sum += int64(r.Seq)
		if r.DecBig.Valid {
			big.CntValue++
		}
	}
	str := Checksum{Table: TEdgeStr, SumExpr: "n", CntExpr: "s_val"}
	for _, r := range EdgeStrRows() {
		str.Count++
		str.Sum += int64(r.N)
		if r.SVal.Valid {
			str.CntValue++
		}
	}
	ts := Checksum{Table: TEdgeTs, SumExpr: "seq", CntExpr: "ts6"}
	for _, r := range EdgeTsRows() {
		ts.Count++
		ts.Sum += int64(r.Seq)
		ts.CntValue++
	}
	seq := Checksum{Table: TEdgeSeq, SumExpr: "amount", CntExpr: "payload"}
	for _, r := range EdgeSeqRows() {
		seq.Count++
		seq.Sum += int64(r.Amount)
		if r.Payload.Valid {
			seq.CntValue++
		}
	}
	child := Checksum{Table: TEdgeChild, SumExpr: "weight", CntExpr: "payload"}
	for _, r := range EdgeChildRows() {
		child.Count++
		child.Sum += int64(r.Weight)
		if r.PayloadHex.Valid {
			child.CntValue++
		}
	}
	parent := Checksum{Table: TEdgeParent, SumExpr: "parent_id", CntExpr: "ck1"}
	for _, r := range EdgeParentRows() {
		parent.Count++
		parent.Sum += r.ParentID
		if r.Ck1.Valid {
			parent.CntValue++
		}
	}
	return []Checksum{hist, big, str, ts, seq, child, parent}
}
