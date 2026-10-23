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
	"fmt"
	"strings"
	"testing"
)

// RONDB-1144: the widest row a table can have, computed from the limits
// rdrs2 reports, so the same test exercises 512 columns on 26.02 and 4096
// on 26.10. The limits that apply, taken from DBDICT and DBTUP (errors 738
// and 851 on both versions; 944 and 941 on 26.10 only) and confirmed by
// creating tables:
//   - the fixed-size part of a row is bounded by the attribute offset
//     field: 11 bits (2047 words, error 851) on 26.02, 12 bits on 26.10
//     (MAX_FIXED_SIZE_IN_WORDS 4070, error 944). The smaller bound is used
//     so one layout creates on both; the INT key and a CHAR at its width
//     rounded up to a word count against it, plus a tuple header and null
//     bits, for which 16 words of headroom are kept here;
//   - the in-memory varsize part is at most 32632 bytes on 26.10, and
//     32032 while pre-26.10 data nodes are in the cluster (error 941, 26.10
//     only); the smaller value is used. It holds a VARCHAR at its width
//     plus one length byte rounded up to a word, plus a 2-byte offset per
//     column and a small header;
//   - MySQL's row cap of 65535 bytes for every engine (HA_MAX_REC_LENGTH),
//     counting a VARCHAR as its width plus one;
//   - the NDB row size rdrs2 reports (MaxRowBytes, MAX_TUPLE_SIZE_IN_WORDS),
//     which DBDICT checks against the whole record in words (error 738),
//     so with the same word-rounded sizes; on 26.02, which has no
//     varsize-part cap, this is the one that binds.
// Neither kind alone gives the widest width (4095 x CHAR(4) exceeds the
// fixed part, and VARCHAR alone stops at width 3), so the row is NFixed CHAR(Width) columns followed by
// VARCHAR(Width) columns, all latin1 NOT NULL, named c0..c(N-1), plus an
// INT primary key id. Width is the largest one that satisfies every cap:
// 55 on 26.02 (145 CHAR + 366 VARCHAR, 28 475-byte rows), 7 on 26.10
// (1015 CHAR + 3080 VARCHAR, 31 749 bytes).

const (
	mysqlMaxRowBytes   = 65535
	ndbFixedPartWords  = 2047  // AO_ATTRIBUTE_OFFSET_MASK on 26.02; 4070 on 26.10
	ndbTupleHeadroom   = 16    // words left for the tuple header and null bits
	ndbVarPartBytes    = 32032 // OLD_MAX_VAR_SIZE_IN_WORDS * 4, the smaller cap
	ndbVarPartHeader   = 8     // bytes
	ndbVarColumnOffset = 2     // bytes per varsize column
)

type WideRowSpec struct {
	NCols  int // columns besides the key
	NFixed int // the first NFixed are CHAR(Width), the rest VARCHAR(Width)
	Width  int
}

func align4(n int) int {
	return (n + 3) &^ 3
}

func WidestRowSpec(limits Limits) WideRowSpec {
	budget := int(limits.MaxRowBytes)
	if budget > mysqlMaxRowBytes {
		budget = mysqlMaxRowBytes
	}
	ncols := int(limits.MaxColumns) - 1                    // the primary key takes one
	fixedWords := ndbFixedPartWords - ndbTupleHeadroom - 1 // the INT key
	for width := 255; width >= 1; width-- {
		nfixed := fixedWords / (align4(width) / 4)
		if nfixed > ncols {
			nfixed = ncols
		}
		nvar := ncols - nfixed
		if ndbVarPartHeader+nvar*(align4(width+1)+ndbVarColumnOffset) > ndbVarPartBytes {
			continue
		}
		if nfixed*width+nvar*(width+1)+4 > budget {
			continue
		}
		fixedBytes := 4 + nfixed*align4(width) + 4*ndbTupleHeadroom
		varBytes := ndbVarPartHeader + nvar*(align4(width+1)+ndbVarColumnOffset)
		if fixedBytes+varBytes > int(limits.MaxRowBytes) {
			continue
		}
		return WideRowSpec{NCols: ncols, NFixed: nfixed, Width: width}
	}
	return WideRowSpec{NCols: ncols, NFixed: 0, Width: 1}
}

// RowBytes is the MySQL row size the spec produces.
func (s WideRowSpec) RowBytes() int {
	return s.NFixed*s.Width + (s.NCols-s.NFixed)*(s.Width+1) + 4
}

// CreateWideRowTable creates db.table for the spec and inserts one row per
// entry of rows: id -> fill byte, every column REPEAT(CHAR(fill), Width)
// (the fill is never a space, so CHAR padding does not change what is
// read back).
// The table is dropped when the test ends.
func CreateWideRowTable(t testing.TB, db, table string, spec WideRowSpec, rows map[int]byte) {
	t.Helper()
	var cols strings.Builder
	for i := 0; i < spec.NCols; i++ {
		kind := "VARCHAR"
		if i < spec.NFixed {
			kind = "CHAR"
		}
		cols.WriteString(fmt.Sprintf(", c%d %s(%d) CHARACTER SET latin1 NOT NULL", i, kind, spec.Width))
	}
	run(t, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, table))
	run(t, fmt.Sprintf("CREATE TABLE %s.%s (id INT NOT NULL%s, PRIMARY KEY (id)) ENGINE=ndbcluster",
		db, table, cols.String()))
	t.Cleanup(func() {
		_ = RunQueriesOnDataCluster(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", db, table))
	})
	for id, fill := range rows {
		var vals strings.Builder
		for i := 0; i < spec.NCols; i++ {
			vals.WriteString(fmt.Sprintf(", REPEAT(CHAR(%d), %d)", fill, spec.Width))
		}
		run(t, fmt.Sprintf("INSERT INTO %s.%s VALUES (%d%s)", db, table, id, vals.String()))
	}
}

// ExpectedWideValue is what every column of a row filled with fill holds.
func ExpectedWideValue(spec WideRowSpec, fill byte) string {
	return strings.Repeat(string([]byte{fill}), spec.Width)
}

// ColumnName of column i.
func (s WideRowSpec) ColumnName(i int) string {
	return fmt.Sprintf("c%d", i)
}
