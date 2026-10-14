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
// on 26.10. Three limits apply: the NDB row size (MaxRowBytes), MySQL's
// own row cap of 65535 bytes for every engine (HA_MAX_REC_LENGTH), and
// DBDICT's accounting of every attribute in 4-byte words.

const mysqlMaxRowBytes = 65535

// WideRowSpec is NCols VARCHAR(Width) latin1 NOT NULL columns c0..c(N-1)
// plus an INT primary key id.
type WideRowSpec struct {
	NCols int
	Width int
}

func WidestRowSpec(limits Limits) WideRowSpec {
	budget := int(limits.MaxRowBytes)
	if budget > mysqlMaxRowBytes {
		budget = mysqlMaxRowBytes
	}
	ncols := int(limits.MaxColumns) - 1 // the primary key takes one
	words := budget/4 - 1 - 2           // id, and slack for rounding
	width := (words/ncols)*4 - 1        // 1-byte length prefix per VARCHAR
	if width > 255 {
		width = 255
	}
	if width < 1 {
		width = 1
	}
	return WideRowSpec{NCols: ncols, Width: width}
}

// RowBytes is the MySQL row size the spec produces (length prefix per
// column plus the id).
func (s WideRowSpec) RowBytes() int {
	return s.NCols*(s.Width+1) + 4
}

// CreateWideRowTable creates db.table for the spec and inserts one row per
// entry of rows: id -> fill byte, every column REPEAT(CHAR(fill), Width).
// The table is dropped when the test ends.
func CreateWideRowTable(t testing.TB, db, table string, spec WideRowSpec, rows map[int]byte) {
	t.Helper()
	var cols strings.Builder
	for i := 0; i < spec.NCols; i++ {
		cols.WriteString(fmt.Sprintf(", c%d VARCHAR(%d) CHARACTER SET latin1 NOT NULL", i, spec.Width))
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
