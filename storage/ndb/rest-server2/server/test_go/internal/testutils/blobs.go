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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// RONDB-1144 blob helpers. Large values are written through MySQL in
// 1 MiB base64 chunks: the schema runner splits statements on ';' and strips
// '--' comments, and base64 contains neither. Each chunk is its own
// statement (and transaction).

const blobChunkBytes = 1024 * 1024

// DeterministicBytes returns n pseudo-random bytes for seed, the same on
// every run.
func DeterministicBytes(n int, seed int64) []byte {
	data := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(data)
	return data
}

// PrintableBytes returns n bytes of lower-case letters derived from seed:
// text that neither charset conversion nor JSON escaping grows.
func PrintableBytes(n int, seed int64) []byte {
	data := DeterministicBytes(n, seed)
	for i := range data {
		data[i] = 'a' + data[i]%26
	}
	return data
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// SetBlob stores data in db.table.col of the row with the given integer
// primary key id, replacing the current value.
func SetBlob(t testing.TB, db, table, col string, id int, data []byte) {
	t.Helper()
	if len(data) == 0 {
		run(t, fmt.Sprintf("UPDATE %s.%s SET %s = '' WHERE id = %d", db, table, col, id))
		return
	}
	for off := 0; off < len(data); off += blobChunkBytes {
		end := off + blobChunkBytes
		if end > len(data) {
			end = len(data)
		}
		chunk := base64.StdEncoding.EncodeToString(data[off:end])
		var stmt string
		if off == 0 {
			stmt = fmt.Sprintf("UPDATE %s.%s SET %s = FROM_BASE64('%s') WHERE id = %d", db, table, col, chunk, id)
		} else {
			stmt = fmt.Sprintf("UPDATE %s.%s SET %s = CONCAT(%s, FROM_BASE64('%s')) WHERE id = %d", db, table, col, col, chunk, id)
		}
		run(t, stmt)
	}
}

// SetBlobNull sets db.table.col of row id to NULL.
func SetBlobNull(t testing.TB, db, table, col string, id int) {
	t.Helper()
	run(t, fmt.Sprintf("UPDATE %s.%s SET %s = NULL WHERE id = %d", db, table, col, id))
}

// SetRepeatedByte stores n copies of byte b in db.table.col of row id,
// through REPEAT(CHAR(b)) so control characters reach the column verbatim.
func SetRepeatedByte(t testing.TB, db, table, col string, id int, b byte, n int) {
	t.Helper()
	run(t, fmt.Sprintf("UPDATE %s.%s SET %s = REPEAT(CHAR(%d), %d) WHERE id = %d", db, table, col, b, n, id))
}

// DBSHA256 returns hex(SHA2(col, 256)) of db.table row id as MySQL computes
// it, so a value read through rdrs2 can be checked without pulling it a
// second time through the test's own comparison helpers.
func DBSHA256(t testing.TB, db, table, col string, id int) string {
	t.Helper()
	conn, err := CreateMySQLConnectionDataCluster()
	if err != nil {
		t.Fatalf("mysql connection: %v", err)
	}
	defer conn.Close()
	var sum string
	query := fmt.Sprintf("SELECT LOWER(SHA2(%s, 256)) FROM %s.%s WHERE id = %d", col, db, table, id)
	if err := conn.QueryRow(query).Scan(&sum); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return sum
}

// DecodeBase64Value decodes a base64 column value as rdrs2 returns binary
// columns.
func DecodeBase64Value(t testing.TB, value string) []byte {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		t.Fatalf("value is not base64: %v (first bytes %q)", err, firstBytes(value))
	}
	return data
}

func firstBytes(s string) string {
	if len(s) > 32 {
		return s[:32]
	}
	return s
}

// run executes one statement on the data cluster. RunQueriesOnDataCluster
// splits its input on ';' and drops the last fragment (the empty one after
// a file's final semicolon), so a statement without one is silently
// skipped: the terminator is added here so no caller can forget it.
func run(t testing.TB, stmt string) {
	t.Helper()
	if err := RunQueriesOnDataCluster(stmt + ";"); err != nil {
		t.Fatalf("%s: %v", firstBytes(stmt), err)
	}
}
