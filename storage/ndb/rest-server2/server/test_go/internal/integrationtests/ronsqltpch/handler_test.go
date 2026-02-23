/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation. The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
*/

package ronsqltpch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/version"
)

// TPC-H queries adapted for RonSQL.
// These match the bench_*_ndbapi programs in block_unit_test/.
// Skipped: Q9 (needs LIKE), Q12 (needs CASE+IN).
// BETWEEN rewritten to >= AND <=; ORDER BY omitted (sort externally).
var tpchQueries = []struct {
	Name string
	SQL  string
}{
	{"Q4", `SELECT o.o_orderpriority, COUNT(*)
		FROM tpch_lineitem AS l
		JOIN tpch_orders AS o ON o.o_orderkey = l.l_orderkey
		GROUP BY o.o_orderpriority`},

	{"minmax", `SELECT n.n_name,
		MIN(s.s_acctbal), MAX(s.s_acctbal), COUNT(*), SUM(s.s_acctbal)
		FROM tpch_supplier AS s
		JOIN tpch_nation AS n ON n.n_nationkey = s.s_nationkey
		GROUP BY n.n_name`},

	{"Q3", `SELECT o.o_orderyear, o.o_orderpriority,
		SUM(l.l_extendedprice * (1 - l.l_discount)), COUNT(*)
		FROM tpch_lineitem AS l
		JOIN tpch_orders AS o ON o.o_orderkey = l.l_orderkey
		JOIN tpch_customer AS c ON c.c_custkey = o.o_custkey
		WHERE c.c_mktsegment = 'BUILDING'
		GROUP BY o.o_orderyear, o.o_orderpriority`},

	{"Q5", `SELECT n.n_name,
		SUM(l.l_extendedprice * (1 - l.l_discount))
		FROM tpch_lineitem AS l
		JOIN tpch_orders AS o ON o.o_orderkey = l.l_orderkey
		JOIN tpch_customer AS c ON c.c_custkey = o.o_custkey
		JOIN tpch_nation AS n ON n.n_nationkey = c.c_nationkey
		JOIN tpch_region AS r ON r.r_regionkey = n.n_regionkey
		WHERE r.r_name = 'ASIA'
		GROUP BY n.n_name`},

	{"Q2", `SELECT r.r_name,
		MIN(ps.ps_supplycost), MAX(ps.ps_supplycost),
		SUM(ps.ps_supplycost), COUNT(*)
		FROM tpch_partsupp AS ps
		JOIN tpch_part AS p ON p.p_partkey = ps.ps_partkey
		JOIN tpch_supplier AS s ON s.s_suppkey = ps.ps_suppkey
		JOIN tpch_nation AS n ON n.n_nationkey = s.s_nationkey
		JOIN tpch_region AS r ON r.r_regionkey = n.n_regionkey
		WHERE p.p_size > 25
		GROUP BY r.r_name`},

	{"Q10", `SELECT c.c_name,
		SUM(l.l_extendedprice * (1 - l.l_discount)), COUNT(*)
		FROM tpch_lineitem AS l
		JOIN tpch_orders AS o ON o.o_orderkey = l.l_orderkey
		JOIN tpch_customer AS c ON c.c_custkey = o.o_custkey
		JOIN tpch_nation AS n ON n.n_nationkey = c.c_nationkey
		GROUP BY c.c_name`},

	{"Q11", `SELECT ps.ps_partkey,
		SUM(ps.ps_supplycost * ps.ps_availqty)
		FROM tpch_partsupp AS ps
		JOIN tpch_supplier AS s ON s.s_suppkey = ps.ps_suppkey
		JOIN tpch_nation AS n ON n.n_nationkey = s.s_nationkey
		WHERE n.n_name = 'GERMANY'
		GROUP BY ps.ps_partkey`},

	{"nogroup", `SELECT COUNT(*),
		SUM(l.l_extendedprice), SUM(l.l_quantity),
		MIN(l.l_extendedprice), MAX(l.l_extendedprice)
		FROM tpch_lineitem AS l
		JOIN tpch_orders AS o ON o.o_orderkey = l.l_orderkey
		JOIN tpch_customer AS c ON c.c_custkey = o.o_custkey
		WHERE c.c_mktsegment = 'AUTOMOBILE'`},

	{"orderscan", `SELECT o.o_orderyear,
		SUM(o.o_totalprice), COUNT(*),
		MIN(o.o_totalprice), MAX(o.o_totalprice)
		FROM tpch_orders AS o
		JOIN tpch_customer AS c ON c.c_custkey = o.o_custkey
		WHERE o.o_orderyear >= 1994 AND o.o_orderyear <= 1996
		GROUP BY o.o_orderyear`},

	{"datescan", `SELECT l.l_shipmode,
		SUM(l.l_extendedprice * (1 - l.l_discount)), COUNT(*)
		FROM tpch_lineitem AS l
		JOIN tpch_orders AS o ON o.o_orderkey = l.l_orderkey
		WHERE l.l_shipdate >= '1994-01-01' AND l.l_shipdate <= '1994-12-31'
		GROUP BY l.l_shipmode`},
}

func ronsqlURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/ronsql",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
	)
	if conf.Security.TLS.EnableTLS {
		url = "https://" + url
	} else {
		url = "http://" + url
	}
	return url
}

// queryRonSQL sends a RonSQL query via the REST API and returns the
// tab-separated response body (header + data rows).
func queryRonSQL(t *testing.T, query string) string {
	t.Helper()
	reqBody := map[string]string{
		"database":     "test",
		"query":        query,
		"outputFormat": "TEXT",
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("failed to marshal RonSQL request: %v", err)
	}

	_, respBody := testclient.SendHttpRequest(
		t,
		http.MethodPost,
		ronsqlURL(),
		string(bodyBytes),
		"",
		http.StatusOK,
	)
	return string(respBody)
}

// queryMySQL runs the same query via MySQL and returns the result
// formatted as tab-separated text with a header line, matching the
// RonSQL TEXT output format.
func queryMySQL(t *testing.T, query string) string {
	t.Helper()
	db, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		t.Fatalf("failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("USE test")
	if rows != nil {
		rows.Close()
	}
	if err != nil {
		t.Fatalf("USE test failed: %v", err)
	}

	rows, err = db.Query(query)
	if err != nil {
		t.Fatalf("MySQL query failed: %v\nquery: %s", err, query)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	var lines []string
	lines = append(lines, strings.Join(cols, "\t"))

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		var fields []string
		for _, v := range vals {
			switch val := v.(type) {
			case nil:
				fields = append(fields, "NULL")
			case []byte:
				fields = append(fields, string(val))
			default:
				fields = append(fields, fmt.Sprintf("%v", val))
			}
		}
		lines = append(lines, strings.Join(fields, "\t"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration error: %v", err)
	}

	return strings.Join(lines, "\n")
}

// parseTSV splits tab-separated text into a header line and sorted
// data lines. Empty trailing lines are removed.
func parseTSV(tsv string) (header string, dataRows []string) {
	lines := strings.Split(strings.TrimRight(tsv, "\n"), "\n")
	if len(lines) == 0 {
		return "", nil
	}
	header = lines[0]
	dataRows = lines[1:]
	sort.Strings(dataRows)
	return header, dataRows
}

func TestRonSQLTpchQueries(t *testing.T) {
	for _, q := range tpchQueries {
		t.Run(q.Name, func(t *testing.T) {
			ronsqlResult := queryRonSQL(t, q.SQL)
			mysqlResult := queryMySQL(t, q.SQL)

			ronsqlHeader, ronsqlRows := parseTSV(ronsqlResult)
			mysqlHeader, mysqlRows := parseTSV(mysqlResult)

			if ronsqlHeader != mysqlHeader {
				t.Errorf("header mismatch\n  RonSQL: %s\n  MySQL:  %s",
					ronsqlHeader, mysqlHeader)
			}

			if len(ronsqlRows) != len(mysqlRows) {
				t.Fatalf("row count mismatch: RonSQL=%d MySQL=%d",
					len(ronsqlRows), len(mysqlRows))
			}

			for i := range ronsqlRows {
				if ronsqlRows[i] != mysqlRows[i] {
					t.Errorf("row %d mismatch\n  RonSQL: %s\n  MySQL:  %s",
						i, ronsqlRows[i], mysqlRows[i])
				}
			}
		})
	}
}
