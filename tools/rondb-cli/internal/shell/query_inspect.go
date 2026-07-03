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

package shell

// Query inspection commands over the named analytics query registry
// (ronsqlBenchQueries in ronsql_bench.go):
//
//   .list_query_ronsql / .list_query_sql   names + descriptions per namespace
//   .query_ronsql / .query_sql <name>      print the SQL a name represents
//   .explain_ronsql / .explain_sql <name>  EXPLAIN via RonSQL / MySQL server
//
// These make it easy to see exactly what a benchmark runs and how each
// engine plans it, without digging in the source.

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/chzyer/readline"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// ronsqlQueryNameCompletions returns completer items for .query_ronsql /
// .explain_ronsql (MySQLOnly entries excluded — they cannot run on RonSQL).
func ronsqlQueryNameCompletions() []readline.PrefixCompleterInterface {
	var items []readline.PrefixCompleterInterface
	for _, q := range ronsqlBenchQueries {
		if !q.MySQLOnly {
			items = append(items, readline.PcItem(q.Name))
		}
	}
	return items
}

// sqlQueryNameCompletions returns completer items for .query_sql /
// .explain_sql.
func sqlQueryNameCompletions() []readline.PrefixCompleterInterface {
	var items []readline.PrefixCompleterInterface
	for _, q := range ronsqlBenchQueries {
		items = append(items, readline.PcItem(q.sqlBenchName()))
	}
	return items
}

// sampleBenchSQL returns a runnable form of the query with {KEY} / {KEY2}
// substituted using the same seed-1 key the benchmark warmup uses, plus the
// key (0 when the query has no placeholder).
func (s *Shell) sampleBenchSQL(q *RonSQLBenchQuery) (string, int) {
	if !q.RandKey {
		return q.SQL, 0
	}
	maxKey := s.resolveRonSQLBenchKeyRange(q)
	key := pickBenchKey(q, rand.New(rand.NewSource(1)), maxKey)
	return substituteBenchKey(q, key), key
}

// printBenchQueryDetails prints one registry entry: description, database,
// cross-namespace naming, the SQL template, and placeholder semantics.
func printBenchQueryDetails(q *RonSQLBenchQuery, name string, ronsqlSide bool) {
	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("%s - %s", name, q.Description)))
	fmt.Printf("  Category: %s   Database: %s\n", q.Category, q.Database)
	if q.MySQLOnly {
		fmt.Printf("  MySQL-only: shape outside the RonSQL envelope (ORDER BY/LIMIT/subqueries); runs via .bench_sql %s\n",
			q.sqlBenchName())
	} else if q.SQLName != "" {
		if ronsqlSide {
			fmt.Printf("  Named %s in .query_sql/.explain_sql/.bench_sql\n", q.sqlBenchName())
		} else {
			fmt.Printf("  Named %s in .query_ronsql/.explain_ronsql/.bench_ronsql\n", q.Name)
		}
	}
	fmt.Println()
	fmt.Println(strings.TrimSpace(q.SQL))
	fmt.Println()
	if q.RandKey {
		fmt.Println(ui.Info(fmt.Sprintf("{KEY} is a random key per benchmark request, 1..max from: %s (default %d)",
			q.KeySQL, q.KeyDefault)))
		if q.KeySpan > 0 {
			fmt.Println(ui.Info(fmt.Sprintf("{KEY2} = {KEY} + %d", q.KeySpan)))
		}
		fmt.Println()
	}
}

// printQueryRonSQL implements .query_ronsql <name>.
func (s *Shell) printQueryRonSQL(name string) error {
	q := findRonSQLBenchQuery(name)
	if q == nil {
		return fmt.Errorf("unknown RonSQL query: %s (see .list_query_ronsql)", name)
	}
	printBenchQueryDetails(q, q.Name, true)
	return nil
}

// printQuerySQL implements .query_sql <name>.
func (s *Shell) printQuerySQL(name string) error {
	q := findSQLBenchQuery(name)
	if q == nil {
		return fmt.Errorf("unknown SQL query: %s (see .list_query_sql)", name)
	}
	printBenchQueryDetails(q, q.sqlBenchName(), false)
	return nil
}

// runExplainRonSQL implements .explain_ronsql <name>: EXPLAIN via the RonSQL
// REST endpoint. explainMode=FORCE makes RonSQL treat the unmodified query
// as EXPLAIN, and EXPLAIN output requires TEXT format (JSON is not
// implemented for EXPLAIN). The plan is printed without executing the query.
func (s *Shell) runExplainRonSQL(name string) error {
	q := findRonSQLBenchQuery(name)
	if q == nil {
		return fmt.Errorf("unknown RonSQL query: %s (see .list_query_ronsql)", name)
	}
	if q.MySQLOnly {
		return fmt.Errorf("%s uses ORDER BY/LIMIT outside the RonSQL envelope; use .explain_sql %s",
			q.Name, q.sqlBenchName())
	}

	sqlText, key := s.sampleBenchSQL(q)

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("EXPLAIN %s (RonSQL) - %s", q.Name, q.Description)))
	if q.RandKey {
		fmt.Println(ui.Info(fmt.Sprintf("Using sample key {KEY}=%d", key)))
	}
	fmt.Println()
	fmt.Println(strings.TrimSpace(sqlText))
	fmt.Println()

	req := RonSQLRequest{
		Query:        sqlText,
		Database:     q.Database,
		ExplainMode:  "FORCE",
		OutputFormat: "TEXT",
	}
	endpoint := "/" + APIVersion + "/ronsql"
	data, duration, err := s.restClient.Post(endpoint, req)
	if err != nil {
		if len(data) > 0 {
			fmt.Println(ui.Error("RonSQL response:"))
			fmt.Println(strings.TrimSpace(string(data)))
		}
		return fmt.Errorf("RonSQL EXPLAIN failed: %w", err)
	}
	if len(data) > 0 {
		fmt.Println(strings.TrimSpace(string(data)))
	}
	fmt.Println(ui.Timing(duration))
	return nil
}

// runExplainSQL implements .explain_sql <name>: EXPLAIN through the MySQL
// server, followed by SHOW WARNINGS (NDB reports pushed-join status and
// can't-push reasons as notes there).
func (s *Shell) runExplainSQL(name string) error {
	q := findSQLBenchQuery(name)
	if q == nil {
		return fmt.Errorf("unknown SQL query: %s (see .list_query_sql)", name)
	}

	sqlText, key := s.sampleBenchSQL(q)

	// Dedicated connection with the query's database as default schema so
	// unqualified table names resolve regardless of the shell's current USE.
	c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
		Host:     s.config.MySQLHost,
		Port:     s.config.MySQLPort,
		User:     s.mysqlUser,
		Password: s.mysqlPass,
		TLS:      s.config.TLS,
		Database: q.Database,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer c.Close()

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("EXPLAIN %s (MySQL server) - %s", q.sqlBenchName(), q.Description)))
	if q.RandKey {
		fmt.Println(ui.Info(fmt.Sprintf("Using sample key {KEY}=%d", key)))
	}
	fmt.Println()
	fmt.Println(strings.TrimSpace(sqlText))
	fmt.Println()

	columns, rows, duration, err := c.Query("EXPLAIN " + sqlText)
	if err != nil {
		return fmt.Errorf("EXPLAIN failed: %w", err)
	}
	fmt.Print(ui.RenderSQLResultWithDuration(columns, rows, duration))

	// SHOW WARNINGS must run on the EXPLAIN's session; this client is used
	// sequentially by one goroutine, so the pool reuses that connection.
	wcols, wrows, _, err := c.Query("SHOW WARNINGS")
	if err == nil && len(wrows) > 0 {
		fmt.Println()
		fmt.Println(ui.Info("Warnings/notes (NDB pushdown status):"))
		fmt.Print(ui.RenderSQLResult(wcols, wrows))
	}
	return nil
}
