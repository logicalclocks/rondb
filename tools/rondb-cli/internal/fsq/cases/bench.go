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

package cases

// The fs_hw benchmark registry (E5, benchmarks.md §2-§3): one entry per
// Hopsworks serving shape, generated from the same emitter as the golden
// tests, with request-time placeholders where the bound cases carry keys.
// The shell registers the entries in the .bench_ronsql / .bench_sql
// registry; ResolveBenchPlaceholders renders them per request.

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// BenchEntry is one fs_hw benchmark: a statement template whose
// placeholders are resolved per request (BenchPlaceholderLegend).
type BenchEntry struct {
	Name        string
	Shape       string
	Description string
	SQL         string
	MySQLOnly   bool     // a production MySQL twin: runs through .bench_sql only
	Rows        string   // expected result rows per request
	PlanPins    []string // substrings expected in the RonSQL EXPLAIN (warmup warnings, benchmarks.md §7)
}

// BenchPlaceholderLegend documents the request-time placeholders.
const BenchPlaceholderLegend = "Placeholders per request: {KEY} random customer in 1..max (KeySQL); {KEYS:n} n distinct customers; " +
	"{SKEY}/{SKEYS:n} string keys of customers 1..max/10; {ACCT} random account in 1..max/2 with {CUR} one of its currencies; " +
	"{NOW-7d} etc. the TIMESTAMP literal FS_NOW minus the window"

// BenchEntries generates the fs_hw registry for the database cfg.DB (the
// MySQL twins are database-qualified; the RonSQL text is not).
func BenchEntries(cfg Config) ([]BenchEntry, error) {
	b := newBuilder(cfg)
	var out []BenchEntry
	var errs []error
	fail := func(name string, err error) { errs = append(errs, fmt.Errorf("%s: %w", name, err)) }
	tx := b.fg(data.TTransactions)
	leaf := func(feature string, cond spec.SqlCondition, value string) spec.Filter {
		c := cond
		return spec.Filter{FG: tx.ID, Logic: spec.LogicSingle, Feature: feature, Condition: &c, Value: strp(value)}
	}
	day := int64(86400)

	// emitted binds the template-carrying DTO of a view with placeholders:
	// batch > 0 renders the key list placeholder for that many keys.
	emitted := func(name string, v *spec.View, batch int) (StatementGroup, bool) {
		dtos, err := emit.Build(v)
		if err != nil {
			fail(name, err)
			return StatementGroup{}, false
		}
		for _, st := range dtos {
			if st.QueryRonsql == nil && len(st.SnowflakeTemplates) == 0 {
				continue
			}
			keys, err := benchKeys(st, batch)
			if err != nil {
				fail(name, err)
				return StatementGroup{}, false
			}
			g, err := benchBind(st, keys)
			if err != nil {
				fail(name, err)
				return StatementGroup{}, false
			}
			return g, true
		}
		fail(name, fmt.Errorf("no RonSQL template emitted"))
		return StatementGroup{}, false
	}
	ronsql := func(g StatementGroup, i int) string {
		if i < len(g.Statements) {
			return g.Statements[i].RonSQL
		}
		return ""
	}
	twin := func(g StatementGroup) string {
		if g.MySQL != nil {
			return *g.MySQL
		}
		return ""
	}
	add := func(e BenchEntry) {
		if e.SQL == "" {
			fail(e.Name, fmt.Errorf("empty statement"))
			return
		}
		out = append(out, e)
	}

	// Plan pins from the first run's EXPLAIN output (benchmarks.md §7):
	// point reads on the ordered PK index, IN lists as a table scan (F12,
	// pinned as observed so a planner fix shows up as a pin warning),
	// collect in index order, snowflake CTE bodies and PK lookups.
	indexPins := []string{"Execute as index scan.", "Index: `PRIMARY`"}
	scanPins := []string{"Execute as table scan."}
	collectPins := func(n int) []string {
		return []string{"Execute as index scan.", "ORDER BY: index order (SF_OrderBy | SF_Descending", fmt.Sprintf("Result limited to %d rows.", n)}
	}
	snowPins := []string{"Body root: INDEX_SCAN using PRIMARY", "[ROOT] CTE_SCAN", "[INNER] PK_LOOKUP"}
	snowBatchPins := []string{"Body root: TABLE_SCAN", "[ROOT] CTE_SCAN", "[INNER] PK_LOOKUP"}
	leftPins := []string{"Body root: INDEX_SCAN using PRIMARY", "[ROOT] CTE_SCAN", "[LEFT JOIN] PK_LOOKUP"}

	point := spec.AggSpec{{Key: "amount", Fns: []string{"count", "sum", "max"}}, {Key: "fee", Fns: []string{"min"}}}
	batchAgg := spec.AggSpec{{Key: "amount", Fns: []string{"count", "sum"}}}
	greatest := spec.AggSpec{{Key: "*", Fns: []string{"count"}}, {Key: "amount,fee", Fns: []string{"greatest", "least"}}}

	add(BenchEntry{Name: "fs_hw_floor", Shape: "-", Description: "Fixed-overhead floor: COUNT(*) over countries_1 (40 rows)", Rows: "1",
		SQL: "SELECT COUNT(*) FROM `countries_1`;"})
	if g, ok := emitted("fs_hw_agg_point", b.aggView("hw-agg", data.TTransactions, "tx_", point, nil, false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_agg_point", Shape: "S1", Description: "point aggregate over one customer's history", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	if g, ok := emitted("fs_hw_agg_window7d", b.aggView("hw-w7", data.TTransactions, "tx_", point, i64p(7*day), false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_agg_window7d", Shape: "S2", Description: "point aggregate with a 7-day window", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	if g, ok := emitted("fs_hw_agg_greatest", b.aggView("hw-gr", data.TTransactions, "tx_", greatest, nil, false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_agg_greatest", Shape: "S5", Description: "MAX(GREATEST) / MAX(LEAST) fold", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	if g, ok := emitted("fs_hw_agg_filter", b.aggView("hw-flt", data.TTransactions, "tx_", point, nil, false,
		[]spec.Filter{leaf("category", spec.CondEquals, "grocery"), leaf("amount", spec.CondGreaterThanOrEqual, "300")}), 0); ok {
		add(BenchEntry{Name: "fs_hw_agg_filter", Shape: "S4", Description: "point aggregate with two feature-view filters", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	for _, n := range []int{10, 100, 1000} {
		name := fmt.Sprintf("fs_hw_agg_batch%d", n)
		if g, ok := emitted(name, b.aggView("hw-b", data.TTransactions, "tx_", batchAgg, nil, true, nil), n); ok {
			add(BenchEntry{Name: name, Shape: "S3", Description: fmt.Sprintf("batch aggregate, IN list of %d + GROUP BY (F12: table scan)", n), Rows: fmt.Sprintf("<= %d", n), SQL: ronsql(g, 0), PlanPins: scanPins})
		}
	}
	if g, ok := emitted("fs_hw_agg_batch100_window", b.aggView("hw-bw", data.TTransactions, "tx_", batchAgg, i64p(30*day), true, nil), 100); ok {
		add(BenchEntry{Name: "fs_hw_agg_batch100_window", Shape: "S3+S2", Description: "batch of 100 with a 30-day window (F12: table scan)", Rows: "<= 100", SQL: ronsql(g, 0), PlanPins: scanPins})
	}
	for _, n := range []int{5, 50} {
		name := fmt.Sprintf("fs_hw_collect%d", n)
		g, ok := emitted(name, b.collectView("hw-c", n, false, false), 0)
		if !ok {
			continue
		}
		direct, err := directCollect(ronsql(g, 0))
		if err != nil {
			fail(name, err)
			continue
		}
		add(BenchEntry{Name: name, Shape: "S6b", Description: fmt.Sprintf("collect the newest %d rows, direct form", n), Rows: fmt.Sprintf("<= %d", n), SQL: direct, PlanPins: collectPins(n)})
		if n == 5 {
			add(BenchEntry{Name: "fs_hw_collect5_cte", Shape: "S6", Description: "collect the newest 5 rows, Hopsworks CTE form (F0: clean rejection expected)", Rows: "<= 5 (or REJECT)", SQL: ronsql(g, 0)})
			add(BenchEntry{Name: "fs_hw_collect5_twin", Shape: "S6 twin", Description: "production MySQL collect statement (ROW_NUMBER window, rank <= 5)", Rows: "<= 5", MySQLOnly: true, SQL: twin(g)})
		}
	}
	if g, ok := emitted("fs_hw_snow1_point", b.snowflakeView("hw-s1", 1, spec.JoinInner, false), 0); ok {
		add(BenchEntry{Name: "fs_hw_snow1_point", Shape: "S7", Description: "snowflake 1-hop INNER (customer -> region)", Rows: "<= 1", SQL: ronsql(g, 0), PlanPins: snowPins})
		add(BenchEntry{Name: "fs_hw_snow1_twin", Shape: "S7 twin", Description: "production MySQL nested join, 1 hop", Rows: "<= 1", MySQLOnly: true, SQL: twin(g)})
	}
	var snow2 string
	if g, ok := emitted("fs_hw_snow2_point", b.snowflakeView("hw-s2", 2, spec.JoinInner, false), 0); ok {
		snow2 = ronsql(g, 0)
		add(BenchEntry{Name: "fs_hw_snow2_point", Shape: "S7", Description: "snowflake 2-hop INNER (customer -> region -> country)", Rows: "<= 1", SQL: snow2, PlanPins: snowPins})
		add(BenchEntry{Name: "fs_hw_snow2_twin", Shape: "S7 twin", Description: "production MySQL nested join, 2 hops", Rows: "<= 1", MySQLOnly: true, SQL: twin(g)})
	}
	if g, ok := emitted("fs_hw_snow1_batch100", b.snowflakeView("hw-s1b", 1, spec.JoinInner, true), 100); ok {
		add(BenchEntry{Name: "fs_hw_snow1_batch100", Shape: "S7 batch", Description: "snowflake 1-hop batch of 100 (root key projected; F12: CTE body table scan)", Rows: "<= 100", SQL: ronsql(g, 0), PlanPins: snowBatchPins})
	}
	if g, ok := emitted("fs_hw_snow2_left_chain", b.snowflakeView("hw-s8", 2, spec.JoinLeft, false), 0); ok {
		add(BenchEntry{Name: "fs_hw_snow2_left_chain", Shape: "S8", Description: "LEFT per-chain template for the country node (two inner hops, one projection)", Rows: "<= 1", SQL: ronsql(g, 1), PlanPins: snowPins})
	}
	if snow2 != "" {
		left := strings.Replace(snow2, " FROM `b` JOIN ", " FROM `b` LEFT JOIN ", 1)
		left = strings.ReplaceAll(left, "` JOIN `countries_1`", "` LEFT JOIN `countries_1`")
		add(BenchEntry{Name: "fs_hw_snow2_left_single", Shape: "S8b", Description: "single-statement LEFT JOIN hops (future Hopsworks shape)", Rows: "1", SQL: left, PlanPins: leftPins})
	}
	if g, ok := emitted("fs_hw_strkey_point", b.aggView("hw-str", data.TTxStr, "s_", point, nil, false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_strkey_point", Shape: "S10", Description: "point aggregate over a VARCHAR entity key", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	if g, ok := emitted("fs_hw_strkey_batch100", b.aggView("hw-strb", data.TTxStr, "s_", batchAgg, nil, true, nil), 100); ok {
		add(BenchEntry{Name: "fs_hw_strkey_batch100", Shape: "S10+S3", Description: "batch of 100 string keys (F12: table scan; F10 crashes the pushed mysqld arm)", Rows: "<= 100", SQL: ronsql(g, 0), PlanPins: scanPins})
	}
	hist := spec.AggSpec{{Key: "*", Fns: []string{"count"}}, {Key: "delta", Fns: []string{"sum"}}}
	if g, ok := emitted("fs_hw_composite_point", b.aggView("hw-hist", data.TBalanceHist, "b_", hist, i64p(90*day), false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_composite_point", Shape: "S9", Description: "composite entity key (account, currency) with a 90-day window", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	if g, ok := emitted("fs_hw_hash_point", b.aggView("hw-hash", data.TTxHash, "tx_", point, nil, false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_hash_point", Shape: "S1 hash PK", Description: "point aggregate on the hash-only PK twin (no ordered index: scan)", Rows: "1", SQL: ronsql(g, 0), PlanPins: scanPins})
	}
	sessions := spec.AggSpec{{Key: "*", Fns: []string{"count"}}, {Key: "duration", Fns: []string{"sum"}}}
	if g, ok := emitted("fs_hw_sessions_window2h", b.aggView("hw-sess", data.TSessions, "se_", sessions, i64p(2*3600), false, nil), 0); ok {
		add(BenchEntry{Name: "fs_hw_sessions_window2h", Shape: "S2 TIMESTAMP(3)", Description: "2-hour window on sessions_1 (TIMESTAMP(3) event time, ttl index)", Rows: "1", SQL: ronsql(g, 0), PlanPins: indexPins})
	}
	if len(errs) > 0 {
		return nil, errs[0]
	}
	return out, nil
}

// aggView is a single-join aggregate feature view over any history table.
func (b *builder) aggView(name, table, prefix string, agg spec.AggSpec, window *int64, batch bool, filters []spec.Filter) *spec.View {
	fg := b.fg(table)
	j := spec.Join{Index: 0, Parent: 0, FG: fg.ID, Type: spec.JoinInner, Prefix: strp(prefix), Aggregate: agg, Window: window, Features: aggOutputs(agg)}
	return b.view(name, []spec.FeatureGroup{fg}, []spec.Join{j}, filters, spec.Options{Batch: batch})
}

// benchKeys maps the DTO parameters to placeholders: one scalar per
// parameter, or one list placeholder for a batch.
func benchKeys(st emit.Statement, batch int) ([]bind.Arg, error) {
	var keys []bind.Arg
	for _, p := range st.PreparedStatementParameters {
		var scalar, list string
		switch p.Name {
		case "customer_id":
			scalar, list = "{KEY}", "{KEYS:%d}"
		case "customer_key":
			scalar, list = "{SKEY}", "{SKEYS:%d}"
		case "account_id":
			scalar = "{ACCT}"
		case "currency":
			scalar = "{CUR}"
		default:
			return nil, fmt.Errorf("no placeholder for parameter %s", p.Name)
		}
		if batch > 0 {
			if list == "" {
				return nil, fmt.Errorf("parameter %s has no batch placeholder", p.Name)
			}
			keys = append(keys, bind.List([]string{fmt.Sprintf(list, batch)}))
			continue
		}
		keys = append(keys, bind.Scalar(scalar))
	}
	return keys, nil
}

// benchBind binds a DTO with placeholder keys and a window placeholder.
func benchBind(st emit.Statement, keys []bind.Arg) (StatementGroup, error) {
	g := StatementGroup{DTO: st}
	ronArgs := append([]bind.Arg(nil), keys...)
	if st.AggregateWindow != nil {
		ronArgs = append(ronArgs, bind.Scalar(windowPlaceholder(*st.AggregateWindow)))
	}
	if st.QueryOnline != nil {
		mysqlArgs := append([]bind.Arg(nil), ronArgs...)
		if st.CollectN != nil {
			mysqlArgs = append(mysqlArgs, bind.Scalar(bind.Int(int64(*st.CollectN))))
		}
		twin, err := bind.Bind(*st.QueryOnline, mysqlArgs)
		if err != nil {
			return StatementGroup{}, fmt.Errorf("mysql twin: %w", err)
		}
		g.MySQL = &twin
	}
	if st.QueryRonsql != nil {
		r, err := bind.Bind(*st.QueryRonsql, ronArgs)
		if err != nil {
			return StatementGroup{}, fmt.Errorf("ronsql: %w", err)
		}
		g.Statements = append(g.Statements, Statement{Label: "ronsql", RonSQL: r})
	}
	for i, t := range st.SnowflakeTemplates {
		r, err := bind.Bind(t, keys)
		if err != nil {
			return StatementGroup{}, fmt.Errorf("snowflake template %d: %w", i, err)
		}
		g.Statements = append(g.Statements, Statement{Label: fmt.Sprintf("snowflake[%d]", i), RonSQL: r})
	}
	return g, nil
}

// windowPlaceholder renders a window of seconds as {NOW-7d} / {NOW-2h} / {NOW-90s}.
func windowPlaceholder(secs int64) string {
	switch {
	case secs%86400 == 0:
		return fmt.Sprintf("{NOW-%dd}", secs/86400)
	case secs%3600 == 0:
		return fmt.Sprintf("{NOW-%dh}", secs/3600)
	}
	return fmt.Sprintf("{NOW-%ds}", secs)
}

// RenderBenchRegistry prints the entries as the reviewable golden dump.
func RenderBenchRegistry(entries []BenchEntry) string {
	var sb strings.Builder
	sb.WriteString("# fs_hw benchmark registry — generated by cases.BenchEntries (RONDB-1121 E5); do not edit by hand.\n")
	sb.WriteString("# " + BenchPlaceholderLegend + "\n")
	for _, e := range entries {
		side := "ronsql+mysql"
		if e.MySQLOnly {
			side = "mysql-only"
		}
		fmt.Fprintf(&sb, "\n=== %s [%s] %s rows=%s\n-- %s\n%s\n", e.Name, e.Shape, side, e.Rows, e.Description, e.SQL)
		if len(e.PlanPins) > 0 {
			fmt.Fprintf(&sb, "-- plan pins: %s\n", strings.Join(e.PlanPins, " | "))
		}
	}
	return sb.String()
}

// ---- request-time placeholders --------------------------------------------------

var benchPlaceholder = regexp.MustCompile(`\{(KEY|KEYS:[0-9]+|SKEY|SKEYS:[0-9]+|ACCT|CUR|NOW-[0-9]+[hds])\}`)

// ResolveBenchPlaceholders substitutes every fs_hw placeholder of sql for
// one request.  maxKey is the customer domain E (KeySQL); the account
// domain is 1..E/2 and the string-history domain 1..E/10 (data_model.md);
// {ACCT} and {CUR} of one statement refer to the same account; now is the
// FS_NOW reference clock.
func ResolveBenchPlaceholders(sql string, rng *rand.Rand, maxKey int, now time.Time) string {
	if maxKey < 1 {
		maxKey = 1
	}
	strDomain := maxKey / 10
	if strDomain < 1 {
		strDomain = 1
	}
	acctDomain := maxKey / 2
	if acctDomain < 1 {
		acctDomain = 1
	}
	account := int64(0)
	return benchPlaceholder.ReplaceAllStringFunc(sql, func(m string) string {
		tok := m[1 : len(m)-1]
		switch {
		case tok == "KEY":
			return strconv.Itoa(rng.Intn(maxKey) + 1)
		case strings.HasPrefix(tok, "KEYS:"):
			n, _ := strconv.Atoi(tok[5:])
			return joinInts(distinctKeys(rng, maxKey, n))
		case tok == "SKEY":
			return bind.Str(data.CustomerKey(int64(rng.Intn(strDomain) + 1)))
		case strings.HasPrefix(tok, "SKEYS:"):
			n, _ := strconv.Atoi(tok[6:])
			keys := distinctKeys(rng, strDomain, n)
			parts := make([]string, len(keys))
			for i, k := range keys {
				parts[i] = bind.Str(data.CustomerKey(int64(k)))
			}
			return strings.Join(parts, ", ")
		case tok == "ACCT" || tok == "CUR":
			if account == 0 {
				account = int64(rng.Intn(acctDomain) + 1)
			}
			if tok == "ACCT" {
				return strconv.FormatInt(account, 10)
			}
			return bind.Str(data.Currencies[rng.Intn(data.NCurrencies(account))])
		default: // NOW-<n><unit>
			n, _ := strconv.Atoi(tok[4 : len(tok)-1])
			unit := time.Second
			switch tok[len(tok)-1] {
			case 'h':
				unit = time.Hour
			case 'd':
				unit = 24 * time.Hour
			}
			return bind.Timestamp(now.Add(-time.Duration(n) * unit))
		}
	})
}

// distinctKeys draws n distinct keys from 1..max (all of them when n >= max).
func distinctKeys(rng *rand.Rand, max, n int) []int {
	if n >= max {
		out := make([]int, max)
		for i := range out {
			out[i] = i + 1
		}
		return out
	}
	if n > max/2 {
		perm := rng.Perm(max)[:n]
		out := make([]int, n)
		for i, p := range perm {
			out[i] = p + 1
		}
		return out
	}
	seen := make(map[int]bool, n)
	out := make([]int, 0, n)
	for len(out) < n {
		k := rng.Intn(max) + 1
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

func joinInts(vs []int) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ", ")
}
