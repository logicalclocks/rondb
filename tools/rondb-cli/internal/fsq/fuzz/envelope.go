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

package fuzz

// The envelope-level generator (RONDB-1121 E7, random_generator.md §5):
// RonSQL statements over the feature-store schema beyond the Hopsworks
// shapes, each tagged with the constructs it contains so the expectation
// table (§5.3) decides whether a RonSQL rejection is expected.  Every
// identifier is backticked, every table carries an explicit AS alias,
// columns are qualified whenever more than one table is in scope, and the
// statement ends with ';'.

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
)

// EnvVersion is part of every envelope case id.  v2 (2026-10-06): the
// real-join and former-hazard productions, HAVING over aggregate
// functions and the colcol-mixed-type probe change what every seed
// generates.
const EnvVersion = "env-v2"

// Expectation is one row of the expectation table: a construct present in
// a statement and the RonSQL outcome it implies.
type Expectation struct {
	Construct string
	Reject    bool
	Pattern   string // substring of the RonSQL error message
	Finding   string // ledger id
	Note      string
	// TaggedOnly matches Pattern only for a case that carries Construct
	// (the message is generic, e.g. "Could not find column"), never in the
	// untagged-fallback scan.
	TaggedOnly bool
	// AltPatterns are additional accepted messages for the same construct
	// (one unsupported construct can surface more than one permanent-error
	// message depending on the companion clauses, e.g. HAVING).
	AltPatterns []string
	// KnownWrong marks a construct the engine now RUNS (it used to reject)
	// but whose result diverges from MySQL: a documented wrong result, not a
	// clean rejection.  A result mismatch on such a case is KNOWN-WRONG.
	KnownWrong bool
}

// Matches reports whether an engine error message satisfies the expectation.
func (e Expectation) Matches(message string) bool {
	if e.Pattern != "" && strings.Contains(message, e.Pattern) {
		return true
	}
	for _, alt := range e.AltPatterns {
		if strings.Contains(message, alt) {
			return true
		}
	}
	return false
}

// Expectations is the expectation table (random_generator.md §5.3); the
// unit test checks every pattern against the engine sources.
var Expectations = []Expectation{
	{Construct: "cte-partial-key", Reject: true, Pattern: "Partial CTE lookup key not supported", Finding: "F18", Note: "partial-key CTE lookup (binds fewer than all virtual-key columns) whose demoted root would be an index scan below the CTE scan: rejected since the F18 fix (it ran and returned the wrong row count)"},
	{Construct: "join-no-index", Reject: true, Pattern: "no suitable index on join columns", Note: "a join column without a usable index"},
	{Construct: "orderby-in-subquery", Reject: true, Pattern: "ORDER BY / LIMIT in a subquery is not supported", Note: "ORDER BY / LIMIT inside an IN (subquery)"},
	{Construct: "greatest-eq-where", Reject: true, Pattern: "GREATEST/LEAST with = or !=", Note: "GREATEST/LEAST compared with = or != in WHERE"},
	{Construct: "greatest-non-int", Reject: true, Pattern: "column operand must be an integer", Note: "GREATEST/LEAST over a non-integer column"},
	{Construct: "avg-string", Reject: true, Pattern: "AVG over string columns is not supported", Note: "AVG over a string column (F16, fixed: was the generic 'Failed writing aggregation program. Please report a bug.')"},
	{Construct: "avg-temporal", Reject: true, Pattern: "AVG over temporal columns is not supported", Note: "AVG over a temporal column (F16, fixed: was the generic 'Failed writing aggregation program. Please report a bug.')"},
	{Construct: "index-hint-joined", Reject: true, Pattern: "Index hints (FORCE/USE/IGNORE INDEX) are only supported on the", Note: "an index hint on a joined table"},
	{Construct: "cross-table-where", Reject: true, Pattern: "Cross-table WHERE", Note: "a WHERE atom comparing columns of two tables outside the supported forms"},
	{Construct: "syntax", Reject: true, Pattern: "Syntax error", Note: "implicit alias, DISTINCT, BETWEEN, OFFSET, UNION, RIGHT JOIN: outside the grammar"},
	{Construct: "having", Reject: true, Pattern: "HAVING can only reference aggregate functions", TaggedOnly: true, Note: "HAVING on the alias of an aggregate output: HAVING supports aggregate functions (and SELECT-list subquery aliases) only, and rejects any other identifier before column resolution, whatever ORDER BY / LIMIT accompanies it (was 'Could not find column', or the F17 internal error with ORDER BY on the alias + LIMIT)"},
	{Construct: "left-join-where-null", Reject: true, Pattern: "WHERE on a LEFT JOIN's right side that holds for NULL is not", Finding: "F32", Note: "a WHERE condition on the right side of a LEFT JOIN of real tables that RonSQL cannot prove NULL-rejecting (IS NULL, an OR with IS NULL): the table's operation could only apply it as part of the join condition"},
	{Construct: "subquery-joined", Reject: true, Pattern: "A WHERE condition with a subquery", Finding: "F32b", Note: "IN (subquery) / EXISTS / a correlated subquery on a joined table's column: its result replaces the node only at execution time and is known to reach the root table's filter only"},
	{Construct: "fanout-sibling", Reject: true, Pattern: "Aggregation references a column from a join branch that is not an", Finding: "D5", Note: "an aggregation over a CTE_LOOKUP child grouped by a real-table sibling under the same parent (fan-out across a non-ancestor sibling; it crashed RDRS before the guard)"},
	{Construct: "colcol-mixed-type", Reject: true, Pattern: "requires columns of identical type, precision, length, scale", Finding: "D4", Note: "column-vs-column comparison in a CTE-body WHERE between columns of different types (INT vs BIGINT): supported only for identical types since RONDB-1107"},
}

// ExpectationFor returns the table row of a construct.
func ExpectationFor(construct string) (Expectation, bool) {
	for _, e := range Expectations {
		if e.Construct == construct {
			return e, true
		}
	}
	return Expectation{}, false
}

// EnvCase is one generated statement.
type EnvCase struct {
	ID         string
	Seed       uint64
	Index      int
	Production string
	SQL        string
	Ordered    bool     // ORDER BY present: ordered comparison
	Constructs []string // tags matched against the expectation table
	Signature  string
	Hazard     string // discovery-log id when the case is a hazard translation
}

// ExpectReject reports the first expectation-table rejection the case's
// constructs imply.
func (c EnvCase) ExpectReject() (Expectation, bool) {
	for _, k := range c.Constructs {
		if e, ok := ExpectationFor(k); ok && e.Reject {
			return e, true
		}
	}
	return Expectation{}, false
}

// KnownWrong reports a construct the engine runs with a known-wrong result.
func (c EnvCase) KnownWrong() (Expectation, bool) {
	for _, k := range c.Constructs {
		if e, ok := ExpectationFor(k); ok && e.KnownWrong {
			return e, true
		}
	}
	return Expectation{}, false
}

// Productions are the sampled statement families with their weights
// (random_generator.md §5.2).
var Productions = []struct {
	Name   string
	Weight int
}{
	{"single-agg", 15}, {"collect-direct", 10}, {"cte-per-fg", 20}, {"snow-inner", 15}, {"snow-left", 10},
	{"batch-in", 8}, {"filters", 8}, {"having-order", 6}, {"cte-body-orderby", 4}, {"probe", 4},
	{"real-join", 8}, {"former-hazard", 4},
}

// EnvGen samples envelope cases.
type EnvGen struct {
	g *Gen
}

// NewEnvelope prepares the envelope generator over the same schema and
// data model as the spec generator.
func NewEnvelope(cfg Config) *EnvGen { return &EnvGen{g: New(cfg)} }

// Config returns the generator configuration.
func (e *EnvGen) Config() Config { return e.g.cfg }

// Case regenerates envelope case i of a seed; only restricts the
// productions (nil = all).
func (e *EnvGen) Case(seed uint64, i int, only map[string]bool) EnvCase {
	s := &sampler{g: e.g, rng: rand.New(rand.NewPCG(seed^0x9e3779b97f4a7c15, uint64(i)))}
	c := EnvCase{ID: fmt.Sprintf("%s-%d-%d", EnvVersion, seed, i), Seed: seed, Index: i}
	var names []string
	var weights []int
	for _, p := range Productions {
		if only == nil || only[p.Name] {
			names = append(names, p.Name)
			weights = append(weights, p.Weight)
		}
	}
	if len(names) == 0 {
		names, weights = []string{"single-agg"}, []int{1}
	}
	c.Production = names[s.choose(weights...)]
	es := &envSampler{sampler: s, c: &c}
	switch c.Production {
	case "single-agg":
		es.singleAgg(false)
	case "collect-direct":
		es.collectDirect()
	case "cte-per-fg":
		es.ctePerFG()
	case "snow-inner":
		es.snowflake(false)
	case "snow-left":
		es.snowflake(true)
	case "batch-in":
		es.batchIn()
	case "filters":
		es.singleAgg(true)
	case "having-order":
		es.havingOrder()
	case "cte-body-orderby":
		es.cteBodyOrderBy()
	case "probe":
		es.probe()
	case "real-join":
		es.realJoin()
	case "former-hazard":
		es.formerHazard()
	}
	c.Signature = c.Production + "|" + strings.Join(c.Constructs, ",")
	return c
}

// ---- rendering helpers -----------------------------------------------------------

type envSampler struct {
	*sampler
	c *EnvCase
}

func q(name string) string { return "`" + name + "`" }

func qc(alias, name string) string { return q(alias) + "." + q(name) }

func (es *envSampler) tag(constructs ...string) {
	es.c.Constructs = append(es.c.Constructs, constructs...)
}

// keyList renders an IN list of n customer ids drawn by class.
func (es *envSampler) keyList(n int) string {
	parts := make([]string, 0, n)
	seen := map[int64]bool{}
	for len(parts) < n {
		c, _ := es.customerKey()
		if seen[c] {
			c = int64(es.rng.IntN(int(es.g.cfg.Scale.E))) + 1
		}
		if seen[c] {
			continue
		}
		seen[c] = true
		parts = append(parts, strconv.FormatInt(c, 10))
	}
	return strings.Join(parts, ", ")
}

func (es *envSampler) oneKey() string {
	c, _ := es.customerKey()
	return strconv.FormatInt(c, 10)
}

// windowBound renders a DATE_SUB bound on the event time.
func (es *envSampler) windowBound(alias string) string {
	units := []struct {
		n    int
		unit string
	}{{1, "HOUR"}, {6, "HOUR"}, {1, "DAY"}, {7, "DAY"}, {30, "DAY"}, {90, "DAY"}}
	u := units[es.rng.IntN(len(units))]
	col := q("event_time")
	if alias != "" {
		col = qc(alias, "event_time")
	}
	es.tag("date-sub")
	return col + " >= DATE_SUB('" + data.FSNowString + "', INTERVAL " + strconv.Itoa(u.n) + " " + u.unit + ")"
}

// txFilter renders one WHERE atom on transactions_1 (alias optional):
// comparisons, IN lists on non-key columns, LIKE, IS [NOT] NULL, NOT, OR.
func (es *envSampler) txFilter(alias string) string {
	col := func(n string) string {
		if alias == "" {
			return q(n)
		}
		return qc(alias, n)
	}
	switch es.choose(35, 15, 15, 10, 10, 15) {
	case 0:
		ops := []string{"=", "<>", ">", ">=", "<", "<="}
		return col("amount") + " " + ops[es.rng.IntN(len(ops))] + " " + strconv.Itoa(100+es.rng.IntN(900))
	case 1:
		es.tag("in-list")
		var vals []string
		for i := 0; i < 1+es.rng.IntN(3); i++ {
			vals = append(vals, bind.Str(data.Categories[es.rng.IntN(len(data.Categories))]))
		}
		return col("category") + " IN (" + strings.Join(vals, ", ") + ")"
	case 2:
		es.tag("like")
		return col("category") + " LIKE " + bind.Str(data.Categories[es.rng.IntN(len(data.Categories))][:2]+"%")
	case 3:
		es.tag("is-null")
		if es.pct(50) {
			return col("merchant_id") + " IS NULL"
		}
		return col("merchant_id") + " IS NOT NULL"
	case 4:
		es.tag("not")
		return "NOT " + col("fee") + " > " + strconv.Itoa(-10+es.rng.IntN(50))
	default:
		es.tag("or")
		return "(" + col("category") + " = " + bind.Str(data.Categories[es.rng.IntN(len(data.Categories))]) + " OR " +
			col("amount") + " >= " + strconv.Itoa(100+es.rng.IntN(900)) + ")"
	}
}

// ---- productions ---------------------------------------------------------------

// singleAgg: the S1-like baseline; withFilters mixes in the filter atoms.
func (es *envSampler) singleAgg(withFilters bool) {
	es.tag("single-agg")
	where := q("customer_id") + " = " + es.oneKey()
	if es.pct(50) {
		where += " AND " + es.windowBound("")
	}
	if withFilters {
		es.tag("filters")
		for i := 0; i < 1+es.rng.IntN(3); i++ {
			where += " AND " + es.txFilter("")
		}
	}
	outputs := []string{"COUNT(*) AS `n`", "SUM(`amount`) AS `amount_sum`", "MIN(`fee`) AS `fee_min`", "MAX(`amount`) AS `amount_max`"}
	if es.pct(30) {
		outputs = append(outputs, "MAX(GREATEST(`amount`, `fee`)) AS `g`", "MIN(LEAST(`amount`, `fee`)) AS `l`")
		es.tag("greatest")
	}
	if es.pct(15) {
		outputs = append(outputs, "AVG(`amount`) AS `amount_avg`")
		es.tag("avg")
	}
	if es.pct(5) {
		// Signature-only tag: MIN/MAX over a temporal column runs since F9
		// (unquoted JSON output) was fixed in RONDB-1124 M1.1.
		outputs = append(outputs, "MAX(`event_time`) AS `event_time_max`")
		es.tag("temporal-minmax")
	}
	es.c.SQL = "SELECT " + strings.Join(outputs, ", ") + " FROM `transactions_1` WHERE " + where + ";"
}

// collectDirect: the S6b projection-only form with ORDER BY / LIMIT.
func (es *envSampler) collectDirect() {
	es.tag("collect-direct", "orderby-limit")
	n := []int{1, 5, 50}[es.rng.IntN(3)]
	dir := "DESC"
	if es.pct(40) {
		dir = "ASC"
	}
	where := q("customer_id") + " = " + es.oneKey()
	if es.pct(30) {
		where += " AND " + es.txFilter("")
	}
	es.c.Ordered = true
	es.c.SQL = "SELECT `customer_id`, `event_time`, `amount`, `category` FROM `transactions_1` WHERE " + where +
		" ORDER BY `event_time` " + dir + " LIMIT " + strconv.Itoa(n) + ";"
}

// ctePerFG: one aggregating CTE per history group keyed by the entity,
// joined to the entity table (or to each other) in an aggregating main.
func (es *envSampler) ctePerFG() {
	es.tag("cte-per-fg")
	keys := es.keyList(1 + es.rng.IntN(10))
	type cte struct{ name, sql, out string }
	var ctes []cte
	add := func(name, table, col string) {
		ctes = append(ctes, cte{name, q(name) + " AS (SELECT `customer_id` AS `k`, COUNT(*) AS `n`, SUM(" + q(col) + ") AS `s` FROM " + q(table) +
			" WHERE `customer_id` IN (" + keys + ") GROUP BY `customer_id`)", "s"})
	}
	add("tx", "transactions_1", "amount")
	if es.pct(60) {
		add("se", "sessions_1", "duration")
	}
	if es.pct(30) {
		add("tx2", "transactions_1", "fee")
	}
	var with []string
	for _, c := range ctes {
		with = append(with, c.sql)
	}
	var sel, joins []string
	if es.pct(70) {
		// entity table root, CTE lookups as children
		sel = append(sel, "`c`.`customer_id` AS `customer_id`")
		left := false
		for _, c := range ctes {
			// LEFT is the feature-view default: a key the IN list leaves out
			// NULL-extends into the join-aggregation leaf, through a chain of
			// CTE_LOOKUPs when several CTEs are LEFT-joined (F15, fixed).
			jt := "JOIN"
			if es.pct(60) {
				jt, left = "LEFT JOIN", true
			}
			joins = append(joins, jt+" "+q(c.name)+" ON "+qc(c.name, "k")+" = `c`.`customer_id`")
			sel = append(sel, "MAX("+qc(c.name, "n")+") AS "+q(c.name+"_n"), "SUM("+qc(c.name, c.out)+") AS "+q(c.name+"_s"))
		}
		if left {
			es.tag("left-join")
		}
		es.c.SQL = "WITH " + strings.Join(with, ", ") + " SELECT " + strings.Join(sel, ", ") + " FROM `customers_1` AS `c` " +
			strings.Join(joins, " ") + " GROUP BY `c`.`customer_id`;"
		es.tag("real-root-cte-lookup")
		return
	}
	// CTE root joined to the other CTEs
	root := ctes[0]
	sel = append(sel, qc(root.name, "k")+" AS `k`", "MAX("+qc(root.name, "n")+") AS `n0`")
	for _, c := range ctes[1:] {
		joins = append(joins, "JOIN "+q(c.name)+" ON "+qc(c.name, "k")+" = "+qc(root.name, "k"))
		sel = append(sel, "MAX("+qc(c.name, "n")+") AS "+q(c.name+"_n"))
	}
	es.c.SQL = "WITH " + strings.Join(with, ", ") + " SELECT " + strings.Join(sel, ", ") + " FROM " + q(root.name) + " " +
		strings.Join(joins, " ") + " GROUP BY " + qc(root.name, "k") + ";"
	es.tag("cte-root")
}

// snowflake: the S7 / S8b shapes from a CTE root; string roots bind the CTE
// body on a VARCHAR key (F14, fixed in WP-F F3: compared strictly now);
// batch mode projects the root key.
func (es *envSampler) snowflake(left bool) {
	root, key, keyLit := "customers_1", "customer_id", es.oneKey()
	if es.pct(20) {
		root, key = "customers_str_1", "customer_key"
		c, _ := es.customerKey()
		keyLit = bind.Str(data.CustomerKey(c))
		es.tag("string-snowflake")
	}
	batch := es.pct(25)
	depth := 1 + es.rng.IntN(2)
	jt := "JOIN"
	if left {
		jt = "LEFT JOIN"
		es.tag("snow-left", "left-join")
	} else {
		es.tag("snow-inner")
	}
	cteCols := q("region_id")
	where := q(key) + " = " + keyLit
	if batch {
		cteCols = q(key) + ", " + q("region_id")
		where = q(key) + " IN (" + es.keyList(1+es.rng.IntN(20)) + ")"
		if key == "customer_key" {
			where = q(key) + " = " + keyLit
			batch = false
		} else {
			es.tag("batch")
		}
	}
	sel := []string{qc("j2", "region_name") + " AS `r_region_name`", qc("j2", "population") + " AS `r_population`"}
	joins := jt + " `regions_1` AS `j2` ON " + qc("j2", "region_id") + " = " + qc("b", "region_id")
	if depth == 2 {
		sel = append(sel, qc("j3", "country_name")+" AS `c_country_name`", qc("j3", "continent")+" AS `c_continent`")
		joins += " " + jt + " `countries_1` AS `j3` ON " + qc("j3", "country_id") + " = " + qc("j2", "country_id")
	}
	if batch {
		sel = append(sel, qc("b", key)+" AS "+q(key))
	}
	es.c.SQL = "WITH `b` AS (SELECT " + cteCols + ", COUNT(*) AS `hw_cnt` FROM " + q(root) + " WHERE " + where + " GROUP BY " + cteCols + ") SELECT " +
		strings.Join(sel, ", ") + " FROM `b` " + joins + ";"
}

// batchIn: the S3 batch aggregate with lists of 1 / 10 / 100 / 1000 keys.
func (es *envSampler) batchIn() {
	n := []int{1, 10, 100, 1000}[es.choose(15, 40, 35, 10)]
	if int64(n) > es.g.cfg.Scale.E {
		n = int(es.g.cfg.Scale.E)
	}
	es.tag("batch-in", "batch")
	where := q("customer_id") + " IN (" + es.keyList(n) + ")"
	if es.pct(40) {
		where += " AND " + es.windowBound("")
	}
	es.c.SQL = "SELECT `customer_id`, COUNT(*) AS `n`, SUM(`amount`) AS `amount_sum`, MAX(`fee`) AS `fee_max` FROM `transactions_1` WHERE " + where +
		" GROUP BY `customer_id`;"
}

// havingOrder: HAVING, ORDER BY on GROUP BY columns / aggregate aliases and
// LIMIT on an aggregating statement (the key as a tie-breaker keeps the
// order deterministic).
func (es *envSampler) havingOrder() {
	es.tag("having-order", "batch")
	where := q("customer_id") + " IN (" + es.keyList(5+es.rng.IntN(20)) + ")"
	sql := "SELECT `customer_id`, COUNT(*) AS `n`, SUM(`amount`) AS `amount_sum` FROM `transactions_1` WHERE " + where + " GROUP BY `customer_id`"
	having, order := es.pct(70), es.pct(80)
	aggHaving := having && es.pct(50)
	// HAVING on an aggregate alias is a clean reject (HAVING takes aggregate
	// functions only); HAVING over aggregate functions, and ORDER BY on a
	// GROUP BY column or an aggregate alias + LIMIT, are supported and pass.
	// An alias HAVING case rejects whatever ORDER BY accompanies it, so the
	// "having" tag carries it.
	switch {
	case having && !aggHaving:
		es.tag("having")
	case order:
		es.tag("orderby-limit")
	}
	switch {
	case aggHaving:
		es.tag("having-agg")
		sql += " HAVING " + []string{
			"COUNT(*) > " + strconv.Itoa(es.rng.IntN(6)),
			"SUM(`amount`) > " + strconv.Itoa(500*(1+es.rng.IntN(10))),
			"COUNT(*) >= " + strconv.Itoa(1+es.rng.IntN(4)) + " AND MAX(`fee`) < " + strconv.Itoa(10+es.rng.IntN(40)),
		}[es.rng.IntN(3)]
	case having:
		sql += " HAVING `n` > " + strconv.Itoa(es.rng.IntN(6))
	}
	if order {
		es.c.Ordered = true
		target := []string{"`amount_sum` DESC", "`n` DESC", "`customer_id` ASC"}[es.rng.IntN(3)]
		sql += " ORDER BY " + target
		if !strings.HasPrefix(target, "`customer_id`") {
			sql += ", `customer_id` ASC"
		}
		sql += " LIMIT " + strconv.Itoa(1+es.rng.IntN(10))
	}
	es.c.SQL = sql + ";"
}

// cteBodyOrderBy: ORDER BY / LIMIT inside a CTE body — the Hopsworks
// collect form (non-aggregating body over a partial key, F0) or an
// aggregating body top-N.
func (es *envSampler) cteBodyOrderBy() {
	if es.pct(50) {
		// Signature-only tag since RONDB-1124 M1.3 collapsed this form (F0);
		// a derived table's row order is unspecified, so the set is compared.
		es.tag("cte-body-orderby-nonagg")
		es.c.SQL = "WITH t AS (SELECT `customer_id`, `event_time`, `amount`, `category` FROM `transactions_1` WHERE `customer_id` = " + es.oneKey() +
			" ORDER BY `event_time` DESC LIMIT 5) SELECT `customer_id`, `event_time`, `amount`, `category` FROM t;"
		return
	}
	es.tag("cte-body-orderby-agg")
	es.c.SQL = "WITH `t` AS (SELECT `customer_id` AS `k`, COUNT(*) AS `n` FROM `transactions_1` WHERE `customer_id` IN (" + es.keyList(10) +
		") GROUP BY `customer_id` ORDER BY `n` DESC LIMIT 3) SELECT MAX(`t`.`n`) AS `mx`, COUNT(*) AS `c` FROM `t`;"
}

// probe: one known-unsupported construct (§5.3) that must reject cleanly.
func (es *envSampler) probe() {
	k := es.oneKey()
	switch es.choose(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1) {
	case 0:
		es.tag("probe", "cte-partial-key")
		es.c.SQL = "WITH `x` AS (SELECT `account_id` AS `k1`, `currency` AS `k2`, COUNT(*) AS `n` FROM `balance_hist_1` GROUP BY `account_id`, `currency`) " +
			"SELECT `b`.`account_id`, SUM(`x`.`n`) AS `n` FROM `balances_1` AS `b` JOIN `x` ON `x`.`k1` = `b`.`account_id` GROUP BY `b`.`account_id`;"
	case 1:
		// Planned as a LEFT-joined CTE_LOOKUP under a PK-lookup root, not a
		// CTE_SCAN (the tag is historical).  F19, fixed: the lookup root
		// dropped the missed parent row; the case now compares strictly.
		es.tag("probe", "cte-scan-outer-child")
		es.c.SQL = "WITH `t` AS (SELECT `merchant_id` AS `k`, COUNT(*) AS `n` FROM `transactions_1` WHERE `customer_id` = " + k + " GROUP BY `merchant_id`) " +
			"SELECT `m`.`mcc`, SUM(`t`.`n`) AS `n` FROM `merchants_1` AS `m` LEFT JOIN `t` ON `t`.`k` = `m`.`merchant_id` WHERE `m`.`merchant_id` = 1 GROUP BY `m`.`mcc`;"
	case 2:
		es.tag("probe", "inner-below-left")
		es.c.SQL = "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = " + k + " GROUP BY `region_id`) " +
			"SELECT `j2`.`region_name` AS `r`, `j3`.`country_name` AS `c` FROM `b` LEFT JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id` " +
			"JOIN `countries_1` AS `j3` ON `j3`.`country_id` = `j2`.`country_id`;"
	case 3:
		es.tag("probe", "join-no-index")
		es.c.SQL = "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = " + k + " GROUP BY `region_id`) " +
			"SELECT `j2`.`region_id` AS `r` FROM `b` JOIN `regions_1` AS `j2` ON `j2`.`population` = `b`.`region_id`;"
	case 4:
		es.tag("probe", "greatest-eq-where")
		es.c.SQL = "SELECT COUNT(*) AS `n` FROM `transactions_1` WHERE `customer_id` = " + k + " AND GREATEST(`amount`, `fee`) = 500;"
	case 5:
		es.tag("probe", "greatest-non-int")
		es.c.SQL = "SELECT MAX(GREATEST(`amount_dec`, `fee`)) AS `g` FROM `transactions_1` WHERE `customer_id` = " + k + ";"
	case 6:
		es.tag("probe", "avg-string")
		es.c.SQL = "SELECT AVG(`category`) AS `a` FROM `transactions_1` WHERE `customer_id` = " + k + ";"
	case 7:
		es.tag("probe", "avg-temporal")
		es.c.SQL = "SELECT AVG(`event_time`) AS `a` FROM `transactions_1` WHERE `customer_id` = " + k + ";"
	case 8:
		es.tag("probe", "index-hint-joined")
		es.c.SQL = "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = " + k + " GROUP BY `region_id`) " +
			"SELECT `j2`.`region_name` AS `r` FROM `b` JOIN `regions_1` AS `j2` FORCE INDEX (PRIMARY) ON `j2`.`region_id` = `b`.`region_id`;"
	case 9:
		es.tag("probe", "cross-table-where")
		es.c.SQL = "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = " + k + " GROUP BY `region_id`) " +
			"SELECT `j2`.`region_name` AS `r` FROM `b` JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id` WHERE `j2`.`population` > `b`.`hw_cnt`;"
	case 10:
		es.tag("probe", "syntax")
		es.c.SQL = []string{
			"SELECT COUNT(*) AS `n` FROM `transactions_1` t WHERE t.`customer_id` = " + k + ";",
			"SELECT COUNT(DISTINCT `category`) AS `n` FROM `transactions_1` WHERE `customer_id` = " + k + ";",
			"SELECT COUNT(*) AS `n` FROM `transactions_1` WHERE `customer_id` = " + k + " AND `amount` BETWEEN 100 AND 500;",
			"SELECT `customer_id`, `amount` FROM `transactions_1` WHERE `customer_id` = " + k + " ORDER BY `event_time` DESC LIMIT 5 OFFSET 2;",
			"SELECT COUNT(*) AS `n` FROM `transactions_1` WHERE `customer_id` = " + k + " UNION SELECT COUNT(*) AS `n` FROM `sessions_1` WHERE `customer_id` = " + k + ";",
			"WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = " + k + " GROUP BY `region_id`) SELECT `j2`.`region_name` AS `r` FROM `b` RIGHT JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id`;",
		}[es.rng.IntN(6)]
	case 11:
		es.tag("probe", "orderby-in-subquery")
		es.c.SQL = "SELECT COUNT(*) AS `n` FROM `transactions_1` WHERE `customer_id` IN (SELECT `customer_id` FROM `customers_1` WHERE `region_id` = 5 ORDER BY `customer_id`);"
	default:
		// The D4 hazard's translation (INT fee vs BIGINT amount).
		es.tag("probe", "colcol-mixed-type")
		es.c.SQL = "WITH `liq` AS (SELECT `merchant_id` AS `k`, COUNT(*) AS `n`, SUM(`amount`) AS `q` FROM `transactions_1` WHERE `fee` < `amount` GROUP BY `merchant_id`) " +
			"SELECT `m`.`mcc` AS `mcc`, SUM(`liq`.`n`) AS `n`, SUM(`liq`.`q`) AS `q` FROM `merchants_1` AS `m` JOIN `liq` ON `liq`.`k` = `m`.`merchant_id` GROUP BY `m`.`mcc`;"
	}
}

// realJoin: an aggregating join of two real tables, customers_1 AS c and
// regions_1 AS r, INNER or LEFT, with one WHERE condition on the joined
// table r — the shapes of F32 / F32b, which no other production builds
// (its filters are single-table): comparison, LIKE, GREATEST / LEAST and
// XOR on a joined table, NOT / IS NULL / OR on a LEFT JOIN's right side,
// IN (subquery) on a joined column.  The data has misses on both hops:
// customers with no region (c mod 13 = 0) or a dangling one (c mod 29 =
// 0), regions with no country (r mod 17 = 0).  On a LEFT JOIN a
// condition RonSQL proves NULL-rejecting (comparison, LIKE, GREATEST /
// LEAST compared with a constant, NOT over a comparison or IS NULL, XOR
// with a comparison operand) promotes the join to INNER and must match
// MySQL; IS NULL and an OR with IS NULL are rejected.
func (es *envSampler) realJoin() {
	es.tag("real-join")
	left := es.pct(50)
	jt := "JOIN"
	if left {
		jt = "LEFT JOIN"
		es.tag("left-join")
	}
	r := func(n string) string { return qc("r", n) }
	popK := strconv.Itoa(12345 * (1 + es.rng.IntN(199)))
	where := qc("c", "customer_id") + " IN (" + es.keyList(5+es.rng.IntN(26)) + ")"
	if es.pct(30) {
		es.tag("greatest-where")
		where += " AND GREATEST(" + qc("c", "age") + ", " + qc("c", "credit_score") + ") > " + strconv.Itoa(300+es.rng.IntN(551))
	}
	var atom string
	leftRejects := false
	switch es.choose(20, 15, 10, 5, 10, 10, 10, 10, 10) {
	case 0:
		es.tag("joined-cmp")
		atom = r("population") + " > " + popK
	case 1:
		es.tag("joined-like")
		atom = r("region_name") + " LIKE 'Region " + strconv.Itoa(1+es.rng.IntN(9)) + "%'"
	case 2:
		es.tag("joined-greatest")
		atom = "GREATEST(" + r("region_id") + ", " + r("country_id") + ") > " + strconv.Itoa(1+es.rng.IntN(60))
	case 3:
		es.tag("joined-least")
		atom = "LEAST(" + r("region_id") + ", " + r("country_id") + ") <= " + strconv.Itoa(1+es.rng.IntN(60))
	case 4:
		es.tag("joined-xor")
		// Parenthesized as a whole: XOR binds looser than AND (MySQL and
		// RonSQL alike), so a bare atom would XOR the whole preceding WHERE.
		atom = "((" + r("population") + " > " + popK + ") XOR (" + r("country_id") + " > " + strconv.Itoa(1+es.rng.IntN(40)) + "))"
	case 5:
		es.tag("joined-not-isnull")
		atom = "NOT (" + r("country_id") + " IS NULL)"
	case 6:
		es.tag("joined-is-null")
		atom = r("country_id") + " IS NULL"
		leftRejects = true
	case 7:
		es.tag("joined-or-isnull")
		atom = "(" + r("country_id") + " IS NULL OR " + r("population") + " > " + popK + ")"
		leftRejects = true
	default:
		// Rejected on INNER and LEFT alike, before the LEFT JOIN check.
		es.tag("subquery-joined")
		atom = r("country_id") + " IN (SELECT `k`.`country_id` FROM `countries_1` AS `k`)"
	}
	if left && leftRejects {
		es.tag("left-join-where-null")
	}
	es.c.SQL = "SELECT " + qc("c", "region_id") + " AS `rid`, COUNT(*) AS `n`, COUNT(" + r("region_id") + ") AS `nr`, SUM(" + r("population") +
		") AS `pop`, MAX(" + r("country_id") + ") AS `mc` FROM `customers_1` AS `c` " + jt + " `regions_1` AS `r` ON " + r("region_id") +
		" = " + qc("c", "region_id") + " WHERE " + where + " AND " + atom + " GROUP BY " + qc("c", "region_id") + ";"
}

// formerHazard: the translation of a fixed hazard row (hazards.go), run as
// a regular case: compared with MySQL, or expected to reject when the fix
// was a clean rejection.
func (es *envSampler) formerHazard() {
	h := FormerHazards[es.rng.IntN(len(FormerHazards))]
	es.tag("former-hazard", "former-hazard-"+h.ID)
	if h.Construct != "" {
		es.tag(h.Construct)
	}
	es.c.SQL = h.SQL
}
