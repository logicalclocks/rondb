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

// Package cases enumerates the deterministic case matrix of the framework
// (framework_design.md §9): every Hopsworks shape of shape_catalog.md over
// the data_model.md feature groups, bound to keys chosen by key class, plus
// the framework-built target shapes (S6b, S8b) and the edge-fixture
// requirement cases.  The Hopsworks statements come from the emitter port;
// the binder substitutes the `?` markers; known engine outcomes come from
// the expectation table in known.go.
package cases

import (
	"fmt"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// Expect is a known engine outcome for a statement.
type Expect struct {
	Finding string      // ledger id, e.g. "F0"
	Pattern string      // substring of the RonSQL error (rejections) or note (wrong results)
	Wrong   *WrongValue // exact single-row mismatch; nil grants no wrong-result exemption
	// UnquotedTemporal lists the only output columns eligible for F9 matching.
	UnquotedTemporal []string
}

// Statement is one bound statement of a case.
type Statement struct {
	Label  string
	RonSQL string // the text run on RonSQL and, for L1, on MySQL
}

// StatementGroup preserves one emitted DTO and its bound query family.
// Several RonSQL templates can correspond to one production MySQL query.
// A MySQL-only DTO is retained with an empty Statements slice.
type StatementGroup struct {
	DTO        emit.Statement // original, unbound metadata; treat as read-only
	MySQL      *string        // bound queryOnline; nil means absent, never a fallback
	Statements []Statement    // bound RonSQL templates, in DTO order
}

// Case is one verification case.
type Case struct {
	ID           string
	Shape        string
	Mode         string // single | batchN | edge
	Note         string
	Origin       string // "hopsworks" (emitter port) | "framework" (target/edge shape)
	Statements   []Statement
	Groups       []StatementGroup
	Ordered      bool
	ExpectReject *Expect
	KnownWrong   *Expect
	// KnownError is a verify-only expectation: the RonSQL response is known
	// to be unusable (F9: unparsable JSON) although the TEXT form compared
	// by the MTR test is fine.  Reported as KNOWN-ERROR, not as a failure.
	KnownError *Expect
	Hazard     string // finding id: not executed unless hazards are enabled
	MTR        bool   // part of the golden MTR test
	// Canon selects the MTR-side canonicalization applied to both engines'
	// TEXT output before the diff: "" (byte-strict) or CanonNumeric.
	Canon string
	// RelatedShapes associates edge probes with serving-shape checks.
	// Shape remains the primary label (EDGE) used by the MTR fixtures.
	RelatedShapes []string
}

// Shapes returns each associated shape once, with the primary label first.
func (c Case) Shapes() []string {
	var out []string
	seen := map[string]bool{}
	for _, shape := range append([]string{c.Shape}, c.RelatedShapes...) {
		if shape != "" && !seen[shape] {
			out = append(out, shape)
			seen[shape] = true
		}
	}
	return out
}

// MatchesShapes selects the primary label or any related serving shape.
// A nil selection means all cases; selecting several shapes never repeats a case.
func (c Case) MatchesShapes(selected map[string]bool) bool {
	if selected == nil {
		return true
	}
	for _, shape := range c.Shapes() {
		if selected[shape] {
			return true
		}
	}
	return false
}

// CanonNumeric strips trailing fractional zeros on both sides (RonSQL prints
// AVG with four decimals regardless of the input type, F3, and drops the
// scale of DECIMAL MIN/MAX, F2); values still have to agree exactly.  It is
// set automatically for statements that use AVG or touch a DECIMAL, DOUBLE
// or FLOAT column.
const CanonNumeric = "numeric"

// Config selects the data set the cases bind to.
type Config struct {
	DB    string
	Scale data.Scale
	Now   time.Time // window reference (FS_NOW)
}

type builder struct {
	cfg  Config
	fgs  map[string]spec.FeatureGroup // by table name, ids assigned
	frac map[string]bool              // DECIMAL/DOUBLE/FLOAT column names of every table
	out  []Case
	errs []error
}

// newBuilder prepares the feature groups (ids assigned, online database
// set) and the fractional-column set of the data set.
func newBuilder(cfg Config) *builder {
	if cfg.Now.IsZero() {
		cfg.Now = data.FSNow
	}
	b := &builder{cfg: cfg, fgs: map[string]spec.FeatureGroup{}, frac: map[string]bool{}}
	for i, fg := range data.Schema() {
		fg.ID = i + 1
		fg.FeaturestoreID = 1
		fg.OnlineDB = cfg.DB
		b.fgs[fg.TableName()] = fg
	}
	for _, fg := range append(data.Schema(), data.EdgeSchema()...) {
		for _, f := range fg.Features {
			switch spec.BaseType(f.Type) {
			case "decimal", "double", "float":
				b.frac[f.Name] = true
			}
		}
	}
	return b
}

// Enumerate returns the case matrix for the configuration, deterministic
// for a given scale factor.
func Enumerate(cfg Config) ([]Case, error) {
	b := newBuilder(cfg)
	b.aggregateCases()
	b.filterCases()
	b.collectCases()
	b.snowflakeCases()
	b.compositeAndStringCases()
	b.edgeCases()
	if len(b.errs) > 0 {
		return nil, b.errs[0]
	}
	return b.out, nil
}

// ---- view construction ---------------------------------------------------------

func (b *builder) fg(table string) spec.FeatureGroup { return b.fgs[table] }

func sel(names ...string) []spec.TDFeature {
	out := make([]spec.TDFeature, len(names))
	for i, n := range names {
		out[i] = spec.TDFeature{Name: n}
	}
	return out
}

func (b *builder) view(name string, fgs []spec.FeatureGroup, joins []spec.Join, filters []spec.Filter, opt spec.Options) *spec.View {
	return &spec.View{Name: name, FGs: fgs, Joins: joins, Filters: filters, Options: opt, MaxCollectN: 50, MaxCollectCells: 100}
}

// aggregate output selections: the feature-view features an aggregate node
// declares are the outputs, resolved to the source columns by the emitter.
func aggOutputs(agg spec.AggSpec) []spec.TDFeature {
	var out []spec.TDFeature
	for _, e := range agg {
		for _, fn := range e.Fns {
			name := "count"
			if e.Key != "*" {
				name = strings.ReplaceAll(e.Key, ",", "_") + "_" + strings.ToLower(fn)
			}
			out = append(out, spec.TDFeature{Name: name, Type: strp("bigint")})
		}
	}
	return out
}

func strp(s string) *string { return &s }
func intp(i int) *int       { return &i }
func i64p(i int64) *int64   { return &i }

// ---- binding -------------------------------------------------------------------

type keyVals map[string]bind.Arg

func (b *builder) keyArg(fg spec.FeatureGroup, name string, values []interface{}) bind.Arg {
	f, _ := fg.Feature(name)
	lits := make([]string, len(values))
	for i, v := range values {
		switch x := v.(type) {
		case int64:
			lits[i] = bind.Int(x)
		case int:
			lits[i] = bind.Int(int64(x))
		case string:
			lits[i] = bind.LiteralFor(f.Type, x)
		default:
			lits[i] = fmt.Sprint(v)
		}
	}
	if len(lits) == 1 {
		return bind.Scalar(lits[0])
	}
	return bind.List(lits)
}

// DTOKey holds pre-rendered scalar literals by parameter name. Extra names
// are allowed so one key can serve several DTOs with different parameters.
type DTOKey map[string]bind.Arg

// BindDTO binds captured or reconstructed queryOnline and RonSQL templates
// without rebuilding SQL or changing the DTO. Parameter indexes are 1-based.
// Single requests contain exactly one key; batches contain one or more keys
// and use one list marker, including tuple lists for composite entity keys.
// QueryOnlineScan remains unbound in DTO; it is not part of this L2 path.
// now must be explicit for windowed statements so both references use the
// same cutoff. Callers must render key literals according to feature types.
func BindDTO(st emit.Statement, keys []DTOKey, batch bool, now time.Time) (StatementGroup, error) {
	fail := func(reason string) (StatementGroup, error) {
		return StatementGroup{}, fmt.Errorf("DTO %d: %s", st.PreparedStatementIndex, reason)
	}
	if len(keys) == 0 || (!batch && len(keys) != 1) {
		return fail("need one key for a single request or a nonempty batch")
	}
	if len(st.PreparedStatementParameters) == 0 {
		return fail("no entity-key parameters")
	}
	names := make([]string, len(st.PreparedStatementParameters))
	seen := map[string]bool{}
	for _, p := range st.PreparedStatementParameters {
		if p.Name == "" || seen[p.Name] || p.Index < 1 || p.Index > len(names) || names[p.Index-1] != "" {
			return fail("parameter names must be unique and indexes must cover 1..N exactly once")
		}
		names[p.Index-1], seen[p.Name] = p.Name, true
	}
	if st.AggregateWindow != nil {
		seconds := *st.AggregateWindow
		if now.IsZero() || seconds < 0 || seconds > int64((1<<63-1)/int64(time.Second)) {
			return fail("window needs an explicit reference time and a nonnegative duration that fits time.Duration")
		}
	}
	if st.CollectN != nil && *st.CollectN <= 0 {
		return fail("collect limit must be positive")
	}
	var args []bind.Arg
	var values []string
	for row, key := range keys {
		literals := make([]string, len(names))
		for i, name := range names {
			a, ok := key[name]
			if !ok || a.IsList || strings.TrimSpace(a.Literal) == "" {
				return fail(fmt.Sprintf("key %d parameter %s needs a scalar literal", row, name))
			}
			literals[i] = a.Literal
		}
		if !batch {
			for _, literal := range literals {
				args = append(args, bind.Scalar(literal))
			}
		} else if len(literals) == 1 {
			values = append(values, literals[0])
		} else {
			values = append(values, "("+strings.Join(literals, ", ")+")")
		}
	}
	if batch {
		args = []bind.Arg{bind.List(values)}
	}
	group, err := bindStatementArgs(st, args, now)
	if err != nil {
		return StatementGroup{}, fmt.Errorf("DTO %d: %w", st.PreparedStatementIndex, err)
	}
	return group, nil
}

// bindStatement retains one DTO and binds both query families. Window
// markers use Now - window; the MySQL collect cap gets N.
func (b *builder) bindStatement(st emit.Statement, keys keyVals) (StatementGroup, error) {
	var args []bind.Arg
	for _, p := range st.PreparedStatementParameters {
		a, ok := keys[p.Name]
		if !ok {
			return StatementGroup{}, fmt.Errorf("no value for parameter %s", p.Name)
		}
		args = append(args, a)
	}
	return bindStatementArgs(st, args, b.cfg.Now)
}

// bindStatementArgs is shared by the existing case builder and A6 binding.
func bindStatementArgs(st emit.Statement, args []bind.Arg, now time.Time) (StatementGroup, error) {
	group := StatementGroup{DTO: st}
	ronArgs := append([]bind.Arg(nil), args...)
	if st.AggregateWindow != nil {
		ronArgs = append(ronArgs, bind.Scalar(bind.Timestamp(now.Add(-time.Duration(*st.AggregateWindow)*time.Second))))
	}
	if st.QueryOnline != nil {
		mysqlArgs := append([]bind.Arg(nil), ronArgs...)
		if st.CollectN != nil {
			mysqlArgs = append(mysqlArgs, bind.Scalar(bind.Int(int64(*st.CollectN))))
		}
		twin, err := bind.Bind(*st.QueryOnline, mysqlArgs)
		if err != nil {
			return StatementGroup{}, fmt.Errorf("mysql queryOnline (DTO %d): %w", st.PreparedStatementIndex, err)
		}
		group.MySQL = &twin
	}
	if st.QueryRonsql != nil {
		r, err := bind.Bind(*st.QueryRonsql, ronArgs)
		if err != nil {
			return StatementGroup{}, fmt.Errorf("ronsql: %w", err)
		}
		group.Statements = append(group.Statements, Statement{Label: "ronsql", RonSQL: r})
	}
	for i, t := range st.SnowflakeTemplates {
		r, err := bind.Bind(t, args)
		if err != nil {
			return StatementGroup{}, fmt.Errorf("snowflake template %d: %w", i, err)
		}
		group.Statements = append(group.Statements, Statement{Label: fmt.Sprintf("snowflake[%d]", i), RonSQL: r})
	}
	return group, nil
}

func (b *builder) add(c Case) {
	if c.Shape == "EDGE" {
		shapes, ok := edgeShapes[c.ID]
		if !ok || len(shapes) == 0 {
			b.fail(c.ID, fmt.Errorf("edge case has no serving-shape association"))
			return
		}
		c.RelatedShapes = append([]string(nil), shapes...)
	}
	if c.Canon == "" && b.needsNumericCanon(c) {
		c.Canon = CanonNumeric
	}
	b.out = append(b.out, c)
}

// needsNumericCanon reports whether a statement of the case produces a
// value whose text form is known to differ only in trailing zeros (F2/F3):
// an AVG, or any DECIMAL/DOUBLE/FLOAT column of the data set.
func (b *builder) needsNumericCanon(c Case) bool {
	for _, st := range c.Statements {
		if strings.Contains(st.RonSQL, "AVG(") {
			return true
		}
		for col := range b.frac {
			if strings.Contains(st.RonSQL, "`"+col+"`") {
				return true
			}
		}
	}
	return false
}

// knownError marks an already-added case with a verify-only expected
// execution error (the MTR TEXT compare is unaffected).
func (b *builder) knownError(id string, e *Expect) {
	for i := range b.out {
		if b.out[i].ID == id {
			b.out[i].KnownError = e
			return
		}
	}
	b.fail(id, fmt.Errorf("knownError: no such case"))
}

func (b *builder) fail(id string, err error) {
	b.errs = append(b.errs, fmt.Errorf("%s: %w", id, err))
}

// emitCase builds a view, emits it and binds every RonSQL statement.
func (b *builder) emitCase(id, shape, mode, note string, v *spec.View, keys keyVals, ordered bool, expect *Expect, mtr bool) {
	statements, err := emit.Build(v)
	if err != nil {
		b.fail(id, err)
		return
	}
	c := Case{ID: id, Shape: shape, Mode: mode, Note: note, Origin: "hopsworks", Ordered: ordered, ExpectReject: expect, MTR: mtr}
	for _, st := range statements {
		bound, err := b.bindStatement(st, keys)
		if err != nil {
			b.fail(id, err)
			return
		}
		c.Groups = append(c.Groups, bound)
		c.Statements = append(c.Statements, bound.Statements...)
	}
	if len(c.Statements) == 0 {
		b.fail(id, fmt.Errorf("no RonSQL statement emitted"))
		return
	}
	b.add(c)
}

// sqlCase adds a framework-built statement.
func (b *builder) sqlCase(id, shape, mode, note, sql string, ordered bool, expect, wrong *Expect, hazard string, mtr bool) {
	b.add(Case{ID: id, Shape: shape, Mode: mode, Note: note, Origin: "framework", Ordered: ordered,
		Statements: []Statement{{Label: "sql", RonSQL: sql}}, ExpectReject: expect, KnownWrong: wrong, Hazard: hazard, MTR: mtr})
}

// ---- aggregate shapes (S1-S5) ------------------------------------------------------

var txAgg = spec.AggSpec{
	{Key: "*", Fns: []string{"count"}},
	{Key: "amount", Fns: []string{"count", "sum", "min", "max"}},
	{Key: "fee", Fns: []string{"min", "max"}},
	{Key: "amount,fee", Fns: []string{"greatest", "least"}},
}

func (b *builder) txAggView(name string, agg spec.AggSpec, window *int64, batch bool, filters []spec.Filter) *spec.View {
	fg := b.fg(data.TTransactions)
	j := spec.Join{Index: 0, Parent: 0, FG: fg.ID, Type: spec.JoinInner, Prefix: strp("tx_"), Aggregate: agg, Window: window,
		Features: aggOutputs(agg)}
	return b.view(name, []spec.FeatureGroup{fg}, []spec.Join{j}, filters, spec.Options{Batch: batch})
}

func ints(vs ...int64) []interface{} {
	out := make([]interface{}, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

func (b *builder) aggregateCases() {
	fg := b.fg(data.TTransactions)
	key := func(c int64) keyVals { return keyVals{"customer_id": b.keyArg(fg, "customer_id", ints(c))} }
	for _, k := range []struct {
		c    int64
		note string
	}{{31, "300 rows (class 15)"}, {17, "one row"}, {16, "no rows: COUNT 0, others NULL"}, {1001, "missing entity"}} {
		b.emitCase(fmt.Sprintf("S1-k%d", k.c), "S1", "single", k.note, b.txAggView("s1", txAgg, nil, false, nil), key(k.c), false, nil, true)
	}
	avg := spec.AggSpec{{Key: "amount", Fns: []string{"avg", "sum"}}, {Key: "score", Fns: []string{"avg", "sum", "min", "max"}}}
	b.emitCase("S1-avg-k31", "S1", "single", "AVG over BIGINT and DOUBLE (formatting F3 handled by the canonicalizer)",
		b.txAggView("s1avg", avg, nil, false, nil), key(31), false, nil, true)
	decimal := spec.AggSpec{{Key: "amount_dec", Fns: []string{"sum", "min", "max"}}}
	b.emitCase("S1-decimal-k31", "S1", "single", "DECIMAL(12,2) aggregates (scale formatting F2 handled by the canonicalizer)",
		b.txAggView("s1dec", decimal, nil, false, nil), key(31), false, nil, true)
	for _, w := range []struct {
		c    int64
		secs int64
		note string
	}{{111, 7 * 86400, "daily rows at exact day bounds: 7 rows, bound inclusive"}, {21, 30 * 86400, "6-hourly rows, 30-day window"},
		{1, 3600, "hourly rows with 17-minute offset, 1-hour window: 0 rows"}, {111, 90 * 86400, "90-day window: 90 rows"}} {
		b.emitCase(fmt.Sprintf("S2-k%d-w%ds", w.c, w.secs), "S2", "single", w.note,
			b.txAggView("s2", txAgg, i64p(w.secs), false, nil), key(w.c), false, nil, true)
	}
	batchKeys := func(n int) []interface{} {
		var ks []interface{}
		mixed := []int64{16, 17, 20, 31, 111, 1001, 13, 29}
		for i := 0; len(ks) < n; i++ {
			if i < len(mixed) {
				ks = append(ks, mixed[i])
			} else {
				ks = append(ks, int64(i-len(mixed)+1))
			}
		}
		return ks
	}
	for _, n := range []int{10, 100, 1000} {
		if int64(n) > b.cfg.Scale.E+8 {
			continue
		}
		b.emitCase(fmt.Sprintf("S3-b%d", n), "S3", fmt.Sprintf("batch%d", n), "IN list + GROUP BY, mixed key classes",
			b.txAggView("s3", txAgg, nil, true, nil), keyVals{"customer_id": b.keyArg(fg, "customer_id", batchKeys(n))}, false, nil, n <= 100)
	}
	b.emitCase("S3-b100-w30d", "S3", "batch100", "batch with a 30-day window",
		b.txAggView("s3w", txAgg, i64p(30*86400), true, nil), keyVals{"customer_id": b.keyArg(fg, "customer_id", batchKeys(100))}, false, nil, true)
}

// ---- filters (S4) --------------------------------------------------------------------

func (b *builder) filterCases() {
	fg := b.fg(data.TTransactions)
	key := keyVals{"customer_id": b.keyArg(fg, "customer_id", ints(31))}
	leaf := func(feature string, cond spec.SqlCondition, value string) spec.Filter {
		c := cond
		return spec.Filter{FG: fg.ID, Logic: spec.LogicSingle, Feature: feature, Condition: &c, Value: strp(value)}
	}
	cases := []struct {
		id, note string
		filters  []spec.Filter
	}{
		{"S4-eq-string", "category = 'grocery' (collation: matches Grocery rows too on MySQL)", []spec.Filter{leaf("category", spec.CondEquals, "grocery")}},
		{"S4-ge-numeric", "amount >= 300", []spec.Filter{leaf("amount", spec.CondGreaterThanOrEqual, "300")}},
		{"S4-ne", "fee <> 0", []spec.Filter{leaf("fee", spec.CondNotEquals, "0")}},
		{"S4-like", "category LIKE 'gro%'", []spec.Filter{leaf("category", spec.CondLike, "gro%")}},
		{"S4-two-leaves", "amount > 200 AND category = 'online'", []spec.Filter{leaf("amount", spec.CondGreaterThan, "200"), leaf("category", spec.CondEquals, "online")}},
		{"S4-quote", "category = 'it''s' (no rows; quoting)", []spec.Filter{leaf("category", spec.CondEquals, "it's")}},
	}
	for _, c := range cases {
		b.emitCase(c.id, "S4", "single", c.note, b.txAggView("s4", txAgg, nil, false, c.filters), key, false, nil, true)
	}
	b.emitCase("S4-window-filter-b10", "S4", "batch10", "filter + window in a batch",
		b.txAggView("s4b", txAgg, i64p(30*86400), true, []spec.Filter{leaf("amount", spec.CondGreaterThanOrEqual, "300")}),
		keyVals{"customer_id": b.keyArg(fg, "customer_id", ints(16, 17, 20, 31, 111, 1001, 13, 29, 1, 2))}, false, nil, true)
}

// ---- collect (S6, S6b) ------------------------------------------------------------

func (b *builder) collectView(name string, n int, ascending bool, batch bool) *spec.View {
	fg := b.fg(data.TTransactions)
	j := spec.Join{Index: 0, Parent: 0, FG: fg.ID, Type: spec.JoinInner, Prefix: strp("tx_"), CollectN: intp(n),
		OrderBy: strp("event_time"), Ascending: ascending,
		Features: []spec.TDFeature{{Name: "transactions_collect", Type: strp("array<struct<event_time:timestamp,amount:bigint,category:string>>")}}}
	return b.view(name, []spec.FeatureGroup{fg}, []spec.Join{j}, nil, spec.Options{Batch: batch})
}

func (b *builder) collectCases() {
	fg := b.fg(data.TTransactions)
	key := func(c int64) keyVals { return keyVals{"customer_id": b.keyArg(fg, "customer_id", ints(c))} }
	for _, k := range []struct {
		c    int64
		note string
	}{{21, "exactly 5 rows"}, {31, "300 rows"}, {16, "no rows"}} {
		b.emitCase(fmt.Sprintf("S6-cte-k%d", k.c), "S6", "single", "Hopsworks CTE collect form: "+k.note,
			b.collectView("s6", 5, false, false), key(k.c), false, Known["S6-cte"], true)
		// S6b: the direct single-table form, the same columns and order.
		b.sqlCase(fmt.Sprintf("S6b-k%d", k.c), "S6b", "single", "direct collect form: "+k.note,
			fmt.Sprintf("SELECT `customer_id`, `event_time`, `amount`, `category` FROM `transactions_1` WHERE `customer_id` = %d ORDER BY `event_time` DESC LIMIT 5;", k.c),
			true, nil, nil, "", true)
	}
	b.sqlCase("S6b-k31-limit50", "S6b", "single", "direct collect, N = 50 of 300 rows",
		"SELECT `customer_id`, `event_time`, `amount`, `category` FROM `transactions_1` WHERE `customer_id` = 31 ORDER BY `event_time` DESC LIMIT 50;",
		true, nil, nil, "", true)
}

// ---- snowflake (S7, S8, S8b) ----------------------------------------------------------

func (b *builder) snowflakeView(name string, depth int, hopType spec.JoinType, batch bool) *spec.View {
	cust, reg, ctry := b.fg(data.TCustomers), b.fg(data.TRegions), b.fg(data.TCountries)
	label := spec.Join{Index: 0, Parent: 0, FG: cust.ID, Type: spec.JoinInner, Prefix: strp(""), Features: []spec.TDFeature{{Name: "tier", Label: true}}}
	root := spec.Join{Index: 1, Parent: 0, FG: cust.ID, Type: spec.JoinInner, Prefix: strp("p_"), On: [][2]string{{"customer_id", "customer_id"}},
		Features: sel("tier", "age")}
	child := spec.Join{Index: 2, Parent: 1, FG: reg.ID, Type: hopType, Prefix: strp("r_"), On: [][2]string{{"region_id", "region_id"}},
		Features: sel("region_name", "population")}
	joins := []spec.Join{label, root, child}
	if depth >= 2 {
		joins = append(joins, spec.Join{Index: 3, Parent: 2, FG: ctry.ID, Type: hopType, Prefix: strp("c_"), On: [][2]string{{"country_id", "country_id"}},
			Features: sel("country_name", "continent")})
	}
	return b.view(name, []spec.FeatureGroup{cust, reg, ctry}, joins, nil, spec.Options{Batch: batch})
}

func (b *builder) snowflakeCases() {
	cust := b.fg(data.TCustomers)
	key := func(c int64) keyVals { return keyVals{"customer_id": b.keyArg(cust, "customer_id", ints(c))} }
	keysNote := []struct {
		c    int64
		note string
	}{{21, "region 22 -> country 22"}, {13, "NULL region hop"}, {29, "dangling region hop"}, {16, "region 17 with a NULL country"}, {18, "region 19 with a dangling country"}}
	for _, k := range keysNote {
		b.emitCase(fmt.Sprintf("S7-1hop-k%d", k.c), "S7", "single", "INNER 1-hop: "+k.note, b.snowflakeView("s7a", 1, spec.JoinInner, false), key(k.c), false, nil, true)
		b.emitCase(fmt.Sprintf("S7-2hop-k%d", k.c), "S7", "single", "INNER 2-hop: "+k.note, b.snowflakeView("s7b", 2, spec.JoinInner, false), key(k.c), false, nil, true)
		b.emitCase(fmt.Sprintf("S8-chains-k%d", k.c), "S8", "single", "LEFT per-chain templates: "+k.note, b.snowflakeView("s8", 2, spec.JoinLeft, false), key(k.c), false, nil, true)
	}
	batch := ints(21, 13, 29, 16, 18, 1001, 1, 2, 3, 4)
	b.emitCase("S7-2hop-b10", "S7", "batch10", "INNER 2-hop batch with NULL/dangling/missing keys", b.snowflakeView("s7c", 2, spec.JoinInner, true),
		keyVals{"customer_id": b.keyArg(cust, "customer_id", batch)}, false, nil, true)
	b.emitCase("S8-chains-b10", "S8", "batch10", "LEFT per-chain batch", b.snowflakeView("s8b", 2, spec.JoinLeft, true),
		keyVals{"customer_id": b.keyArg(cust, "customer_id", batch)}, false, nil, true)
	// S8b: the future single-statement LEFT form, derived from the INNER template.
	for _, k := range keysNote {
		single := b.snowflakeStatement(2, k.c)
		if single != "" {
			left := strings.Replace(single, " FROM `b` JOIN ", " FROM `b` LEFT JOIN ", 1)
			left = strings.ReplaceAll(left, "` JOIN `countries_1`", "` LEFT JOIN `countries_1`")
			b.sqlCase(fmt.Sprintf("S8b-k%d", k.c), "S8b", "single", "single-statement LEFT hops: "+k.note, left, false, nil, nil, "", true)
		}
	}
}

// snowflakeStatement returns the bound INNER 2-hop template for a key.
func (b *builder) snowflakeStatement(depth int, c int64) string {
	cust := b.fg(data.TCustomers)
	sts, err := emit.Build(b.snowflakeView("s8b-src", depth, spec.JoinInner, false))
	if err != nil {
		b.fail(fmt.Sprintf("S8b-k%d", c), err)
		return ""
	}
	for _, st := range sts {
		if len(st.SnowflakeTemplates) > 0 {
			bound, err := b.bindStatement(st, keyVals{"customer_id": b.keyArg(cust, "customer_id", ints(c))})
			if err != nil {
				b.fail(fmt.Sprintf("S8b-k%d", c), err)
				return ""
			}
			if len(bound.Statements) > 0 {
				return bound.Statements[0].RonSQL
			}
		}
	}
	b.fail(fmt.Sprintf("S8b-k%d", c), fmt.Errorf("no bound snowflake template"))
	return ""
}

// ---- composite and string keys (S9, S10) -----------------------------------------

func (b *builder) compositeAndStringCases() {
	hist := b.fg(data.TBalanceHist)
	histView := b.histView
	for _, k := range []struct {
		a    int64
		cur  string
		note string
	}{{4, "EUR", "10 rows"}, {2, "USD", "2 rows"}, {5, "SEK", "no history (a mod 5 = 0)"}, {4, "GBP", "unknown currency"}} {
		keys := keyVals{"account_id": b.keyArg(hist, "account_id", ints(k.a)), "currency": b.keyArg(hist, "currency", []interface{}{k.cur})}
		b.emitCase(fmt.Sprintf("S9-a%d-%s", k.a, k.cur), "S9", "single", "composite entity key: "+k.note, histView("s9", nil, false), keys, false, nil, true)
	}
	b.emitCase("S9-a4-EUR-w90d", "S9", "single", "composite key with a 90-day window",
		histView("s9w", i64p(90*86400), false), keyVals{"account_id": b.keyArg(hist, "account_id", ints(4)), "currency": b.keyArg(hist, "currency", []interface{}{"EUR"})}, false, nil, true)

	str := b.fg(data.TTxStr)
	strView := b.strView
	for _, k := range []struct {
		key  string
		note string
	}{{data.CustomerKey(21), "5 rows"}, {data.CustomerKey(10), "upper-case key CUST-…"}, {"cust-00000010", "lower-case literal for an upper-case key (collation)"}, {"cust-99999999", "missing key"}, {"O'Brien", "apostrophe in the key literal"}} {
		b.emitCase("S10-"+sanitize(k.key), "S10", "single", "string entity key: "+k.note, strView("s10", false),
			keyVals{"customer_key": b.keyArg(str, "customer_key", []interface{}{k.key})}, false, nil, true)
	}
	var list []interface{}
	for c := int64(1); c <= 10; c++ {
		list = append(list, data.CustomerKey(c))
	}
	b.emitCase("S10-b10", "S10", "batch10", "string keys IN list + GROUP BY", strView("s10b", true),
		keyVals{"customer_key": b.keyArg(str, "customer_key", list)}, false, nil, true)
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// ---- edge fixtures (data_model.md §11) --------------------------------------------

// Explicit associations for shared edge primitives, not replacements for
// emitted-shape cases. Direct collect probes do not replace the S6 CTE tests.
var edgeShapes = map[string][]string{
	"EDGE-null-ints":              {"S1"},
	"EDGE-null-decimal":           {"S1"},
	"EDGE-null-greatest":          {"S1", "S5"},
	"EDGE-null-string-adjacent":   {"S1"},
	"EDGE-null-avg":               {"S1"},
	"EDGE-all-null":               {"S1"},
	"EDGE-float-exact":            {"S1"},
	"EDGE-float-rounding":         {"S1"},
	"EDGE-date-range":             {"S1", "S4"},
	"EDGE-big-safe":               {"S1"},
	"EDGE-big-limits":             {"S1"},
	"EDGE-decimal-large":          {"S1"},
	"EDGE-big-overflow":           {"S1"},
	"EDGE-str-in-list":            {"S10"},
	"EDGE-str-null-vs-NULL":       {"S10"},
	"EDGE-str-escapes":            {"S10"},
	"EDGE-str-collation":          {"S4", "S10"},
	"EDGE-ts3-cutoff":             {"S2"},
	"EDGE-ts6-cutoff":             {"S2"},
	"EDGE-ts0-batch":              {"S2", "S3"},
	"EDGE-seq-order":              {"S6", "S6b"},
	"EDGE-seq-boundary":           {"S6", "S6b"},
	"EDGE-comp-hop":               {"S7", "S8", "S9"},
	"EDGE-comp-swapped":           {"S7", "S8", "S9"},
	"EDGE-comp-batch":             {"S7", "S8", "S9"},
	"EDGE-comp-binary":            {"S7", "S8"},
	"EDGE-F1-string-reuse":        {"S1"},
	"EDGE-F1-string-reuse-nonull": {"S1"},
}

func (b *builder) edgeCases() {
	e := func(id, note, sql string, ordered bool, expect, wrong *Expect, hazard string) {
		b.sqlCase(id, "EDGE", "edge", note, sql, ordered, expect, wrong, hazard, true)
	}
	e("EDGE-null-ints", "mixed-NULL group: COUNT(*) vs COUNT(col), integer aggregates",
		"SELECT COUNT(*) AS `cnt_all`, COUNT(`i1`) AS `cnt_i1`, COUNT(`s_val`) AS `cnt_s`, SUM(`i1`) AS `i1_sum`, MIN(`i2`) AS `i2_min`, MAX(`i3`) AS `i3_max` FROM `edge_hist_1` WHERE `entity_id` = 1;", false, nil, nil, "")
	e("EDGE-null-decimal", "mixed-NULL group: DECIMAL(18,2) aggregates",
		"SELECT MIN(`dec_val`) AS `dec_min`, MAX(`dec_val`) AS `dec_max`, SUM(`dec_val`) AS `dec_sum` FROM `edge_hist_1` WHERE `entity_id` = 1;", false, nil, nil, "")
	e("EDGE-null-greatest", "GREATEST/LEAST null propagation",
		"SELECT MAX(GREATEST(`i1`, `i2`, `i3`)) AS `g_max`, MIN(LEAST(`i1`, `i2`, `i3`)) AS `l_min` FROM `edge_hist_1` WHERE `entity_id` = 1;", false, nil, nil, "")
	e("EDGE-null-string-adjacent", "string MIN/MAX aggregated adjacently (safe form of F1)",
		"SELECT MAX(`s_val`) AS `s_max`, MIN(`s_val`) AS `s_min` FROM `edge_hist_1` WHERE `entity_id` = 1;", false, nil, nil, "")
	e("EDGE-null-avg", "AVG over INT and DOUBLE with NULLs",
		"SELECT AVG(`i2`) AS `i2_avg`, AVG(`f_double`) AS `fd_avg` FROM `edge_hist_1` WHERE `entity_id` = 1;", false, nil, nil, "")
	e("EDGE-all-null", "all-NULL group",
		"SELECT COUNT(*) AS `cnt_all`, COUNT(`i1`) AS `cnt_i1`, SUM(`i1`) AS `i1_sum`, MAX(`dec_val`) AS `dec_max`, MAX(`d_date`) AS `d_max`, MIN(`s_val`) AS `s_min` FROM `edge_hist_1` WHERE `entity_id` = 2;", false, nil, nil, "")
	// SUM(dec_val) deliberately absent: entity 3's 1.01 + 2.02 + 3.03 is not
	// dyadic, and RonSQL's DECIMAL SUM runs on the DOUBLE path, so the printed
	// value depends on the topology's combine order (F21); this case asserts
	// the exact floats and dates.  DECIMAL SUM stays covered by
	// EDGE-null-decimal (exact operands), EDGE-decimal-large and .fs_verify.
	e("EDGE-float-exact", "exactly representable floats, DATE min/max",
		"SELECT SUM(`f_float`) AS `ff_sum`, SUM(`f_double`) AS `fd_sum`, MIN(`f_double`) AS `fd_min`, MIN(`d_date`) AS `d_min`, MAX(`d_date`) AS `d_max` FROM `edge_hist_1` WHERE `entity_id` = 3;", false, nil, nil, "")
	e("EDGE-float-rounding", "rounding-sensitive floats (MySQL FLOAT display precision, F4)",
		"SELECT SUM(`f_float`) AS `ff_sum`, SUM(`f_double`) AS `fd_sum`, MAX(`f_float`) AS `ff_max`, SUM(`dec_val`) AS `dec_sum` FROM `edge_hist_1` WHERE `entity_id` = 4;", false, nil, Known["F4"], "")
	e("EDGE-date-range", "DATE spread to 9999-12-31",
		"SELECT COUNT(*) AS `cnt`, MIN(`d_date`) AS `d_min`, MAX(`d_date`) AS `d_max` FROM `edge_hist_1` WHERE `entity_id` = 5 AND `d_date` >= '2000-01-01';", false, nil, nil, "")
	e("EDGE-big-safe", "BIGINT around 2^53, SUM = 2^54 - 1",
		"SELECT SUM(`big_val`) AS `big_sum`, MIN(`big_val`) AS `big_min`, MAX(`big_val`) AS `big_max`, SUM(`f_exact`) AS `fe_sum` FROM `edge_big_1` WHERE `entity_id` = 1;", false, nil, nil, "")
	e("EDGE-big-limits", "signed limits (SUM = 0)",
		"SELECT SUM(`big_val`) AS `big_sum`, MIN(`big_val`) AS `big_min`, MAX(`big_val`) AS `big_max` FROM `edge_big_1` WHERE `entity_id` = 2;", false, nil, nil, "")
	e("EDGE-decimal-large", "DECIMAL(18,2) beyond 2^53 cents (F5 wrong value)",
		"SELECT SUM(`dec_big`) AS `dec_sum`, MAX(`dec_big`) AS `dec_max` FROM `edge_big_1` WHERE `entity_id` = 2;", false, nil, Known["F5"], "")
	// Topology-dependent outcome: the clean F6 error when both rows share a
	// fragment (1-2 node groups), the wrapped F22 value when they do not
	// (4 node groups); both are known, neither is a pass.
	e("EDGE-big-overflow", "deliberate BIGINT SUM overflow (F6 clean error, or the F22 wrapped value on more node groups)",
		"SELECT SUM(`big_val`) AS `big_sum` FROM `edge_big_1` WHERE `entity_id` = 3;", false, Known["F6"], Known["F22"], "")
	e("EDGE-str-in-list", "apostrophe, empty, literal-NULL and multibyte keys in an IN list",
		"SELECT COUNT(*) AS `cnt`, SUM(`n`) AS `n_sum`, MIN(`s_key`) AS `k_min`, MAX(`s_key`) AS `k_max` FROM `edge_str_1` WHERE `s_key` IN ('O''Brien', 'NULL', '', '日本語', 'in-list-a');", false, nil, nil, "")
	e("EDGE-str-null-vs-NULL", "SQL NULL vs the string 'NULL' vs '' (typed nullness)",
		"SELECT `s_key`, `s_val`, `n` FROM `edge_str_1` WHERE `s_key` IN ('NULL', 'null-string', 'empty-value') ORDER BY `n`;", true, nil, nil, "")
	e("EDGE-str-escapes", "backslash / tab / newline / 4-byte UTF-8 values",
		"SELECT `s_key`, `s_val`, `n` FROM `edge_str_1` WHERE `s_key` IN ('backslash', 'tab', 'newline', 'emoji-value') ORDER BY `n`;", true, nil, nil, "")
	e("EDGE-str-collation", "case and accent pairs match under utf8mb4_0900_ai_ci",
		"SELECT COUNT(*) AS `cnt`, SUM(`n`) AS `n_sum`, MIN(`n`) AS `n_min`, MAX(`n`) AS `n_max` FROM `edge_str_1` WHERE `s_val` = 'case' OR `s_val` = 'asa';", false, nil, nil, "")
	e("EDGE-ts3-cutoff", "TIMESTAMP(3) inclusive >= at the cutoff",
		"SELECT COUNT(*) AS `cnt3`, MIN(`ts3`) AS `ts3_min` FROM `edge_ts_1` WHERE `entity_id` = 1 AND `ts3` >= '2026-05-01 12:00:00.000';", false, nil, nil, "")
	e("EDGE-ts6-cutoff", "TIMESTAMP(6) inclusive >= at the cutoff",
		"SELECT COUNT(*) AS `cnt6`, MIN(`ts6`) AS `ts6_min` FROM `edge_ts_1` WHERE `entity_id` = 1 AND `ts6` >= '2026-05-01 12:00:00.000000';", false, nil, nil, "")
	e("EDGE-ts0-batch", "batch window at the cutoff, one empty entity",
		"SELECT `entity_id`, COUNT(*) AS `cnt`, MIN(`ts0`) AS `ts0_min` FROM `edge_ts_1` WHERE `entity_id` IN (1, 2, 3) AND `ts0` >= '2026-05-01 12:00:00' GROUP BY `entity_id`;", false, nil, nil, "")
	e("EDGE-seq-order", "collect ordered by an explicit sequence_no that disagrees with event time",
		"SELECT `entity_id`, `sequence_no`, `event_time`, `payload`, `amount` FROM `edge_seq_1` WHERE `entity_id` = 4 ORDER BY `sequence_no` DESC LIMIT 2;", true, nil, nil, "")
	e("EDGE-seq-boundary", "N boundary: 7 rows, LIMIT 8, ascending",
		"SELECT `entity_id`, `sequence_no`, `payload` FROM `edge_seq_1` WHERE `entity_id` = 1 ORDER BY `sequence_no` ASC LIMIT 8;", true, nil, nil, "")
	e("EDGE-comp-hop", "composite child-PK hop (parent 2 -> child B)",
		"WITH `b` AS (SELECT `ck1`, `ck2`, COUNT(*) AS `hw_cnt` FROM `edge_parent_1` WHERE `parent_id` = 2 GROUP BY `ck1`, `ck2`) SELECT `j2`.`label` AS `c_label`, `j2`.`weight` AS `c_weight` FROM `b` JOIN `edge_child_1` AS `j2` ON `j2`.`ck1` = `b`.`ck1` AND `j2`.`ck2` = `b`.`ck2`;", false, nil, nil, "")
	e("EDGE-comp-swapped", "swapped binding selects the other child (A)",
		"WITH `b` AS (SELECT `ck1`, `ck2`, COUNT(*) AS `hw_cnt` FROM `edge_parent_1` WHERE `parent_id` = 2 GROUP BY `ck1`, `ck2`) SELECT `j2`.`label` AS `c_label`, `j2`.`weight` AS `c_weight` FROM `b` JOIN `edge_child_1` AS `j2` ON `j2`.`ck1` = `b`.`ck2` AND `j2`.`ck2` = `b`.`ck1`;", false, nil, nil, "")
	e("EDGE-comp-batch", "batch over dangling, NULL and matching hops",
		"WITH `b` AS (SELECT `parent_id`, `ck1`, `ck2`, COUNT(*) AS `hw_cnt` FROM `edge_parent_1` WHERE `parent_id` IN (1, 3, 4, 5, 6) GROUP BY `parent_id`, `ck1`, `ck2`) SELECT `j2`.`label` AS `c_label`, `b`.`parent_id` AS `parent_id` FROM `b` JOIN `edge_child_1` AS `j2` ON `j2`.`ck1` = `b`.`ck1` AND `j2`.`ck2` = `b`.`ck2`;", false, nil, nil, "")
	e("EDGE-comp-binary", "VARBINARY projection through the snowflake template (F7, emitted shape)",
		"WITH `b` AS (SELECT `ck1`, `ck2`, COUNT(*) AS `hw_cnt` FROM `edge_parent_1` WHERE `parent_id` = 1 GROUP BY `ck1`, `ck2`) SELECT `j2`.`label` AS `c_label`, `j2`.`payload` AS `c_payload` FROM `b` JOIN `edge_child_1` AS `j2` ON `j2`.`ck1` = `b`.`ck1` AND `j2`.`ck2` = `b`.`ck2`;", false, Known["F7"], nil, "")
	e("EDGE-F1-string-reuse", "F1: string aggregated twice with a load in between (CRASH — hazard)",
		"SELECT COUNT(`s_val`) AS `cnt_s`, SUM(`i1`) AS `i1_sum`, MAX(`s_val`) AS `s_max` FROM `edge_hist_1` WHERE `entity_id` = 1;", false, nil, nil, "F1")
	e("EDGE-F1-string-reuse-nonull", "F1 kernel-side variant (DATA NODE CRASH — hazard)",
		"SELECT COUNT(`s_val`) AS `cnt_s`, SUM(`i1`) AS `i1_sum`, MAX(`s_val`) AS `s_max` FROM `edge_hist_1` WHERE `entity_id` = 3;", false, nil, nil, "F1")
	// F9: MIN/MAX over a DATE/TIMESTAMP column is printed unquoted in JSON
	// output (ResultPrinter::print_aggregate_value), so the RDRS body is
	// unparsable; the TEXT form (MTR) is correct.
	for _, id := range []string{"EDGE-float-exact", "EDGE-date-range", "EDGE-ts3-cutoff", "EDGE-ts6-cutoff", "EDGE-ts0-batch"} {
		b.knownError(id, Known["F9"])
	}
}
