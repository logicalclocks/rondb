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

// The spec catalog of the L2 vector oracle (E4, framework_design.md §8):
// one feature-view specification per Hopsworks serving shape, verified
// over seeded entity keys that always include the miss / NULL-hop /
// empty-history classes of data_model.md §6.

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

// Spec is one feature-view specification of the catalog.
type Spec struct {
	ID, Shape, Note string
	View            *spec.View
	Params          []string // DTO parameter names, in order
	Batch           int      // 0 = single-entity statements; else keys per batch statement
	// DirectCollect rewrites the Hopsworks CTE collect template to the
	// direct single-table form (S6b) so the collect fold can run while F0
	// stands; the DTO metadata is unchanged.
	DirectCollect bool
	ExpectReject  *Expect
	// Family selects the data-model expectation (expect.go): "txagg",
	// "stragg", "hist", "collect", "snowflake"; "" = no independent oracle.
	Family string
	Window *int64
	Filter func(data.Tx) bool // txagg: the feature-view filter as a row predicate
	Depth  int                // snowflake hops
	Left   bool               // snowflake LEFT joins (per-chain templates)

	dtos []emit.Statement
	b    *builder
}

// DTOs returns the emitted statements of the spec (unbound).
func (s *Spec) DTOs() []emit.Statement { return s.dtos }

// Specs returns the catalog for the configuration.
func Specs(cfg Config) ([]Spec, error) {
	b := newBuilder(cfg)
	var out []Spec
	add := func(s Spec) {
		s.b = b
		dtos, err := emit.Build(s.View)
		if err != nil {
			b.fail(s.ID, err)
			return
		}
		if len(dtos) == 0 {
			b.fail(s.ID, fmt.Errorf("no statement emitted"))
			return
		}
		s.dtos = dtos
		for _, d := range dtos {
			if len(d.PreparedStatementParameters) > 0 {
				for _, p := range d.PreparedStatementParameters {
					s.Params = append(s.Params, p.Name)
				}
				break
			}
		}
		if len(s.Params) == 0 {
			b.fail(s.ID, fmt.Errorf("no DTO parameters"))
			return
		}
		out = append(out, s)
	}
	tx := b.fg(data.TTransactions)
	leaf := func(feature string, cond spec.SqlCondition, value string) spec.Filter {
		c := cond
		return spec.Filter{FG: tx.ID, Logic: spec.LogicSingle, Feature: feature, Condition: &c, Value: strp(value)}
	}
	day := int64(86400)

	add(Spec{ID: "V-S1-agg", Shape: "S1", Note: "point aggregate over transactions", Family: "txagg",
		View: b.txAggView("v-s1", txAgg, nil, false, nil)})
	for _, w := range []struct {
		id   string
		days int64
	}{{"V-S2-agg-w7d", 7}, {"V-S2-agg-w30d", 30}} {
		add(Spec{ID: w.id, Shape: "S2", Note: fmt.Sprintf("%d-day window", w.days), Family: "txagg", Window: i64p(w.days * day),
			View: b.txAggView("v-s2", txAgg, i64p(w.days*day), false, nil)})
	}
	for _, n := range []int{10, 100} {
		add(Spec{ID: fmt.Sprintf("V-S3-agg-b%d", n), Shape: "S3", Note: fmt.Sprintf("batch of %d, IN list + GROUP BY", n), Family: "txagg", Batch: n,
			View: b.txAggView("v-s3", txAgg, nil, true, nil)})
	}
	add(Spec{ID: "V-S4-ge-300", Shape: "S4", Note: "filter amount >= 300", Family: "txagg",
		Filter: func(t data.Tx) bool { return t.Amount >= 300 },
		View:   b.txAggView("v-s4a", txAgg, nil, false, []spec.Filter{leaf("amount", spec.CondGreaterThanOrEqual, "300")})})
	add(Spec{ID: "V-S4-eq-grocery", Shape: "S4", Note: "filter category = 'grocery' (utf8mb4_0900_ai_ci)", Family: "txagg",
		Filter: func(t data.Tx) bool { return strings.EqualFold(t.Category, "grocery") },
		View:   b.txAggView("v-s4b", txAgg, nil, false, []spec.Filter{leaf("category", spec.CondEquals, "grocery")})})
	add(Spec{ID: "V-S4-like-gro", Shape: "S4", Note: "filter category LIKE 'gro%'", Family: "txagg",
		Filter: func(t data.Tx) bool { return strings.HasPrefix(strings.ToLower(t.Category), "gro") },
		View:   b.txAggView("v-s4c", txAgg, nil, false, []spec.Filter{leaf("category", spec.CondLike, "gro%")})})
	add(Spec{ID: "V-S4-window-b10", Shape: "S4", Note: "filter + 30-day window in a batch", Family: "txagg", Batch: 10, Window: i64p(30 * day),
		Filter: func(t data.Tx) bool { return t.Amount >= 300 },
		View:   b.txAggView("v-s4d", txAgg, i64p(30*day), true, []spec.Filter{leaf("amount", spec.CondGreaterThanOrEqual, "300")})})

	add(Spec{ID: "V-S6-cte-n5", Shape: "S6", Note: "Hopsworks CTE collect form (F0)", Family: "collect", ExpectReject: Known["S6-cte"],
		View: b.collectView("v-s6", 5, false, false)})
	add(Spec{ID: "V-S6b-direct-n5", Shape: "S6b", Note: "direct collect, newest first, N = 5", Family: "collect", DirectCollect: true,
		View: b.collectView("v-s6b", 5, false, false)})
	add(Spec{ID: "V-S6b-direct-asc-n5", Shape: "S6b", Note: "direct collect, client sort ascending", Family: "collect", DirectCollect: true,
		View: b.collectView("v-s6c", 5, true, false)})
	add(Spec{ID: "V-S6b-direct-n50", Shape: "S6b", Note: "direct collect, N = 50", Family: "collect", DirectCollect: true,
		View: b.collectView("v-s6d", 50, false, false)})

	add(Spec{ID: "V-S7-inner-1hop", Shape: "S7", Note: "INNER 1-hop combined template", Family: "snowflake", Depth: 1,
		View: b.snowflakeView("v-s7a", 1, spec.JoinInner, false)})
	add(Spec{ID: "V-S7-inner-2hop", Shape: "S7", Note: "INNER 2-hop combined template", Family: "snowflake", Depth: 2,
		View: b.snowflakeView("v-s7b", 2, spec.JoinInner, false)})
	add(Spec{ID: "V-S8-left-2hop", Shape: "S8", Note: "LEFT 2-hop per-chain templates", Family: "snowflake", Depth: 2, Left: true,
		View: b.snowflakeView("v-s8", 2, spec.JoinLeft, false)})
	add(Spec{ID: "V-S7-inner-2hop-b10", Shape: "S7", Note: "INNER 2-hop batch of 10", Family: "snowflake", Depth: 2, Batch: 10,
		View: b.snowflakeView("v-s7c", 2, spec.JoinInner, true)})
	add(Spec{ID: "V-S8-left-2hop-b10", Shape: "S8", Note: "LEFT 2-hop per-chain batch of 10", Family: "snowflake", Depth: 2, Left: true, Batch: 10,
		View: b.snowflakeView("v-s8b", 2, spec.JoinLeft, true)})

	add(Spec{ID: "V-S9-hist", Shape: "S9", Note: "composite entity key (account, currency)", Family: "hist",
		View: b.histView("v-s9", nil, false)})
	add(Spec{ID: "V-S9-hist-w90d", Shape: "S9", Note: "composite key with a 90-day window", Family: "hist", Window: i64p(90 * day),
		View: b.histView("v-s9w", i64p(90*day), false)})

	add(Spec{ID: "V-S10-str", Shape: "S10", Note: "string entity key", Family: "stragg",
		View: b.strView("v-s10", false)})
	add(Spec{ID: "V-S10-str-b10", Shape: "S10", Note: "string keys, batch of 10", Family: "stragg", Batch: 10,
		View: b.strView("v-s10b", true)})

	if len(b.errs) > 0 {
		return nil, b.errs[0]
	}
	return out, nil
}

// histView is the composite-key aggregate view over balance_hist_1 (S9).
func (b *builder) histView(name string, window *int64, batch bool) *spec.View {
	hist := b.fg(data.TBalanceHist)
	j := spec.Join{Index: 0, Parent: 0, FG: hist.ID, Type: spec.JoinInner, Prefix: strp("b_"), Aggregate: histAgg, Window: window, Features: aggOutputs(histAgg)}
	return b.view(name, []spec.FeatureGroup{hist}, []spec.Join{j}, nil, spec.Options{Batch: batch})
}

var histAgg = spec.AggSpec{{Key: "*", Fns: []string{"count"}}, {Key: "delta", Fns: []string{"sum", "min", "max"}}}

// strView is the aggregate view over the string-keyed transactions_str_1 (S10).
func (b *builder) strView(name string, batch bool) *spec.View {
	str := b.fg(data.TTxStr)
	j := spec.Join{Index: 0, Parent: 0, FG: str.ID, Type: spec.JoinInner, Prefix: strp("s_"), Aggregate: txAgg, Features: aggOutputs(txAgg)}
	return b.view(name, []spec.FeatureGroup{str}, []spec.Join{j}, nil, spec.Options{Batch: batch})
}

// ---- keys ------------------------------------------------------------------------

// customerIDs samples count customer ids: two of every key class first
// (empty / one-row / max-row history, day-bound and hourly spacing, NULL and
// dangling region and country hops, upper-case string key, missing ids),
// then seeded random ids.  Deterministic for a seed.
func customerIDs(sc data.Scale, rng *rand.Rand, count int) []int64 {
	var out []int64
	seen := map[int64]bool{}
	add := func(c int64) {
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	for _, k := range []data.KeyClass{data.ClassNoTx, data.ClassOneTx, data.ClassFiveTx, data.ClassFiftyTx, data.ClassMaxTx,
		data.ClassDayBounds, data.ClassHourly, data.ClassNullRegion, data.ClassDanglingRegion, data.ClassNullCountry,
		data.ClassDanglingCountry, data.ClassUpperKey, data.ClassMissing} {
		for _, c := range data.Pick(k, 2, sc) {
			add(c)
		}
	}
	for len(out) < count {
		add(rng.Int63n(sc.E) + 1)
	}
	return out
}

// SampleKeys returns at least count entity keys for the spec.
func (s *Spec) SampleKeys(seed int64, count int) []vector.Key {
	rng := rand.New(rand.NewSource(seed))
	sc := s.b.cfg.Scale
	var out []vector.Key
	switch s.Family {
	case "hist":
		seen := map[string]bool{}
		add := func(a int64, cur string) {
			k := vector.Key{strconv.FormatInt(a, 10), cur}
			if !seen[k.Text()] {
				seen[k.Text()] = true
				out = append(out, k)
			}
		}
		// Accounts 1..5 cover every history class and currency count.
		for a := int64(1); a <= 5 && a <= sc.A; a++ {
			for _, cur := range data.Currencies {
				add(a, cur)
			}
		}
		add(4, "GBP")
		add(sc.A+1, "EUR")
		for len(out) < count {
			add(rng.Int63n(sc.A)+1, data.Currencies[rng.Intn(len(data.Currencies))])
		}
	case "stragg":
		for _, c := range customerIDs(sc, rng, count) {
			out = append(out, vector.Key{data.CustomerKey(c)})
		}
		if s.Batch == 0 {
			// Collation and quoting probes (single statements only: a batch
			// GROUP BY returns the stored form of the key).
			out = append(out, vector.Key{"cust-00000010"}, vector.Key{"O'Brien"})
		}
	default:
		for _, c := range customerIDs(sc, rng, count) {
			out = append(out, vector.Key{strconv.FormatInt(c, 10)})
		}
	}
	return out
}

// Units groups the keys into execution units: one per key for single
// statements, chunks of Batch keys for batch statements.
func (s *Spec) Units(keys []vector.Key) [][]vector.Key {
	var out [][]vector.Key
	if s.Batch <= 0 {
		for _, k := range keys {
			out = append(out, []vector.Key{k})
		}
		return out
	}
	for i := 0; i < len(keys); i += s.Batch {
		j := i + s.Batch
		if j > len(keys) {
			j = len(keys)
		}
		out = append(out, keys[i:j])
	}
	return out
}

var cteCollect = regexp.MustCompile(`(?s)^WITH t AS \((.*)\) SELECT .* FROM t;$`)

// directCollect turns the Hopsworks CTE collect template into its body,
// the direct single-table form (S6b).
func directCollect(template string) (string, error) {
	m := cteCollect.FindStringSubmatch(strings.TrimSpace(template))
	if m == nil {
		return "", fmt.Errorf("not a CTE collect template: %s", template)
	}
	return m[1] + ";", nil
}

// Bind binds every DTO of the spec to the keys (one key for a single
// statement, all keys of a batch).
func (s *Spec) Bind(keys []vector.Key) ([]StatementGroup, error) {
	fg := s.View.FGs[0]
	kv := keyVals{}
	for i, prm := range s.Params {
		f, ok := fg.Feature(prm)
		if !ok {
			return nil, fmt.Errorf("parameter %s is not a feature of %s", prm, fg.Name)
		}
		vals := make([]interface{}, 0, len(keys))
		for _, k := range keys {
			if i >= len(k) {
				return nil, fmt.Errorf("key %v lacks parameter %s", k, prm)
			}
			if spec.IsIntegerType(f.Type) {
				n, err := strconv.ParseInt(k[i], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("key %v: %s is not an integer", k, prm)
				}
				vals = append(vals, n)
			} else {
				vals = append(vals, k[i])
			}
		}
		kv[prm] = s.b.keyArg(fg, prm, vals)
	}
	var groups []StatementGroup
	for _, dto := range s.dtos {
		st := dto
		if s.DirectCollect && st.QueryRonsql != nil {
			direct, err := directCollect(*st.QueryRonsql)
			if err != nil {
				return nil, err
			}
			st.QueryRonsql = &direct
		}
		g, err := s.b.bindStatement(st, kv)
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// Plan is the fold recipe of one DTO of the spec.
func (s *Spec) Plan(dto emit.Statement) vector.Plan {
	var fields []string
	for _, j := range s.View.Joins {
		for _, f := range j.Features {
			if f.Type != nil {
				if names := emit.ParseStructFieldNames(*f.Type); len(names) > 0 {
					fields = names
				}
			}
		}
	}
	return vector.PlanFor(dto, s.Batch > 0, fields)
}

// prefix is the feature-view prefix of the spec's first join.
func (s *Spec) prefix() string {
	if len(s.View.Joins) > 0 && s.View.Joins[0].Prefix != nil {
		return *s.View.Joins[0].Prefix
	}
	return ""
}
