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

// Package fuzz is the spec-level random generator (RONDB-1121 E6,
// random_generator.md §4): seeded feature-view specifications sampled
// inside the Hopsworks definition-time and serving-time gates, each with
// the outcome the ported emitter must produce (a named gate, or a template
// count), entity keys drawn by class from the data model, and a shape
// signature for de-duplicating findings.  Case (seed, i) is regenerated
// from its own PRNG, so any case is reproducible without replaying a run.
package fuzz

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

// SpecVersion is part of every case id; a grammar change bumps it.
const SpecVersion = "spec-v1"

// Config selects the data set the cases bind to.
type Config struct {
	DB    string
	Scale data.Scale
	Now   time.Time
}

// Expect is the outcome the emitter must produce for a case.
type Expect struct {
	// Gate is the GateError code the definition validator (definition
	// cases) or emit.Build (serving cases) must return; "" = no gate.
	Gate string
	// Templates is the number of RonSQL templates Build must emit when
	// no gate applies (-1: not checked, used while shrinking).
	Templates int
	// KnownError names the engine finding that makes the RonSQL response
	// unusable although the statement is legal: "F9" for MIN/MAX over a
	// DATE/TIMESTAMP column (unparsable JSON).
	KnownError string
	// Reason documents a deliberate violation.
	Reason string
}

// Case is one generated specification.
type Case struct {
	ID    string
	Seed  uint64
	Index int
	Kind  string // "definition" | "serving"
	View  *spec.View
	// KeyNames are the root entity-key parameter names in DTO order;
	// Keys holds one literal set per requested entity (Batch > 1: many).
	KeyNames    []string
	Keys        []vector.Key
	KeyClass    string
	Batch       int
	LeftSubtree bool // every nested join is LEFT (vector policy: missing equals NULL)
	Expect      Expect
	Signature   string
}

// Gen samples cases over the data-model schema.
type Gen struct {
	cfg Config
	fgs map[string]spec.FeatureGroup // by feature-group name
}

// New prepares the generator (feature-group ids assigned like fsq/cases).
func New(cfg Config) *Gen {
	if cfg.Now.IsZero() {
		cfg.Now = data.FSNow
	}
	if cfg.Scale.E == 0 {
		cfg.Scale = data.NewScale(0.01)
	}
	g := &Gen{cfg: cfg, fgs: map[string]spec.FeatureGroup{}}
	for i, fg := range data.Schema() {
		fg.ID = i + 1
		fg.FeaturestoreID = 1
		fg.OnlineDB = cfg.DB
		g.fgs[fg.Name] = fg
	}
	return g
}

// Config returns the generator configuration.
func (g *Gen) Config() Config { return g.cfg }

// Case regenerates case i of a seed.
func (g *Gen) Case(seed uint64, i int) Case {
	s := &sampler{g: g, rng: rand.New(rand.NewPCG(seed, uint64(i))),
		c: Case{ID: fmt.Sprintf("%s-%d-%d", SpecVersion, seed, i), Seed: seed, Index: i}}
	if s.pct(10) {
		s.definition()
	} else {
		s.serving()
	}
	return s.c
}

// ---- sampling helpers ------------------------------------------------------------

type sampler struct {
	g   *Gen
	rng *rand.Rand
	c   Case
	sig []string
}

func (s *sampler) pct(p int) bool { return s.rng.IntN(100) < p }

// choose returns the index of one weighted option.
func (s *sampler) choose(weights ...int) int {
	total := 0
	for _, w := range weights {
		total += w
	}
	r := s.rng.IntN(total)
	for i, w := range weights {
		if r < w {
			return i
		}
		r -= w
	}
	return len(weights) - 1
}

func (s *sampler) pickString(opts []string) string { return opts[s.rng.IntN(len(opts))] }

func strp(v string) *string { return &v }
func intp(v int) *int       { return &v }
func i64p(v int64) *int64   { return &v }

// historyFor lists the history feature groups keyed like the entity group.
func historyFor(root string) []string {
	switch root {
	case "customers":
		return []string{"transactions", "sessions"}
	case "customers_str":
		return []string{"transactions_str"}
	case "balances":
		return []string{"balance_hist"}
	}
	return nil
}

// fkChild is the one snowflake hop of a feature group in the data model:
// customers / customers_str / profiles -> regions -> countries.
func fkChild(name string) (child string, on [2]string, ok bool) {
	switch name {
	case "customers", "customers_str", "profiles":
		return "regions", [2]string{"region_id", "region_id"}, true
	case "regions":
		return "countries", [2]string{"country_id", "country_id"}, true
	}
	return "", [2]string{}, false
}

func pkNames(fg spec.FeatureGroup) []string {
	var out []string
	for _, f := range fg.Features {
		if f.Primary {
			out = append(out, f.Name)
		}
	}
	return out
}

// valueFeatures are the scalar, non-key, non-event-time features.
func valueFeatures(fg spec.FeatureGroup) []spec.Feature {
	var out []spec.Feature
	for _, f := range fg.Features {
		if f.Primary || f.Name == fg.EventTime || spec.IsComplexType(f.Type) {
			continue
		}
		out = append(out, f)
	}
	return out
}

func isTemporal(t string) bool {
	switch spec.BaseType(t) {
	case "timestamp", "date", "datetime":
		return true
	}
	return false
}

func isString(t string) bool { return spec.BaseType(t) == "string" }

// sampleFeatures picks lo..hi distinct value features (in schema order).
func (s *sampler) sampleFeatures(fg spec.FeatureGroup, lo, hi int) []spec.Feature {
	vals := valueFeatures(fg)
	if len(vals) == 0 {
		return nil
	}
	n := lo + s.rng.IntN(hi-lo+1)
	if n > len(vals) {
		n = len(vals)
	}
	perm := s.rng.Perm(len(vals))[:n]
	sort.Ints(perm)
	out := make([]spec.Feature, 0, n)
	for _, i := range perm {
		out = append(out, vals[i])
	}
	return out
}

// tdFeatures turns features into training-dataset features, flagging a
// few as label / helper when the view exercises the helper options.
func (s *sampler) tdFeatures(fs []spec.Feature, flags bool) []spec.TDFeature {
	out := make([]spec.TDFeature, 0, len(fs))
	for _, f := range fs {
		t := f.Type
		tdf := spec.TDFeature{Name: f.Name, Type: &t}
		if flags {
			switch s.choose(70, 10, 10, 10) {
			case 1:
				tdf.Label = true
			case 2:
				tdf.InferenceHelper = true
			case 3:
				tdf.TrainingHelper = true
			}
		}
		out = append(out, tdf)
	}
	return out
}

// selectedNames mirrors emit.getTrainingDatasetFeatures for a join under
// the view's options: the names a snowflake template projects.
func selectedNames(opt spec.Options, j spec.Join) []string {
	var out []string
	for _, tdf := range j.Features {
		if opt.InferenceHelpers {
			if tdf.InferenceHelper {
				out = append(out, tdf.Name)
			}
			continue
		}
		if tdf.Label || tdf.TrainingHelper {
			continue
		}
		if !(opt.Logging || opt.VectorWithHelpers || !tdf.InferenceHelper) {
			continue
		}
		out = append(out, tdf.Name)
	}
	return out
}

// ---- aggregate and collect specs ---------------------------------------------------

// aggOutputs names the outputs the way the emitter does.
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

// collationAmbiguous lists the string columns whose value domain holds
// collation-equal variants (data_model.md §5: category grocery/Grocery,
// travel/Travel; device web/WEB); MIN/MAX over them is not comparable (F8).
var collationAmbiguous = map[string]bool{"category": true, "device": true}

// validAggregate samples 1-4 entries inside the type matrix.  String
// columns get exactly one function (F1: a string column aggregated twice
// with another column load in between crashes RDRS); MIN/MAX over the
// event time is sampled rarely and flagged as the F9 known error.
func (s *sampler) validAggregate(fg spec.FeatureGroup) (spec.AggSpec, string) {
	known := ""
	var numeric, ints, strs []string
	for _, f := range valueFeatures(fg) {
		switch {
		case spec.IsIntegerType(f.Type):
			ints = append(ints, f.Name)
			numeric = append(numeric, f.Name)
		case spec.IsNumericType(f.Type):
			numeric = append(numeric, f.Name)
		case isString(f.Type):
			strs = append(strs, f.Name)
		}
	}
	var agg spec.AggSpec
	used := map[string]bool{}
	n := 1 + s.rng.IntN(4)
	for k := 0; k < n; k++ {
		switch s.choose(15, 45, 15, 15, 5, 5) {
		case 0:
			if !used["*"] {
				used["*"] = true
				agg = append(agg, spec.AggEntry{Key: "*", Fns: []string{"count"}})
			}
		case 1:
			if len(numeric) > 0 {
				col := s.pickString(numeric)
				if !used[col] {
					used[col] = true
					fns := []string{"count", "sum", "min", "max", "avg"}
					m := 1 + s.rng.IntN(3)
					perm := s.rng.Perm(len(fns))[:m]
					sort.Ints(perm)
					var pick []string
					for _, i := range perm {
						pick = append(pick, fns[i])
					}
					agg = append(agg, spec.AggEntry{Key: col, Fns: pick})
				}
			}
		case 2:
			if len(strs) > 0 {
				col := s.pickString(strs)
				if !used[col] {
					used[col] = true
					fns := []string{"count", "min", "max"}
					if collationAmbiguous[col] {
						// F8: MIN/MAX over collation-equal values ('grocery' vs
						// 'Grocery') has an unspecified representative on both engines.
						fns = []string{"count"}
					}
					agg = append(agg, spec.AggEntry{Key: col, Fns: []string{s.pickString(fns)}})
				}
			}
		case 3:
			if len(ints) >= 2 {
				perm := s.rng.Perm(len(ints))[:2]
				key := ints[perm[0]] + "," + ints[perm[1]]
				if !used[key] {
					used[key] = true
					fns := []string{"greatest"}
					if s.pct(50) {
						fns = append(fns, "least")
					}
					agg = append(agg, spec.AggEntry{Key: key, Fns: fns})
				}
			}
		case 4:
			if fg.EventTime != "" && !used[fg.EventTime] {
				used[fg.EventTime] = true
				agg = append(agg, spec.AggEntry{Key: fg.EventTime, Fns: []string{s.pickString([]string{"min", "max"})}})
				known = "F9"
			}
		case 5:
			if len(numeric) > 0 {
				col := s.pickString(numeric)
				if !used[col] {
					used[col] = true
					agg = append(agg, spec.AggEntry{Key: col, Fns: []string{"count"}})
				}
			}
		}
	}
	if len(agg) == 0 {
		agg = spec.AggSpec{{Key: "*", Fns: []string{"count"}}}
	}
	return agg, known
}

// collectFeature builds the synthesized array<struct<...>> feature of a
// collect join with 1-3 value fields, N in {1, 5, 50} within the width
// limit (N x fields <= 100).
func (s *sampler) collectFeature(fg spec.FeatureGroup) (spec.TDFeature, int, []string) {
	n := []int{1, 5, 50}[s.rng.IntN(3)]
	hi := 3
	if n == 50 {
		hi = 2
	}
	fields := s.sampleFeatures(fg, 1, hi)
	if len(fields) == 0 {
		fields = valueFeatures(fg)[:1]
	}
	var parts, names []string
	for _, f := range fields {
		parts = append(parts, f.Name+":"+f.Type)
		names = append(names, f.Name)
	}
	t := "array<struct<" + strings.Join(parts, ",") + ">>"
	return spec.TDFeature{Name: emit.CollectFeatureName(fg), Type: &t}, n, names
}

// ---- keys ------------------------------------------------------------------------

// keyClassWeights follows random_generator.md §4.1.9.
var keyClasses = []struct {
	name   string
	weight int
	class  data.KeyClass
}{
	{"ordinary", 50, data.ClassAny}, {"no-rows", 10, data.ClassNoTx}, {"exactly-n", 10, data.ClassFiveTx},
	{"bound-aligned", 10, data.ClassDayBounds}, {"null-hop", 5, data.ClassNullRegion}, {"dangling-hop", 5, data.ClassDanglingRegion},
	{"missing", 10, data.ClassMissing},
}

// customerKey draws one customer id of a weighted class.
func (s *sampler) customerKey() (int64, string) {
	sc := s.g.cfg.Scale
	weights := make([]int, len(keyClasses))
	for i, k := range keyClasses {
		weights[i] = k.weight
	}
	k := keyClasses[s.choose(weights...)]
	if k.class == data.ClassMissing {
		return sc.E + 1 + int64(s.rng.IntN(1000)), k.name
	}
	for try := 0; try < 64; try++ {
		c := int64(s.rng.IntN(int(sc.E))) + 1
		if data.InClass(k.class, c, sc) {
			return c, k.name
		}
	}
	if ids := data.Pick(k.class, 1, sc); len(ids) > 0 {
		return ids[0], k.name
	}
	return int64(s.rng.IntN(int(sc.E))) + 1, "ordinary"
}

// keysFor draws n entity keys for the root feature group.
func (s *sampler) keysFor(root spec.FeatureGroup, n int) ([]string, []vector.Key, string) {
	sc := s.g.cfg.Scale
	var keys []vector.Key
	class := ""
	names := pkNames(root)
	for i := 0; i < n; i++ {
		var k vector.Key
		var cls string
		switch root.Name {
		case "customers", "profiles":
			c, cl := s.customerKey()
			k, cls = vector.Key{strconv.FormatInt(c, 10)}, cl
		case "customers_str":
			c, cl := s.customerKey()
			k, cls = vector.Key{data.CustomerKey(c)}, cl
		case "balances":
			a := int64(s.rng.IntN(int(sc.A))) + 1
			cls = "ordinary"
			if a%5 == 0 {
				cls = "no-rows"
			}
			cur := data.Currencies[s.rng.IntN(data.NCurrencies(a))]
			if s.pct(15) {
				cur, cls = "GBP", "missing"
			}
			if s.pct(10) {
				a, cls = sc.A+1+int64(s.rng.IntN(100)), "missing"
			}
			k = vector.Key{strconv.FormatInt(a, 10), cur}
		}
		if i == 0 {
			class = cls
		} else if cls != class {
			class = "mixed"
		}
		keys = append(keys, k)
	}
	return names, keys, class
}

// ---- filters ---------------------------------------------------------------------

// literalFor draws a value inside the feature's domain (data_model.md §5).
func (s *sampler) literalFor(fg spec.FeatureGroup, f spec.Feature) string {
	sc := s.g.cfg.Scale
	switch f.Name {
	case "category":
		return data.Categories[s.rng.IntN(len(data.Categories))]
	case "device":
		return data.Devices[s.rng.IntN(len(data.Devices))]
	case "channel":
		return data.Channels[s.rng.IntN(len(data.Channels))]
	case "tier":
		return data.Tiers[s.rng.IntN(len(data.Tiers))]
	case "currency":
		return data.Currencies[s.rng.IntN(len(data.Currencies))]
	case "amount":
		return strconv.Itoa(100 + s.rng.IntN(900))
	case "fee":
		return strconv.Itoa(-10 + s.rng.IntN(50))
	case "merchant_id":
		return strconv.FormatInt(int64(s.rng.IntN(int(sc.M)))+1, 10)
	case "flag":
		return strconv.Itoa(s.rng.IntN(2))
	case "score":
		return strconv.Itoa(s.rng.IntN(97)) + ".25"
	case "amount_dec":
		return fmt.Sprintf("%d.%02d", s.rng.IntN(900), s.rng.IntN(100))
	case "duration":
		return strconv.Itoa(30 + s.rng.IntN(3600))
	case "pages":
		return strconv.Itoa(1 + s.rng.IntN(40))
	case "bytes":
		return strconv.Itoa(s.rng.IntN(10000000))
	case "delta":
		return strconv.Itoa(-100 + s.rng.IntN(200))
	}
	if spec.IsNumericType(f.Type) {
		return strconv.Itoa(s.rng.IntN(100))
	}
	return "x"
}

// filterLeaf builds one renderable filter on a feature of fg.
func (s *sampler) filterLeaf(fg spec.FeatureGroup, joinIndex *int) (spec.Filter, bool) {
	vals := valueFeatures(fg)
	var cands []spec.Feature
	for _, f := range vals {
		if !isTemporal(f.Type) && spec.BaseType(f.Type) != "boolean" {
			cands = append(cands, f)
		}
	}
	if len(cands) == 0 {
		return spec.Filter{}, false
	}
	f := cands[s.rng.IntN(len(cands))]
	conds := []spec.SqlCondition{spec.CondEquals, spec.CondNotEquals, spec.CondGreaterThan, spec.CondGreaterThanOrEqual, spec.CondLessThan, spec.CondLessThanOrEqual}
	value := s.literalFor(fg, f)
	cond := conds[s.rng.IntN(len(conds))]
	if isString(f.Type) && s.pct(30) {
		cond = spec.CondLike
		if len(value) > 2 {
			value = value[:2] + "%"
		} else {
			value += "%"
		}
	}
	c := cond
	return spec.Filter{FG: fg.ID, JoinIndex: joinIndex, Logic: spec.LogicSingle, Feature: f.Name, Condition: &c, Value: strp(value)}, true
}

// ---- serving cases ---------------------------------------------------------------

func (s *sampler) serving() {
	c := &s.c
	c.Kind = "serving"
	roots := []string{"customers", "customers_str", "balances", "profiles"}
	root := s.g.fgs[roots[s.choose(60, 20, 10, 10)]]
	s.sig = append(s.sig, "serving", "root="+root.Name)
	v := &spec.View{Name: c.ID, MaxCollectN: 50, MaxCollectCells: 100}
	fgSet := map[int]spec.FeatureGroup{root.ID: root}
	opt := spec.Options{InferenceHelpers: s.pct(10), Logging: s.pct(10), VectorWithHelpers: s.pct(10)}
	flags := opt.InferenceHelpers || opt.Logging || opt.VectorWithHelpers
	mode := []int{0, 10, 100, 1000}[s.choose(60, 20, 15, 5)]
	if mode > int(s.g.cfg.Scale.E) {
		mode = int(s.g.cfg.Scale.E)
	}
	opt.Batch = mode > 0
	c.Batch = mode
	v.Options = opt
	s.sig = append(s.sig, "mode="+map[bool]string{false: "single", true: "batch" + strconv.Itoa(mode)}[mode > 0])

	// Join 0: the entity group and 0-3 of its plain features (a MySQL-only
	// point read; label features are never selected).
	joins := []spec.Join{{Index: 0, Parent: 0, FG: root.ID, Type: spec.JoinInner, Prefix: strp(""),
		Features: s.tdFeatures(s.sampleFeatures(root, 0, 3), flags)}}
	next := 1
	topology := s.choose(55, 35, 10) // star, snowflake, both
	if _, _, hasChain := fkChild(root.Name); !hasChain {
		topology = 0 // no FK chain from this entity group: star only
	}
	expected := 0
	hasAggOrCollect := false
	var aggJoins []int // join indexes carrying aggregate/collect (filter targets)
	gate := ""
	reason := ""
	setGate := func(code, why string) {
		if gate == "" {
			gate, reason = code, why
		}
	}
	rootKeys := pkNames(root)

	if topology != 1 {
		hist := historyFor(root.Name)
		n := 1 + s.rng.IntN(3)
		for k := 0; k < n; k++ {
			kind := s.choose(50, 35, 15) // aggregate, collect, plain
			if len(hist) == 0 {
				kind = 2
			}
			switch kind {
			case 0, 1:
				fg := s.g.fgs[hist[s.rng.IntN(len(hist))]]
				fgSet[fg.ID] = fg
				var on [][2]string
				for _, k := range rootKeys {
					on = append(on, [2]string{k, k})
				}
				j := spec.Join{Index: next, Parent: 0, FG: fg.ID, Type: spec.JoinLeft, Prefix: strp(fmt.Sprintf("j%d_", next)), On: on}
				if s.pct(30) {
					j.Type = spec.JoinInner
				}
				entityKeys := len(pkNames(fg)) - 1 // minus the event time
				if kind == 0 {
					agg, known := s.validAggregate(fg)
					j.Aggregate, j.Features = agg, aggOutputs(agg)
					if known != "" {
						c.Expect.KnownError = known
					}
					if s.pct(60) {
						j.Window = i64p([]int64{3600, 86400, 7 * 86400, 30 * 86400, 90 * 86400}[s.rng.IntN(5)])
					}
					if !opt.Batch || entityKeys == 1 {
						expected++
					}
					s.sig = append(s.sig, "agg:"+fg.Name+map[bool]string{false: "", true: "(w)"}[j.Window != nil])
				} else {
					tdf, n, _ := s.collectFeature(fg)
					j.CollectN, j.OrderBy, j.Ascending, j.Features = intp(n), strp(fg.EventTime), s.pct(50), []spec.TDFeature{tdf}
					if !opt.Batch {
						expected++
					}
					s.sig = append(s.sig, "collect:"+fg.Name+":"+strconv.Itoa(n))
				}
				hasAggOrCollect = true
				aggJoins = append(aggJoins, next)
				joins = append(joins, j)
				next++
			default:
				fs := s.sampleFeatures(root, 1, 3)
				if len(fs) == 0 {
					continue
				}
				var on [][2]string
				for _, k := range rootKeys {
					on = append(on, [2]string{k, k})
				}
				joins = append(joins, spec.Join{Index: next, Parent: 0, FG: root.ID, Type: spec.JoinLeft, Prefix: strp(fmt.Sprintf("p%d_", next)), On: on,
					Features: s.tdFeatures(fs, flags)})
				s.sig = append(s.sig, "plain:"+root.Name)
				next++
			}
		}
	}

	if topology != 0 {
		// Snowflake subtree: a self-join over the entity group as the root,
		// then the FK chain (regions -> countries), depth 1-2.
		var on [][2]string
		for _, k := range rootKeys {
			on = append(on, [2]string{k, k})
		}
		rootJoin := spec.Join{Index: next, Parent: 0, FG: root.ID, Type: spec.JoinInner, Prefix: strp("p_"), On: on,
			Features: s.tdFeatures(s.sampleFeatures(root, 1, 2), flags)}
		if len(rootJoin.Features) == 0 {
			rootJoin.Features = []spec.TDFeature{{Name: root.Features[len(root.Features)-1].Name}}
		}
		rootIdx := next
		next++
		joins = append(joins, rootJoin)
		jt := s.choose(60, 30, 7, 3) // LEFT, INNER, mixed, other
		hopType := []spec.JoinType{spec.JoinLeft, spec.JoinInner}[jt%2]
		depth := 1 + s.rng.IntN(2)
		parentName, parentIdx := root.Name, rootIdx
		var nested []spec.Join
		for d := 0; d < depth; d++ {
			child, cond, ok := fkChild(parentName)
			if !ok {
				break
			}
			cfg := s.g.fgs[child]
			fgSet[cfg.ID] = cfg
			j := spec.Join{Index: next, Parent: parentIdx, FG: cfg.ID, Type: hopType, Prefix: strp(child[:1] + "_"), On: [][2]string{cond},
				Features: s.tdFeatures(s.sampleFeatures(cfg, 1, 2), flags)}
			if jt == 2 && d == depth-1 && depth > 1 {
				j.Type = []spec.JoinType{spec.JoinLeft, spec.JoinInner}[(jt+1)%2] // mixed types within the subtree
			}
			if jt == 3 && d == 0 {
				j.Type = []spec.JoinType{spec.JoinRight, spec.JoinFull}[s.rng.IntN(2)]
			}
			violation := s.choose(85, 10, 3, 2)
			if gate != "" {
				violation = 0 // one deliberate violation per case
			}
			switch violation {
			case 1: // no / partial key columns
				j.On = nil
				setGate(spec.CodeJoinOnPartialPrimaryKey, "nested join without its child primary key")
			case 2: // right column is not the child primary key
				j.On = [][2]string{{cond[0], valueFeatures(cfg)[0].Name}}
				setGate(spec.CodeForeignKeyNotPrimaryKey, "nested join on a non-key child column")
			case 3: // unknown join feature
				j.On = [][2]string{{cond[0], "no_such_column"}}
				setGate(spec.CodeFeatureDoesNotExist, "nested join on a missing child column")
			}
			nested = append(nested, j)
			joins = append(joins, j)
			parentName, parentIdx = child, next
			next++
		}
		uniform := jt == 0 || jt == 1 || (jt == 2 && depth == 1)
		if jt == 2 && depth == 1 {
			uniform = true // a single hop cannot be mixed
		}
		allLeft := uniform && hopType == spec.JoinLeft
		c.LeftSubtree = allLeft
		s.sig = append(s.sig, fmt.Sprintf("snow=%s:%d", []string{"LEFT", "INNER", "MIXED", "OTHER"}[jt], len(nested)))
		if gate == "" {
			switch {
			case opt.Batch && len(rootKeys) != 1, !uniform:
			case hopType == spec.JoinInner:
				total := 0
				for _, j := range nested {
					total += len(selectedNames(opt, j))
				}
				if total > 0 {
					expected++
				}
			default:
				for _, j := range nested {
					if len(selectedNames(opt, j)) > 0 {
						expected++
					}
				}
			}
		}
		if hasAggOrCollect && gate == "" && s.pct(50) {
			// filters on a snowflake view with aggregate/collect joins are gated
			f, ok := s.filterLeaf(s.g.fgs["transactions"], nil)
			if ok {
				v.Filters = []spec.Filter{f}
				setGate(spec.CodeCollectUnsupportedOnlineFilt, "filters on a snowflake view with aggregate/collect joins")
				s.sig = append(s.sig, "filters=snowflake")
			}
		}
	} else {
		// Star filters (applied to aggregate/collect joins only).
		nf := []int{0, 1, 2, 3}[s.choose(50, 30, 12, 5)]
		if s.pct(3) && hasAggOrCollect {
			v.Filters = []spec.Filter{{FG: 0, Logic: spec.LogicOr}}
			setGate(spec.CodeCollectUnsupportedOnlineFilt, "OR filter on a collect/aggregate view")
			s.sig = append(s.sig, "filters=OR")
		} else if s.pct(5) && len(aggJoins) > 0 {
			// a filter on a boolean feature is not renderable online
			idx := aggJoins[s.rng.IntN(len(aggJoins))]
			for _, cand := range joins {
				if cand.Index != idx {
					continue
				}
				for _, f := range fgSet[cand.FG].Features {
					if spec.BaseType(f.Type) == "boolean" {
						cond := spec.CondEquals
						v.Filters = []spec.Filter{{FG: cand.FG, Logic: spec.LogicSingle, Feature: f.Name, Condition: &cond, Value: strp("1")}}
						setGate(spec.CodeCollectUnsupportedOnlineFilt, "filter on a boolean feature")
						s.sig = append(s.sig, "filters=boolean")
						break
					}
				}
			}
		} else if nf > 0 && len(aggJoins) > 0 {
			for k := 0; k < nf; k++ {
				idx := aggJoins[s.rng.IntN(len(aggJoins))]
				var j spec.Join
				for _, cand := range joins {
					if cand.Index == idx {
						j = cand
					}
				}
				fg := fgSet[j.FG]
				var pin *int
				if s.pct(50) {
					pin = intp(idx)
				}
				if s.pct(5) && gate == "" {
					// feature-vs-feature comparison: not renderable online
					f := valueFeatures(fg)[0]
					cond := spec.CondEquals
					v.Filters = append(v.Filters, spec.Filter{FG: fg.ID, JoinIndex: pin, Logic: spec.LogicSingle, Feature: f.Name,
						Condition: &cond, Value: strp(f.Name), ValueFeatureGroupID: intp(fg.ID)})
					setGate(spec.CodeCollectUnsupportedOnlineFilt, "feature-vs-feature filter")
					continue
				}
				if f, ok := s.filterLeaf(fg, pin); ok {
					v.Filters = append(v.Filters, f)
				}
			}
			if len(v.Filters) > 0 {
				s.sig = append(s.sig, "filters="+strconv.Itoa(len(v.Filters)))
			}
		}
	}
	if gate == "" && s.pct(2) {
		// a feature group that is not online-enabled
		off := false
		fg := fgSet[joins[len(joins)-1].FG]
		fg.Online = &off
		fgSet[fg.ID] = fg
		setGate(spec.CodeFeaturestoreOnlineNotEnabled, "feature group not online-enabled")
	}
	for _, fg := range fgSet {
		v.FGs = append(v.FGs, fg)
	}
	sort.Slice(v.FGs, func(a, b int) bool { return v.FGs[a].ID < v.FGs[b].ID })
	v.Joins = joins
	c.View = v
	n := 1
	if opt.Batch {
		n = mode
	}
	c.KeyNames, c.Keys, c.KeyClass = s.keysFor(root, n)
	s.sig = append(s.sig, "keys="+c.KeyClass)
	c.Expect.Gate, c.Expect.Reason = gate, reason
	c.Expect.Templates = expected
	if gate != "" {
		c.Expect.Templates = 0
		c.Expect.KnownError = ""
		s.sig = append(s.sig, "gate="+gate)
	}
	c.Signature = strings.Join(s.sig, "|")
}

// ---- definition cases -------------------------------------------------------------

func (s *sampler) definition() {
	c := &s.c
	c.Kind = "definition"
	names := []string{"transactions", "sessions", "balance_hist", "transactions_str", "balances"}
	fg := s.g.fgs[names[s.choose(35, 20, 15, 15, 15)]]
	v := &spec.View{Name: c.ID, FGs: []spec.FeatureGroup{fg}, Joins: []spec.Join{}, MaxCollectN: 50, MaxCollectCells: 100}
	d := &spec.Definition{FG: fg.ID}
	s.sig = append(s.sig, "definition", "fg="+fg.Name)
	gate, reason := "", ""
	if s.pct(50) {
		// collect definition
		tdf, n, fields := s.collectFeature(fg)
		_ = tdf
		d.CollectN, d.SourceFeatures, d.Ascending = intp(n), fields, s.pct(50)
		if fg.EventTime == "" {
			gate, reason = spec.CodeCollectNoOrderBy, "collect on a feature group without event time"
		} else if s.pct(40) {
			switch s.choose(1, 1, 1, 1, 1, 1, 1) {
			case 0:
				d.CollectN, gate, reason = intp(0), spec.CodeCollectInvalidN, "collect N = 0"
			case 1:
				d.CollectN, gate, reason = intp(51), spec.CodeCollectNTooLarge, "collect N above the maximum"
			case 2:
				d.CollectN, gate, reason = intp(50), spec.CodeCollectTooWide, "collect 50 x 3 fields above the cell limit"
				d.SourceFeatures = nil
				for _, f := range valueFeatures(fg) {
					if len(d.SourceFeatures) < 3 {
						d.SourceFeatures = append(d.SourceFeatures, f.Name)
					}
				}
				if len(d.SourceFeatures) < 3 {
					d.CollectN, gate, reason = intp(51), spec.CodeCollectNTooLarge, "collect N above the maximum"
				}
			case 3:
				d.OrderBy, gate, reason = strp("no_such_column"), spec.CodeCollectNoServableIndex, "collect order column is not a feature"
			case 4:
				d.SourceFeatures, gate, reason = pkNames(fg), spec.CodeCollectInvalidN, "collect selects only key columns"
			case 5:
				d.Aggregate, gate, reason = spec.AggSpec{{Key: "*", Fns: []string{"count"}}}, spec.CodeAggregateWithCollect, "collect and aggregate on one feature group"
			case 6:
				d.OrderBy, gate, reason = strp(valueFeatures(fg)[0].Name), spec.CodeCollectNoServableIndex, "collect order column is not the last primary-key column"
			}
		}
		s.sig = append(s.sig, "collect")
	} else {
		agg, _ := s.validAggregate(fg)
		d.Aggregate = agg
		if fg.EventTime == "" {
			gate, reason = spec.CodeAggregateInvalid, "aggregate on a feature group without event time"
		} else if s.pct(40) {
			vals := valueFeatures(fg)
			var strs, nonInt []string
			for _, f := range vals {
				if isString(f.Type) {
					strs = append(strs, f.Name)
				}
				if !spec.IsIntegerType(f.Type) {
					nonInt = append(nonInt, f.Name)
				}
			}
			switch s.choose(1, 1, 1, 1, 1, 1, 1) {
			case 0:
				if len(strs) > 0 {
					d.Aggregate = spec.AggSpec{{Key: strs[0], Fns: []string{"sum"}}}
					gate, reason = spec.CodeAggregateInvalid, "SUM over a string feature"
				}
			case 1:
				if len(strs) > 0 {
					d.Aggregate = spec.AggSpec{{Key: strs[0], Fns: []string{"avg"}}}
					gate, reason = spec.CodeAggregateInvalid, "AVG over a string feature"
				}
			case 2:
				if len(nonInt) > 0 && len(vals) > 1 {
					d.Aggregate = spec.AggSpec{{Key: nonInt[0] + "," + vals[0].Name, Fns: []string{"greatest"}}}
					gate, reason = spec.CodeAggregateInvalid, "GREATEST over a non-integer feature"
				}
			case 3:
				d.Aggregate = spec.AggSpec{{Key: "*", Fns: []string{"sum"}}}
				gate, reason = spec.CodeAggregateInvalid, "SUM on the * key"
			case 4:
				d.Aggregate = spec.AggSpec{{Key: vals[0].Name, Fns: []string{"median"}}}
				gate, reason = spec.CodeAggregateInvalid, "unknown aggregation function"
			case 5:
				d.Aggregate = spec.AggSpec{{Key: "*", Fns: []string{"count"}}, {Key: "*", Fns: []string{"count"}}}
				gate, reason = spec.CodeAggregateInvalid, "duplicate aggregate output"
			case 6:
				d.Window, gate, reason = i64p(0), spec.CodeAggregateInvalid, "non-positive window"
			}
		} else if s.pct(50) {
			d.Window = i64p([]int64{3600, 86400, 7 * 86400, 30 * 86400}[s.rng.IntN(4)])
		}
		for _, e := range d.Aggregate {
			if e.Key != "*" {
				for _, part := range strings.Split(e.Key, ",") {
					d.SourceFeatures = append(d.SourceFeatures, part)
				}
			}
		}
		s.sig = append(s.sig, "aggregate")
	}
	v.Definition = d
	c.View = v
	c.Expect = Expect{Gate: gate, Reason: reason, Templates: 0}
	if gate != "" {
		s.sig = append(s.sig, "gate="+gate)
	}
	c.Signature = strings.Join(s.sig, "|")
}
