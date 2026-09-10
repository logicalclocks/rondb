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

// Package vector is the L2 oracle (framework_design.md §8): the client-side
// folds Hopsworks applies to a serving read, modelled for both paths — the
// RonSQL template set and the production MySQL statement — and a
// policy-aware comparison of the assembled per-entity feature vectors.
//
// Feature names are the feature-view names: the join prefix applied to the
// RonSQL outputs after the fetch, and the MySQL aliases as emitted.  A
// feature absent from a Vector was not served (an INNER miss drops the row,
// a LEFT chain with no row contributes nothing); MySQL's LEFT JOIN yields a
// NULL cell instead, which Policy.MissingEqualsNull accepts and counts.
package vector

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

// Kind is the serving family of a DTO.
type Kind int

const (
	// PointRead has no RonSQL template (served by the pk-read path).
	PointRead Kind = iota
	Aggregate
	Collect
	Snowflake
)

func (k Kind) String() string {
	switch k {
	case Aggregate:
		return "aggregate"
	case Collect:
		return "collect"
	case Snowflake:
		return "snowflake"
	}
	return "point-read"
}

// Cell is one feature value.  Missing marks, inside a collect element, a
// field the row lacked; a missing feature is simply absent from its Vector.
type Cell struct {
	Null    bool
	Missing bool
	Text    string
	// Array is non-nil (possibly empty) for a served collect feature; its
	// elements are in client order.
	Array []Element
}

// Element is one collect array element; Values align with Plan.Fields.
type Element struct {
	Values []Cell
}

// Vector maps feature names to cells.
type Vector map[string]Cell

// Vectors maps Key.Text() to the entity's vector.
type Vectors map[string]Vector

// Key is one bound entity key: one literal per DTO parameter, in DTO order.
type Key []string

// Text is the map key of a Key.
func (k Key) Text() string { return strings.Join(k, "\x1f") }

// RankColumn is the MySQL collect helper column the fold drops.
const RankColumn = "hopsworks_collect_rank"

// Plan is the fold recipe derived from one DTO.
type Plan struct {
	Kind   Kind
	Prefix string   // feature-view prefix of the join
	Params []string // DTO parameter names (unprefixed key columns), in order
	Batch  bool

	// Aggregate: the prefixed output names, and those defaulting to 0 for
	// an entity the batch GROUP BY did not return (the rest default to NULL).
	Outputs      []string
	CountOutputs map[string]bool

	// Snowflake: required projection aliases for each template, in DTO order.
	TemplateOutputs [][]string

	// Collect: the prefixed array feature, the unprefixed order column, the
	// client sort direction and the persisted struct fields in schema order.
	CollectFeature string
	OrderBy        string
	Ascending      bool
	Fields         []string
}

// PlanFor derives the fold recipe of a DTO.  structFields are the collect
// feature's struct field names (from the feature view's array<struct<...>>
// type); they are ignored for other kinds.
func PlanFor(dto emit.Statement, batch bool, structFields []string) Plan {
	p := Plan{Batch: batch}
	if dto.Prefix != nil {
		p.Prefix = *dto.Prefix
	}
	for _, prm := range dto.PreparedStatementParameters {
		p.Params = append(p.Params, prm.Name)
	}
	switch {
	case dto.CollectN != nil:
		p.Kind = Collect
		if dto.CollectFeatureName != nil {
			p.CollectFeature = p.Prefix + *dto.CollectFeatureName
		}
		if dto.CollectOrderBy != nil {
			p.OrderBy = *dto.CollectOrderBy
		}
		p.Ascending = dto.CollectAscending != nil && *dto.CollectAscending
		p.Fields = append([]string(nil), structFields...)
	case dto.AggregateFeatureNames != nil:
		p.Kind = Aggregate
		p.Outputs = append([]string(nil), dto.AggregateFeatureNames...)
		p.CountOutputs = map[string]bool{}
		for _, o := range p.Outputs {
			// Output naming: "count" for COUNT(*), "<source>_<fn>" otherwise;
			// the function is always the last suffix.
			if o == p.Prefix+"count" || strings.HasSuffix(o, "_count") {
				p.CountOutputs[o] = true
			}
		}
	case len(dto.SnowflakeTemplates) > 0:
		p.Kind = Snowflake
		// The served features are the projection aliases of the templates
		// (already prefixed); a chain that returns no row still counts.
		seen := map[string]bool{}
		for _, t := range dto.SnowflakeTemplates {
			var outputs []string
			for _, a := range templateAliases(t) {
				if a == "hw_cnt" || (batch && len(p.Params) > 0 && a == p.Params[0]) {
					continue
				}
				outputs = append(outputs, a)
				if !seen[a] {
					seen[a] = true
					p.Outputs = append(p.Outputs, a)
				}
			}
			p.TemplateOutputs = append(p.TemplateOutputs, outputs)
		}
	default:
		p.Kind = PointRead
	}
	return p
}

var aliasRE = regexp.MustCompile("AS `([^`]+)`")

// templateAliases lists the `AS \`alias\“ names of a template's final
// SELECT list (the part after the CTE), in order.
func templateAliases(template string) []string {
	body := template
	if i := strings.LastIndex(template, ") SELECT "); i >= 0 {
		body = template[i:]
	}
	if i := strings.Index(body, " FROM "); i >= 0 {
		body = body[:i]
	}
	var out []string
	for _, m := range aliasRE.FindAllStringSubmatch(body, -1) {
		out = append(out, m[1])
	}
	return out
}

// Types maps a MySQL result's column names to their type names.
func Types(res *exec.Result) map[string]string {
	out := map[string]string{}
	if res == nil {
		return out
	}
	for i, col := range res.Columns {
		if i < len(res.Types) {
			out[col] = res.Types[i]
		}
	}
	return out
}

func cellOf(c exec.Cell) Cell { return Cell{Null: c.Null, Text: c.Text} }

func indexOf(cols []string, name string) int {
	for i, c := range cols {
		if c == name {
			return i
		}
	}
	return -1
}

// FoldRonsql folds the RonSQL template results of one DTO (one Result per
// template, in DTO order) for the requested keys: aggregate outputs get the
// prefix applied and batch entities absent from the GROUP BY output get the
// empty-set defaults; collect rows are sorted and folded into the array
// feature; snowflake templates are overlaid by output alias, batch rows
// keyed by the appended root key column.
func FoldRonsql(p Plan, results []*exec.Result, keys []Key) (Vectors, error) {
	out := Vectors{}
	switch p.Kind {
	case Aggregate:
		if len(results) > 0 {
			if err := requireColumns(results[0], p.Outputs, p.Prefix); err != nil {
				return nil, err
			}
			if err := foldRows(p, results[0], keys, p.Params, p.Prefix, out); err != nil {
				return nil, err
			}
		}
		if p.Batch {
			aggregateDefaults(p, out, keys)
		}
	case Collect:
		if len(results) > 0 && len(keys) > 0 {
			c, err := foldCollect(p, results[0], "")
			if err != nil {
				return nil, err
			}
			out[keys[0].Text()] = Vector{p.CollectFeature: c}
		}
	case Snowflake:
		if len(results) != len(p.TemplateOutputs) {
			return nil, fmt.Errorf("snowflake result count %d, want %d", len(results), len(p.TemplateOutputs))
		}
		for i, res := range results {
			if err := requireColumns(res, p.TemplateOutputs[i], ""); err != nil {
				return nil, fmt.Errorf("snowflake template %d: %w", i, err)
			}
			if err := foldRows(p, res, keys, p.Params, "", out); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// FoldMysql folds the production MySQL statement's result: aliases are
// already prefixed, batch rows carry the prefixed key columns, collect rows
// carry the rank helper, LEFT JOIN misses are NULL cells.
func FoldMysql(p Plan, res *exec.Result, keys []Key) (Vectors, error) {
	out := Vectors{}
	if err := requireColumns(res, p.Outputs, ""); err != nil {
		return nil, err
	}
	keyCols := make([]string, len(p.Params))
	for i, prm := range p.Params {
		keyCols[i] = p.Prefix + prm
	}
	switch p.Kind {
	case Aggregate:
		if err := foldRows(p, res, keys, keyCols, "", out); err != nil {
			return nil, err
		}
		if p.Batch {
			aggregateDefaults(p, out, keys)
		}
	case Collect:
		if len(keys) > 0 {
			c, err := foldCollect(p, res, p.Prefix)
			if err != nil {
				return nil, err
			}
			out[keys[0].Text()] = Vector{p.CollectFeature: c}
		}
	default:
		if err := foldRows(p, res, keys, keyCols, "", out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// requireColumns distinguishes an empty result (JSON has no header) from a
// returned row missing a declared feature. outPrefix is applied after a
// RonSQL aggregate fetch, so remove it when checking the raw column names.
func requireColumns(res *exec.Result, outputs []string, outPrefix string) error {
	if res == nil {
		return fmt.Errorf("missing result")
	}
	if len(res.Rows) == 0 {
		return nil
	}
	for _, output := range outputs {
		col := strings.TrimPrefix(output, outPrefix)
		if indexOf(res.Columns, col) < 0 {
			return fmt.Errorf("result lacks the output column %q (columns %v)", col, res.Columns)
		}
	}
	return nil
}

// foldRows overlays every row onto the vector of its entity: in a batch the
// key is read from keyCols, otherwise the rows belong to keys[0].  Output
// names get outPrefix applied; key columns are not features. Each result
// must contain at most one row per requested entity. Duplicate detection is
// local to this call, so separate snowflake templates can still overlay.
func foldRows(p Plan, res *exec.Result, keys []Key, keyCols []string, outPrefix string, out Vectors) error {
	if res == nil {
		return nil
	}
	if !p.Batch && len(keys) != 1 {
		return fmt.Errorf("single-entity result requires exactly one requested key, got %d", len(keys))
	}
	if p.Batch && len(keyCols) == 0 {
		return fmt.Errorf("batch result has no declared key columns")
	}
	requested := map[string]bool{}
	for _, key := range keys {
		requested[key.Text()] = true
	}
	seen := map[string]bool{}
	var keyIdx []int
	isKey := map[int]bool{}
	if p.Batch {
		for _, kc := range keyCols {
			i := indexOf(res.Columns, kc)
			if i < 0 {
				if len(res.Rows) == 0 {
					return nil
				}
				return fmt.Errorf("batch result lacks the key column %q (columns %v)", kc, res.Columns)
			}
			keyIdx = append(keyIdx, i)
			isKey[i] = true
		}
	}
	for rowIndex, row := range res.Rows {
		if len(row) != len(res.Columns) {
			return fmt.Errorf("row %d has %d cells for %d columns", rowIndex, len(row), len(res.Columns))
		}
		var kt string
		if p.Batch {
			k := make(Key, len(keyIdx))
			for i, idx := range keyIdx {
				if row[idx].Null {
					return fmt.Errorf("row %d has NULL key column %q", rowIndex, keyCols[i])
				}
				k[i] = row[idx].Text
			}
			kt = k.Text()
		} else {
			kt = keys[0].Text()
		}
		if !requested[kt] {
			return fmt.Errorf("row %d has unrequested entity key %q", rowIndex, kt)
		}
		if seen[kt] {
			return fmt.Errorf("result contains duplicate entity key %q", kt)
		}
		seen[kt] = true
		v := out[kt]
		if v == nil {
			v = Vector{}
			out[kt] = v
		}
		for i, col := range res.Columns {
			if isKey[i] {
				continue
			}
			v[outPrefix+col] = cellOf(row[i])
		}
	}
	return nil
}

// aggregateDefaults synthesizes the SQL empty-set defaults (COUNT 0, other
// functions NULL) for batch entities without a GROUP BY row.
func aggregateDefaults(p Plan, out Vectors, keys []Key) {
	for _, k := range keys {
		if _, ok := out[k.Text()]; ok {
			continue
		}
		v := Vector{}
		for _, o := range p.Outputs {
			if p.CountOutputs[o] {
				v[o] = Cell{Text: "0"}
			} else {
				v[o] = Cell{Null: true}
			}
		}
		out[k.Text()] = v
	}
}

// foldCollect sorts the rows by the order column (newest first, oldest first
// when ascending) and folds them into one array cell whose elements carry
// exactly the struct fields; serving keys and the rank helper are dropped.
func foldCollect(p Plan, res *exec.Result, colPrefix string) (Cell, error) {
	orderIdx := indexOf(res.Columns, colPrefix+p.OrderBy)
	if orderIdx < 0 && len(res.Rows) > 0 {
		return Cell{}, fmt.Errorf("collect result lacks the order column %q (columns %v)", colPrefix+p.OrderBy, res.Columns)
	}
	fieldIdx := make([]int, len(p.Fields))
	for i, f := range p.Fields {
		fieldIdx[i] = indexOf(res.Columns, colPrefix+f)
		if fieldIdx[i] < 0 && len(res.Rows) > 0 {
			return Cell{}, fmt.Errorf("collect result lacks the field %q (columns %v)", colPrefix+f, res.Columns)
		}
	}
	rows := append([][]exec.Cell(nil), res.Rows...)
	if orderIdx >= 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			a, b := rows[i][orderIdx], rows[j][orderIdx]
			if p.Ascending {
				return lessCell(a, b)
			}
			return lessCell(b, a)
		})
	}
	elems := make([]Element, 0, len(rows))
	for _, row := range rows {
		e := Element{Values: make([]Cell, len(p.Fields))}
		for i, idx := range fieldIdx {
			if idx < 0 || idx >= len(row) {
				e.Values[i] = Cell{Missing: true}
			} else {
				e.Values[i] = cellOf(row[idx])
			}
		}
		elems = append(elems, e)
	}
	return Cell{Array: elems}, nil
}

// lessCell orders collect rows by their order column: NULL first, numbers
// numerically, everything else (timestamps, strings) as text.
func lessCell(a, b exec.Cell) bool {
	if a.Null || b.Null {
		return a.Null && !b.Null
	}
	fa, ea := strconv.ParseFloat(a.Text, 64)
	fb, eb := strconv.ParseFloat(b.Text, 64)
	if ea == nil && eb == nil {
		return fa < fb
	}
	return a.Text < b.Text
}

// Policy controls the comparison.
type Policy struct {
	// MissingEqualsNull is enabled only for LEFT snowflake comparisons.
	// It accepts a chain with no row against MySQL's NULL cells, counted as
	// LeftMiss. Folds reject returned rows that lack declared columns.
	MissingEqualsNull bool
	Tolerance         float64 // DOUBLE/FLOAT relative tolerance (canon default when 0)
}

// Mismatch is one differing cell.
type Mismatch struct {
	Key, Feature, Ref, Got string
}

// Report is the outcome of Compare.
type Report struct {
	Compared   int // cells compared
	LeftMiss   int // missing-vs-NULL cells accepted under the policy
	Mismatches []Mismatch
	NotServed  []string // ref features outside the compared set (served by no template)
}

// Compare compares got (RonSQL path) against ref (MySQL path) for the keys.
// The compared feature set is `only` when given, else every feature the
// RonSQL path served for any key plus the plan's declared outputs; ref
// features outside it are reported as NotServed.  types maps ref column
// names (prefixed feature names; collect fields as prefix+field) to MySQL
// type names for the canonical comparison.
func Compare(p Plan, ref, got Vectors, keys []Key, types map[string]string, pol Policy, only map[string]bool) Report {
	rep := Report{}
	served := only
	if served == nil {
		served = map[string]bool{}
		for _, v := range got {
			for f := range v {
				served[f] = true
			}
		}
		for _, o := range p.Outputs {
			served[o] = true
		}
		if p.Kind == Collect && p.CollectFeature != "" {
			served[p.CollectFeature] = true
		}
	}
	feats := sortedKeys(served)
	notServed := map[string]bool{}
	for _, k := range keys {
		kt := k.Text()
		rv, gv := ref[kt], got[kt]
		for f := range rv {
			if !served[f] {
				notServed[f] = true
			}
		}
		for _, f := range feats {
			rc, rok := rv[f]
			gc, gok := gv[f]
			rep.Compared++
			eq, left := cellsMatch(p, f, rc, rok, gc, gok, types, pol)
			if !eq {
				rep.Mismatches = append(rep.Mismatches, Mismatch{Key: strings.Join(k, ","), Feature: f, Ref: Render(rc, rok), Got: Render(gc, gok)})
			} else if left {
				rep.LeftMiss++
			}
		}
	}
	rep.NotServed = sortedKeys(notServed)
	return rep
}

// cellsMatch compares one feature (or one collect field, recursively).
func cellsMatch(p Plan, f string, a Cell, aok bool, b Cell, bok bool, types map[string]string, pol Policy) (equal, leftMiss bool) {
	amiss, bmiss := !aok || a.Missing, !bok || b.Missing
	switch {
	case amiss && bmiss:
		return true, false
	case amiss || bmiss:
		if p.Kind == Snowflake && pol.MissingEqualsNull && !amiss && bmiss && a.Null && a.Array == nil {
			return true, true
		}
		return false, false
	}
	if a.Array != nil || b.Array != nil {
		if a.Array == nil || b.Array == nil || len(a.Array) != len(b.Array) {
			return false, false
		}
		for i := range a.Array {
			for j, field := range p.Fields {
				var av, bv Cell
				var aok2, bok2 bool
				if j < len(a.Array[i].Values) {
					av, aok2 = a.Array[i].Values[j], true
				}
				if j < len(b.Array[i].Values) {
					bv, bok2 = b.Array[i].Values[j], true
				}
				eq, lm := cellsMatch(p, p.Prefix+field, av, aok2, bv, bok2, types, pol)
				if !eq {
					return false, false
				}
				leftMiss = leftMiss || lm
			}
		}
		return true, leftMiss
	}
	return canon.CellsEqual(exec.Cell{Null: a.Null, Text: a.Text}, exec.Cell{Null: b.Null, Text: b.Text}, types[f], pol.Tolerance), false
}

// Render prints a cell for a report line.
func Render(c Cell, ok bool) string {
	switch {
	case !ok || c.Missing:
		return "<missing>"
	case c.Array != nil:
		parts := make([]string, 0, len(c.Array))
		for _, e := range c.Array {
			vals := make([]string, len(e.Values))
			for i, v := range e.Values {
				vals[i] = Render(v, true)
			}
			parts = append(parts, "("+strings.Join(vals, ",")+")")
		}
		s := "[" + strings.Join(parts, "; ") + "]"
		if len(s) > 200 {
			s = s[:200] + "…"
		}
		return s
	case c.Null:
		return "NULL"
	}
	return c.Text
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
