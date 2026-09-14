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

// Data-model expectations of the vector oracle: the vector a spec must
// serve for a key, computed from the data formulas (data_model.md §5)
// rather than from either engine, so that two identical folds are not the
// only evidence. Missing features stand for no MySQL row (INNER miss or
// unknown entity); LEFT-hop misses on an existing entity are explicit NULL
// cells, so the MySQL fold can be checked without a missing-equals-NULL policy.

import (
	"strconv"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

var txAggOutputs = []string{"count", "amount_count", "amount_sum", "amount_min", "amount_max", "fee_min", "fee_max", "amount_fee_greatest", "amount_fee_least"}
var histOutputs = []string{"count", "delta_sum", "delta_min", "delta_max"}

func prefixed(pfx string, names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = pfx + n
	}
	return out
}

// ExpectedFeatures lists the feature names the data-model oracle specifies
// for the spec; nil when the spec has no independent expectation.
func (s *Spec) ExpectedFeatures() []string {
	pfx := s.prefix()
	switch s.Family {
	case "txagg", "stragg":
		return prefixed(pfx, txAggOutputs)
	case "hist":
		return prefixed(pfx, histOutputs)
	case "collect":
		return []string{pfx + "transactions_collect"}
	case "snowflake":
		out := []string{"r_region_name", "r_population"}
		if s.Depth >= 2 {
			out = append(out, "c_country_name", "c_continent")
		}
		return out
	}
	return nil
}

func text(s string) vector.Cell { return vector.Cell{Text: s} }
func itoa(v int64) string       { return strconv.FormatInt(v, 10) }

// customerOfKey parses a customers_str key ("cust-00000042", any case).
func customerOfKey(key string) (int64, bool) {
	lower := strings.ToLower(key)
	if !strings.HasPrefix(lower, "cust-") {
		return 0, false
	}
	n, err := strconv.ParseInt(lower[5:], 10, 64)
	return n, err == nil
}

func (s *Spec) windowStart() (time.Time, bool) {
	if s.Window == nil {
		return time.Time{}, false
	}
	return s.b.cfg.Now.Add(-time.Duration(*s.Window) * time.Second), true
}

// txRows returns the transactions of customer c that the spec's window and
// filter admit, newest first.
func (s *Spec) txRows(c int64) []data.Tx {
	sc := s.b.cfg.Scale
	if c < 1 || c > sc.E {
		return nil
	}
	start, windowed := s.windowStart()
	var rows []data.Tx
	for i := 1; i <= data.NTx(c); i++ {
		tx := data.TxRow(c, i, sc)
		if windowed && tx.EventTime.Before(start) {
			continue
		}
		if s.Filter != nil && !s.Filter(tx) {
			continue
		}
		rows = append(rows, tx)
	}
	return rows
}

func txAggVector(pfx string, rows []data.Tx) vector.Vector {
	v := vector.Vector{}
	n := int64(len(rows))
	v[pfx+"count"], v[pfx+"amount_count"] = text(itoa(n)), text(itoa(n))
	if n == 0 {
		for _, o := range txAggOutputs[2:] {
			v[pfx+o] = vector.Cell{Null: true}
		}
		return v
	}
	var sum, amin, amax, fmin, fmax, gmax, lmax int64
	for i, tx := range rows {
		fee := int64(tx.Fee)
		g, l := tx.Amount, fee
		if fee > g {
			g, l = fee, tx.Amount
		}
		if i == 0 {
			amin, amax, fmin, fmax, gmax, lmax = tx.Amount, tx.Amount, fee, fee, g, l
		}
		sum += tx.Amount
		amin, amax = min64(amin, tx.Amount), max64(amax, tx.Amount)
		fmin, fmax = min64(fmin, fee), max64(fmax, fee)
		gmax, lmax = max64(gmax, g), max64(lmax, l)
	}
	v[pfx+"amount_sum"], v[pfx+"amount_min"], v[pfx+"amount_max"] = text(itoa(sum)), text(itoa(amin)), text(itoa(amax))
	v[pfx+"fee_min"], v[pfx+"fee_max"] = text(itoa(fmin)), text(itoa(fmax))
	v[pfx+"amount_fee_greatest"], v[pfx+"amount_fee_least"] = text(itoa(gmax)), text(itoa(lmax))
	return v
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Expected returns the data-model vector of the spec for a key, or false
// when the spec has no independent expectation.
func (s *Spec) Expected(key vector.Key) (vector.Vector, bool) {
	sc := s.b.cfg.Scale
	pfx := s.prefix()
	switch s.Family {
	case "txagg":
		c, _ := strconv.ParseInt(key[0], 10, 64)
		return txAggVector(pfx, s.txRows(c)), true
	case "stragg":
		// transactions_str_1 holds the first StrCustomers customers under
		// their string keys; the engines match keys case-insensitively.
		var c int64
		if n, ok := customerOfKey(key[0]); ok && n <= sc.StrCustomers {
			c = n
		}
		return txAggVector(pfx, s.txRows(c)), true
	case "hist":
		a, _ := strconv.ParseInt(key[0], 10, 64)
		j := -1
		for i, cur := range data.Currencies {
			if len(key) > 1 && cur == key[1] {
				j = i
			}
		}
		start, windowed := s.windowStart()
		var deltas []int64
		if a >= 1 && a <= sc.A && j >= 0 && j < data.NCurrencies(a) {
			for i := 1; i <= data.NBal(a); i++ {
				r := data.BalanceHistRow(a, j, i)
				if windowed && r.EventTime.Before(start) {
					continue
				}
				deltas = append(deltas, r.Delta)
			}
		}
		v := vector.Vector{pfx + "count": text(itoa(int64(len(deltas))))}
		if len(deltas) == 0 {
			for _, o := range histOutputs[1:] {
				v[pfx+o] = vector.Cell{Null: true}
			}
			return v, true
		}
		sum, dmin, dmax := int64(0), deltas[0], deltas[0]
		for _, d := range deltas {
			sum += d
			dmin, dmax = min64(dmin, d), max64(dmax, d)
		}
		v[pfx+"delta_sum"], v[pfx+"delta_min"], v[pfx+"delta_max"] = text(itoa(sum)), text(itoa(dmin)), text(itoa(dmax))
		return v, true
	case "collect":
		c, _ := strconv.ParseInt(key[0], 10, 64)
		join := s.View.Joins[0]
		n := 0
		if join.CollectN != nil {
			n = *join.CollectN
		}
		elems := []vector.Element{}
		if c >= 1 && c <= sc.E {
			m := data.NTx(c)
			if n < m {
				m = n
			}
			// i = 1 is the newest row: the statement keeps the newest N.
			for i := 1; i <= m; i++ {
				tx := data.TxRow(c, i, sc)
				elems = append(elems, vector.Element{Values: []vector.Cell{
					text(data.FormatTimestamp(tx.EventTime)), text(itoa(tx.Amount)), text(tx.Category)}})
			}
		}
		if join.Ascending {
			for i, j := 0, len(elems)-1; i < j; i, j = i+1, j-1 {
				elems[i], elems[j] = elems[j], elems[i]
			}
		}
		return vector.Vector{pfx + "transactions_collect": {Array: elems}}, true
	case "snowflake":
		c, _ := strconv.ParseInt(key[0], 10, 64)
		v := vector.Vector{}
		if c < 1 || c > sc.E {
			return v, true
		}
		if s.Left {
			for _, f := range s.ExpectedFeatures() {
				v[f] = vector.Cell{Null: true}
			}
		}
		r := data.RegionOf(c)
		if !r.Valid || r.V > data.RegionCount {
			return v, true // LEFT: NULL hop columns; INNER: no row
		}
		region := data.RegionRow(r.V)
		v["r_region_name"], v["r_population"] = text(region.Name), text(itoa(region.Population))
		if s.Depth < 2 {
			return v, true
		}
		k := region.CountryID
		if !k.Valid || k.V > data.CountryCount {
			if s.Left {
				return v, true // region features remain; country columns are NULL
			}
			return vector.Vector{}, true // INNER: the combined template drops the whole row
		}
		country := data.CountryRow(k.V)
		v["c_country_name"], v["c_continent"] = text(country.Name), text(country.Continent)
		return v, true
	}
	return nil, false
}
