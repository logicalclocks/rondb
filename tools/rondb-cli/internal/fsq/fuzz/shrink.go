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

// Shrinking (random_generator.md §6): delta debugging over the case's own
// structure.  Each candidate is re-classified by the caller's oracle and
// kept only when the failure class is preserved.

import (
	"encoding/json"
	"regexp"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

// Clone deep-copies a case (the view through JSON, keys by value).
func Clone(c Case) Case {
	out := c
	if c.View != nil {
		raw, _ := json.Marshal(c.View)
		v := &spec.View{}
		_ = json.Unmarshal(raw, v)
		out.View = v
	}
	out.Keys = make([]vector.Key, len(c.Keys))
	for i, k := range c.Keys {
		out.Keys[i] = append(vector.Key(nil), k...)
	}
	out.KeyNames = append([]string(nil), c.KeyNames...)
	return out
}

// Candidates lists the one-step reductions of a case, leaves first: drop
// a childless join, a filter, a selected feature, an aggregate entry or
// function, the window, halve the batch, reduce collect N, clear helper
// options.  Expectations are relaxed (Templates -1) since the emitter
// outcome of a reduced case is re-derived by running it.
func Candidates(c Case) []Case {
	var out []Case
	add := func(mut func(*Case) bool) {
		cand := Clone(c)
		cand.Expect.Templates = -1
		if mut(&cand) {
			out = append(out, cand)
		}
	}
	v := c.View
	if v == nil {
		return nil
	}
	parents := map[int]bool{}
	for _, j := range v.Joins {
		if j.Index != 0 {
			parents[j.Parent] = true
		}
	}
	for i := range v.Joins {
		i := i
		if v.Joins[i].Index != 0 && !parents[v.Joins[i].Index] {
			add(func(k *Case) bool {
				k.View.Joins = append(k.View.Joins[:i:i], k.View.Joins[i+1:]...)
				return true
			})
		}
	}
	for i := range v.Filters {
		i := i
		add(func(k *Case) bool {
			k.View.Filters = append(k.View.Filters[:i:i], k.View.Filters[i+1:]...)
			return true
		})
	}
	for ji := range v.Joins {
		j := v.Joins[ji]
		minKeep := 1
		if j.Index == 0 || j.Aggregate != nil || j.CollectN != nil {
			minKeep = 0
		}
		if j.Aggregate != nil || j.CollectN != nil {
			continue
		}
		for fi := range j.Features {
			if len(j.Features) <= minKeep {
				break
			}
			ji, fi := ji, fi
			add(func(k *Case) bool {
				jj := &k.View.Joins[ji]
				jj.Features = append(jj.Features[:fi:fi], jj.Features[fi+1:]...)
				return true
			})
		}
		for ei := range j.Aggregate {
			ji, ei := ji, ei
			if len(j.Aggregate) > 1 {
				add(func(k *Case) bool {
					jj := &k.View.Joins[ji]
					jj.Aggregate = append(jj.Aggregate[:ei:ei], jj.Aggregate[ei+1:]...)
					jj.Features = aggOutputs(jj.Aggregate)
					return true
				})
			}
			for fi := range j.Aggregate[ei].Fns {
				if len(j.Aggregate[ei].Fns) <= 1 {
					break
				}
				fi := fi
				add(func(k *Case) bool {
					e := &k.View.Joins[ji].Aggregate[ei]
					e.Fns = append(e.Fns[:fi:fi], e.Fns[fi+1:]...)
					k.View.Joins[ji].Features = aggOutputs(k.View.Joins[ji].Aggregate)
					return true
				})
			}
		}
		if j.Window != nil {
			ji := ji
			add(func(k *Case) bool { k.View.Joins[ji].Window = nil; return true })
		}
		if j.CollectN != nil && *j.CollectN > 1 {
			ji := ji
			add(func(k *Case) bool {
				n := 1
				if *k.View.Joins[ji].CollectN > 5 {
					n = 5
				}
				k.View.Joins[ji].CollectN = &n
				return true
			})
		}
	}
	if v.Definition != nil {
		d := v.Definition
		for ei := range d.Aggregate {
			ei := ei
			if len(d.Aggregate) > 1 {
				add(func(k *Case) bool {
					dd := k.View.Definition
					dd.Aggregate = append(dd.Aggregate[:ei:ei], dd.Aggregate[ei+1:]...)
					return true
				})
			}
		}
		if d.Window != nil {
			add(func(k *Case) bool { k.View.Definition.Window = nil; return true })
		}
	}
	if c.Batch > 1 {
		add(func(k *Case) bool {
			n := len(k.Keys) / 2
			if n < 1 {
				n = 1
			}
			k.Keys = k.Keys[:n]
			k.Batch = n
			return true
		})
	}
	if c.Batch == 1 {
		add(func(k *Case) bool {
			k.Batch = 0
			k.View.Options.Batch = false
			return true
		})
	}
	if v.Options.InferenceHelpers || v.Options.Logging || v.Options.VectorWithHelpers {
		add(func(k *Case) bool {
			k.View.Options.InferenceHelpers, k.View.Options.Logging, k.View.Options.VectorWithHelpers = false, false, false
			return true
		})
	}
	return out
}

// Shrink reduces a failing case while oracle keeps reporting the same
// failure class as for the original; at most maxSteps candidates are
// tried.  It returns the smallest case found and the number of oracle
// calls made.
func Shrink(c Case, oracle func(Case) string, maxSteps int) (Case, int) {
	target := oracle(c)
	steps := 1
	cur := c
	for steps < maxSteps {
		progressed := false
		for _, cand := range Candidates(cur) {
			if steps >= maxSteps {
				break
			}
			steps++
			if oracle(cand) == target {
				cur = cand
				progressed = true
				break
			}
		}
		if !progressed {
			break
		}
	}
	cur.Expect.Templates = -1
	return cur, steps
}

var cteCollect = regexp.MustCompile(`(?s)^WITH t AS \((.*)\) SELECT .* FROM t;$`)

// DirectCollect rewrites the Hopsworks CTE collect template to its body
// (the S6b direct form) so the collect statement runs while F0 stands.
func DirectCollect(sql string) (string, bool) {
	m := cteCollect.FindStringSubmatch(sql)
	if m == nil {
		return sql, false
	}
	return m[1] + ";", true
}
