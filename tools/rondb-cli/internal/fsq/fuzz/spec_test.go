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

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

func gen() *Gen { return New(Config{DB: "test", Scale: data.NewScale(0.01)}) }

func TestCaseDeterminism(t *testing.T) {
	g := gen()
	for i := 0; i < 20; i++ {
		a, b := g.Case(7, i), g.Case(7, i)
		ra, _ := json.Marshal(a)
		rb, _ := json.Marshal(b)
		if string(ra) != string(rb) {
			t.Fatalf("case %d differs between regenerations", i)
		}
		if a.ID != "spec-v1-7-"+itoa(i) {
			t.Errorf("id %s", a.ID)
		}
	}
	if x, y := g.Case(7, 3), g.Case(8, 3); x.Signature == y.Signature && x.View.Name == y.View.Name {
		t.Error("different seeds must not produce the same case")
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// TestExpectationsAgreeWithEmitter is the generator's self-check: every
// case's predicted gate and template count must be what the ported
// emitter (or the definition validator) actually produces.
func TestExpectationsAgreeWithEmitter(t *testing.T) {
	g := gen()
	seen := map[string]int{}
	for i := 0; i < 2000; i++ {
		c := g.Case(1, i)
		key := c.Kind
		if c.Expect.Gate != "" {
			key += ":" + c.Expect.Gate
		}
		seen[key]++
		if c.Kind == "definition" {
			_, err := spec.ValidateDefinition(c.View)
			var ge *spec.GateError
			switch {
			case c.Expect.Gate == "" && err != nil:
				t.Errorf("%s (%s): valid definition rejected: %v", c.ID, c.Expect.Reason, err)
			case c.Expect.Gate != "" && (err == nil || !errors.As(err, &ge) || ge.Code != c.Expect.Gate):
				t.Errorf("%s (%s): expected gate %s, got %v", c.ID, c.Expect.Reason, c.Expect.Gate, err)
			}
			continue
		}
		dtos, err := emit.Build(c.View)
		var ge *spec.GateError
		if c.Expect.Gate != "" {
			if err == nil || !errors.As(err, &ge) || ge.Code != c.Expect.Gate {
				t.Errorf("%s (%s) sig=%s: expected gate %s, got %v", c.ID, c.Expect.Reason, c.Signature, c.Expect.Gate, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s sig=%s: unexpected build error %v", c.ID, c.Signature, err)
			continue
		}
		n := 0
		for _, d := range dtos {
			n += d.TemplateCount()
		}
		if n != c.Expect.Templates {
			t.Errorf("%s sig=%s: %d templates emitted, generator expected %d", c.ID, c.Signature, n, c.Expect.Templates)
		}
		// every template binds with the case's keys
		var keys []cases.DTOKey
		root := c.View.FGs[0]
		for _, fg := range c.View.FGs {
			if fg.ID == c.View.Joins[0].FG {
				root = fg
			}
		}
		for _, k := range c.Keys {
			dk := cases.DTOKey{}
			for i, name := range c.KeyNames {
				f, _ := root.Feature(name)
				dk[name] = bind.Scalar(bind.LiteralFor(f.Type, k[i]))
			}
			keys = append(keys, dk)
		}
		for _, d := range dtos {
			if d.TemplateCount() == 0 {
				continue
			}
			if _, err := cases.BindDTO(d, keys, c.Batch > 0, g.Config().Now); err != nil {
				t.Errorf("%s sig=%s: bind: %v", c.ID, c.Signature, err)
			}
		}
	}
	for _, want := range []string{"serving", "definition", "serving:" + spec.CodeJoinOnPartialPrimaryKey, "serving:" + spec.CodeCollectUnsupportedOnlineFilt,
		"definition:" + spec.CodeAggregateInvalid, "definition:" + spec.CodeCollectNTooLarge} {
		if seen[want] == 0 {
			t.Errorf("2000 cases never produced %s (%v)", want, seen)
		}
	}
}

func TestKeysAndSignatures(t *testing.T) {
	g := gen()
	sc := g.Config().Scale
	classes := map[string]int{}
	shapes := map[string]int{}
	for i := 0; i < 400; i++ {
		c := g.Case(3, i)
		if c.Kind != "serving" {
			continue
		}
		classes[c.KeyClass]++
		if c.Batch > 0 && len(c.Keys) != c.Batch {
			t.Errorf("%s: %d keys for batch %d", c.ID, len(c.Keys), c.Batch)
		}
		if c.Batch == 0 && len(c.Keys) != 1 {
			t.Errorf("%s: %d keys for a single request", c.ID, len(c.Keys))
		}
		for _, k := range c.Keys {
			if len(k) != len(c.KeyNames) {
				t.Errorf("%s: key %v vs names %v", c.ID, k, c.KeyNames)
			}
		}
		if strings.Contains(c.Signature, "snow=") {
			shapes["snow"]++
		}
		if strings.Contains(c.Signature, "agg:") {
			shapes["agg"]++
		}
		if strings.Contains(c.Signature, "collect:") {
			shapes["collect"]++
		}
		if strings.Contains(c.Signature, "mode=batch") {
			shapes["batch"]++
		}
	}
	for _, want := range []string{"ordinary", "no-rows", "missing", "null-hop"} {
		if classes[want] == 0 {
			t.Errorf("key class %s never drawn (%v)", want, classes)
		}
	}
	for _, want := range []string{"snow", "agg", "collect", "batch"} {
		if shapes[want] == 0 {
			t.Errorf("shape %s never drawn (%v)", want, shapes)
		}
	}
	_ = sc
}

func TestShrinkConverges(t *testing.T) {
	g := gen()
	var c Case
	for i := 0; i < 400; i++ {
		c = g.Case(5, i)
		if c.Kind == "serving" && c.Expect.Gate == "" && len(c.View.Joins) >= 3 && len(c.View.Filters) >= 1 && c.Batch > 1 {
			break
		}
	}
	if c.Kind != "serving" || len(c.View.Joins) < 3 || len(c.View.Filters) < 1 {
		t.Skip("no suitable rich case in the first 400 of seed 5")
	}
	// Synthetic oracle: fails iff the view still has a filter and at least two joins.
	oracle := func(k Case) string {
		if len(k.View.Filters) >= 1 && len(k.View.Joins) >= 2 {
			return "WRONG-RESULT"
		}
		return "PASS"
	}
	min, steps := Shrink(c, oracle, 500)
	if oracle(min) != "WRONG-RESULT" {
		t.Fatal("shrinking lost the failure")
	}
	if len(min.View.Joins) != 2 || len(min.View.Filters) != 1 || min.Batch > 1 {
		t.Errorf("not minimal: joins=%d filters=%d batch=%d after %d steps", len(min.View.Joins), len(min.View.Filters), min.Batch, steps)
	}
	if len(c.View.Joins) < 3 {
		t.Error("original must be unchanged")
	}
}

func TestDirectCollect(t *testing.T) {
	cte := "WITH t AS (SELECT `customer_id`, `event_time`, `amount` FROM `transactions_1` WHERE `customer_id` = 21 ORDER BY `event_time` DESC LIMIT 5) SELECT `customer_id`, `event_time`, `amount` FROM t;"
	direct, ok := DirectCollect(cte)
	if !ok || !strings.HasPrefix(direct, "SELECT `customer_id`") || !strings.HasSuffix(direct, "LIMIT 5;") {
		t.Errorf("direct: %v %s", ok, direct)
	}
	if _, ok := DirectCollect("SELECT 1;"); ok {
		t.Error("non-CTE statement must not be rewritten")
	}
	if g := New(Config{}); g.Config().Now != data.FSNow || g.Config().Scale.E == 0 {
		t.Error("defaults")
	}
	_ = time.Second
}
