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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
)

func envGen() *EnvGen { return NewEnvelope(Config{DB: "test", Scale: data.NewScale(0.01)}) }

// collationTie matches MIN / MAX over a column whose domain holds
// collation-equal variants (category: travel / Travel, device: web / WEB):
// the representative is unspecified, so a compared case must not use it
// (the F8 rule of smoke.md).
var collationTie = regexp.MustCompile("(MIN|MAX)\\([^)]*`(category|device)`\\)")

// bareLowPrecedence reports an OR / XOR outside every parenthesis and
// string literal.  The generators join WHERE atoms with AND, and OR / XOR
// bind looser than AND, so such an operator would apply to the whole
// preceding WHERE (a cross-table condition) instead of to its atom.
func bareLowPrecedence(sql string) bool {
	depth, quoted := 0, false
	for i := 0; i < len(sql); i++ {
		switch c := sql[i]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (strings.HasPrefix(sql[i:], " OR ") || strings.HasPrefix(sql[i:], " XOR ")):
			return true
		}
	}
	return false
}

// syntaxOK is the small checker of random_generator.md §9: balanced
// parentheses and backticks, explicit AS aliases after every table, a
// trailing semicolon.
func syntaxOK(sql string) string {
	if strings.Count(sql, "(") != strings.Count(sql, ")") {
		return "unbalanced parentheses"
	}
	if strings.Count(sql, "`")%2 != 0 {
		return "unbalanced backticks"
	}
	if !strings.HasSuffix(sql, ";") {
		return "missing semicolon"
	}
	for _, m := range regexp.MustCompile("(FROM|JOIN) `[a-z_0-9]+`( [^ ;]+)?").FindAllStringSubmatch(sql, -1) {
		next := strings.TrimSpace(m[2])
		if next != "" && next != "AS" && next != "WHERE" && next != "GROUP" && next != "ORDER" && next != "LEFT" && next != "JOIN" &&
			!strings.HasPrefix(next, "ON") && next != "HAVING" && next != "LIMIT" && next != "t" {
			return "table without AS alias: " + m[0]
		}
	}
	return ""
}

func TestEnvelopeDeterminismAndSyntax(t *testing.T) {
	g := envGen()
	prods := map[string]int{}
	constructs := map[string]int{}
	for i := 0; i < 500; i++ {
		a, b := g.Case(11, i, nil), g.Case(11, i, nil)
		if a.SQL != b.SQL || a.Signature != b.Signature {
			t.Fatalf("case %d differs between regenerations", i)
		}
		if a.ID != EnvVersion+"-11-"+itoa(i) {
			t.Errorf("id %s", a.ID)
		}
		prods[a.Production]++
		for _, k := range a.Constructs {
			constructs[k]++
		}
		if _, syntax := a.ExpectReject(); syntax {
			continue // probes are deliberately outside the grammar
		}
		if collationTie.MatchString(a.SQL) {
			t.Errorf("%s (%s): MIN / MAX over a collation-equal string domain\n%s", a.ID, a.Production, a.SQL)
		}
		if bareLowPrecedence(a.SQL) {
			t.Errorf("%s (%s): OR / XOR outside parentheses\n%s", a.ID, a.Production, a.SQL)
		}
		if msg := syntaxOK(a.SQL); msg != "" {
			t.Errorf("%s (%s): %s\n%s", a.ID, a.Production, msg, a.SQL)
		}
	}
	for _, p := range Productions {
		if prods[p.Name] == 0 {
			t.Errorf("production %s never sampled (%v)", p.Name, prods)
		}
	}
	for _, k := range []string{"date-sub", "in-list", "like", "is-null", "or", "not", "having", "having-agg", "orderby-limit", "left-join", "string-snowflake", "batch", "real-join", "former-hazard"} {
		if constructs[k] == 0 {
			t.Errorf("construct %s never sampled", k)
		}
	}
	only := map[string]bool{"probe": true}
	if c := g.Case(11, 0, only); c.Production != "probe" {
		t.Errorf("--only must restrict productions: %s", c.Production)
	}
}

func TestExpectationPatternsExistInEngine(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..", "..", "storage", "ndb", "src", "ronsql")
	var sources []string
	for _, name := range []string{"RonSQLPreparer.cpp", "QueryPlanner.cpp", "RonSQLPreparer.hpp", "ResultPrinter.cpp"} {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Skipf("engine sources not available: %v", err)
		}
		sources = append(sources, string(raw))
	}
	all := strings.Join(sources, "\n")
	for _, e := range Expectations {
		if e.Pattern == "" {
			continue // known-wrong rows carry no engine message
		}
		if !strings.Contains(all, e.Pattern) {
			t.Errorf("expectation %s: pattern %q not found in the engine sources", e.Construct, e.Pattern)
		}
	}
	seen := map[string]bool{}
	for _, e := range Expectations {
		if seen[e.Construct] {
			t.Errorf("duplicate construct %s", e.Construct)
		}
		seen[e.Construct] = true
	}
}

func TestHazardTranslationsCoverOpenLogRows(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "..", "mysql-test", "suite", "ronsql_cte", "findings", "_discovery_log.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("discovery log not available: %v", err)
	}
	have := map[string]bool{}
	closed := map[string]bool{}
	for _, h := range Hazards {
		have[h.ID] = true
		if msg := syntaxOK(h.SQL); msg != "" {
			t.Errorf("hazard %s: %s", h.ID, msg)
		}
	}
	// The id may be bold (| **D6** |).
	row := regexp.MustCompile(`^\| \*{0,2}(D[0-9]+)\*{0,2} \| ([^|]*) \| ([^|]*) \| ([^|]*) \| ([^|]*) \| ([^|]*)`)
	for _, line := range strings.Split(string(raw), "\n") {
		m := row.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		id, symptom, disposition := m[1], m[4], m[6]
		open := !strings.Contains(disposition, "FIXED") && !strings.Contains(disposition, "RESOLVED")
		hazard := strings.Contains(symptom, "HANG") || strings.Contains(symptom, "CRASH")
		if open && hazard && !have[id] {
			t.Errorf("discovery-log row %s (%s) is an open hazard without a feature-store translation", id, strings.TrimSpace(symptom)[:40])
		}
		closed[id] = !open
	}
	cs := HazardCases(1)
	if len(cs) != len(Hazards) {
		t.Errorf("hazard cases: %d for %d hazards", len(cs), len(Hazards))
	}
	if len(cs) > 0 && (cs[0].Hazard != Hazards[0].ID || cs[0].Index >= 0) {
		t.Errorf("hazard cases: %+v", cs[0])
	}
	// A former hazard runs as a regular case: its row must be closed.
	for _, h := range FormerHazards {
		if !closed[h.ID] {
			t.Errorf("former hazard %s: its discovery-log row is missing or not FIXED / RESOLVED", h.ID)
		}
		if msg := syntaxOK(h.SQL); msg != "" {
			t.Errorf("former hazard %s: %s", h.ID, msg)
		}
		if h.Construct == "" && collationTie.MatchString(h.SQL) {
			t.Errorf("former hazard %s: MIN / MAX over a collation-equal string domain", h.ID)
		}
		if h.Construct != "" {
			if e, ok := ExpectationFor(h.Construct); !ok || !e.Reject {
				t.Errorf("former hazard %s: construct %q is not a rejection of the expectation table", h.ID, h.Construct)
			}
		}
	}
}

// The real-join production reaches every condition form, and the
// expectation table decides each outcome: a LEFT JOIN rejects IS NULL, an
// OR with IS NULL and XOR on its right side; IN (subquery) on the joined
// table rejects on both join types; everything else must match MySQL.
func TestRealJoinOutcomes(t *testing.T) {
	g := envGen()
	only := map[string]bool{"real-join": true}
	seen := map[string]int{}
	for i := 0; i < 400; i++ {
		c := g.Case(5, i, only)
		has := map[string]bool{}
		for _, k := range c.Constructs {
			has[k] = true
			seen[k]++
		}
		e, reject := c.ExpectReject()
		left := has["left-join"]
		switch {
		case has["subquery-joined"]:
			if !reject || e.Construct != "subquery-joined" {
				t.Errorf("%s: IN (subquery) on the joined table must expect subquery-joined: %+v\n%s", c.ID, e, c.SQL)
			}
		case left && (has["joined-xor"] || has["joined-is-null"] || has["joined-or-isnull"]):
			if !reject || e.Construct != "left-join-where-null" {
				t.Errorf("%s: LEFT JOIN with a condition RonSQL cannot prove NULL-rejecting must expect left-join-where-null: %+v\n%s", c.ID, e, c.SQL)
			}
		default:
			if reject {
				t.Errorf("%s: must run and match MySQL, expects %s\n%s", c.ID, e.Construct, c.SQL)
			}
			if msg := syntaxOK(c.SQL); msg != "" {
				t.Errorf("%s: %s\n%s", c.ID, msg, c.SQL)
			}
		}
		if bareLowPrecedence(c.SQL) {
			t.Errorf("%s: OR / XOR outside parentheses\n%s", c.ID, c.SQL)
		}
	}
	for _, k := range []string{"left-join", "greatest-where", "joined-cmp", "joined-like", "joined-greatest", "joined-least", "joined-xor",
		"joined-not-isnull", "joined-is-null", "joined-or-isnull", "subquery-joined", "left-join-where-null"} {
		if seen[k] == 0 {
			t.Errorf("real-join never produced %s (%v)", k, seen)
		}
	}
}

// The former-hazard production runs every fixed hazard; only D5's fix is a
// rejection.
func TestFormerHazardProduction(t *testing.T) {
	g := envGen()
	only := map[string]bool{"former-hazard": true}
	ids := map[string]bool{}
	for i := 0; i < 200; i++ {
		c := g.Case(7, i, only)
		var id string
		for _, k := range c.Constructs {
			if strings.HasPrefix(k, "former-hazard-") {
				id = strings.TrimPrefix(k, "former-hazard-")
			}
		}
		ids[id] = true
		e, reject := c.ExpectReject()
		if (id == "D5") != reject || (reject && e.Construct != "fanout-sibling") {
			t.Errorf("%s (%s): expectation %+v", c.ID, id, e)
		}
	}
	for _, h := range FormerHazards {
		if !ids[h.ID] {
			t.Errorf("former hazard %s never generated", h.ID)
		}
	}
}
