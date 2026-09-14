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
		if a.ID != "env-v1-11-"+itoa(i) {
			t.Errorf("id %s", a.ID)
		}
		prods[a.Production]++
		for _, k := range a.Constructs {
			constructs[k]++
		}
		if _, syntax := a.ExpectReject(); syntax {
			continue // probes are deliberately outside the grammar
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
	for _, k := range []string{"date-sub", "in-list", "like", "is-null", "or", "not", "having", "orderby-limit", "left-join", "string-snowflake", "batch"} {
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
		if e.Pattern == "" || e.Finding == "F9" {
			continue // F9's pattern is the framework's JSON parser message
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
	for _, h := range Hazards {
		have[h.ID] = true
		if msg := syntaxOK(h.SQL); msg != "" {
			t.Errorf("hazard %s: %s", h.ID, msg)
		}
	}
	row := regexp.MustCompile(`^\| (D[0-9]+) \| ([^|]*) \| ([^|]*) \| ([^|]*) \| ([^|]*) \| ([^|]*)`)
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
	}
	if cs := HazardCases(1); len(cs) != len(Hazards) || cs[0].Hazard != Hazards[0].ID || cs[0].Index >= 0 {
		t.Errorf("hazard cases: %+v", cs[0])
	}
}
