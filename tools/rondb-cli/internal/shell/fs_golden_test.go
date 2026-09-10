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

package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

// Synthetic query responses test runner policy, not Java SQL conformance.
func goldenTestGroups() (cases.StatementGroup, cases.StatementGroup, vector.Plan) {
	mysqlSQL := "SELECT 'north' AS `r_name`, 'gold' AS `p_tier`"
	ronSQL := "SELECT 'north' AS `r_name`"
	dto := emit.Statement{PreparedStatementIndex: 1, QueryOnline: &mysqlSQL,
		PreparedStatementParameters: []emit.Param{{Name: "entity_id", Index: 1}},
		SnowflakeTemplates:          []string{ronSQL}}
	group := cases.StatementGroup{DTO: dto, MySQL: &mysqlSQL,
		Statements: []cases.Statement{{Label: "snowflake[0]", RonSQL: ronSQL}}}
	return group, group, vector.PlanFor(dto, false, nil)
}

func goldenTestResponse(columns []string, rows ...[]exec.Cell) exec.Response {
	return exec.Response{Outcome: exec.OK, Result: &exec.Result{Columns: columns, Rows: rows}}
}

func TestGoldenGroupIndependentChecks(t *testing.T) {
	ref := goldenTestResponse([]string{"r_name", "p_tier"}, []exec.Cell{{Text: "north"}, {Text: "gold"}})
	wrongTwin := goldenTestResponse([]string{"r_name", "p_tier"}, []exec.Cell{{Text: "north"}, {Text: "wrong"}})
	ron := goldenTestResponse([]string{"r_name"}, []exec.Cell{{Text: "north"}})
	reject := exec.Response{Outcome: exec.CleanReject, Message: "known limit"}
	known := goldenGroupOpts{expectReject: &cases.Expect{Finding: "TEST", Pattern: "known limit"}}
	allowed := goldenGroupOpts{allowReject: true}
	for _, tc := range []struct {
		name              string
		twin, ron         exec.Response
		opts              goldenGroupOpts
		wantTwin, wantRon string
		failed            bool
	}{
		{"pass", ref, ron, goldenGroupOpts{}, "PASS", "PASS", false},
		{"unserved-field-differs", wrongTwin, ron, goldenGroupOpts{}, "FAIL", "PASS", true},
		{"mismatch-then-known-reject", wrongTwin, reject, known, "FAIL", "REJECT(expected)", true},
		{"mismatch-then-allowed-reject", wrongTwin, reject, allowed, "FAIL", "REJECT(allowed)", true},
		{"known-reject", ref, reject, known, "PASS", "REJECT(expected)", false},
		{"allowed-reject", ref, reject, allowed, "PASS", "REJECT(allowed)", false},
		{"unexpected-reject", ref, reject, goldenGroupOpts{}, "PASS", "REJECT", true},
		{"newly-supported", ref, ron, known, "PASS", "PASS(was-expected-reject)", false},
		{"malformed-json", ref, exec.Response{Outcome: exec.Error, Message: "malformed JSON"}, allowed, "PASS", "ERROR", true},
		{"timeout", ref, exec.Response{Outcome: exec.Timeout}, allowed, "PASS", "TIMEOUT", true},
		{"crash", ref, exec.Response{Outcome: exec.Crash}, allowed, "PASS", "CRASH", true},
		{"retry-exhausted", ref, exec.Response{Outcome: exec.Retryable}, allowed, "PASS", "RETRY-EXHAUSTED", true},
		{"missing-ron-result", ref, exec.Response{Outcome: exec.OK}, allowed, "PASS", "ERROR", true},
		{"short-ron-row", ref, goldenTestResponse([]string{"r_name"}, []exec.Cell{}), allowed, "PASS", "ERROR", true},
		{"mysql-error", exec.Response{Outcome: exec.Error, Message: "twin failed"}, ron, goldenGroupOpts{}, "MYSQL-ERROR", "PASS", true},
		{"missing-twin-result", exec.Response{Outcome: exec.OK}, ron, goldenGroupOpts{}, "MYSQL-ERROR", "PASS", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captured, rebuilt, plan := goldenTestGroups()
			calls := 0
			my := vectorQueryFunc(func(_ context.Context, sql string) exec.Response {
				calls++
				if sql != *captured.MySQL || calls > 2 {
					t.Fatalf("unexpected MySQL execution: %s", sql)
				}
				if calls == 1 {
					return ref
				}
				return tc.twin
			})
			rd := vectorQueryFunc(func(_ context.Context, sql string) exec.Response {
				if sql != rebuilt.Statements[0].RonSQL {
					t.Fatalf("RonSQL text changed: %s", sql)
				}
				return tc.ron
			})
			r := runGoldenGroup(context.Background(), captured, rebuilt, plan, []vector.Key{{"1"}}, my, rd, tc.opts)
			if r.Twin.Status != tc.wantTwin || r.RonSQL.Status != tc.wantRon || r.failed() != tc.failed {
				t.Fatalf("got twin=%s ron=%s failed=%t; want %s/%s/%t", r.Twin.Status, r.RonSQL.Status, r.failed(), tc.wantTwin, tc.wantRon, tc.failed)
			}
			if calls != 2 || len(r.Queries) != 3 || r.Queries[0].Path != "java-mysql" ||
				r.Queries[1].Path != "go-mysql" || r.Queries[2].Path != "go-ronsql/snowflake[0]" {
				t.Fatal("execution evidence was lost or the two MySQL reads were deduplicated")
			}
			if r.RonSQL.Vector != nil && !reflect.DeepEqual(r.RonSQL.Vector.NotServed, []string{"p_tier"}) {
				t.Fatal("RonSQL's unserved field must remain visible")
			}
			if tc.wantTwin == "FAIL" && (r.Twin.Result == nil || r.Twin.Result.Equal || r.Twin.Result.Diff == "") {
				t.Fatal("twin mismatch evidence was lost")
			}
		})
	}
}

func TestGoldenGroupLeftPolicy(t *testing.T) {
	ref := goldenTestResponse([]string{"r_name", "p_tier"}, []exec.Cell{{Null: true}, {Text: "gold"}})
	my := vectorQueryFunc(func(context.Context, string) exec.Response { return ref })
	rd := vectorQueryFunc(func(context.Context, string) exec.Response { return goldenTestResponse(nil) })
	for _, left := range []bool{false, true} {
		captured, rebuilt, plan := goldenTestGroups()
		r := runGoldenGroup(context.Background(), captured, rebuilt, plan, []vector.Key{{"1"}}, my, rd, goldenGroupOpts{left: left})
		want := "FAIL"
		if left {
			want = "PASS"
		}
		if r.Twin.Status != "PASS" || r.RonSQL.Status != want || r.RonSQL.Vector == nil ||
			(r.RonSQL.Vector.LeftMiss == 1) != left {
			t.Fatalf("wrong LEFT policy: %+v", r)
		}
	}
}

func TestGoldenGroupNoTemplate(t *testing.T) {
	ref := goldenTestResponse([]string{"r_name", "p_tier"}, []exec.Cell{{Text: "north"}, {Text: "gold"}})
	my := vectorQueryFunc(func(context.Context, string) exec.Response { return ref })
	for _, mysqlOnly := range []bool{false, true} {
		captured, rebuilt, plan := goldenTestGroups()
		rebuilt.Statements = nil
		want := "TEMPLATE-ERROR"
		if mysqlOnly {
			captured.DTO.SnowflakeTemplates = nil
			rebuilt.DTO.SnowflakeTemplates = nil
			want = "UNTESTED"
		}
		r := runGoldenGroup(context.Background(), captured, rebuilt, plan, []vector.Key{{"1"}}, my, nil, goldenGroupOpts{})
		if r.Twin.Status != "PASS" || r.RonSQL.Status != want || r.RonSQL.Vector != nil ||
			len(r.Queries) != 2 || r.failed() == mysqlOnly {
			t.Fatalf("missing template falsely established RonSQL coverage: %+v", r)
		}
	}
}

func TestGoldenGroupReferenceErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  exec.Response
	}{
		{"query-error", exec.Response{Outcome: exec.Error, Message: "reference failed"}},
		{"missing-result", exec.Response{Outcome: exec.OK}},
		{"headerless-empty", goldenTestResponse(nil)},
		{"duplicate-columns", goldenTestResponse([]string{"r_name", "r_name"})},
		{"short-row", goldenTestResponse([]string{"r_name"}, []exec.Cell{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captured, rebuilt, plan := goldenTestGroups()
			my := vectorQueryFunc(func(context.Context, string) exec.Response { return tc.ref })
			r := runGoldenGroup(context.Background(), captured, rebuilt, plan, []vector.Key{{"1"}}, my, nil, goldenGroupOpts{allowReject: true})
			if r.Twin.Status != "MYSQL-ERROR" || r.RonSQL.Status != "UNTESTED" ||
				!r.failed() || len(r.Queries) != 1 {
				t.Fatalf("invalid reference did not stop comparisons: %+v", r)
			}
		})
	}
	// Both row sets are empty, but a MySQL twin must still supply headers.
	captured, rebuilt, plan := goldenTestGroups()
	calls := 0
	my := vectorQueryFunc(func(context.Context, string) exec.Response {
		calls++
		if calls == 1 {
			return goldenTestResponse([]string{"r_name", "p_tier"})
		}
		return goldenTestResponse(nil)
	})
	r := runGoldenGroup(context.Background(), captured, rebuilt, plan, []vector.Key{{"1"}}, my, nil, goldenGroupOpts{})
	if r.Twin.Status != "MYSQL-ERROR" || !strings.Contains(r.Twin.Message, "column metadata") {
		t.Fatal("empty-result RonSQL allowance leaked into the MySQL comparison")
	}
}

func TestPrepareGoldenFixtureCorpus(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "fsq", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[emit.GoldenOutputKind]int{}
	mysqlOnly, multiDTO := false, false
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			before, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			captured, err := fixture.CapturedOutput()
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := prepareGoldenFixture(fixture)
			if err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(fixture)
			if err != nil || string(before) != string(after) {
				t.Fatal("preparation changed fixture input, capture or provenance")
			}
			if prepared.Kind != captured.Kind || !reflect.DeepEqual(prepared.Exception, captured.Exception) {
				t.Fatal("capture classification changed")
			}
			kinds[prepared.Kind]++
			if prepared.Kind != emit.GoldenStatements {
				if prepared.Pairs != nil {
					t.Fatal("non-serving fixture acquired executable pairs")
				}
				// No emitter call is needed for a non-serving classification.
				fixture.FGs, fixture.Joins = nil, nil
				got, err := prepareGoldenFixture(fixture)
				if err != nil || !reflect.DeepEqual(got, prepared) {
					t.Fatal("non-serving classification depends on emitter input")
				}
				return
			}
			if len(prepared.Pairs) != len(captured.Statements) {
				t.Fatal("serving DTO lost")
			}
			multiDTO = multiDTO || len(prepared.Pairs) > 1
			for i, pair := range prepared.Pairs {
				if !reflect.DeepEqual(pair.Captured, captured.Statements[i]) {
					t.Fatal("captured SQL or DTO changed")
				}
				mysqlOnly = mysqlOnly || pair.Captured.TemplateCount() == 0
				// Rebuilt DTO pointers must not alias the Java capture.
				if pair.Rebuilt.QueryOnline != nil {
					*pair.Rebuilt.QueryOnline = "changed rebuilt query"
				}
				if !reflect.DeepEqual(pair.Captured, captured.Statements[i]) {
					t.Fatal("rebuilt DTO aliases captured query")
				}
			}
		})
	}
	if kinds[emit.GoldenStatements] == 0 || kinds[emit.GoldenGate] == 0 ||
		kinds[emit.GoldenDefinition] == 0 || !mysqlOnly || !multiDTO {
		t.Fatal("corpus did not exercise every classification and DTO shape")
	}
}

func TestPairGoldenStatements(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]emit.Statement) []emit.Statement
		want string
	}{
		{"count", func(s []emit.Statement) []emit.Statement { return nil }, "count differs"},
		{"index", func(s []emit.Statement) []emit.Statement {
			s[0].PreparedStatementIndex++
			return s
		}, "indexes differ"},
		{"feature-group", func(s []emit.Statement) []emit.Statement {
			s[0].FeatureGroupID++
			return s
		}, "metadata differs"},
		{"parameters", func(s []emit.Statement) []emit.Statement {
			s[0].PreparedStatementParameters[0].Index++
			return s
		}, "metadata differs"},
		{"window", func(s []emit.Statement) []emit.Statement {
			window := int64(3600)
			s[0].AggregateWindow = &window
			return s
		}, "metadata differs"},
		{"database", func(s []emit.Statement) []emit.Statement {
			db := "different_fs"
			s[0].RonsqlDatabase = &db
			return s
		}, "metadata differs"},
		{"missing-query", func(s []emit.Statement) []emit.Statement {
			s[0].QueryOnline = nil
			return s
		}, "MySQL query"},
		{"blank-query", func(s []emit.Statement) []emit.Statement {
			sql := "  "
			s[0].QueryOnline = &sql
			return s
		}, "MySQL query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, _, _ := goldenTestGroups()
			got, _, _ := goldenTestGroups()
			pairs, err := pairGoldenStatements([]emit.Statement{ref.DTO}, tc.edit([]emit.Statement{got.DTO}))
			if err == nil || !strings.Contains(err.Error(), tc.want) || pairs != nil {
				t.Fatalf("pairs=%+v error=%v", pairs, err)
			}
		})
	}
	ref, _, _ := goldenTestGroups()
	duplicates := []emit.Statement{ref.DTO, ref.DTO}
	if pairs, err := pairGoldenStatements(duplicates, duplicates); err == nil || pairs != nil {
		t.Fatal("duplicate DTO indexes accepted")
	}
	ref.DTO.QueryOnline = nil
	if pairs, err := pairGoldenStatements([]emit.Statement{ref.DTO}, []emit.Statement{ref.DTO}); err == nil || pairs != nil {
		t.Fatal("missing captured MySQL query accepted")
	}
	// Empty statements remain a serving classification, with no executions.
	fixture := emit.GoldenFixture{Expected: json.RawMessage(`{"statements":[],"gateOrException":null}`)}
	prepared, err := prepareGoldenFixture(fixture)
	if err != nil || prepared.Kind != emit.GoldenStatements || prepared.Pairs == nil || len(prepared.Pairs) != 0 {
		t.Fatalf("empty serving fixture: %+v, %v", prepared, err)
	}
}

func TestPairGoldenStatementsPreservesSQLDifferences(t *testing.T) {
	ref, _, _ := goldenTestGroups()
	got, _, _ := goldenTestGroups()
	mysqlSQL, scanSQL, ronSQL := "SELECT 'different mysql'", "SELECT 'different scan'", "SELECT 'different ron'"
	got.DTO.QueryOnline, got.DTO.QueryOnlineScan, got.DTO.QueryRonsql = &mysqlSQL, &scanSQL, &ronSQL
	got.DTO.SnowflakeTemplates = nil
	pairs, err := pairGoldenStatements([]emit.Statement{ref.DTO}, []emit.Statement{got.DTO})
	if err != nil || len(pairs) != 1 {
		t.Fatalf("SQL differences blocked runtime comparison: %v", err)
	}
	if !reflect.DeepEqual(pairs[0].Captured, ref.DTO) || !reflect.DeepEqual(pairs[0].Rebuilt, got.DTO) {
		t.Fatal("pairing rewrote SQL")
	}
	// A missing RonSQL template must also reach the independent runtime check.
	got.DTO.QueryRonsql = nil
	if _, err := pairGoldenStatements([]emit.Statement{ref.DTO}, []emit.Statement{got.DTO}); err != nil {
		t.Fatalf("template count preempted MySQL comparison: %v", err)
	}
}

func TestGoldenInputsCorpus(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "fsq", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[vector.Kind]bool{}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			output, err := fixture.CapturedOutput()
			if err != nil {
				t.Fatal(err)
			}
			for _, dto := range output.Statements {
				before, err := json.Marshal([]interface{}{fixture.View, dto})
				if err != nil {
					t.Fatal(err)
				}
				inputs, err := goldenInputsFor(fixture.View, dto)
				if err != nil {
					t.Fatal(err)
				}
				again, err := goldenInputsFor(fixture.View, dto)
				if err != nil || !reflect.DeepEqual(inputs, again) {
					t.Fatal("nondeterministic inputs")
				}
				after, err := json.Marshal([]interface{}{fixture.View, dto})
				if err != nil || string(before) != string(after) {
					t.Fatal("input planning changed persisted input or captured DTO")
				}
				kinds[inputs.Plan.Kind] = true
				if inputs.Plan.Batch != fixture.Options.Batch || len(inputs.Keys) != len(inputs.BindKeys) {
					t.Fatal("batch mode or raw/bind key alignment changed")
				}
				fg, _ := fixture.View.FG(dto.FeatureGroupID)
				wantKeys := 4
				if fg.Name == "profiles" {
					wantKeys = 10
				} else if len(dto.PreparedStatementParameters) == 2 {
					wantKeys = 6
				}
				if len(inputs.Keys) != wantKeys {
					t.Fatalf("got %d keys, want %d", len(inputs.Keys), wantKeys)
				}
				seen := map[string]bool{}
				for i, key := range inputs.Keys {
					if seen[key.Text()] || len(key) != len(inputs.Plan.Params) {
						t.Fatal("duplicate key or incorrect width")
					}
					seen[key.Text()] = true
					for _, param := range dto.PreparedStatementParameters {
						raw := key[param.Index-1]
						column, _ := fg.Feature(param.Name)
						literal := raw
						if column.Type == "string" {
							literal = data.SQLString(raw)
						}
						arg := inputs.BindKeys[i][param.Name]
						if inputs.Plan.Params[param.Index-1] != param.Name || raw == "" ||
							arg.IsList || arg.Literal != literal {
							t.Fatal("fold parameters, raw keys and typed bind keys disagree")
						}
					}
					if _, err := cases.BindDTO(dto, []cases.DTOKey{inputs.BindKeys[i]}, fixture.Options.Batch, data.FSNow); err != nil {
						t.Fatal(err)
					}
				}
				if fixture.Options.Batch {
					if _, err := cases.BindDTO(dto, inputs.BindKeys, true, data.FSNow); err != nil {
						t.Fatal(err)
					}
				}
				if dto.CollectN != nil {
					order := "event_time"
					if name == "collect_explicit_order" {
						order = "sequence_no"
					}
					if !reflect.DeepEqual(inputs.Plan.Fields, []string{order, "amount"}) {
						t.Fatalf("wrong persisted collect fields: %v", inputs.Plan.Fields)
					}
				}
			}
		})
	}
	if len(kinds) != 4 {
		t.Fatal("corpus did not cover every fold kind, including MySQL-only point reads")
	}
}

func goldenInputCase(t *testing.T, name string) (spec.View, emit.Statement) {
	t.Helper()
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "fsq", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	fixture, ok := fixtures[name]
	if !ok {
		t.Fatalf("missing fixture %s", name)
	}
	output, err := fixture.CapturedOutput()
	if err != nil || len(output.Statements) == 0 {
		t.Fatalf("no captured DTO in %s: %v", name, err)
	}
	return fixture.View, output.Statements[0]
}

func TestGoldenInputsKeysAndSchema(t *testing.T) {
	view, dto := goldenInputCase(t, "aggregate_string_key")
	inputs, err := goldenInputsFor(view, dto)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inputs.Keys, []vector.Key{{"entity-1"}, {"O'Brien"}, {"café"}, {"entity-4"}}) ||
		inputs.BindKeys[1]["entity_id"].Literal != "'O''Brien'" || inputs.BindKeys[2]["entity_id"].Literal != "'café'" {
		t.Fatal("string probes or typed quoting changed")
	}
	view, dto = goldenInputCase(t, "aggregate_composite_batch")
	params := dto.PreparedStatementParameters
	params[0], params[1] = params[1], params[0]
	inputs, err = goldenInputsFor(view, dto)
	if err != nil {
		t.Fatal(err)
	}
	want := []vector.Key{{"1", "EUR"}, {"2", "USD"}, {"3", "EUR"}, {"4", "USD"}, {"1", "GBP"}, {"1", "USD"}}
	if !reflect.DeepEqual(inputs.Keys, want) || !reflect.DeepEqual(inputs.Plan.Params, []string{"entity_id", "currency"}) ||
		dto.PreparedStatementParameters[0].Index != 2 || inputs.BindKeys[0]["entity_id"].Literal != "1" ||
		inputs.BindKeys[0]["currency"].Literal != "'EUR'" {
		t.Fatal("composite pairing, parameter-index order or capture preservation failed")
	}
	bound, err := cases.BindDTO(dto, inputs.BindKeys, true, data.FSNow)
	tuples := "IN ((1, 'EUR'), (2, 'USD'), (3, 'EUR'), (4, 'USD'), (1, 'GBP'), (1, 'USD'))"
	if err != nil || bound.MySQL == nil || !strings.Contains(*bound.MySQL, tuples) {
		t.Fatalf("composite probes did not bind as the exact requested tuples: %v", err)
	}
	view, dto = goldenInputCase(t, "collect_desc")
	schema := "array<struct<amount:bigint,event_time:timestamp>>"
	view.Joins[0].Features[0].Type = &schema
	other := view.Joins[0]
	other.Index = 99
	otherSchema := "array<struct<fee:int>>"
	other.Features = []spec.TDFeature{{Name: "events_collect", Type: &otherSchema}}
	view.Joins = append(view.Joins, other)
	inputs, err = goldenInputsFor(view, dto)
	if err != nil || !reflect.DeepEqual(inputs.Plan.Fields, []string{"amount", "event_time"}) {
		t.Fatalf("collect used another join or source-column order: %+v, %v", inputs.Plan, err)
	}
}

func TestGoldenInputsRejectAmbiguousMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*spec.View, *emit.Statement)
	}{
		{"missing-fg", func(v *spec.View, d *emit.Statement) { v.FGs = nil }},
		{"duplicate-fg", func(v *spec.View, d *emit.Statement) { v.FGs = append(v.FGs, v.FGs[0]) }},
		{"unknown-layout", func(v *spec.View, d *emit.Statement) { v.FGs[0].Features[0].Type = "double" }},
		{"missing-join", func(v *spec.View, d *emit.Statement) { v.Joins = nil }},
		{"duplicate-join", func(v *spec.View, d *emit.Statement) { v.Joins = append(v.Joins, v.Joins[0]) }},
		{"wrong-fg", func(v *spec.View, d *emit.Statement) { v.Joins[0].FG++ }},
		{"wrong-prefix", func(v *spec.View, d *emit.Statement) { v.Joins[0].Prefix = nil }},
		{"no-params", func(v *spec.View, d *emit.Statement) { d.PreparedStatementParameters = nil }},
		{"bad-index", func(v *spec.View, d *emit.Statement) { d.PreparedStatementParameters[0].Index = 0 }},
		{"duplicate-param", func(v *spec.View, d *emit.Statement) {
			d.PreparedStatementParameters = append(d.PreparedStatementParameters, d.PreparedStatementParameters[0])
		}},
		{"wrong-key", func(v *spec.View, d *emit.Statement) { d.PreparedStatementParameters[0].Name = "event_time" }},
		{"missing-schema", func(v *spec.View, d *emit.Statement) { v.Joins[0].Features[0].Type = nil }},
		{"duplicate-schema", func(v *spec.View, d *emit.Statement) {
			v.Joins[0].Features = append(v.Joins[0].Features, v.Joins[0].Features[0])
		}},
		{"duplicate-field", func(v *spec.View, d *emit.Statement) {
			schema := "array<struct<amount:bigint,amount:bigint>>"
			v.Joins[0].Features[0].Type = &schema
		}},
		{"uncaptured-field", func(v *spec.View, d *emit.Statement) {
			schema := "array<struct<fee:int>>"
			v.Joins[0].Features[0].Type = &schema
		}},
		{"missing-order", func(v *spec.View, d *emit.Statement) { d.CollectOrderBy = nil }},
		{"wrong-direction", func(v *spec.View, d *emit.Statement) { v.Joins[0].Ascending = true }},
		{"wrong-limit", func(v *spec.View, d *emit.Statement) { v.Joins[0].CollectN = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, dto := goldenInputCase(t, "collect_desc")
			tc.edit(&view, &dto)
			inputs, err := goldenInputsFor(view, dto)
			if err == nil || !strings.Contains(err.Error(), "collect_desc DTO 0") ||
				!reflect.DeepEqual(inputs, goldenInputs{}) {
				t.Fatalf("partial inputs=%+v, error=%v", inputs, err)
			}
		})
	}
}

func TestGoldenRequestsCorpus(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "fsq", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			prepared, err := prepareGoldenFixture(fixture)
			if err != nil {
				t.Fatal(err)
			}
			for _, pair := range prepared.Pairs {
				before, err := json.Marshal([]interface{}{fixture, pair})
				if err != nil {
					t.Fatal(err)
				}
				inputs, err := goldenInputsFor(fixture.View, pair.Captured)
				if err != nil {
					t.Fatal(err)
				}
				requests, err := goldenRequestsFor(fixture.View, pair)
				if err != nil {
					t.Fatal(err)
				}
				again, err := goldenRequestsFor(fixture.View, pair)
				if err != nil || !reflect.DeepEqual(requests, again) {
					t.Fatal("nondeterministic requests")
				}
				wantCount := len(inputs.Keys)
				mode := "single"
				if fixture.Options.Batch {
					wantCount++
					mode = "batch-singleton"
				}
				if len(requests) != wantCount {
					t.Fatalf("got %d requests, want %d", len(requests), wantCount)
				}
				for i, request := range requests {
					label := "batch-all"
					keys, bindKeys := inputs.Keys, inputs.BindKeys
					if i < len(inputs.Keys) {
						label = fmt.Sprintf("%s/%d", mode, i+1)
						keys, bindKeys = inputs.Keys[i:i+1], inputs.BindKeys[i:i+1]
					}
					if request.Label != label || !request.Now.Equal(data.FSNow) ||
						!reflect.DeepEqual(request.Plan, inputs.Plan) || !reflect.DeepEqual(request.Keys, keys) {
						t.Fatalf("wrong request identity, clock, plan or keys: %+v", request)
					}
					for _, path := range []struct {
						dto   emit.Statement
						group cases.StatementGroup
					}{{pair.Captured, request.Captured}, {pair.Rebuilt, request.Rebuilt}} {
						want, err := cases.BindDTO(path.dto, bindKeys, fixture.Options.Batch, data.FSNow)
						if err != nil || !reflect.DeepEqual(path.group, want) {
							t.Fatalf("SQL, template count or original DTO changed: %v", err)
						}
					}
				}
				after, err := json.Marshal([]interface{}{fixture, pair})
				if err != nil || string(before) != string(after) {
					t.Fatal("request construction changed fixture or paired DTOs")
				}
				if fixture.Options.Batch {
					requests[0].Keys[0][0] = "changed singleton key"
					if !reflect.DeepEqual(requests[len(requests)-1].Keys, inputs.Keys) {
						t.Fatal("singleton keys alias mixed-batch keys")
					}
				}
			}
		})
	}
}

func TestGoldenRequestsSharedWindowAndDistinctSQL(t *testing.T) {
	view, dto := goldenInputCase(t, "aggregate_window")
	pair := goldenPair{Captured: dto, Rebuilt: dto}
	javaSQL, goSQL := "/* Java */ "+*dto.QueryOnline, "/* Go */ "+*dto.QueryOnline
	pair.Captured.QueryOnline, pair.Rebuilt.QueryOnline = &javaSQL, &goSQL
	requests, err := goldenRequestsFor(view, pair)
	if err != nil {
		t.Fatal(err)
	}
	for i, request := range requests {
		for _, group := range []cases.StatementGroup{request.Captured, request.Rebuilt} {
			if group.MySQL == nil || len(group.Statements) != 1 {
				t.Fatal("missing bound query")
			}
			for _, sql := range []string{*group.MySQL, group.Statements[0].RonSQL} {
				if !strings.Contains(sql, "`entity_id` = "+fmt.Sprint(i+1)+" AND") ||
					!strings.Contains(sql, "`event_time` >= '2026-05-31 23:00:00'") {
					t.Fatalf("key or shared window cutoff differs: %s", sql)
				}
			}
		}
		if !strings.HasPrefix(*request.Captured.MySQL, "/* Java */") ||
			!strings.HasPrefix(*request.Rebuilt.MySQL, "/* Go */") {
			t.Fatal("one SQL version replaced the other")
		}
	}
}

func TestGoldenRequestsBindingErrors(t *testing.T) {
	for _, path := range []string{"captured", "rebuilt"} {
		t.Run(path, func(t *testing.T) {
			view, dto := goldenInputCase(t, "aggregate_single")
			pair := goldenPair{Captured: dto, Rebuilt: dto}
			bad := "SELECT ? + ?"
			if path == "captured" {
				pair.Captured.QueryOnline = &bad
			} else {
				pair.Rebuilt.QueryOnline = &bad
			}
			requests, err := goldenRequestsFor(view, pair)
			if requests != nil || err == nil || !strings.Contains(err.Error(), path+" binding") {
				t.Fatalf("requests=%+v error=%v", requests, err)
			}
		})
	}
	view, dto := goldenInputCase(t, "aggregate_single")
	pair := goldenPair{Captured: dto, Rebuilt: dto}
	pair.Rebuilt.PreparedStatementIndex++
	if requests, err := goldenRequestsFor(view, pair); err == nil || requests != nil {
		t.Fatal("unmatched DTO pair produced requests")
	}
	view.FGs = nil
	pair.Rebuilt = dto
	if requests, err := goldenRequestsFor(view, pair); err == nil || requests != nil {
		t.Fatal("invalid source layout produced requests")
	}
}

type goldenTestOwner struct {
	databases []string
	cleanup   func(context.Context) error
}

func (o *goldenTestOwner) Databases() []string {
	return append([]string(nil), o.databases...)
}
func (o *goldenTestOwner) Cleanup(ctx context.Context) error { return o.cleanup(ctx) }

type goldenTestSession struct {
	my    vectorQuerier
	ron   func(context.Context, string, string) (vectorQuerier, error)
	close func() error
}

func (s *goldenTestSession) MySQL() vectorQuerier { return s.my }
func (s *goldenTestSession) RonSQL(ctx context.Context, db, probe string) (vectorQuerier, error) {
	return s.ron(ctx, db, probe)
}
func (s *goldenTestSession) Close() error { return s.close() }

// Lifecycle tests use synthetic query responses, never database connections.
func TestGoldenFixtureLifecycle(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "fsq", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, status string
		stop         bool
		requests     int
	}{
		{"success", "PASS", false, 4},
		{"twin-failure", "FAIL", false, 4},
		{"load-error", "ERROR", true, 0},
		{"open-error", "ERROR", true, 0},
		{"ron-open-error", "ERROR", false, 4},
		{"close-error", "ERROR", true, 4},
		{"cleanup-error", "ERROR", true, 4},
		{"cleanup-left-owned", "ERROR", true, 4},
		{"cancel-load", "ERROR", true, 0},
		{"cancel-query", "ERROR", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var trace []string
			checkContext := func(ctx context.Context) {
				t.Helper()
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("operation lacks a deadline")
				}
			}
			owner := &goldenTestOwner{databases: []string{"golden_1_fs"}}
			owner.cleanup = func(ctx context.Context) error {
				trace = append(trace, "cleanup")
				checkContext(ctx)
				if ctx.Err() != nil {
					t.Fatal("cleanup inherited cancellation")
				}
				if tc.name == "cleanup-error" {
					return fmt.Errorf("cleanup failed")
				}
				if tc.name != "cleanup-left-owned" {
					owner.databases = nil
				}
				return nil
			}
			mysqlCalls := 0
			session := &goldenTestSession{
				my: vectorQueryFunc(func(ctx context.Context, sql string) exec.Response {
					trace = append(trace, "mysql")
					checkContext(ctx)
					mysqlCalls++
					if tc.name == "cancel-query" {
						cancel()
						return exec.Response{Outcome: exec.Error, Message: "cancelled"}
					}
					if tc.name == "twin-failure" && mysqlCalls == 2 {
						return goldenTestResponse([]string{"wrong"})
					}
					return goldenTestResponse([]string{"value"})
				}),
				ron: func(ctx context.Context, db, probe string) (vectorQuerier, error) {
					trace = append(trace, "open-ron")
					checkContext(ctx)
					if db != "golden_1_fs" || probe != "SELECT COUNT(*) FROM `events_1`;" {
						t.Fatalf("wrong captured database or probe: %s %s", db, probe)
					}
					if tc.name == "ron-open-error" {
						return nil, fmt.Errorf("RonSQL unavailable")
					}
					return vectorQueryFunc(func(ctx context.Context, sql string) exec.Response {
						trace = append(trace, "ron")
						checkContext(ctx)
						return exec.Response{Outcome: exec.CleanReject, Message: "known collect limit"}
					}), nil
				},
				close: func() error {
					trace = append(trace, "close")
					if tc.name == "close-error" {
						return fmt.Errorf("close failed")
					}
					return nil
				},
			}
			runtime := goldenRuntime{
				Load: func(ctx context.Context, fgs []spec.FeatureGroup) (goldenOwned, error) {
					trace = append(trace, "load")
					checkContext(ctx)
					if !reflect.DeepEqual(fgs, fixtures["collect_desc"].FGs) {
						t.Fatal("loader did not receive the captured feature groups")
					}
					if tc.name == "cancel-load" {
						cancel()
						return owner, ctx.Err()
					}
					if tc.name == "load-error" {
						return owner, fmt.Errorf("partial load")
					}
					return owner, nil
				},
				Open: func(ctx context.Context) (goldenSession, error) {
					trace = append(trace, "open")
					checkContext(ctx)
					if tc.name == "open-error" {
						return session, fmt.Errorf("partial open")
					}
					return session, nil
				},
			}
			options := goldenRunOptions{timeout: time.Second, cleanupTimeout: time.Second,
				group: func(emit.GoldenFixture, goldenRequest) goldenGroupOpts {
					return goldenGroupOpts{allowReject: true}
				}}
			result := runGoldenFixture(ctx, fixtures["collect_desc"], runtime, options)
			if result.Status != tc.status || result.Stop != tc.stop || len(result.Requests) != tc.requests {
				t.Fatalf("result=%+v trace=%v", result, trace)
			}
			if trace[0] != "load" || trace[len(trace)-1] != "cleanup" {
				t.Fatalf("incorrect lifecycle: %v", trace)
			}
			opened := tc.name != "load-error" && tc.name != "cancel-load"
			if opened && trace[len(trace)-2] != "close" {
				t.Fatalf("connections were not closed before cleanup: %v", trace)
			}
			if tc.name == "ron-open-error" && mysqlCalls != 8 {
				t.Fatal("RonSQL connection error prevented MySQL comparisons")
			}
			if tc.name == "twin-failure" && (result.Requests[0].Result.Twin.Status != "FAIL" ||
				result.Requests[3].Result.Twin.Status != "PASS") {
				t.Fatal("later success hid an earlier comparison failure")
			}
			wantRemaining := tc.name == "cleanup-error" || tc.name == "cleanup-left-owned"
			if (len(result.RemainingDatabases) > 0) != wantRemaining ||
				!reflect.DeepEqual(result.Databases, []string{"golden_1_fs"}) {
				t.Fatal("database ownership diagnostics lost")
			}
		})
	}
}

func TestGoldenCorpusSerialStopAndNonExecution(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "fsq", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	// Non-execution classifications need neither connections nor timeouts.
	for _, name := range []string{"definition_collect_valid", "filter_or_rejected"} {
		result := runGoldenFixture(context.Background(), fixtures[name], goldenRuntime{}, goldenRunOptions{})
		if result.Status != "UNTESTED" || result.Stop || result.PlannedRequests != 0 {
			t.Fatalf("%s: %+v", name, result)
		}
	}
	for _, cleanupFails := range []bool{false, true} {
		active, loads := false, 0
		runtime := goldenRuntime{
			Load: func(context.Context, []spec.FeatureGroup) (goldenOwned, error) {
				if active {
					t.Fatal("next fixture started before cleanup")
				}
				active, loads = true, loads+1
				owner := &goldenTestOwner{databases: []string{"golden_1_fs"}}
				owner.cleanup = func(context.Context) error {
					if cleanupFails {
						return fmt.Errorf("cleanup failed")
					}
					active, owner.databases = false, nil
					return nil
				}
				return owner, nil
			},
			Open: func(context.Context) (goldenSession, error) {
				return &goldenTestSession{
					my: vectorQueryFunc(func(context.Context, string) exec.Response {
						return goldenTestResponse([]string{"value"})
					}),
					ron: func(context.Context, string, string) (vectorQuerier, error) {
						t.Fatal("MySQL-only DTO opened RonSQL")
						return nil, nil
					},
					close: func() error { return nil },
				}, nil
			},
		}
		selection := map[string]emit.GoldenFixture{
			"snowflake_cross_store_gated": fixtures["snowflake_cross_store_gated"],
			"collect_batch_gated":         fixtures["collect_batch_gated"],
		}
		results := runGoldenFixtures(context.Background(), selection, runtime,
			goldenRunOptions{timeout: time.Second, cleanupTimeout: time.Second})
		want := 2
		if cleanupFails {
			want = 1
		}
		if loads != want || len(results) != want || results[0].Name != "collect_batch_gated" {
			t.Fatalf("wrong order or failed cleanup did not stop corpus: %+v", results)
		}
		if !cleanupFails {
			for _, result := range results {
				if result.Status != "PASS" || len(result.Requests) == 0 {
					t.Fatalf("MySQL-only fixture was not compared: %+v", result)
				}
				for _, request := range result.Requests {
					if request.Result.Twin.Status != "PASS" || request.Result.RonSQL.Status != "UNTESTED" {
						t.Fatal("MySQL-only result claimed RonSQL coverage")
					}
				}
			}
		}
	}
}

type goldenTestConnection struct {
	probe func(context.Context) error
	close func() error
}

func (c *goldenTestConnection) Query(context.Context, string) exec.Response {
	return goldenTestResponse([]string{"value"})
}
func (c *goldenTestConnection) Probe(ctx context.Context) error { return c.probe(ctx) }
func (c *goldenTestConnection) Close() error                    { return c.close() }

func TestGoldenLiveSessionOwnership(t *testing.T) {
	probeFailure, closeFailure := errors.New("probe failed"), errors.New("close failed")
	var closed []string
	created := 0
	my := &goldenTestConnection{close: func() error {
		closed = append(closed, "mysql")
		return closeFailure
	}}
	session := &goldenLiveSession{my: my, newRon: func(db, probe string) goldenProbeConnection {
		created++
		id := created
		if db != fmt.Sprintf("golden_%d_fs", id) || probe != "SELECT COUNT(*) FROM `events_1`;" {
			t.Fatalf("factory received wrong database or probe: %s %s", db, probe)
		}
		return &goldenTestConnection{
			probe: func(ctx context.Context) error {
				if ctx.Err() != nil {
					t.Fatal("probe inherited an expired open context")
				}
				if id == 2 {
					return probeFailure
				}
				return nil
			},
			close: func() error {
				closed = append(closed, fmt.Sprintf("ron-%d", id))
				if id == 1 {
					return closeFailure
				}
				return nil
			},
		}
	}}
	if session.MySQL() != my || created != 0 {
		t.Fatal("MySQL unavailable or RonSQL opened eagerly")
	}
	ctx := context.Background()
	engine, err := session.RonSQL(ctx, "golden_1_fs", "SELECT COUNT(*) FROM `events_1`;")
	if err != nil || engine == nil {
		t.Fatalf("first RonSQL open: %v", err)
	}
	engine, err = session.RonSQL(ctx, "golden_2_fs", "SELECT COUNT(*) FROM `events_1`;")
	if engine != nil || !errors.Is(err, probeFailure) {
		t.Fatalf("failed probe was returned as usable: %v", err)
	}
	err = session.Close()
	if !errors.Is(err, closeFailure) || !reflect.DeepEqual(closed, []string{"ron-2", "ron-1", "mysql"}) {
		t.Fatalf("partial clients or remaining connections were not closed: %v %v", closed, err)
	}
	if session.Close() != err || len(closed) != 3 || session.MySQL() != nil {
		t.Fatal("repeated Close hid an error, repeated cleanup or exposed a closed oracle")
	}
	if engine, err := session.RonSQL(ctx, "golden_1_fs", "probe"); engine != nil || err == nil || created != 2 {
		t.Fatal("closed session created another client")
	}
}

func TestGoldenLiveSessionPreflight(t *testing.T) {
	created := 0
	session := &goldenLiveSession{newRon: func(string, string) goldenProbeConnection {
		created++
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if engine, err := session.RonSQL(ctx, "db", "probe"); engine != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled open accepted")
	}
	session.ronDisabled = true
	if engine, err := session.RonSQL(context.Background(), "db", "probe"); engine != nil || err == nil {
		t.Fatal("disabled RDRS opened")
	}
	session.ronDisabled = false
	if engine, err := session.RonSQL(context.Background(), "", "probe"); engine != nil || err == nil {
		t.Fatal("missing database accepted")
	}
	if created != 0 {
		t.Fatal("invalid request invoked client factory")
	}
	if engine, err := session.RonSQL(context.Background(), "db", "probe"); engine != nil || err == nil {
		t.Fatal("nil client accepted")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGoldenRuntimeAdaptersWithoutConnections(t *testing.T) {
	shell := &Shell{config: Config{Host: "not-used.invalid", MySQLPort: 3306, RestPort: 4406},
		mysqlUser: "tester", mysqlPass: "pw"}
	runtime, err := shell.fsGoldenRuntime(time.Second)
	if err != nil || runtime.Load == nil || runtime.Open == nil {
		t.Fatalf("runtime construction failed: %v", err)
	}
	// Invalid schema fails before opening MySQL. The adapter must translate
	// *GoldenLoad(nil) into a nil interface, not a phantom cleanup owner.
	owner, err := runtime.Load(context.Background(), nil)
	if owner != nil || err == nil {
		t.Fatal("failed load returned a typed nil owner")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session, err := runtime.Open(ctx)
	if session != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled open returned a session or attempted setup")
	}
	if _, err := shell.fsGoldenRuntime(0); err == nil {
		t.Fatal("unbounded HTTP timeout accepted")
	}
	shell.config.NoMySQL = true
	if _, err := shell.fsGoldenRuntime(time.Second); err == nil {
		t.Fatal("disabled MySQL accepted")
	}
	shell.config.NoMySQL, shell.config.NoRDRS = false, true
	shell.config.RestPort = 0
	if _, err := shell.fsGoldenRuntime(time.Second); err != nil {
		t.Fatal("disabled RDRS unnecessarily required an endpoint")
	}
	shell.config.MySQLPort = 0
	if _, err := shell.fsGoldenRuntime(time.Second); err == nil {
		t.Fatal("invalid MySQL endpoint accepted")
	}
}

func TestGoldenCLIOptions(t *testing.T) {
	base := []string{"--golden", "corpus", "--json", "report.json"}
	for _, extra := range [][]string{
		{"--vectors"}, {"--db", "test"}, {"--now", "2026-01-01T00:00:00Z"},
		{"--threads", "2"}, {"--unknown"}, {"extra"},
		{"--timeout", "0s"}, {"--cleanup-timeout", "-1s"},
		{"--tolerance", "NaN"}, {"--tolerance", "+Inf"}, {"--tolerance", "-1"},
		{"--allow-reject=maybe"}, {"--quiet=maybe"},
	} {
		args := append(append([]string(nil), base...), extra...)
		if _, err := parseGoldenOptions(parseFSArgs(args)); err == nil {
			t.Fatalf("invalid options accepted: %v", args)
		}
	}
	for _, args := range [][]string{nil, {"--golden"}, {"--golden", "corpus"}, {"--golden", "corpus", "--json"}} {
		if _, err := parseGoldenOptions(parseFSArgs(args)); err == nil {
			t.Fatalf("missing required value accepted: %v", args)
		}
	}
	args := append(append([]string(nil), base...), "--tolerance", "0", "--allow-reject=false", "--quiet")
	o, err := parseGoldenOptions(parseFSArgs(args))
	if err != nil || o.tolerance != 0 || o.allowReject || !o.quiet || o.timeout != 30*time.Second {
		t.Fatalf("options=%+v error=%v", o, err)
	}
	if err := (&Shell{}).runFSVerify([]string{"--golden", "corpus", "--vectors"}); err == nil ||
		!strings.Contains(err.Error(), "--vectors") {
		t.Fatal("mixed modes bypassed golden validation")
	}
}

func TestGoldenCLIPolicyScope(t *testing.T) {
	for _, tc := range []struct {
		name          string
		left, collect bool
	}{
		{"collect_desc", false, true},
		{"collect_batch_gated", false, false},
		{"aggregate_single", false, false},
		{"snowflake_left", true, false},
		{"snowflake_inner", false, false},
	} {
		view, dto := goldenInputCase(t, tc.name)
		inputs, err := goldenInputsFor(view, dto)
		if err != nil {
			t.Fatal(err)
		}
		request := goldenRequest{Plan: inputs.Plan, Captured: cases.StatementGroup{DTO: dto}}
		policy := (goldenCLIOptions{tolerance: 1e-9}).policy(emit.GoldenFixture{View: view}, request)
		if policy.left != tc.left || (policy.expectReject != nil) != tc.collect || policy.allowReject {
			t.Fatalf("%s: wrong policy %+v", tc.name, policy)
		}
		if tc.collect && policy.expectReject != cases.Known["S6-cte"] {
			t.Fatal("collect rejection did not use the existing narrow expectation")
		}
	}
	view, dto := goldenInputCase(t, "snowflake_left")
	view.Joins = append(view.Joins, spec.Join{Index: 99, Parent: 0, Type: spec.JoinInner})
	if !goldenLeftSubtree(view, dto.PreparedStatementIndex) {
		t.Fatal("unrelated INNER join disabled LEFT policy")
	}
	view.Joins[3].Type = spec.JoinInner
	if goldenLeftSubtree(view, dto.PreparedStatementIndex) {
		t.Fatal("mixed subtree received LEFT policy")
	}
	view.Joins[3].Type = spec.JoinLeft
	view.Joins = append(view.Joins, view.Joins[2])
	if goldenLeftSubtree(view, dto.PreparedStatementIndex) {
		t.Fatal("ambiguous join indexes received LEFT policy")
	}
}

func TestGoldenBinaryRejectPolicy(t *testing.T) {
	view, dto := goldenInputCase(t, "snowflake_binary")
	inputs, err := goldenInputsFor(view, dto)
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest{Plan: inputs.Plan, Captured: cases.StatementGroup{DTO: dto}}
	fixture := emit.GoldenFixture{View: view}
	policy := (goldenCLIOptions{}).policy(fixture, request)
	if policy.expectReject == nil || policy.expectReject.Finding != "F7" || policy.allowReject {
		t.Fatalf("missing narrow F7 policy: %+v", policy)
	}
	for _, tc := range []struct {
		outcome exec.Outcome
		message string
		status  string
	}{
		{exec.CleanReject, "Unsupported column type (17) in pass-through result.", "REJECT(expected)"},
		{exec.CleanReject, "Unsupported column type (18) in pass-through result.", "REJECT"},
		{exec.CleanReject, "some other rejection", "REJECT"},
		{exec.Error, "malformed JSON: Unsupported column type (17) in pass-through result.", "ERROR"},
	} {
		captured, rebuilt, plan := goldenTestGroups()
		my := vectorQueryFunc(func(context.Context, string) exec.Response {
			return goldenTestResponse([]string{"r_name", "p_tier"}, []exec.Cell{{Text: "north"}, {Text: "gold"}})
		})
		rd := vectorQueryFunc(func(context.Context, string) exec.Response {
			return exec.Response{Outcome: tc.outcome, Message: tc.message}
		})
		result := runGoldenGroup(context.Background(), captured, rebuilt, plan, []vector.Key{{"1"}}, my, rd, policy)
		if result.Twin.Status != "PASS" || result.RonSQL.Status != tc.status ||
			result.failed() != (tc.status != "REJECT(expected)") {
			t.Fatalf("%s: wrong classification: %+v", tc.message, result)
		}
	}
	fixture.Name = "snowflake_inner"
	if (goldenCLIOptions{}).policy(fixture, request).expectReject != nil {
		t.Fatal("F7 policy leaked to another fixture")
	}
	fixture.Name = "snowflake_binary"
	request.Captured.DTO.PreparedStatementIndex++
	if (goldenCLIOptions{}).policy(fixture, request).expectReject != nil {
		t.Fatal("F7 policy leaked to another DTO")
	}
}

func TestGoldenCLIReportAndFailure(t *testing.T) {
	for _, name := range []string{"definition_collect_valid", "aggregate_single"} {
		t.Run(name, func(t *testing.T) {
			o := goldenCLIOptions{dir: filepath.Join("..", "fsq", "testdata", "hopsworks_golden"),
				fixture: name, jsonPath: filepath.Join(t.TempDir(), "run.json"),
				timeout: time.Second, cleanupTimeout: time.Second, tolerance: 1e-9, quiet: true}
			factoryCalls, loadCalls := 0, 0
			factory := func(time.Duration) (goldenRuntime, error) {
				factoryCalls++
				return goldenRuntime{
					Load: func(context.Context, []spec.FeatureGroup) (goldenOwned, error) {
						loadCalls++
						return nil, fmt.Errorf("synthetic setup failure")
					},
					Open: func(context.Context) (goldenSession, error) {
						t.Fatal("opened a query session after failed setup")
						return nil, nil
					},
				}, nil
			}
			var output bytes.Buffer
			runErr := runGoldenCommand(context.Background(), o, factory, &output)
			serving := name == "aggregate_single"
			wantCalls := 0
			if serving {
				wantCalls = 1
			}
			if (runErr != nil) != serving || factoryCalls != wantCalls || loadCalls != wantCalls {
				t.Fatalf("error=%v factory=%d loads=%d", runErr, factoryCalls, loadCalls)
			}
			raw, err := os.ReadFile(o.jsonPath)
			if err != nil {
				t.Fatal(err)
			}
			var report goldenReport
			if err := json.Unmarshal(raw, &report); err != nil {
				t.Fatal(err)
			}
			fixtures, err := emit.LoadGoldenFixtures(o.dir)
			if err != nil {
				t.Fatal(err)
			}
			captured := report.Fixtures[name]
			if report.SchemaVersion != 1 || report.DataVersion != data.GoldenDataVersion ||
				!report.Now.Equal(data.FSNow) || report.EmitterRef != emit.HopsworksRef ||
				!reflect.DeepEqual(captured.Provenance, fixtures[name].Provenance) || len(report.Results) != 1 {
				t.Fatal("report lost identity, data clock or provenance")
			}
			got, err := captured.CapturedOutput()
			if err != nil {
				t.Fatal(err)
			}
			want, err := fixtures[name].CapturedOutput()
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("report changed the captured Java output")
			}
			if serving {
				if report.Results[0].Status != "ERROR" || !report.Results[0].Stop {
					t.Fatal("failed run was not retained")
				}
			} else if !strings.Contains(output.String(), "UNTESTED no successful SQL comparisons") {
				t.Fatal("definition-only run claimed SQL success")
			}
			// An existing report is never overwritten, and no new load starts.
			oldLoads := loadCalls
			if err := runGoldenCommand(context.Background(), o, factory, &output); err == nil {
				t.Fatal("existing report accepted")
			}
			unchanged, err := os.ReadFile(o.jsonPath)
			if err != nil || !bytes.Equal(raw, unchanged) || loadCalls != oldLoads {
				t.Fatal("existing report changed or database work started")
			}
		})
	}
}

func TestGoldenReportProtectsCorpus(t *testing.T) {
	root := t.TempDir()
	corpus := filepath.Join(root, "corpus")
	if err := os.Mkdir(corpus, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := goldenReportOutsideCorpus(corpus, filepath.Join(corpus, "new.json")); err == nil {
		t.Fatal("new report could invalidate the corpus inventory")
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(corpus, alias); err != nil {
		t.Fatal(err)
	}
	if err := goldenReportOutsideCorpus(corpus, filepath.Join(alias, "new.json")); err == nil {
		t.Fatal("symlink bypassed corpus protection")
	}
	if err := goldenReportOutsideCorpus(alias, filepath.Join(root, "run.json")); err != nil {
		t.Fatal(err)
	}
}

func TestGoldenSummaryDoesNotHideFailures(t *testing.T) {
	results := []goldenFixtureResult{{Name: "first", Status: "FAIL",
		Requests: []goldenRequestResult{{Result: goldenGroupResult{
			Twin: goldenCheck{Status: "FAIL"}, RonSQL: goldenCheck{Status: "REJECT(allowed)"}}}}},
		{Name: "second", Status: "PASS", Requests: []goldenRequestResult{{Result: goldenGroupResult{
			Twin: goldenCheck{Status: "PASS"}, RonSQL: goldenCheck{Status: "UNTESTED"}}}}}}
	var output bytes.Buffer
	if err := printGoldenResults(&output, 2, results, true); err == nil ||
		!strings.Contains(output.String(), "failed=1 mysql-pass=1 ronsql-pass=0 ronsql-rejected=1 ronsql-untested=1") {
		t.Fatalf("failure or coverage hidden: %v %s", err, output.String())
	}
	if err := printGoldenResults(&output, 3, results[1:], true); err == nil {
		t.Fatal("incomplete corpus run succeeded")
	}
}
