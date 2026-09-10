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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

type goldenCheck struct {
	Status  string         `json:"status"`
	Message string         `json:"message,omitempty"`
	Rows    int            `json:"rows"`
	Result  *canon.Report  `json:"result,omitempty"`
	Vector  *vector.Report `json:"vector,omitempty"`
}

type goldenQuery struct {
	Path    string `json:"path"`
	SQL     string `json:"sql"`
	Outcome string `json:"outcome"`
	Message string `json:"message,omitempty"`
}

type goldenGroupResult struct {
	Index   int           `json:"dtoIndex"`
	Twin    goldenCheck   `json:"mysqlTwin"`
	RonSQL  goldenCheck   `json:"ronsql"`
	Queries []goldenQuery `json:"queries"`
}

func (r goldenGroupResult) failed() bool {
	return isFailure(r.Twin.Status) || isFailure(r.RonSQL.Status)
}

type goldenGroupOpts struct {
	tolerance    float64
	left         bool
	allowReject  bool
	expectReject *cases.Expect
}

// runGoldenGroup compares one already-bound DTO and execution unit.
// The caller supplies matching captured/reconstructed DTOs, a fold plan
// derived from the capture, identical keys/time and the correct databases.
// No setup, cleanup or SQL regeneration happens here.
//
// Compare complete MySQL result multisets, including fields not served by
// RonSQL and collect ranks. Then compare RonSQL vectors against the captured
// reference. Keep both outcomes: a RonSQL rejection cannot hide a twin bug.
func runGoldenGroup(ctx context.Context, captured, rebuilt cases.StatementGroup, plan vector.Plan,
	keys []vector.Key, my, rd vectorQuerier, o goldenGroupOpts) goldenGroupResult {
	r := goldenGroupResult{Index: captured.DTO.PreparedStatementIndex,
		Twin: goldenCheck{Status: "UNTESTED"}, RonSQL: goldenCheck{Status: "UNTESTED"}}
	if len(keys) == 0 || (!plan.Batch && len(keys) != 1) {
		r.Twin.Status, r.Twin.Message = "BIND-ERROR", "invalid execution-unit key count"
		return r
	}
	for _, key := range keys {
		if len(key) != len(plan.Params) {
			r.Twin.Status, r.Twin.Message = "BIND-ERROR", "key width differs from fold parameters"
			return r
		}
	}
	if captured.DTO.PreparedStatementIndex != rebuilt.DTO.PreparedStatementIndex ||
		captured.MySQL == nil || rebuilt.MySQL == nil {
		r.Twin.Status, r.Twin.Message = "REFERENCE-ERROR", "DTO indexes differ or a MySQL query is absent"
		return r
	}
	if my == nil {
		r.Twin.Status, r.Twin.Message = "MYSQL-ERROR", "MySQL engine is unavailable"
		return r
	}
	query := func(engine vectorQuerier, path, sql string) exec.Response {
		response := engine.Query(ctx, sql)
		if response.Outcome == exec.OK {
			if err := validateGoldenResult(response.Result); err != nil {
				response.Outcome, response.Message = exec.Error, err.Error()
			} else if strings.HasSuffix(path, "-mysql") && len(response.Result.Columns) == 0 {
				// MySQL SELECTs carry metadata even with no rows. Do not
				// inherit canon's headerless-empty RonSQL allowance here.
				response.Outcome, response.Message = exec.Error, "MySQL result lacks column metadata"
			}
		}
		r.Queries = append(r.Queries, goldenQuery{Path: path, SQL: sql,
			Outcome: response.Outcome.String(), Message: response.Message})
		return response
	}
	ref := query(my, "java-mysql", *captured.MySQL)
	if ref.Outcome != exec.OK {
		r.Twin.Status, r.Twin.Message = "MYSQL-ERROR", "captured: "+ref.Message
		r.RonSQL.Message = "captured reference unavailable"
		return r
	}
	r.Twin.Rows = len(ref.Result.Rows)
	twin := query(my, "go-mysql", *rebuilt.MySQL)
	if twin.Outcome != exec.OK {
		r.Twin.Status, r.Twin.Message = "MYSQL-ERROR", "reconstructed: "+twin.Message
	} else {
		report := canon.Compare(ref.Result, twin.Result, canon.Options{Tolerance: o.tolerance})
		r.Twin.Result = &report
		r.Twin.Status = "PASS"
		if !report.Equal {
			r.Twin.Status, r.Twin.Message = "FAIL", report.Reason
		}
	}
	if len(rebuilt.Statements) != captured.DTO.TemplateCount() {
		r.RonSQL.Status, r.RonSQL.Message = "TEMPLATE-ERROR", "reconstructed template count differs from capture"
		return r
	}
	if len(rebuilt.Statements) == 0 {
		r.RonSQL.Message = "no RonSQL template; MySQL comparison only"
		return r
	}
	if plan.Kind < vector.Aggregate || plan.Kind > vector.Snowflake ||
		((plan.Kind == vector.Aggregate || plan.Kind == vector.Collect) && len(rebuilt.Statements) != 1) ||
		(plan.Kind == vector.Collect && (plan.Batch || len(plan.Fields) == 0 || plan.OrderBy == "" || plan.CollectFeature == "")) {
		r.RonSQL.Status, r.RonSQL.Message = "FOLD-ERROR", "unsupported or incomplete fold plan"
		return r
	}
	if rd == nil {
		r.RonSQL.Status, r.RonSQL.Message = "ERROR", "RonSQL engine is unavailable"
		return r
	}
	var results []*exec.Result
	for _, st := range rebuilt.Statements {
		response := query(rd, "go-ronsql/"+st.Label, st.RonSQL)
		if response.Outcome != exec.OK {
			r.RonSQL.Status, r.RonSQL.Message = response.Outcome.String(), response.Message
			if response.Outcome == exec.CleanReject {
				r.RonSQL.Status = "REJECT"
				if o.expectReject != nil && o.expectReject.Pattern != "" && strings.Contains(response.Message, o.expectReject.Pattern) {
					r.RonSQL.Status, r.RonSQL.Message = "REJECT(expected)", o.expectReject.Finding
				} else if o.allowReject {
					r.RonSQL.Status = "REJECT(allowed)"
				}
			}
			return r
		}
		r.RonSQL.Rows += len(response.Result.Rows)
		results = append(results, response.Result)
	}
	refVectors, err := vector.FoldMysql(plan, ref.Result, keys)
	if err != nil {
		r.RonSQL.Status, r.RonSQL.Message = "FOLD-ERROR", "captured MySQL: "+err.Error()
		return r
	}
	ronVectors, err := vector.FoldRonsql(plan, results, keys)
	if err != nil {
		r.RonSQL.Status, r.RonSQL.Message = "FOLD-ERROR", "RonSQL: "+err.Error()
		return r
	}
	policy := vector.Policy{Tolerance: o.tolerance, MissingEqualsNull: o.left && plan.Kind == vector.Snowflake}
	report := vector.Compare(plan, refVectors, ronVectors, keys, vector.Types(ref.Result), policy, nil)
	r.RonSQL.Vector = &report
	switch {
	case len(report.Mismatches) > 0:
		r.RonSQL.Status, r.RonSQL.Message = "FAIL", fmt.Sprintf("%d vector mismatch(es)", len(report.Mismatches))
	case report.Compared == 0:
		r.RonSQL.Message = "no vector cells compared"
	default:
		r.RonSQL.Status = "PASS"
		if o.expectReject != nil {
			r.RonSQL.Status, r.RonSQL.Message = "PASS(was-expected-reject)", o.expectReject.Finding
		}
	}
	return r
}

// Reject malformed OK results before either comparator or fold indexes rows.
// In particular, a short collect row must not reach its sorting callback.
func validateGoldenResult(result *exec.Result) error {
	if result == nil {
		return fmt.Errorf("missing result")
	}
	seen := map[string]bool{}
	for _, column := range result.Columns {
		if seen[column] {
			return fmt.Errorf("duplicate output column %q", column)
		}
		seen[column] = true
	}
	for i, row := range result.Rows {
		if len(result.Columns) == 0 || len(row) != len(result.Columns) {
			return fmt.Errorf("row %d has %d cells for %d columns", i, len(row), len(result.Columns))
		}
	}
	return nil
}

type goldenPair struct {
	Captured emit.Statement
	Rebuilt  emit.Statement
}

type goldenPrepared struct {
	Kind      emit.GoldenOutputKind
	Exception *emit.GoldenException
	Pairs     []goldenPair
}

// prepareGoldenFixture accepts a fixture from LoadGoldenFixtures. Gates and
// definition-only outputs are classifications, not successful SQL executions.
// Regenerate only the serving input, on a deep copy: Build normalizes nested
// joins in place. Never reconstruct or normalize the captured expected output.
func prepareGoldenFixture(f emit.GoldenFixture) (goldenPrepared, error) {
	output, err := f.CapturedOutput()
	if err != nil {
		return goldenPrepared{}, err
	}
	prepared := goldenPrepared{Kind: output.Kind, Exception: output.Exception}
	if output.Kind != emit.GoldenStatements {
		return prepared, nil
	}
	raw, err := json.Marshal(f.View)
	if err != nil {
		return goldenPrepared{}, fmt.Errorf("%s: copy serving input: %w", f.Name, err)
	}
	var input spec.View
	if err := json.Unmarshal(raw, &input); err != nil {
		return goldenPrepared{}, fmt.Errorf("%s: copy serving input: %w", f.Name, err)
	}
	rebuilt, err := emit.Build(&input)
	if err != nil {
		return goldenPrepared{}, fmt.Errorf("%s: rebuild captured serving statements: %w", f.Name, err)
	}
	prepared.Pairs, err = pairGoldenStatements(output.Statements, rebuilt)
	if err != nil {
		return goldenPrepared{}, fmt.Errorf("%s: %w", f.Name, err)
	}
	return prepared, nil
}

// Preserve DTO order and all SQL text, including MySQL-only DTOs. Non-SQL
// metadata must match before using the captured fold recipe for both paths.
// SQL differences are deliberately left for execution; E2 tests byte equality.
func pairGoldenStatements(captured, rebuilt []emit.Statement) ([]goldenPair, error) {
	if len(captured) != len(rebuilt) {
		return nil, fmt.Errorf("DTO count differs: captured %d, rebuilt %d", len(captured), len(rebuilt))
	}
	pairs := make([]goldenPair, 0, len(captured))
	seen := map[int]bool{}
	for i, ref := range captured {
		got := rebuilt[i]
		if seen[ref.PreparedStatementIndex] {
			return nil, fmt.Errorf("duplicate captured DTO index %d", ref.PreparedStatementIndex)
		}
		seen[ref.PreparedStatementIndex] = true
		if ref.PreparedStatementIndex != got.PreparedStatementIndex {
			return nil, fmt.Errorf("DTO position %d: indexes differ: captured %d, rebuilt %d",
				i, ref.PreparedStatementIndex, got.PreparedStatementIndex)
		}
		if ref.QueryOnline == nil || strings.TrimSpace(*ref.QueryOnline) == "" ||
			got.QueryOnline == nil || strings.TrimSpace(*got.QueryOnline) == "" {
			return nil, fmt.Errorf("DTO %d: missing captured or rebuilt MySQL query", ref.PreparedStatementIndex)
		}
		if !reflect.DeepEqual(goldenMetadata(ref), goldenMetadata(got)) {
			return nil, fmt.Errorf("DTO %d: non-SQL metadata differs", ref.PreparedStatementIndex)
		}
		pairs = append(pairs, goldenPair{Captured: ref, Rebuilt: got})
	}
	return pairs, nil
}

// Work on value copies; do not alter either DTO's pointers or slices.
// RonSQL template presence/count is checked independently by runGoldenGroup,
// so a template error cannot prevent the MySQL comparison from being recorded.
func goldenMetadata(dto emit.Statement) emit.Statement {
	dto.QueryOnline, dto.QueryOnlineScan, dto.QueryRonsql = nil, nil, nil
	dto.SnowflakeTemplates = nil
	return dto
}

type goldenInputs struct {
	Plan     vector.Plan
	Keys     []vector.Key
	BindKeys []cases.DTOKey
}

// goldenInputsFor derives a fold recipe and deterministic probes from a
// captured DTO and its persisted input, without calling the Go emitter.
// Keys contain raw values in parameter-index order; BindKeys contain typed
// SQL literals for those same values. This is a key bank, not an execution
// schedule: single requests still need one key, batches need explicit units.
func goldenInputsFor(v spec.View, dto emit.Statement) (goldenInputs, error) {
	fail := func(message string) (goldenInputs, error) {
		return goldenInputs{}, fmt.Errorf("%s DTO %d: %s", v.Name, dto.PreparedStatementIndex, message)
	}
	var fg spec.FeatureGroup
	matches := 0
	for _, candidate := range v.FGs {
		if candidate.ID == dto.FeatureGroupID {
			fg, matches = candidate, matches+1
		}
	}
	if matches != 1 {
		return fail("need exactly one matching feature group")
	}
	if _, err := data.GoldenRows(fg); err != nil {
		return fail(err.Error())
	}
	var join spec.Join
	matches = 0
	for _, candidate := range v.Joins {
		if candidate.Index == dto.PreparedStatementIndex {
			join, matches = candidate, matches+1
		}
	}
	prefix := ""
	if dto.Prefix != nil {
		prefix = *dto.Prefix
	}
	if matches != 1 || join.FG != fg.ID || join.PrefixOrEmpty() != prefix {
		return fail("join index, feature group or prefix does not identify one source join")
	}
	// PlanFor reads slice order, while BindDTO reads parameter indexes.
	// Reorder a private slice so the fold and binder agree without changing
	// the captured DTO (including captures whose parameter slice is unsorted).
	ordered := dto
	ordered.PreparedStatementParameters = make([]emit.Param, len(dto.PreparedStatementParameters))
	names := map[string]bool{}
	for _, param := range dto.PreparedStatementParameters {
		if param.Name == "" || names[param.Name] || param.Index < 1 ||
			param.Index > len(ordered.PreparedStatementParameters) ||
			ordered.PreparedStatementParameters[param.Index-1].Name != "" {
			return fail("parameter names must be unique and indexes must cover 1..N")
		}
		column, ok := fg.Feature(param.Name)
		if !ok || !column.Primary {
			return fail("parameter is not a source primary-key column")
		}
		names[param.Name] = true
		ordered.PreparedStatementParameters[param.Index-1] = param
	}
	_, composite := fg.Feature("currency")
	expectedParams := 1
	if composite {
		expectedParams++
	}
	if (fg.Name != "events" && fg.Name != "profiles") || !names["entity_id"] ||
		len(names) != expectedParams || (composite && !names["currency"]) {
		return fail("unsupported A6 entity-key layout")
	}
	var fields []string
	if !reflect.DeepEqual(join.CollectN, dto.CollectN) {
		return fail("collect limit differs from source join")
	}
	if dto.CollectN != nil {
		if *dto.CollectN <= 0 || dto.CollectFeatureName == nil ||
			*dto.CollectFeatureName != emit.CollectFeatureName(fg) ||
			dto.CollectOrderBy == nil || !reflect.DeepEqual(join.OrderBy, dto.CollectOrderBy) ||
			dto.CollectAscending == nil || *dto.CollectAscending != join.Ascending {
			return fail("incomplete or inconsistent collect metadata")
		}
		if _, ok := fg.Feature(*dto.CollectOrderBy); !ok {
			return fail("collect order column is absent from source")
		}
		matches = 0
		for _, feature := range join.Features {
			if feature.Name == *dto.CollectFeatureName {
				matches++
				if feature.Type != nil {
					fields = emit.ParseStructFieldNames(*feature.Type)
				}
			}
		}
		if matches != 1 || len(fields) == 0 {
			return fail("need one persisted collect struct schema on the matching join")
		}
		sources := map[string]bool{}
		for _, name := range dto.CollectSourceFeatures {
			sources[name] = true
		}
		seen := map[string]bool{}
		for _, name := range fields {
			if _, ok := fg.Feature(name); !ok || name == "" || seen[name] || !sources[name] {
				return fail("collect struct fields must be unique captured source columns")
			}
			seen[name] = true
		}
	}
	inputs := goldenInputs{Plan: vector.PlanFor(ordered, v.Options.Batch, fields)}
	type probe struct {
		entity   int64
		currency string
	}
	count := int64(4) // Event entities 1..3 exist; 4 is absent.
	if fg.Name == "profiles" {
		count = 10 // Nine join hit/miss/NULL cases; 10 is absent.
	}
	var probes []probe
	for entity := int64(1); entity <= count; entity++ {
		currency := "EUR"
		if entity%2 == 0 {
			currency = "USD"
		}
		probes = append(probes, probe{entity, currency})
	}
	if composite {
		// A missing currency and a second partner for entity 1. Keep the
		// bank non-Cartesian so accidental cross-product binding is visible.
		probes = append(probes, probe{1, "GBP"}, probe{1, "USD"})
	}
	entityColumn, _ := fg.Feature("entity_id")
	for _, probe := range probes {
		values := map[string]string{
			"entity_id": data.GoldenEntityKey(probe.entity, entityColumn.Type == "string"),
			"currency":  probe.currency,
		}
		key := make(vector.Key, len(inputs.Plan.Params))
		bindKey := cases.DTOKey{}
		for i, name := range inputs.Plan.Params {
			column, _ := fg.Feature(name)
			key[i] = values[name]
			bindKey[name] = bind.Scalar(bind.LiteralFor(column.Type, key[i]))
		}
		inputs.Keys = append(inputs.Keys, key)
		inputs.BindKeys = append(inputs.BindKeys, bindKey)
	}
	return inputs, nil
}

type goldenRequest struct {
	Label    string
	Now      time.Time
	Plan     vector.Plan
	Keys     []vector.Key
	Captured cases.StatementGroup
	Rebuilt  cases.StatementGroup
}

// goldenRequestsFor binds one validated DTO pair without executing SQL.
// Single fixtures get one request per probe. Batch fixtures get every probe
// as a singleton batch plus one mixed batch; never change the captured mode.
// Both SQL versions use the same typed keys and the data set's fixed clock.
// Plans and original DTO metadata are read-only; each request owns its keys.
func goldenRequestsFor(v spec.View, pair goldenPair) ([]goldenRequest, error) {
	if _, err := pairGoldenStatements([]emit.Statement{pair.Captured}, []emit.Statement{pair.Rebuilt}); err != nil {
		return nil, fmt.Errorf("%s: %w", v.Name, err)
	}
	inputs, err := goldenInputsFor(v, pair.Captured)
	if err != nil {
		return nil, err
	}
	now := data.FSNow.UTC()
	if now.IsZero() {
		return nil, fmt.Errorf("%s: A6 data reference time is unset", v.Name)
	}
	var requests []goldenRequest
	add := func(label string, indexes []int) error {
		request := goldenRequest{Label: label, Now: now, Plan: inputs.Plan}
		var keys []cases.DTOKey
		for _, index := range indexes {
			request.Keys = append(request.Keys, append(vector.Key(nil), inputs.Keys[index]...))
			keys = append(keys, inputs.BindKeys[index])
		}
		var err error
		request.Captured, err = cases.BindDTO(pair.Captured, keys, inputs.Plan.Batch, now)
		if err != nil {
			return fmt.Errorf("%s %s captured binding: %w", v.Name, label, err)
		}
		request.Rebuilt, err = cases.BindDTO(pair.Rebuilt, keys, inputs.Plan.Batch, now)
		if err != nil {
			return fmt.Errorf("%s %s rebuilt binding: %w", v.Name, label, err)
		}
		requests = append(requests, request)
		return nil
	}
	mode := "single"
	if inputs.Plan.Batch {
		mode = "batch-singleton"
	}
	var all []int
	for i := range inputs.Keys {
		all = append(all, i)
		if err := add(fmt.Sprintf("%s/%d", mode, i+1), []int{i}); err != nil {
			return nil, err
		}
	}
	if inputs.Plan.Batch && len(all) > 1 {
		if err := add("batch-all", all); err != nil {
			return nil, err
		}
	}
	return requests, nil
}

type goldenOwned interface {
	Databases() []string
	Cleanup(context.Context) error
}

// A session owns every connection it creates, including partial opens.
// Close must release them all before the fixture databases can be dropped.
type goldenSession interface {
	MySQL() vectorQuerier
	RonSQL(context.Context, string, string) (vectorQuerier, error) // database, probe SQL
	Close() error
}

type goldenRuntime struct {
	// Return any partially acquired owner/session even on error.
	// Absent resources must be nil interfaces, not typed nil pointers.
	Load func(context.Context, []spec.FeatureGroup) (goldenOwned, error)
	Open func(context.Context) (goldenSession, error)
}

type goldenRunOptions struct {
	timeout, cleanupTimeout time.Duration
	// The caller selects policy per fixture/request, not globally by SQL text.
	group func(emit.GoldenFixture, goldenRequest) goldenGroupOpts
}

type goldenRequestResult struct {
	Label  string            `json:"label"`
	Now    time.Time         `json:"now"`
	Keys   []vector.Key      `json:"keys"`
	Result goldenGroupResult `json:"result"`
}

type goldenFixtureResult struct {
	Name               string                `json:"name"`
	Kind               emit.GoldenOutputKind `json:"kind"`
	Status             string                `json:"status"`
	Message            string                `json:"message,omitempty"`
	Exception          *emit.GoldenException `json:"exception,omitempty"`
	PlannedRequests    int                   `json:"plannedRequests"`
	Requests           []goldenRequestResult `json:"requests"`
	Errors             []string              `json:"errors,omitempty"`
	Databases          []string              `json:"databases,omitempty"`
	RemainingDatabases []string              `json:"remainingDatabases,omitempty"`
	Stop               bool                  `json:"stop"`
}

// runGoldenFixtures is deliberately serial. A cleanup/close failure or
// cancellation stops the corpus; ordinary comparison failures do not.
// Callers retain the original fixtures for provenance and report artifacts.
func runGoldenFixtures(ctx context.Context, fixtures map[string]emit.GoldenFixture,
	runtime goldenRuntime, o goldenRunOptions) []goldenFixtureResult {
	var names []string
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)
	var results []goldenFixtureResult
	for _, name := range names {
		result := runGoldenFixture(ctx, fixtures[name], runtime, o)
		results = append(results, result)
		if result.Stop {
			break
		}
	}
	return results
}

func runGoldenFixture(ctx context.Context, fixture emit.GoldenFixture,
	runtime goldenRuntime, o goldenRunOptions) (result goldenFixtureResult) {
	result.Name, result.Status = fixture.Name, "UNTESTED"
	fail := func(err error) {
		result.Status = "ERROR"
		result.Errors = append(result.Errors, err.Error())
	}
	cancelled := func() bool {
		if err := ctx.Err(); err != nil {
			fail(err)
			result.Stop = true
			return true
		}
		return false
	}
	if cancelled() {
		return
	}
	prepared, err := prepareGoldenFixture(fixture)
	if err != nil {
		fail(err)
		return
	}
	result.Kind, result.Exception = prepared.Kind, prepared.Exception
	if prepared.Kind != emit.GoldenStatements {
		result.Message = "captured non-execution result; no SQL was run"
		return
	}
	var groups [][]goldenRequest
	for _, pair := range prepared.Pairs {
		requests, err := goldenRequestsFor(fixture.View, pair)
		if err != nil {
			fail(err)
			return
		}
		if pair.Captured.TemplateCount() > 0 {
			fg, _ := fixture.View.FG(pair.Captured.FeatureGroupID)
			if pair.Captured.RonsqlDatabase == nil || *pair.Captured.RonsqlDatabase != fg.OnlineDatabase() {
				fail(fmt.Errorf("DTO %d: RonSQL database differs from captured source group", pair.Captured.PreparedStatementIndex))
				return
			}
		}
		result.PlannedRequests += len(requests)
		groups = append(groups, requests)
	}
	if result.PlannedRequests == 0 {
		result.Message = "empty captured statement set; no SQL was run"
		return
	}
	if o.timeout <= 0 || o.cleanupTimeout <= 0 || runtime.Load == nil || runtime.Open == nil {
		fail(fmt.Errorf("positive timeouts and a complete golden runtime are required"))
		result.Stop = true
		return
	}
	var owned goldenOwned
	var session goldenSession
	defer func() {
		if session != nil {
			if err := session.Close(); err != nil {
				fail(fmt.Errorf("close query connections: %w", err))
				result.Stop = true
			}
		}
		if owned != nil {
			// Preserve context values, but not cancellation or its deadline.
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cleanupTimeout)
			defer cancel()
			err := owned.Cleanup(cleanupCtx)
			result.RemainingDatabases = owned.Databases()
			if err != nil {
				fail(fmt.Errorf("cleanup: %w", err))
				result.Stop = true
			}
			if len(result.RemainingDatabases) > 0 {
				fail(fmt.Errorf("cleanup left owned databases: %v", result.RemainingDatabases))
				result.Stop = true
			}
		}
	}()
	setupCtx, cancel := context.WithTimeout(ctx, o.timeout)
	owned, err = runtime.Load(setupCtx, fixture.FGs)
	if err == nil {
		err = setupCtx.Err()
	}
	cancel()
	if owned != nil {
		result.Databases = owned.Databases()
	}
	if err != nil || owned == nil {
		fail(fmt.Errorf("load fixture (owner present=%t): %v", owned != nil, err))
		// A failed CREATE may have an uncertain outcome; do not advance.
		result.Stop = true
		return
	}
	if cancelled() {
		return
	}
	openCtx, cancel := context.WithTimeout(ctx, o.timeout)
	session, err = runtime.Open(openCtx)
	if err == nil {
		err = openCtx.Err()
	}
	cancel()
	if err != nil || session == nil {
		fail(fmt.Errorf("open query session: %v", err))
		result.Stop = true
		return
	}
	my := session.MySQL()
	if my == nil {
		fail(fmt.Errorf("query session has no MySQL connection"))
		result.Stop = true
		return
	}
	result.Status = "PASS"
	for _, requests := range groups {
		if cancelled() {
			return
		}
		first := requests[0]
		var rd vectorQuerier
		var openErr error
		if len(first.Rebuilt.Statements) > 0 && len(first.Rebuilt.Statements) == first.Captured.DTO.TemplateCount() {
			fg, _ := fixture.View.FG(first.Captured.DTO.FeatureGroupID)
			openCtx, cancel := context.WithTimeout(ctx, o.timeout)
			rd, openErr = session.RonSQL(openCtx, *first.Captured.DTO.RonsqlDatabase,
				fmt.Sprintf("SELECT COUNT(*) FROM `%s`;", fg.TableName()))
			if openErr == nil {
				openErr = openCtx.Err()
			}
			cancel()
			if openErr == nil && rd == nil {
				openErr = fmt.Errorf("no RonSQL connection returned")
			}
			if openErr != nil {
				fail(fmt.Errorf("DTO %d open RonSQL: %w", first.Captured.DTO.PreparedStatementIndex, openErr))
				rd = nil // Still run the two MySQL paths.
			}
		}
		for _, request := range requests {
			if cancelled() {
				return
			}
			var policy goldenGroupOpts
			if o.group != nil {
				policy = o.group(fixture, request)
			}
			var ron vectorQuerier
			if rd != nil {
				ron = goldenTimedQuery{rd, o.timeout}
			}
			check := runGoldenGroup(ctx, request.Captured, request.Rebuilt, request.Plan, request.Keys,
				goldenTimedQuery{my, o.timeout}, ron, policy)
			if openErr != nil && check.RonSQL.Status == "ERROR" {
				check.RonSQL.Message = openErr.Error()
			}
			result.Requests = append(result.Requests, goldenRequestResult{
				Label: request.Label, Now: request.Now, Keys: request.Keys, Result: check})
			if check.failed() && result.Status == "PASS" {
				result.Status = "FAIL"
			}
		}
	}
	cancelled()
	return
}

type goldenTimedQuery struct {
	engine  vectorQuerier
	timeout time.Duration
}

func (q goldenTimedQuery) Query(ctx context.Context, sql string) exec.Response {
	queryCtx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	return q.engine.Query(queryCtx, sql)
}

type goldenConnection interface {
	vectorQuerier
	Close() error
}

type goldenProbeConnection interface {
	goldenConnection
	Probe(context.Context) error
}

// goldenLiveSession owns the oracle and all lazy RonSQL clients. It is
// serial, like the fixture runner; do not keep using its engines after Close.
type goldenLiveSession struct {
	my          goldenConnection
	ron         []goldenProbeConnection
	newRon      func(string, string) goldenProbeConnection
	ronDisabled bool
	closed      bool
	closeErr    error
}

func (s *goldenLiveSession) MySQL() vectorQuerier {
	if s.closed {
		return nil
	}
	return s.my
}

func (s *goldenLiveSession) RonSQL(ctx context.Context, database, probe string) (vectorQuerier, error) {
	if s.closed {
		return nil, fmt.Errorf("golden query session is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.ronDisabled {
		return nil, fmt.Errorf("RDRS is disabled")
	}
	if database == "" || probe == "" || s.newRon == nil {
		return nil, fmt.Errorf("RonSQL needs a database, probe and client factory")
	}
	engine := s.newRon(database, probe)
	if engine == nil {
		return nil, fmt.Errorf("RonSQL factory returned no client")
	}
	// Own the client before probing so failed probes are also cleaned up.
	s.ron = append(s.ron, engine)
	if err := engine.Probe(ctx); err != nil {
		return nil, fmt.Errorf("RonSQL probe in %s: %w", database, err)
	}
	return engine, nil
}

func (s *goldenLiveSession) Close() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	var failures []error
	for i := len(s.ron) - 1; i >= 0; i-- {
		if err := s.ron[i].Close(); err != nil {
			failures = append(failures, fmt.Errorf("close RonSQL client %d: %w", i, err))
		}
	}
	if s.my != nil {
		if err := s.my.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close MySQL: %w", err))
		}
	}
	s.closeErr = errors.Join(failures...)
	return s.closeErr
}

// fsGoldenRuntime constructs adapters only; no connection or database change
// happens until the runner invokes Load/Open. Use separate per-fixture pools,
// never the shell's interactive MySQL connection or a previous fixture's pool.
func (s *Shell) fsGoldenRuntime(timeout time.Duration) (goldenRuntime, error) {
	if timeout <= 0 || s.config.NoMySQL {
		return goldenRuntime{}, fmt.Errorf("A6 requires a positive timeout and MySQL enabled")
	}
	host := s.config.MySQLHost
	if host == "" {
		host = s.config.Host
	}
	rhost := s.config.RDRSHost
	if rhost == "" {
		rhost = s.config.Host
	}
	if host == "" || s.config.MySQLPort < 1 || s.config.MySQLPort > 65535 {
		return goldenRuntime{}, fmt.Errorf("A6 requires a valid MySQL host and port")
	}
	if !s.config.NoRDRS && (rhost == "" || s.config.RestPort < 1 || s.config.RestPort > 65535) {
		return goldenRuntime{}, fmt.Errorf("A6 requires a valid RDRS host and port")
	}
	target := data.Target{Host: host, Port: s.config.MySQLPort, User: s.mysqlUser,
		Password: s.mysqlPass, TLS: s.config.TLS}
	oracle := exec.MySQLConfig{Host: host, Port: target.Port, User: target.User,
		Password: target.Password, TLS: target.TLS,
		Charset: "utf8mb4", SQLMode: "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION"}
	// No default MySQL database: captured queryOnline SQL is fully qualified.
	// Freeze configuration so later shell changes cannot split a fixture's targets.
	rport, rtls, apiKey, disabled := s.config.RestPort, s.config.RDRSTLS, s.config.RDRSAPIKey, s.config.NoRDRS
	return goldenRuntime{
		Load: func(ctx context.Context, fgs []spec.FeatureGroup) (goldenOwned, error) {
			owned, err := data.LoadGolden(ctx, target, fgs)
			if owned == nil {
				return nil, err // Do not return a typed nil owner.
			}
			return owned, err // Preserve ownership on a partial load failure.
		},
		Open: func(ctx context.Context) (goldenSession, error) {
			my, err := exec.OpenMySQL(ctx, oracle)
			if err != nil {
				return nil, err
			}
			return &goldenLiveSession{my: my, ronDisabled: disabled,
				newRon: func(database, probe string) goldenProbeConnection {
					return exec.NewRDRS(rhost, rport, rtls, apiKey, database, APIVersion, probe, timeout)
				}}, nil
		},
	}, nil
}

type goldenCLIOptions struct {
	dir, fixture, jsonPath  string
	timeout, cleanupTimeout time.Duration
	tolerance               float64
	allowReject, quiet      bool
}

func parseGoldenOptions(a fsArgs) (goldenCLIOptions, error) {
	o := goldenCLIOptions{dir: a.str("golden", ""), fixture: a.str("fixture", ""),
		jsonPath: a.str("json", ""), timeout: 30 * time.Second,
		cleanupTimeout: 30 * time.Second, tolerance: 1e-9}
	for name := range a.flags {
		switch name {
		case "golden", "fixture", "json", "timeout", "cleanup-timeout", "tolerance", "allow-reject", "quiet":
		default:
			return o, fmt.Errorf("--%s is not supported with --golden", name)
		}
	}
	if len(a.pos) != 0 || o.dir == "" || o.dir == "true" || o.jsonPath == "" || o.jsonPath == "true" {
		return o, fmt.Errorf("usage: .fs_verify --golden DIR --json NEW_FILE [--fixture NAME]")
	}
	for name, target := range map[string]*time.Duration{"timeout": &o.timeout, "cleanup-timeout": &o.cleanupTimeout} {
		if value, ok := a.flags[name]; ok {
			duration, err := time.ParseDuration(value)
			if err != nil || duration <= 0 {
				return o, fmt.Errorf("--%s needs a positive duration", name)
			}
			*target = duration
		}
	}
	if value, ok := a.flags["tolerance"]; ok {
		tolerance, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(tolerance) || math.IsInf(tolerance, 0) || tolerance < 0 {
			return o, fmt.Errorf("--tolerance needs a finite nonnegative number")
		}
		o.tolerance = tolerance
	}
	for name, target := range map[string]*bool{"allow-reject": &o.allowReject, "quiet": &o.quiet} {
		if value, ok := a.flags[name]; ok {
			flag, err := strconv.ParseBool(value)
			if err != nil {
				return o, fmt.Errorf("--%s needs a boolean", name)
			}
			*target = flag
		}
	}
	return o, nil
}

func (o goldenCLIOptions) policy(f emit.GoldenFixture, request goldenRequest) goldenGroupOpts {
	policy := goldenGroupOpts{tolerance: o.tolerance, allowReject: o.allowReject}
	if request.Plan.Kind == vector.Collect && request.Captured.DTO.TemplateCount() > 0 {
		policy.expectReject = cases.Known["S6-cte"]
	}
	if request.Plan.Kind == vector.Snowflake {
		policy.left = goldenLeftSubtree(f.View, request.Captured.DTO.PreparedStatementIndex)
		// F7: only the captured binary-projection DTO and its observed type.
		if f.Name == "snowflake_binary" && request.Captured.DTO.PreparedStatementIndex == 1 {
			policy.expectReject = &cases.Expect{Finding: "F7",
				Pattern: "Unsupported column type (17) in pass-through result."}
		}
	}
	return policy
}

// Ignore the subtree root's incoming join and unrelated joins. Accept only
// a nonempty, unambiguous subtree whose descendant joins are all LEFT.
// An omitted join type means LEFT, as in View.Normalize.
func goldenLeftSubtree(v spec.View, root int) bool {
	joins := map[int]spec.Join{}
	children := map[int][]int{}
	for _, join := range v.Joins {
		if _, duplicate := joins[join.Index]; duplicate {
			return false
		}
		joins[join.Index] = join
		children[join.Parent] = append(children[join.Parent], join.Index)
	}
	if _, ok := joins[root]; !ok {
		return false
	}
	seen := map[int]bool{root: true}
	queue := []int{root}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			if child == 0 && parent == 0 {
				continue // Label/root convention, not a descendant edge.
			}
			join := joins[child]
			if seen[child] || (join.Type != "" && join.Type != spec.JoinLeft) {
				return false
			}
			seen[child] = true
			queue = append(queue, child)
		}
	}
	return len(seen) > 1
}

type goldenReport struct {
	SchemaVersion int                           `json:"schemaVersion"`
	DataVersion   string                        `json:"dataVersion"`
	Now           time.Time                     `json:"now"`
	EmitterRef    string                        `json:"emitterRef"`
	Tolerance     float64                       `json:"tolerance"`
	AllowReject   bool                          `json:"allowReject"`
	Fixtures      map[string]emit.GoldenFixture `json:"fixtures"`
	Results       []goldenFixtureResult         `json:"results"`
}

func (s *Shell) runFSGolden(a fsArgs) error {
	o, err := parseGoldenOptions(a)
	if err != nil {
		return err
	}
	o.quiet = o.quiet || s.quiet
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runGoldenCommand(ctx, o, s.fsGoldenRuntime, os.Stdout)
}

// The required new report file is reserved before any database work.
// Never overwrite a previous run or a file in the captured corpus.
func runGoldenCommand(ctx context.Context, o goldenCLIOptions,
	makeRuntime func(time.Duration) (goldenRuntime, error), out io.Writer) error {
	fixtures, err := emit.LoadGoldenFixtures(o.dir)
	if err != nil {
		return err
	}
	if o.fixture != "" {
		fixture, ok := fixtures[o.fixture]
		if !ok {
			return fmt.Errorf("unknown golden fixture %q", o.fixture)
		}
		fixtures = map[string]emit.GoldenFixture{o.fixture: fixture}
	}
	needRuntime := false
	for _, fixture := range fixtures {
		output, err := fixture.CapturedOutput()
		if err != nil {
			return err
		}
		needRuntime = needRuntime || (output.Kind == emit.GoldenStatements && len(output.Statements) > 0)
	}
	var runtime goldenRuntime
	if needRuntime {
		runtime, err = makeRuntime(o.timeout)
		if err != nil {
			return err
		}
	}
	if err := goldenReportOutsideCorpus(o.dir, o.jsonPath); err != nil {
		return err
	}
	file, err := os.OpenFile(o.jsonPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create new golden report: %w", err)
	}
	defer file.Close()
	results := runGoldenFixtures(ctx, fixtures, runtime, goldenRunOptions{
		timeout: o.timeout, cleanupTimeout: o.cleanupTimeout, group: o.policy})
	report := goldenReport{SchemaVersion: 1, DataVersion: data.GoldenDataVersion, Now: data.FSNow.UTC(),
		EmitterRef: emit.HopsworksRef, Tolerance: o.tolerance, AllowReject: o.allowReject,
		Fixtures: fixtures, Results: results}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	saveErr := encoder.Encode(report)
	closeErr := file.Close()
	runErr := printGoldenResults(out, len(fixtures), results, o.quiet)
	if saveErr != nil || closeErr != nil {
		return errors.Join(runErr, fmt.Errorf("save golden report: %w", errors.Join(saveErr, closeErr)))
	}
	return runErr
}

func printGoldenResults(out io.Writer, selected int, results []goldenFixtureResult, quiet bool) error {
	failed, mysqlPass, ronPass, rejected, untested := 0, 0, 0, 0, 0
	for _, fixture := range results {
		if isFailure(fixture.Status) || fixture.Stop {
			failed++
		}
		if !quiet || isFailure(fixture.Status) || fixture.Stop {
			fmt.Fprintf(out, "GOLDEN %s %s kind=%s requests=%d/%d\n",
				fixture.Name, fixture.Status, fixture.Kind, len(fixture.Requests), fixture.PlannedRequests)
			if fixture.Message != "" {
				fmt.Fprintf(out, "  %s\n", fixture.Message)
			}
			for _, message := range fixture.Errors {
				fmt.Fprintf(out, "  ERROR %q\n", message)
			}
		}
		for _, request := range fixture.Requests {
			check := request.Result
			if strings.HasPrefix(check.Twin.Status, "PASS") {
				mysqlPass++
			}
			switch {
			case strings.HasPrefix(check.RonSQL.Status, "PASS"):
				ronPass++
			case strings.HasPrefix(check.RonSQL.Status, "REJECT"):
				rejected++
			case check.RonSQL.Status == "UNTESTED":
				untested++
			}
			if !quiet || check.failed() {
				fmt.Fprintf(out, "  DTO %d %s mysql=%s ronsql=%s\n",
					check.Index, request.Label, check.Twin.Status, check.RonSQL.Status)
				if check.Twin.Message != "" || check.RonSQL.Message != "" {
					fmt.Fprintf(out, "    mysql=%q ronsql=%q\n", check.Twin.Message, check.RonSQL.Message)
				}
			}
		}
	}
	fmt.Fprintf(out, "SUMMARY golden selected=%d attempted=%d failed=%d mysql-pass=%d ronsql-pass=%d ronsql-rejected=%d ronsql-untested=%d\n",
		selected, len(results), failed, mysqlPass, ronPass, rejected, untested)
	if failed > 0 || len(results) != selected {
		return fmt.Errorf("golden run: %d failed fixture(s), %d not attempted", failed, selected-len(results))
	}
	if mysqlPass == 0 {
		fmt.Fprintln(out, "UNTESTED no successful SQL comparisons")
	}
	return nil
}

func goldenReportOutsideCorpus(dir, reportPath string) error {
	realDir := func(path string) (string, error) {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		return filepath.EvalSymlinks(absolute)
	}
	corpus, err := realDir(dir)
	if err != nil {
		return err
	}
	parent, err := realDir(filepath.Dir(reportPath))
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(corpus, parent)
	if err != nil {
		return err
	}
	if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return fmt.Errorf("golden report must be outside the captured corpus directory")
	}
	return nil
}
