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

// .fs_verify --vectors: the L2 vector oracle (RONDB-1121 E4,
// framework_design.md §8).  For every spec of the catalog, over seeded
// entity keys, the RonSQL template set (RDRS) and the production MySQL
// statement are executed, folded the way the serving client folds them,
// and compared per entity; the MySQL-side fold is also checked against
// the data-model expectation of the spec.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// specResult is one SPEC line.
type specResult struct {
	ID               string   `json:"id"`
	Shape            string   `json:"shape"`
	Status           string   `json:"status"`
	Message          string   `json:"message,omitempty"`
	Keys             int      `json:"keys"`
	Units            int      `json:"units"`
	Compared         int      `json:"compared"`
	MySQLOnlyGroups  int      `json:"mysqlOnlyGroups"`
	UncomparedGroups int      `json:"uncomparedGroups"`
	LeftMiss         int      `json:"leftMiss"`
	NotServed        []string `json:"notServed,omitempty"`
	Mismatches       int      `json:"mismatches"`
	ExpectMismatches int      `json:"expectMismatches"`
	LatencyMS        float64  `json:"latencyMs"`
	Detail           []string `json:"detail,omitempty"`
	Statements       []string `json:"statements,omitempty"`
}

type vectorOpts struct {
	verifyOpts
	seed   int64
	count  int
	specID string
}

const maxDetail = 10

// .fs_verify --vectors [--spec ID | --shape S7,S8] [--seed 1] [--count 120] [--db D] [--sf 0.01]
//
//	[--now TS] [--threads N] [--timeout 30s] [--tolerance 1e-9] [--dump-dir P] [--json P] [--quiet]
func (s *Shell) runFSVectors(a fsArgs) error {
	base, err := parseVerifyOpts(a)
	if err != nil {
		return err
	}
	o := vectorOpts{verifyOpts: base, seed: 1, count: 120, specID: a.str("spec", "")}
	if v, ok := a.flags["seed"]; ok {
		if o.seed, err = strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("invalid --seed %s", v)
		}
	}
	if v, ok := a.flags["count"]; ok {
		if o.count, err = strconv.Atoi(v); err != nil || o.count <= 0 {
			return fmt.Errorf("invalid --count %s", v)
		}
	}
	if s.mysqlClient == nil || s.restClient == nil {
		return fmt.Errorf("MySQL not connected. .fs_verify --vectors needs MySQL and RDRS.")
	}
	now := data.FSNow
	if o.now != "" {
		if now, err = time.Parse(time.RFC3339, o.now); err != nil {
			return fmt.Errorf("invalid --now (RFC 3339 expected): %w", err)
		}
	}
	specs, err := cases.Specs(cases.Config{DB: o.db, Scale: data.NewScale(o.sf), Now: now})
	if err != nil {
		return err
	}
	var selected []*cases.Spec
	for i := range specs {
		sp := &specs[i]
		if o.specID != "" && sp.ID != o.specID {
			continue
		}
		if o.shapes != nil && !o.shapes[sp.Shape] {
			continue
		}
		selected = append(selected, sp)
	}
	if len(selected) == 0 {
		return fmt.Errorf("no specs selected")
	}
	if !o.quiet {
		fmt.Println(ui.Info(fmt.Sprintf(".fs_verify --vectors: %d specs, db=%s sf=%g now=%s seed=%d count=%d threads=%d timeout=%v",
			len(selected), o.db, o.sf, now.UTC().Format(time.RFC3339), o.seed, o.count, o.threads, o.timeout)))
	}
	if err := s.fsWarmRDRS(o.verifyOpts); err != nil {
		return err
	}
	threads := o.threads
	if threads > len(selected) {
		threads = len(selected)
	}
	results := make([]specResult, len(selected))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var engineErr error
	var once sync.Once
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			my, rd, _, err := s.fsEngines(o.verifyOpts)
			if err != nil {
				once.Do(func() { engineErr = err })
				for range jobs {
				}
				return
			}
			defer my.Close()
			defer rd.Close()
			for i := range jobs {
				results[i] = runSpec(context.Background(), selected[i], my, rd, o)
			}
		}()
	}
	for i := range selected {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if engineErr != nil {
		return engineErr
	}
	counts := map[string]int{}
	byShape := map[string][]string{}
	for _, r := range results {
		line := fmt.Sprintf("SPEC %s %s %s keys=%d units=%d compared=%d left-miss=%d", r.ID, r.Shape, r.Status, r.Keys, r.Units, r.Compared, r.LeftMiss)
		if r.MySQLOnlyGroups > 0 {
			line += fmt.Sprintf(" mysql-only-groups=%d", r.MySQLOnlyGroups)
		}
		if r.UncomparedGroups > 0 {
			line += fmt.Sprintf(" uncompared-groups=%d", r.UncomparedGroups)
		}
		if len(r.NotServed) > 0 {
			line += " not-served=" + strings.Join(r.NotServed, ",")
		}
		if !o.quiet {
			// Latency is omitted in --quiet mode so the MTR-recorded lines stay stable.
			line += fmt.Sprintf(" %.1f", r.LatencyMS)
		}
		if r.Message != "" {
			line += " " + r.Message
		}
		if !o.quiet || isFailure(r.Status) {
			fmt.Println(line)
			for _, d := range r.Detail {
				fmt.Println("      | " + d)
			}
		}
		counts[r.Status]++
		byShape[r.Shape] = append(byShape[r.Shape], r.Status)
	}
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fails := 0
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", strings.ToLower(k), counts[k]))
		if isFailure(k) {
			fails += counts[k]
		}
	}
	fmt.Printf("SUMMARY vectors specs=%d %s\n", len(results), strings.Join(parts, " "))
	var shapes []string
	for sh := range byShape {
		shapes = append(shapes, sh)
	}
	sort.Strings(shapes)
	for _, sh := range shapes {
		fmt.Printf("SHAPE %s %s\n", sh, shapeStatus(byShape[sh]))
	}
	if o.dumpDir != "" {
		if err := os.MkdirAll(o.dumpDir, 0o755); err != nil {
			return err
		}
		for _, r := range results {
			if !isFailure(r.Status) {
				continue
			}
			body := r.Status + "\n" + r.Message + "\n" + strings.Join(r.Detail, "\n") + "\n"
			for _, st := range r.Statements {
				body += "-- statement --\n" + st + "\n"
			}
			_ = os.WriteFile(filepath.Join(o.dumpDir, r.ID+".txt"), []byte(body), 0o644)
		}
	}
	if o.jsonPath != "" {
		raw, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(o.jsonPath, raw, 0o644); err != nil {
			return err
		}
	}
	if fails > 0 {
		return fmt.Errorf("%d spec(s) failed", fails)
	}
	return nil
}

// vectorQuerier is the execution surface needed by the spec runner.
type vectorQuerier interface {
	Query(context.Context, string) exec.Response
}

// setMismatchStatus gives observed mismatches precedence over non-failing
// outcomes. Keep the first mismatch's statements and details intact.
func (r *specResult) setMismatchStatus() bool {
	switch {
	case r.Mismatches > 0:
		r.Status, r.Message = "FAIL", fmt.Sprintf("%d vector mismatch(es)", r.Mismatches)
	case r.ExpectMismatches > 0:
		r.Status, r.Message = "EXPECT-FAIL", fmt.Sprintf("%d data-model mismatch(es) on the MySQL path", r.ExpectMismatches)
	default:
		return false
	}
	return true
}

// runSpec executes one spec over its sampled keys.
func runSpec(ctx context.Context, sp *cases.Spec, my, rd vectorQuerier, o vectorOpts) specResult {
	res := specResult{ID: sp.ID, Shape: sp.Shape, Status: "PASS"}
	keys, err := sp.SampleKeys(o.seed, o.count)
	if err != nil {
		res.Status, res.Message = "SAMPLE-ERROR", err.Error()
		return res
	}
	res.Keys = len(keys)
	modelPolicy := vector.Policy{Tolerance: o.tolerance}
	notServed := map[string]bool{}
	expected := sp.ExpectedFeatures()
	var only map[string]bool
	if len(expected) > 0 {
		only = map[string]bool{}
		for _, f := range expected {
			only[f] = true
		}
	}
	fail := func(status, message string, statements ...string) specResult {
		res.NotServed = sortedNames(notServed)
		if !isFailure(status) && res.setMismatchStatus() {
			res.Message += "; stopped after " + status + ": " + message
			return res
		}
		res.Status, res.Message, res.Statements = status, message, statements
		return res
	}
	for _, unit := range sp.Units(keys) {
		res.Units++
		groups, err := sp.Bind(unit)
		if err != nil {
			return fail("BIND-ERROR", err.Error())
		}
		for _, g := range groups {
			if g.MySQL == nil {
				var statements []string
				for _, st := range g.Statements {
					statements = append(statements, st.RonSQL)
				}
				return fail("REFERENCE-ERROR", fmt.Sprintf("DTO %d lacks its production MySQL query",
					g.DTO.PreparedStatementIndex), statements...)
			}
			if len(g.Statements) == 0 {
				// No RonSQL evidence: this may be a point read or a gated DTO.
				res.MySQLOnlyGroups++
				continue
			}
			plan := sp.Plan(g.DTO)
			pol := vector.Policy{MissingEqualsNull: sp.Left && plan.Kind == vector.Snowflake, Tolerance: o.tolerance}
			statements := []string{*g.MySQL}
			myResp := my.Query(ctx, *g.MySQL)
			if myResp.Outcome != exec.OK {
				return fail("MYSQL-ERROR", firstLine(myResp.Message), statements...)
			}
			var ronResults []*exec.Result
			for _, st := range g.Statements {
				statements = append(statements, st.RonSQL)
				rr := rd.Query(ctx, st.RonSQL)
				if rr.Result != nil {
					res.LatencyMS += float64(rr.Result.Latency.Microseconds()) / 1000
				}
				switch rr.Outcome {
				case exec.OK:
					ronResults = append(ronResults, rr.Result)
				case exec.CleanReject:
					if sp.ExpectReject != nil && strings.Contains(rr.Message, sp.ExpectReject.Pattern) {
						return fail("REJECT(expected)", sp.ExpectReject.Finding, statements...)
					}
					if o.allowReject {
						return fail("REJECT(allowed)", firstLine(rr.Message), statements...)
					}
					return fail("REJECT", firstLine(rr.Message), statements...)
				default:
					return fail(rr.Outcome.String(), firstLine(rr.Message), statements...)
				}
			}
			types := vector.Types(myResp.Result)
			refV, err := vector.FoldMysql(plan, myResp.Result, unit)
			if err != nil {
				return fail("FOLD-ERROR", "mysql: "+err.Error(), statements...)
			}
			gotV, err := vector.FoldRonsql(plan, ronResults, unit)
			if err != nil {
				return fail("FOLD-ERROR", "ronsql: "+err.Error(), statements...)
			}
			rep := vector.Compare(plan, refV, gotV, unit, types, pol, nil)
			if rep.Compared == 0 {
				res.UncomparedGroups++
			}
			res.Compared += rep.Compared
			res.LeftMiss += rep.LeftMiss
			for _, f := range rep.NotServed {
				notServed[f] = true
			}
			if len(rep.Mismatches) > 0 {
				if res.Mismatches == 0 {
					res.Statements = statements
				}
				res.Mismatches += len(rep.Mismatches)
				for _, m := range rep.Mismatches {
					if len(res.Detail) < maxDetail {
						res.Detail = append(res.Detail, fmt.Sprintf("key=%s feature=%s mysql=%s ronsql=%s", m.Key, m.Feature, m.Ref, m.Got))
					}
				}
			}
			if only != nil {
				expV := vector.Vectors{}
				for _, k := range unit {
					if v, ok := sp.Expected(k); ok {
						expV[k.Text()] = v
					}
				}
				erep := vector.Compare(plan, expV, refV, unit, types, modelPolicy, only)
				if len(erep.Mismatches) > 0 {
					if res.Mismatches == 0 && res.ExpectMismatches == 0 {
						res.Statements = statements
					}
					res.ExpectMismatches += len(erep.Mismatches)
					for _, m := range erep.Mismatches {
						if len(res.Detail) < maxDetail {
							res.Detail = append(res.Detail, fmt.Sprintf("EXPECT key=%s feature=%s data-model=%s mysql=%s", m.Key, m.Feature, m.Ref, m.Got))
						}
					}
				}
			}
		}
	}
	res.NotServed = sortedNames(notServed)
	if res.setMismatchStatus() {
		return res
	}
	switch {
	case res.Compared == 0:
		res.Status, res.Message = "UNTESTED", "no vector cells compared"
	case res.UncomparedGroups > 0:
		res.Status, res.Message = "UNTESTED", fmt.Sprintf("%d executable DTO group(s) compared no vector cells", res.UncomparedGroups)
	case sp.ExpectReject != nil:
		res.Status, res.Message = "PASS(was-expected-reject)", sp.ExpectReject.Finding+" no longer rejects"
	}
	return res
}

func sortedNames(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
