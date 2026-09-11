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

// .fs_fuzz: the spec-level random generator runner (RONDB-1121 E6,
// random_generator.md §2-§4, §6-§8).  Each case is a seeded feature-view
// specification; the runner checks the emitter against the generator's
// expectation (gate / template count), runs every RonSQL template on both
// engines (L1), optionally folds the vectors against the MySQL twins (L2),
// classifies the outcome, de-duplicates by shape signature and shrinks
// failures.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/fuzz"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// fuzzResult is one CASE line of a fuzz run.
type fuzzResult struct {
	ID         string   `json:"id"`
	Seed       uint64   `json:"seed"`
	Index      int      `json:"index"`
	Kind       string   `json:"kind"`
	Status     string   `json:"status"`
	Message    string   `json:"message,omitempty"`
	Signature  string   `json:"signature"`
	KeyClass   string   `json:"keyClass,omitempty"`
	Templates  int      `json:"templates"`
	Statements []string `json:"statements,omitempty"`
	Diff       string   `json:"diff,omitempty"`
	LatencyMS  float64  `json:"latencyMs"`
	Finding    string   `json:"finding,omitempty"`
	ShrunkView string   `json:"shrunkView,omitempty"`
	ShrinkStep int      `json:"shrinkSteps,omitempty"`
}

// Severity order for the case status (the worst statement wins).
var fuzzSeverity = map[string]int{
	"PASS": 0, "SKIP": 1, "NO-TEMPLATE": 1, "GATED": 1, "CLEAN-REJECT": 2, "KNOWN-ERROR": 3, "KNOWN-WRONG": 3, "KNOWN": 3,
	"TEMPLATE-MISMATCH": 10, "GATE-MISMATCH": 11, "BIND-ERROR": 12, "MYSQL-ERROR": 13, "ERROR": 14,
	"REJECT-UNEXPECTED": 15, "WRONG-RESULT": 16, "RETRY-EXHAUSTED": 17, "TIMEOUT": 18, "CRASH": 19,
}

func fuzzIsFailure(status string) bool { return fuzzSeverity[status] >= 10 }

type fuzzOpts struct {
	verifyOpts
	seed          uint64
	count         int
	vectors       bool
	directCollect bool
	maxRows       int
	shrink        bool
	shrinkSteps   int
	ledgerPath    string
	allowKnown    bool
	knownSigs     map[string]string // signature -> finding (from --ledger)
}

// .fs_fuzz spec --seed S --count N [--sf 0.01] [--db test] [--vectors] [--direct-collect]
//
//	[--threads 4] [--timeout 30s] [--max-rows 10000] [--shrink] [--shrink-steps 200]
//	[--dump-dir P] [--json P] [--ledger P] [--allow-known] [--quiet]
//
// .fs_fuzz show --seed S --index I [--sf] [--db]
// .fs_fuzz replay --file P [same flags as spec]
func (s *Shell) runFSFuzz(args []string) error {
	a := parseFSArgs(args)
	if len(a.pos) == 0 {
		return fmt.Errorf("usage: .fs_fuzz spec|show|replay ... (see .help)")
	}
	base, err := parseVerifyOpts(a)
	if err != nil {
		return err
	}
	o := fuzzOpts{verifyOpts: base, seed: 1, count: 200, maxRows: 10000, shrinkSteps: 200}
	if v, ok := a.flags["seed"]; ok {
		if o.seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			return fmt.Errorf("invalid --seed %s", v)
		}
	}
	if v, ok := a.flags["count"]; ok {
		if o.count, err = strconv.Atoi(v); err != nil || o.count <= 0 {
			return fmt.Errorf("invalid --count %s", v)
		}
	}
	if v, ok := a.flags["max-rows"]; ok {
		if o.maxRows, err = strconv.Atoi(v); err != nil || o.maxRows <= 0 {
			return fmt.Errorf("invalid --max-rows %s", v)
		}
	}
	if v, ok := a.flags["shrink-steps"]; ok {
		if o.shrinkSteps, err = strconv.Atoi(v); err != nil || o.shrinkSteps <= 0 {
			return fmt.Errorf("invalid --shrink-steps %s", v)
		}
	}
	o.vectors, o.directCollect, o.shrink, o.allowKnown = a.has("vectors"), a.has("direct-collect"), a.has("shrink"), a.has("allow-known")
	o.ledgerPath = a.str("ledger", "")
	o.knownSigs = map[string]string{}
	if o.ledgerPath != "" {
		if raw, err := os.ReadFile(o.ledgerPath); err == nil {
			var ledger struct {
				Known []struct {
					Signature, Finding string
				} `json:"known"`
			}
			if err := json.Unmarshal(raw, &ledger); err != nil {
				return fmt.Errorf("--ledger %s: %w", o.ledgerPath, err)
			}
			for _, k := range ledger.Known {
				o.knownSigs[k.Signature] = k.Finding
			}
		}
	}
	now := data.FSNow
	if o.now != "" {
		if now, err = time.Parse(time.RFC3339, o.now); err != nil {
			return fmt.Errorf("invalid --now (RFC 3339 expected): %w", err)
		}
	}
	g := fuzz.New(fuzz.Config{DB: o.db, Scale: data.NewScale(o.sf), Now: now})
	switch a.pos[0] {
	case "show":
		idx := 0
		if v, ok := a.flags["index"]; ok {
			if idx, err = strconv.Atoi(v); err != nil || idx < 0 {
				return fmt.Errorf("invalid --index %s", v)
			}
		}
		return showFuzzCase(g.Case(o.seed, idx), now)
	case "spec":
		var ids []fuzzCaseID
		for i := 0; i < o.count; i++ {
			ids = append(ids, fuzzCaseID{o.seed, i})
		}
		return s.runFuzzCases(g, ids, o)
	case "replay":
		file := a.str("file", "")
		if file == "" {
			return fmt.Errorf(".fs_fuzz replay needs --file <results.json>")
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		var prev []fuzzResult
		if err := json.Unmarshal(raw, &prev); err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		var ids []fuzzCaseID
		for _, r := range prev {
			ids = append(ids, fuzzCaseID{r.Seed, r.Index})
		}
		if len(ids) == 0 {
			return fmt.Errorf("%s holds no cases", file)
		}
		return s.runFuzzCases(g, ids, o)
	}
	return fmt.Errorf("unknown .fs_fuzz subcommand %s (spec | show | replay)", a.pos[0])
}

type fuzzCaseID struct {
	seed  uint64
	index int
}

func showFuzzCase(c fuzz.Case, now time.Time) error {
	raw, _ := json.MarshalIndent(c, "", "  ")
	fmt.Println(string(raw))
	if c.Kind == "definition" {
		_, err := spec.ValidateDefinition(c.View)
		fmt.Printf("ValidateDefinition: %v (expected gate %q)\n", err, c.Expect.Gate)
		return nil
	}
	dtos, err := emit.Build(c.View)
	if err != nil {
		fmt.Printf("Build: %v (expected gate %q)\n", err, c.Expect.Gate)
		return nil
	}
	keys, err := fuzzKeys(c)
	if err != nil {
		return err
	}
	for _, d := range dtos {
		g, err := cases.BindDTO(d, keys, c.Batch > 0, now)
		if err != nil {
			fmt.Printf("DTO %d: bind error %v\n", d.PreparedStatementIndex, err)
			continue
		}
		for _, st := range g.Statements {
			fmt.Printf("DTO %d [%s] %s\n", d.PreparedStatementIndex, st.Label, st.RonSQL)
		}
		if g.MySQL != nil {
			fmt.Printf("DTO %d [mysql twin] %s\n", d.PreparedStatementIndex, strings.ReplaceAll(*g.MySQL, "\n", " "))
		}
	}
	return nil
}

// fuzzKeys renders the case's keys as typed DTO keys.
func fuzzKeys(c fuzz.Case) ([]cases.DTOKey, error) {
	var root spec.FeatureGroup
	found := false
	for _, fg := range c.View.FGs {
		if len(c.View.Joins) > 0 && fg.ID == c.View.Joins[0].FG {
			root, found = fg, true
		}
	}
	if !found {
		return nil, fmt.Errorf("no root feature group")
	}
	var keys []cases.DTOKey
	for _, k := range c.Keys {
		dk := cases.DTOKey{}
		for i, name := range c.KeyNames {
			f, ok := root.Feature(name)
			if !ok || i >= len(k) {
				return nil, fmt.Errorf("key parameter %s", name)
			}
			dk[name] = bind.Scalar(bind.LiteralFor(f.Type, k[i]))
		}
		keys = append(keys, dk)
	}
	return keys, nil
}

func (s *Shell) runFuzzCases(g *fuzz.Gen, ids []fuzzCaseID, o fuzzOpts) error {
	if s.mysqlClient == nil || s.restClient == nil {
		return fmt.Errorf("MySQL not connected. .fs_fuzz needs MySQL and RDRS.")
	}
	if !o.quiet {
		fmt.Println(ui.Info(fmt.Sprintf(".fs_fuzz spec: %d cases, seed=%d db=%s sf=%g threads=%d timeout=%v vectors=%v direct-collect=%v shrink=%v",
			len(ids), o.seed, o.db, o.sf, o.threads, o.timeout, o.vectors, o.directCollect, o.shrink)))
	}
	threads := o.threads
	if threads > len(ids) {
		threads = len(ids)
	}
	results := make([]fuzzResult, len(ids))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var engineErr error
	var once sync.Once
	var crashed int32
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
				c := g.Case(ids[i].seed, ids[i].index)
				if atomic.LoadInt32(&crashed) != 0 {
					results[i] = fuzzResult{ID: c.ID, Seed: c.Seed, Index: c.Index, Kind: c.Kind, Status: "SKIP", Message: "run stopped after a crash", Signature: c.Signature}
					continue
				}
				r := runFuzzCase(context.Background(), c, my, rd, o)
				if r.Status == "CRASH" {
					atomic.StoreInt32(&crashed, 1)
				}
				if o.shrink && fuzzIsFailure(r.Status) && r.Status != "CRASH" && r.Status != "TIMEOUT" {
					min, steps := fuzz.Shrink(c, func(k fuzz.Case) string { return runFuzzCase(context.Background(), k, my, rd, o).Status }, o.shrinkSteps)
					raw, _ := json.Marshal(min.View)
					r.ShrunkView, r.ShrinkStep = string(raw), steps
					mr := runFuzzCase(context.Background(), min, my, rd, o)
					if len(mr.Statements) > 0 {
						r.Statements = mr.Statements
					}
				}
				results[i] = r
			}
		}()
	}
	for i := range ids {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if engineErr != nil {
		return engineErr
	}
	counts := map[string]int{}
	sigs := map[string]int{}
	fails := 0
	for _, r := range results {
		status := r.Status
		if o.allowKnown && fuzzIsFailure(status) {
			if f, ok := o.knownSigs[r.Signature]; ok {
				status = "KNOWN"
				r.Finding = f
			}
		}
		counts[status]++
		if fuzzIsFailure(status) {
			fails++
			sigs[r.Signature]++
		}
		if !o.quiet || fuzzIsFailure(status) {
			line := fmt.Sprintf("CASE %s %s %s %.1f %s", r.ID, r.Kind, status, r.LatencyMS, r.Signature)
			if r.Message != "" {
				line += " " + r.Message
			}
			fmt.Println(line)
			if r.Diff != "" && !o.quiet {
				fmt.Print(indent(r.Diff))
			}
		}
	}
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", strings.ToLower(k), counts[k]))
	}
	fmt.Printf("SUMMARY fuzz-spec seed=%d cases=%d %s\n", o.seed, len(results), strings.Join(parts, " "))
	if len(sigs) > 0 {
		var ss []string
		for k := range sigs {
			ss = append(ss, k)
		}
		sort.Strings(ss)
		for _, k := range ss {
			fmt.Printf("SIGNATURE %d %s\n", sigs[k], k)
		}
	}
	if o.dumpDir != "" {
		if err := os.MkdirAll(o.dumpDir, 0o755); err != nil {
			return err
		}
		for _, r := range results {
			if !fuzzIsFailure(r.Status) {
				continue
			}
			c := g.Case(r.Seed, r.Index)
			raw, _ := json.MarshalIndent(struct {
				Result fuzzResult `json:"result"`
				Case   fuzz.Case  `json:"case"`
			}{r, c}, "", "  ")
			_ = os.WriteFile(filepath.Join(o.dumpDir, r.ID+".json"), raw, 0o644)
			_ = os.WriteFile(filepath.Join(o.dumpDir, r.ID+".sql"), []byte(strings.Join(r.Statements, "\n")+"\n"), 0o644)
		}
	}
	if o.jsonPath != "" {
		raw, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(o.jsonPath, raw, 0o644); err != nil {
			return err
		}
	}
	if fails > 0 {
		return fmt.Errorf("%d case(s) failed (%d distinct signature(s))", fails, len(sigs))
	}
	return nil
}

// runFuzzCase classifies one case (random_generator.md §3).
func runFuzzCase(ctx context.Context, c fuzz.Case, my vectorQuerier, rd vectorQuerier, o fuzzOpts) fuzzResult {
	res := fuzzResult{ID: c.ID, Seed: c.Seed, Index: c.Index, Kind: c.Kind, Status: "PASS", Signature: c.Signature, KeyClass: c.KeyClass}
	worst := func(status, message string) {
		if fuzzSeverity[status] > fuzzSeverity[res.Status] {
			res.Status, res.Message = status, message
		}
	}
	var ge *spec.GateError
	if c.Kind == "definition" {
		_, err := spec.ValidateDefinition(c.View)
		switch {
		case err == nil && c.Expect.Gate == "":
			res.Status = "PASS"
		case err != nil && errors.As(err, &ge) && ge.Code == c.Expect.Gate:
			res.Status, res.Message = "GATED", ge.Code
		default:
			res.Status, res.Message = "GATE-MISMATCH", fmt.Sprintf("expected gate %q, got %v", c.Expect.Gate, err)
		}
		return res
	}
	dtos, err := emit.Build(c.View)
	if err != nil {
		switch {
		case errors.As(err, &ge) && ge.Code == c.Expect.Gate:
			res.Status, res.Message = "GATED", ge.Code
		case errors.As(err, &ge):
			res.Status, res.Message = "GATE-MISMATCH", fmt.Sprintf("expected gate %q, got %s: %s", c.Expect.Gate, ge.Code, ge.Message)
		default:
			res.Status, res.Message = "BIND-ERROR", "build: "+err.Error()
		}
		return res
	}
	if c.Expect.Gate != "" {
		res.Status, res.Message = "GATE-MISMATCH", fmt.Sprintf("expected gate %q, the emitter built %d statement(s)", c.Expect.Gate, len(dtos))
		return res
	}
	for _, d := range dtos {
		res.Templates += d.TemplateCount()
	}
	if c.Expect.Templates >= 0 && res.Templates != c.Expect.Templates {
		res.Status, res.Message = "TEMPLATE-MISMATCH", fmt.Sprintf("%d template(s) emitted, expected %d", res.Templates, c.Expect.Templates)
		return res
	}
	if res.Templates == 0 {
		res.Status, res.Message = "NO-TEMPLATE", "MySQL-only statement set"
		return res
	}
	if my == nil || rd == nil {
		res.Status, res.Message = "SKIP", "no engines"
		return res
	}
	keys, err := fuzzKeys(c)
	if err != nil {
		res.Status, res.Message = "BIND-ERROR", err.Error()
		return res
	}
	for _, d := range dtos {
		if d.TemplateCount() == 0 {
			continue
		}
		g, err := cases.BindDTO(d, keys, c.Batch > 0, o.nowOr())
		if err != nil {
			res.Status, res.Message = "BIND-ERROR", err.Error()
			return res
		}
		var ronResults []*exec.Result
		allOK := true
		for _, st := range g.Statements {
			sqlText := st.RonSQL
			isCTECollect := strings.HasPrefix(sqlText, "WITH t AS (")
			if o.directCollect {
				sqlText, _ = fuzz.DirectCollect(sqlText)
				isCTECollect = false
			}
			res.Statements = append(res.Statements, sqlText)
			ordered := strings.Contains(sqlText, " ORDER BY ")
			myResp := my.Query(ctx, sqlText)
			if myResp.Outcome != exec.OK {
				worst("MYSQL-ERROR", firstLine(myResp.Message))
				allOK = false
				continue
			}
			if len(myResp.Result.Rows) > o.maxRows {
				worst("SKIP", fmt.Sprintf("result of %d rows exceeds --max-rows", len(myResp.Result.Rows)))
				allOK = false
				continue
			}
			rr := rd.Query(ctx, sqlText)
			if rr.Result != nil {
				res.LatencyMS += float64(rr.Result.Latency.Microseconds()) / 1000
			}
			switch rr.Outcome {
			case exec.OK:
				rep := canon.Compare(myResp.Result, rr.Result, canon.Options{Ordered: ordered, Tolerance: o.tolerance})
				if !rep.Equal {
					if strings.HasPrefix(sqlText, "WITH `b` AS (") && stringRootKey(c) && len(rr.Result.Rows) == 0 && len(myResp.Result.Rows) > 0 {
						// F14: the string-keyed snowflake body yields nothing through CTE_SCAN.
						worst("KNOWN-WRONG", "F14: "+rep.Reason)
						res.Finding = cases.Known["F14"].Finding
					} else {
						worst("WRONG-RESULT", rep.Reason)
					}
					if res.Diff == "" {
						res.Diff = rep.Diff
					}
					allOK = false
					continue
				}
				ronResults = append(ronResults, rr.Result)
			case exec.CleanReject:
				if isCTECollect && strings.Contains(rr.Message, cases.Known["S6-cte"].Pattern) {
					worst("CLEAN-REJECT", "F0")
					res.Finding = "F0"
				} else {
					worst("REJECT-UNEXPECTED", firstLine(rr.Message))
				}
				allOK = false
			case exec.Retryable:
				worst("RETRY-EXHAUSTED", firstLine(rr.Message))
				allOK = false
			case exec.Timeout:
				worst("TIMEOUT", firstLine(rr.Message))
				allOK = false
			case exec.Crash:
				worst("CRASH", firstLine(rr.Message))
				return res
			default:
				if c.Expect.KnownError == "F9" && strings.Contains(rr.Message, cases.Known["F9"].Pattern) {
					worst("KNOWN-ERROR", "F9: "+firstLine(rr.Message))
					res.Finding = "F9"
				} else {
					worst("ERROR", firstLine(rr.Message))
				}
				allOK = false
			}
		}
		if o.vectors && allOK && g.MySQL != nil && len(ronResults) == len(g.Statements) {
			plan := vector.PlanFor(d, c.Batch > 0, fuzzStructFields(c, d))
			if plan.Kind == vector.PointRead {
				continue
			}
			twin := my.Query(ctx, *g.MySQL)
			if twin.Outcome != exec.OK {
				worst("MYSQL-ERROR", "twin: "+firstLine(twin.Message))
				continue
			}
			refV, err := vector.FoldMysql(plan, twin.Result, c.Keys)
			if err != nil {
				worst("BIND-ERROR", "fold mysql: "+err.Error())
				continue
			}
			gotV, err := vector.FoldRonsql(plan, ronResults, c.Keys)
			if err != nil {
				worst("BIND-ERROR", "fold ronsql: "+err.Error())
				continue
			}
			pol := vector.Policy{MissingEqualsNull: c.LeftSubtree && plan.Kind == vector.Snowflake, Tolerance: o.tolerance}
			rep := vector.Compare(plan, refV, gotV, c.Keys, vector.Types(twin.Result), pol, nil)
			if len(rep.Mismatches) > 0 {
				m := rep.Mismatches[0]
				worst("WRONG-RESULT", fmt.Sprintf("vector: %d mismatch(es), first key=%s feature=%s mysql=%s ronsql=%s", len(rep.Mismatches), m.Key, m.Feature, m.Ref, m.Got))
			}
		}
	}
	return res
}

// stringRootKey reports whether the case's entity key is a string column.
func stringRootKey(c fuzz.Case) bool {
	if len(c.KeyNames) == 0 || len(c.View.Joins) == 0 {
		return false
	}
	for _, fg := range c.View.FGs {
		if fg.ID != c.View.Joins[0].FG {
			continue
		}
		if f, ok := fg.Feature(c.KeyNames[0]); ok {
			return spec.BaseType(f.Type) == "string"
		}
	}
	return false
}

// fuzzStructFields returns the collect struct fields of the join that
// produced the DTO (by prepared-statement index).
func fuzzStructFields(c fuzz.Case, d emit.Statement) []string {
	for _, j := range c.View.Joins {
		if j.Index != d.PreparedStatementIndex || j.CollectN == nil {
			continue
		}
		for _, f := range j.Features {
			if f.Type != nil {
				if names := emit.ParseStructFieldNames(*f.Type); len(names) > 0 {
					return names
				}
			}
		}
	}
	return nil
}

func (o fuzzOpts) nowOr() time.Time {
	if o.now != "" {
		if t, err := time.Parse(time.RFC3339, o.now); err == nil {
			return t
		}
	}
	return data.FSNow
}
