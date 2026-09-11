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

// .fs_fuzz envelope: the envelope-level generator runner (RONDB-1121 E7,
// random_generator.md §5): each case is one RonSQL statement over the
// feature-store schema, run on MySQL (same text) and RDRS, compared typed
// (L1) and classified against the expectation table (L3).  Hazards run
// only with --include-hazards, single-threaded, with a short timeout and
// a crash probe after each.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/fuzz"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

const hazardTimeout = 10 * time.Second

func init() {
	fuzzSeverity["PASS(hazard)"] = 0
	fuzzSeverity["PASS(was-expected-reject)"] = 0
	fuzzSeverity["KNOWN-HAZARD"] = 3
}

// runFSFuzzEnvelope implements `.fs_fuzz envelope --seed S --count N
// [--include-hazards] [--only prod,prod] [--threads] [--timeout 15s]
// [--max-rows] [--dump-dir] [--json] [--ledger P --allow-known] [--quiet]`.
func (s *Shell) runFSFuzzEnvelope(a fsArgs, o fuzzOpts) error {
	if s.mysqlClient == nil || s.restClient == nil {
		return fmt.Errorf("MySQL not connected. .fs_fuzz envelope needs MySQL and RDRS.")
	}
	if _, ok := a.flags["timeout"]; !ok {
		o.timeout = 15 * time.Second
	}
	var only map[string]bool
	if v := a.str("only", ""); v != "" {
		only = map[string]bool{}
		for _, p := range strings.Split(v, ",") {
			only[strings.TrimSpace(p)] = true
		}
	}
	g := fuzz.NewEnvelope(fuzz.Config{DB: o.db, Scale: data_scale(o.sf), Now: o.nowOr()})
	var cs []fuzz.EnvCase
	for i := 0; i < o.count; i++ {
		cs = append(cs, g.Case(o.seed, i, only))
	}
	hazards := a.has("include-hazards")
	if !o.quiet {
		fmt.Println(ui.Info(fmt.Sprintf(".fs_fuzz envelope: %d cases, seed=%d db=%s sf=%g threads=%d timeout=%v hazards=%v",
			len(cs), o.seed, o.db, o.sf, o.threads, o.timeout, hazards)))
	}
	results := make([]fuzzResult, len(cs))
	threads := o.threads
	if threads > len(cs) {
		threads = len(cs)
	}
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
				if atomic.LoadInt32(&crashed) != 0 {
					results[i] = fuzzResult{ID: cs[i].ID, Seed: cs[i].Seed, Index: cs[i].Index, Kind: cs[i].Production, Status: "SKIP",
						Message: "run stopped after a crash", Signature: cs[i].Signature}
					continue
				}
				results[i] = runEnvCase(context.Background(), cs[i], my, rd, o)
				if results[i].Status == "CRASH" {
					atomic.StoreInt32(&crashed, 1)
				}
			}
		}()
	}
	for i := range cs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if engineErr != nil {
		return engineErr
	}
	if hazards && atomic.LoadInt32(&crashed) == 0 {
		// single-threaded, short timeout, crash probe after each
		my, rd, _, err := s.fsEngines(o.verifyOpts)
		if err != nil {
			return err
		}
		ho := o
		ho.timeout = hazardTimeout
		for _, hc := range fuzz.HazardCases(o.seed) {
			r := runEnvCase(context.Background(), hc, my, rd, ho)
			if rd.Probe(context.Background()) != nil && r.Status != "CRASH" {
				r.Status, r.Message = "CRASH", "RDRS did not answer the probe after the hazard"
			}
			switch {
			case r.Status == "PASS":
				r.Status = "PASS(hazard)"
			case fuzzIsFailure(r.Status):
				r.Message = hc.Hazard + ": " + r.Status + " " + r.Message
				r.Status = "KNOWN-HAZARD"
			}
			r.Finding = hc.Hazard
			results = append(results, r)
			if strings.Contains(r.Message, "CRASH") {
				break
			}
		}
		my.Close()
		rd.Close()
	}
	return s.reportFuzz(results, o, "fuzz-envelope", func(r fuzzResult) fuzz.EnvCase {
		if r.Index < 0 {
			for _, hc := range fuzz.HazardCases(r.Seed) {
				if hc.ID == r.ID {
					return hc
				}
			}
		}
		return g.Case(r.Seed, r.Index, only)
	})
}

// reportFuzz prints the CASE / SUMMARY / SIGNATURE lines and writes the
// dumps and JSON of an envelope run.
func (s *Shell) reportFuzz(results []fuzzResult, o fuzzOpts, label string, regen func(fuzzResult) fuzz.EnvCase) error {
	counts := map[string]int{}
	sigs := map[string]int{}
	fails := 0
	for i := range results {
		r := &results[i]
		if o.allowKnown && fuzzIsFailure(r.Status) {
			if f, ok := o.knownSigs[r.Signature]; ok {
				r.Status, r.Finding = "KNOWN", f
			}
		}
		counts[r.Status]++
		if fuzzIsFailure(r.Status) {
			fails++
			sigs[r.Signature]++
		}
		if !o.quiet || fuzzIsFailure(r.Status) {
			line := fmt.Sprintf("CASE %s %s %s %.1f %s", r.ID, r.Kind, r.Status, r.LatencyMS, r.Signature)
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
	fmt.Printf("SUMMARY %s seed=%d cases=%d %s\n", label, o.seed, len(results), strings.Join(parts, " "))
	var ss []string
	for k := range sigs {
		ss = append(ss, k)
	}
	sort.Strings(ss)
	for _, k := range ss {
		fmt.Printf("SIGNATURE %d %s\n", sigs[k], k)
	}
	if o.dumpDir != "" {
		if err := os.MkdirAll(o.dumpDir, 0o755); err != nil {
			return err
		}
		for _, r := range results {
			if !fuzzIsFailure(r.Status) {
				continue
			}
			raw, _ := json.MarshalIndent(struct {
				Result fuzzResult   `json:"result"`
				Case   fuzz.EnvCase `json:"case"`
			}{r, regen(r)}, "", "  ")
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

// runEnvCase classifies one envelope statement (random_generator.md §3).
func runEnvCase(ctx context.Context, c fuzz.EnvCase, my vectorQuerier, rd vectorQuerier, o fuzzOpts) fuzzResult {
	res := fuzzResult{ID: c.ID, Seed: c.Seed, Index: c.Index, Kind: c.Production, Status: "PASS", Signature: c.Signature, Statements: []string{c.SQL}}
	expectReject, expectsReject := c.ExpectReject()
	known, hasKnown := c.Known()
	myResp := my.Query(ctx, c.SQL)
	if myResp.Outcome != exec.OK {
		if clusterDown(myResp.Message) {
			res.Status, res.Message = "CRASH", "cluster down (MySQL): "+firstLine(myResp.Message)
			return res
		}
		if expectsReject && expectReject.Construct == "syntax" {
			// outside both grammars by design: MySQL rejects too (e.g. RIGHT JOIN
			// without the join column, OFFSET forms) — the RonSQL side decides.
		} else {
			res.Status, res.Message = "MYSQL-ERROR", firstLine(myResp.Message)
			return res
		}
	}
	if myResp.Outcome == exec.OK && len(myResp.Result.Rows) > o.maxRows {
		res.Status, res.Message = "SKIP", fmt.Sprintf("result of %d rows exceeds --max-rows", len(myResp.Result.Rows))
		return res
	}
	rr := rd.Query(ctx, c.SQL)
	if rr.Result != nil {
		res.LatencyMS += float64(rr.Result.Latency.Microseconds()) / 1000
	}
	switch rr.Outcome {
	case exec.OK:
		if expectsReject {
			if myResp.Outcome != exec.OK {
				res.Status, res.Message = "REJECT-MISSING", expectReject.Construct+": RonSQL ran a statement MySQL rejects"
				return res
			}
			// The engine now accepts a shape the envelope expected it to reject:
			// verify the result against MySQL before treating it as supported, so
			// a newly-supported-but-wrong shape does not pass silently.
			rep := canon.Compare(myResp.Result, rr.Result, canon.Options{Ordered: c.Ordered, Tolerance: o.tolerance})
			if !rep.Equal {
				res.Diff = rep.Diff
				res.Status, res.Message = "WRONG-RESULT", "was-expected-reject "+expectReject.Construct+": "+rep.Reason
			} else {
				res.Status, res.Message = "PASS(was-expected-reject)", expectReject.Construct+": "+expectReject.Note
			}
			return res
		}
		rep := canon.Compare(myResp.Result, rr.Result, canon.Options{Ordered: c.Ordered, Tolerance: o.tolerance})
		if !rep.Equal {
			res.Diff = rep.Diff
			if kw, ok := c.KnownWrong(); ok {
				res.Status, res.Message, res.Finding = "KNOWN-WRONG", kw.Finding+": "+rep.Reason, kw.Finding
				return res
			}
			res.Status, res.Message = "WRONG-RESULT", rep.Reason
		}
	case exec.CleanReject:
		msg := firstLine(rr.Message)
		if expectsReject && expectReject.Matches(rr.Message) {
			res.Status, res.Message, res.Finding = "CLEAN-REJECT", expectReject.Construct, expectReject.Finding
			return res
		}
		for _, e := range fuzz.Expectations {
			if e.Reject && !e.TaggedOnly && e.Matches(rr.Message) {
				// a table row whose construct the generator did not tag: still a clean, known rejection
				res.Status, res.Message = "CLEAN-REJECT", e.Construct+" (untagged): "+msg
				return res
			}
		}
		res.Status, res.Message = "REJECT-UNEXPECTED", msg
	case exec.Retryable:
		res.Status, res.Message = "RETRY-EXHAUSTED", firstLine(rr.Message)
	case exec.Timeout:
		res.Status, res.Message = "TIMEOUT", firstLine(rr.Message)
	case exec.Crash:
		res.Status, res.Message = "CRASH", firstLine(rr.Message)
	default:
		if hasKnown && known.Finding == "F9" && strings.Contains(rr.Message, known.Pattern) {
			res.Status, res.Message, res.Finding = "KNOWN-ERROR", "F9: "+firstLine(rr.Message), "F9"
		} else {
			res.Status, res.Message = "ERROR", firstLine(rr.Message)
		}
	}
	return res
}
