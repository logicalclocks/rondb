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

// .fs_verify --requirements (RONDB-1121 E8, framework_design.md §9): the
// strict acceptance run.  The versioned manifest (cases.Requirements) is
// resolved against the case matrix, the vector specs and the captured
// Java fixtures; every required piece of evidence is executed — L1 on
// both engines, L2 with --vectors, in-process Java conformance — and each
// requirement is reported as SUPPORTED, UNSUPPORTED, HOPSWORKS-GATED,
// UNTESTED or FAILED.  Regression allowances (known rejections, known
// wrong results, --allow-reject) never count as supported here.  The
// report records the engine and Hopsworks commits, the configuration and
// the fixture provenance.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// requirementResult is one REQUIREMENT line.
type requirementResult struct {
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Branch   string            `json:"branch"`
	Kind     string            `json:"kind"` // emitted | framework | gate
	Status   string            `json:"status"`
	Accepted bool              `json:"accepted"`
	Cases    map[string]string `json:"cases,omitempty"`    // case id -> L1 status
	Specs    map[string]string `json:"specs,omitempty"`    // spec id -> L2 status
	Fixtures map[string]string `json:"fixtures,omitempty"` // fixture -> CONFORMANT | gate:<code> | DIFF | ERROR
	Missing  []string          `json:"missing,omitempty"`
	Findings []string          `json:"findings,omitempty"` // ledger ids behind UNSUPPORTED evidence
	Detail   []string          `json:"detail,omitempty"`
}

var findingID = regexp.MustCompile(`^(F[0-9]+)`)

// findingOf extracts the ledger id a runner attached to a non-pass status
// (REJECT(expected) carries the id as its message, KNOWN-* "F<n>: …",
// HAZARD-SKIPPED the hazard id).
func findingOf(message string) string {
	if m := findingID.FindStringSubmatch(strings.TrimSpace(message)); m != nil {
		return m[1]
	}
	return ""
}

// requirementsReport is the JSON report.
type requirementsReport struct {
	Version           string                 `json:"version"`
	GeneratedAt       string                 `json:"generatedAt"`
	EngineCommit      string                 `json:"engineCommit"`
	HopsworksCommit   string                 `json:"hopsworksCommit"`
	Label             string                 `json:"label,omitempty"`
	Configuration     map[string]interface{} `json:"configuration"`
	FixtureProvenance map[string]interface{} `json:"fixtureProvenance"`
	FixtureDir        string                 `json:"fixtureDir"`
	Requirements      []requirementResult    `json:"requirements"`
	Counts            map[string]int         `json:"counts"`
	Acceptance        string                 `json:"acceptance"`
	CaseResults       []caseResult           `json:"caseResults"`
	SpecResults       []specResult           `json:"specResults,omitempty"`
}

// .fs_verify --requirements [--all] [--vectors] [--req R-S1,R-S7] [--fixtures DIR]
//
//	[--engine-commit H] [--label TEXT] [--seed 1] [--count 120] [--db D] [--sf 0.01]
//	[--threads N] [--timeout 30s] [--tolerance 1e-9] [--now TS] [--include-hazards]
//	[--json P] [--dump-dir P] [--quiet]
func (s *Shell) runFSRequirements(a fsArgs) error {
	if a.has("allow-reject") || a.has("relaxed-headers") {
		return fmt.Errorf("--allow-reject / --relaxed-headers are regression allowances; requirements mode refuses them")
	}
	o, err := parseVerifyOpts(a)
	if err != nil {
		return err
	}
	if s.mysqlClient == nil || s.restClient == nil {
		return fmt.Errorf("MySQL not connected. .fs_verify --requirements needs MySQL and RDRS.")
	}
	now := data.FSNow
	if o.now != "" {
		if now, err = time.Parse(time.RFC3339, o.now); err != nil {
			return fmt.Errorf("invalid --now (RFC 3339 expected): %w", err)
		}
	}
	vo := vectorOpts{verifyOpts: o, seed: 1, count: 120}
	if v, ok := a.flags["seed"]; ok {
		if vo.seed, err = strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("invalid --seed %s", v)
		}
	}
	if v, ok := a.flags["count"]; ok {
		if vo.count, err = strconv.Atoi(v); err != nil || vo.count <= 0 {
			return fmt.Errorf("invalid --count %s", v)
		}
	}
	withVectors := a.has("vectors")
	var only map[string]bool
	if v := a.str("req", ""); v != "" {
		only = map[string]bool{}
		for _, id := range strings.Split(v, ",") {
			only[strings.TrimSpace(id)] = true
		}
	}
	cfg := cases.Config{DB: o.db, Scale: data.NewScale(o.sf), Now: now}
	all, err := cases.Enumerate(cfg)
	if err != nil {
		return err
	}
	specs, err := cases.Specs(cfg)
	if err != nil {
		return err
	}
	resolved := cases.ResolveRequirements(all, specs)

	// fixtures: the captured Java corpus (conformance is in-process)
	fixtureDir, err := findFixtureDir(a.str("fixtures", ""))
	if err != nil {
		return err
	}
	fixtures, err := emit.LoadGoldenFixtures(fixtureDir)
	if err != nil {
		return fmt.Errorf("golden corpus %s: %w", fixtureDir, err)
	}
	hopsworksCommit := ""
	var provenance map[string]interface{}
	for _, f := range fixtures {
		provenance = f.Provenance
		hopsworksCommit, _ = f.Provenance["hopsworksCommit"].(string)
		break
	}

	// the required case and spec sets
	caseByID := map[string]cases.Case{}
	for _, c := range all {
		caseByID[c.ID] = c
	}
	specByID := map[string]*cases.Spec{}
	for i := range specs {
		specByID[specs[i].ID] = &specs[i]
	}
	needCase, needSpec := map[string]bool{}, map[string]bool{}
	for _, r := range resolved {
		if only != nil && !only[r.ID] {
			continue
		}
		for _, id := range r.CaseIDs {
			needCase[id] = true
		}
		if withVectors {
			for _, id := range r.SpecIDs {
				needSpec[id] = true
			}
		}
	}
	var selected []cases.Case
	for id := range needCase {
		selected = append(selected, caseByID[id])
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].ID < selected[j].ID })
	var selSpecs []*cases.Spec
	for id := range needSpec {
		selSpecs = append(selSpecs, specByID[id])
	}
	sort.Slice(selSpecs, func(i, j int) bool { return selSpecs[i].ID < selSpecs[j].ID })

	engineCommit := a.str("engine-commit", "")
	if engineCommit == "" {
		if out, err := osexec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
			engineCommit = strings.TrimSpace(string(out))
		}
	}
	if !o.quiet {
		fmt.Println(ui.Info(fmt.Sprintf(".fs_verify --requirements: %s, %d requirements, %d cases, %d specs, %d fixtures; db=%s sf=%g threads=%d timeout=%v engine=%s hopsworks=%s",
			cases.RequirementsVersion, len(resolved), len(selected), len(selSpecs), len(fixtures), o.db, o.sf, o.threads, o.timeout, short(engineCommit), short(hopsworksCommit))))
	}

	if err := s.fsWarmRDRS(o); err != nil {
		return err
	}
	// L1 over the required cases
	caseStatus := map[string]string{}
	caseFinding := map[string]string{}
	caseResults := make([]caseResult, len(selected))
	if len(selected) > 0 {
		threads := o.threads
		if threads > len(selected) {
			threads = len(selected)
		}
		jobs := make(chan int)
		var wg sync.WaitGroup
		var engineErr error
		var once sync.Once
		for w := 0; w < threads; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				my, rd, cli, err := s.fsEngines(o)
				if err != nil {
					once.Do(func() { engineErr = err })
					for range jobs {
					}
					return
				}
				defer my.Close()
				defer rd.Close()
				if cli != nil {
					defer cli.Close()
				}
				for i := range jobs {
					ctx, cancel := context.WithTimeout(context.Background(), o.timeout+5*time.Second)
					caseResults[i] = runCase(ctx, selected[i], my, rd, o)
					cancel()
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
		for _, r := range caseResults {
			caseStatus[r.ID] = r.Status
			caseFinding[r.ID] = findingOf(r.Message)
			if !o.quiet {
				line := fmt.Sprintf("CASE %s %s %s %s %.1f", r.ID, r.Shape, r.Mode, r.Status, r.LatencyMS)
				if r.Message != "" {
					line += " " + r.Message
				}
				fmt.Println(line)
			}
		}
	}

	// L2 over the required specs
	specStatus := map[string]string{}
	var specResults []specResult
	if len(selSpecs) > 0 {
		my, rd, _, err := s.fsEngines(o)
		if err != nil {
			return err
		}
		for _, sp := range selSpecs {
			ctx, cancel := context.WithTimeout(context.Background(), o.timeout*time.Duration(3+vo.count/10)+10*time.Second)
			r := runSpec(ctx, sp, my, rd, vo)
			cancel()
			specResults = append(specResults, r)
			specStatus[sp.ID] = r.Status
			if !o.quiet {
				fmt.Printf("SPEC %s %s %s %.1f %s\n", r.ID, r.Shape, r.Status, r.LatencyMS, r.Message)
			}
		}
		my.Close()
		rd.Close()
	}

	// Java conformance, in-process
	fixtureStatus := map[string]string{}
	fixtureDetail := map[string]string{}
	for name, f := range fixtures {
		res := emit.ConformFixture(name, f)
		switch {
		case res.Err != nil:
			fixtureStatus[name], fixtureDetail[name] = "ERROR", res.Err.Error()
		case !res.Conformant():
			fixtureStatus[name], fixtureDetail[name] = "DIFF", strings.Join(res.Diffs, "; ")
		case res.Gate != "":
			fixtureStatus[name] = "gate:" + res.Gate
		default:
			fixtureStatus[name] = "CONFORMANT"
		}
	}

	// fold per requirement
	counts := map[string]int{}
	var reqResults []requirementResult
	acceptance := "PASS"
	for _, r := range resolved {
		rr := requirementResult{ID: r.ID, Title: r.Title, Branch: r.Branch, Kind: "emitted", Missing: r.Missing,
			Cases: map[string]string{}, Specs: map[string]string{}, Fixtures: map[string]string{}}
		if !r.Emitted {
			rr.Kind = "framework"
		}
		if r.Gate != "" {
			rr.Kind = "gate"
		}
		var classes []string
		if only != nil && !only[r.ID] {
			rr.Status = cases.ReqUntested
			rr.Detail = append(rr.Detail, "not selected by --req")
		} else {
			for _, id := range r.CaseIDs {
				st := caseStatus[id]
				rr.Cases[id] = st
				cl := cases.EvidenceClass(st)
				classes = append(classes, cl)
				if cl != "pass" {
					rr.Detail = append(rr.Detail, "case "+id+" "+st)
					if f := caseFinding[id]; f != "" && !containsString(rr.Findings, f) {
						rr.Findings = append(rr.Findings, f)
					}
				}
			}
			for _, id := range r.SpecIDs {
				st := specStatus[id]
				if !withVectors {
					st = "UNTESTED"
				}
				rr.Specs[id] = st
				cl := cases.EvidenceClass(st)
				classes = append(classes, cl)
				if cl != "pass" {
					rr.Detail = append(rr.Detail, "spec "+id+" "+st)
				}
			}
			for _, f := range r.Fixtures {
				st, ok := fixtureStatus[f]
				if !ok {
					st = "UNTESTED"
					rr.Missing = append(rr.Missing, "fixture "+f)
				}
				rr.Fixtures[f] = st
				switch {
				case st == "CONFORMANT" || strings.HasPrefix(st, "gate:"):
					if r.Gate == "" && strings.HasPrefix(st, "gate:") {
						// a gate on a non-gate requirement is a conformance failure of the manifest's claim
						classes = append(classes, "failed")
						rr.Detail = append(rr.Detail, "fixture "+f+" raised "+st+" for a non-gate requirement")
					} else {
						classes = append(classes, "pass")
					}
				case st == "UNTESTED":
					classes = append(classes, "untested")
				default:
					classes = append(classes, "failed")
					rr.Detail = append(rr.Detail, "fixture "+f+" "+st+": "+fixtureDetail[f])
				}
			}
			rr.Status = cases.RequirementStatus(r.Requirement, classes, len(rr.Missing))
		}
		rr.Accepted = cases.Accepted(r.Requirement, rr.Status)
		if !rr.Accepted {
			acceptance = "FAIL"
		}
		counts[rr.Status]++
		reqResults = append(reqResults, rr)
		pass := func(m map[string]string, ok func(string) bool) (int, int) {
			n := 0
			for _, st := range m {
				if ok(st) {
					n++
				}
			}
			return n, len(m)
		}
		cp, cn := pass(rr.Cases, func(st string) bool { return cases.EvidenceClass(st) == "pass" })
		sp, sn := pass(rr.Specs, func(st string) bool { return cases.EvidenceClass(st) == "pass" })
		fp, fn := pass(rr.Fixtures, func(st string) bool { return st == "CONFORMANT" || strings.HasPrefix(st, "gate:") })
		line := fmt.Sprintf("REQUIREMENT %s %s %s cases=%d/%d specs=%d/%d fixtures=%d/%d", rr.ID, rr.Status, rr.Kind, cp, cn, sp, sn, fp, fn)
		sort.Strings(rr.Findings)
		if len(rr.Findings) > 0 {
			line += " findings=" + strings.Join(rr.Findings, ",")
		}
		if len(rr.Missing) > 0 {
			line += " missing=" + strings.Join(rr.Missing, ",")
		}
		if len(rr.Detail) > 0 {
			line += " — " + rr.Detail[0]
			if len(rr.Detail) > 1 {
				line += fmt.Sprintf(" (+%d)", len(rr.Detail)-1)
			}
		}
		fmt.Println(line)
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
	fmt.Printf("SUMMARY requirements version=%s requirements=%d %s acceptance=%s engine=%s hopsworks=%s\n",
		cases.RequirementsVersion, len(reqResults), strings.Join(parts, " "), acceptance, short(engineCommit), short(hopsworksCommit))

	if o.jsonPath != "" {
		report := requirementsReport{Version: cases.RequirementsVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			EngineCommit: engineCommit, HopsworksCommit: hopsworksCommit, Label: a.str("label", ""),
			Configuration: map[string]interface{}{"db": o.db, "sf": o.sf, "threads": o.threads, "timeout": o.timeout.String(), "tolerance": o.tolerance,
				"now": now.UTC().Format(time.RFC3339), "vectors": withVectors, "seed": vo.seed, "count": vo.count, "includeHazards": o.includeHazards,
				"mysql": fmt.Sprintf("%s:%d", s.config.MySQLHost, s.config.MySQLPort), "rdrs": fmt.Sprintf("%s:%d", s.config.RDRSHost, s.config.RestPort)},
			FixtureProvenance: provenance, FixtureDir: fixtureDir, Requirements: reqResults, Counts: counts, Acceptance: acceptance,
			CaseResults: caseResults, SpecResults: specResults}
		raw, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(o.jsonPath, raw, 0o644); err != nil {
			return err
		}
	}
	if o.dumpDir != "" {
		if err := os.MkdirAll(o.dumpDir, 0o755); err != nil {
			return err
		}
		for _, r := range caseResults {
			if cases.EvidenceClass(r.Status) == "pass" {
				continue
			}
			base := filepath.Join(o.dumpDir, r.ID)
			_ = os.WriteFile(base+".sql", []byte(r.Statement+"\n"), 0o644)
			_ = os.WriteFile(base+".txt", []byte(r.Status+"\n"+r.Message+"\n"+r.Diff), 0o644)
		}
	}
	if acceptance != "PASS" {
		notAccepted := 0
		for _, rr := range reqResults {
			if !rr.Accepted {
				notAccepted++
			}
		}
		return fmt.Errorf("requirements acceptance FAIL: %d emitted requirement(s) not supported", notAccepted)
	}
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	if commit == "" {
		return "unknown"
	}
	return commit
}

// findFixtureDir locates the captured Java corpus: --fixtures, then
// $RONDB_FS_GOLDEN, then the source tree relative to the working directory
// (the interactive runs start in debug_build/mysql-test or the repo root).
func findFixtureDir(flag string) (string, error) {
	var candidates []string
	if flag != "" {
		candidates = append(candidates, flag)
	}
	if env := os.Getenv("RONDB_FS_GOLDEN"); env != "" {
		candidates = append(candidates, env)
	}
	cwd, _ := os.Getwd()
	rel := filepath.Join("tools", "rondb-cli", "internal", "fsq", "testdata", "hopsworks_golden")
	for _, up := range []string{".", "..", filepath.Join("..", ".."), filepath.Join("..", "..", "..")} {
		candidates = append(candidates, filepath.Join(cwd, up, rel))
	}
	candidates = append(candidates, filepath.Join(cwd, "internal", "fsq", "testdata", "hopsworks_golden"))
	for _, c := range candidates {
		if st, err := os.Stat(filepath.Join(c, "manifest.json")); err == nil && !st.IsDir() {
			return filepath.Clean(c), nil
		}
	}
	return "", fmt.Errorf("Hopsworks golden corpus not found (pass --fixtures DIR or set RONDB_FS_GOLDEN)")
}
