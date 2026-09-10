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

// .fs_verify / .fs_show / MTR template emission (execution phase E3):
// framework_design.md §9-§11.

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

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/mtr"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// caseResult is one CASE line.
type caseResult struct {
	ID        string            `json:"id"`
	Shape     string            `json:"shape"`
	Shapes    []string          `json:"shapes"`
	Mode      string            `json:"mode"`
	Status    string            `json:"status"`
	LatencyMS float64           `json:"latencyMs"`
	Message   string            `json:"message,omitempty"`
	Statement string            `json:"statement,omitempty"`
	Diff      string            `json:"diff,omitempty"`
	Phases    map[string]int64  `json:"phases,omitempty"`
	Rows      int               `json:"rows"`
	Extra     map[string]string `json:"extra,omitempty"`
	Raw       string            `json:"raw,omitempty"` // RonSQL response body on an execution error (truncated)
}

func isFailure(status string) bool {
	switch status {
	case "REJECT(expected)", "REJECT(allowed)", "KNOWN-WRONG", "KNOWN-ERROR", "HEADER-ONLY", "SKIP", "HAZARD-SKIPPED", "UNTESTED":
		return false
	}
	return !strings.HasPrefix(status, "PASS")
}

type verifyOpts struct {
	db, now, dumpDir, jsonPath, cliPath string
	sf, tolerance                       float64
	shapes                              map[string]bool
	caseID                              string
	threads                             int
	timeout                             time.Duration
	includeHazards, allowReject, quiet  bool
	relaxedHeaders, useCLI              bool
}

func parseVerifyOpts(a fsArgs) (verifyOpts, error) {
	o := verifyOpts{db: a.str("db", "test"), now: a.str("now", ""), dumpDir: a.str("dump-dir", ""), jsonPath: a.str("json", ""),
		cliPath: a.str("cli", os.Getenv("RONSQL_CLI_EXE")), caseID: a.str("case", ""), threads: 4, timeout: 30 * time.Second, tolerance: 1e-9}
	var err error
	if o.sf, err = a.float("sf", 0.01); err != nil {
		return o, err
	}
	if v, ok := a.flags["threads"]; ok {
		if o.threads, err = strconv.Atoi(v); err != nil || o.threads <= 0 {
			return o, fmt.Errorf("invalid --threads %s", v)
		}
	}
	if v, ok := a.flags["timeout"]; ok {
		if o.timeout, err = time.ParseDuration(v); err != nil {
			return o, fmt.Errorf("invalid --timeout %s", v)
		}
	}
	if v, ok := a.flags["tolerance"]; ok {
		if o.tolerance, err = strconv.ParseFloat(v, 64); err != nil {
			return o, fmt.Errorf("invalid --tolerance %s", v)
		}
	}
	if v, ok := a.flags["shape"]; ok {
		o.shapes = map[string]bool{}
		for _, s := range strings.Split(v, ",") {
			o.shapes[strings.TrimSpace(s)] = true
		}
	}
	o.includeHazards, o.allowReject, o.quiet = a.has("include-hazards"), a.has("allow-reject"), a.has("quiet")
	o.relaxedHeaders = a.has("relaxed-headers")
	for _, e := range strings.Split(a.str("engines", "rdrs,mysql"), ",") {
		if strings.TrimSpace(e) == "cli" {
			o.useCLI = true
		}
	}
	return o, nil
}

func (s *Shell) fsEngines(o verifyOpts) (*exec.MySQL, *exec.RDRS, *exec.CLI, error) {
	host := s.config.MySQLHost
	if host == "" {
		host = s.config.Host
	}
	my, err := exec.NewMySQL(host, s.config.MySQLPort, s.mysqlUser, s.mysqlPass, s.config.TLS, o.db)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("mysql: %w", err)
	}
	rhost := s.config.RDRSHost
	if rhost == "" {
		rhost = s.config.Host
	}
	rd := exec.NewRDRS(rhost, s.config.RestPort, s.config.RDRSTLS, s.config.RDRSAPIKey, o.db, APIVersion,
		"SELECT COUNT(*) FROM `countries_1`;", o.timeout)
	var cli *exec.CLI
	if o.useCLI {
		if o.cliPath == "" {
			my.Close()
			return nil, nil, nil, fmt.Errorf("--engines cli needs --cli <ronsql_cli path> or RONSQL_CLI_EXE")
		}
		cli = exec.NewCLI(o.cliPath, os.Getenv("NDB_CONNECTSTRING"), o.db, o.timeout)
	}
	return my, rd, cli, nil
}

// runCase executes one case on one engine pair and classifies it.
func runCase(ctx context.Context, c cases.Case, my *exec.MySQL, rd *exec.RDRS, o verifyOpts) caseResult {
	res := caseResult{ID: c.ID, Shape: c.Shape, Shapes: c.Shapes(), Mode: c.Mode, Status: "PASS"}
	if c.Hazard != "" && !o.includeHazards {
		res.Status, res.Message = "HAZARD-SKIPPED", c.Hazard
		return res
	}
	for _, st := range c.Statements {
		res.Statement = st.RonSQL
		mysqlResp := my.Query(ctx, st.RonSQL)
		if mysqlResp.Outcome != exec.OK {
			res.Status, res.Message = "MYSQL-ERROR", mysqlResp.Message
			return res
		}
		ronResp := rd.Query(ctx, st.RonSQL)
		if ronResp.Result != nil {
			res.LatencyMS += float64(ronResp.Result.Latency.Microseconds()) / 1000
			res.Phases = ronResp.Result.Phases
		}
		switch ronResp.Outcome {
		case exec.OK:
			if c.ExpectReject != nil {
				res.Status = "PASS(was-expected-reject)"
				res.Message = c.ExpectReject.Finding + " no longer rejects"
			}
			// No header probe for an empty result: neither the JSON nor the
			// TEXT form carries the column names when there are no rows
			// (canon.Compare treats two empty results as equal).
			rep := canon.Compare(mysqlResp.Result, ronResp.Result, canon.Options{Ordered: c.Ordered, Tolerance: o.tolerance, RelaxedHeaders: o.relaxedHeaders})
			res.Rows = len(mysqlResp.Result.Rows)
			if !rep.Equal {
				res.Diff = rep.Diff
				if c.KnownWrong.MatchesWrong(mysqlResp.Result, ronResp.Result,
					canon.Options{Ordered: c.Ordered, Tolerance: o.tolerance}) {
					res.Status, res.Message = "KNOWN-WRONG", c.KnownWrong.Finding+": "+rep.Reason
					return res
				}
				res.Status, res.Message = "WRONG-RESULT", rep.Reason
				return res
			}
			if rep.HeaderOnly && res.Status == "PASS" {
				res.Status = "HEADER-ONLY"
			}
			if c.KnownWrong != nil && res.Status == "PASS" {
				res.Status, res.Message = "PASS(was-known-wrong)", c.KnownWrong.Finding+" now agrees"
			}
			if c.KnownError != nil && res.Status == "PASS" {
				res.Status, res.Message = "PASS(was-known-error)", c.KnownError.Finding+" now parses"
			}
			if rep.Note != "" && res.Message == "" {
				res.Message = rep.Note
			}
		case exec.CleanReject:
			if c.ExpectReject != nil && strings.Contains(ronResp.Message, c.ExpectReject.Pattern) {
				res.Status, res.Message = "REJECT(expected)", c.ExpectReject.Finding
				return res
			}
			res.Status, res.Message = "REJECT", firstLine(ronResp.Message)
			if o.allowReject {
				res.Status = "REJECT(allowed)"
			}
			return res
		default:
			res.Status, res.Message = ronResp.Outcome.String(), firstLine(ronResp.Message)
			if ronResp.Result != nil && len(ronResp.Result.Raw) > 0 {
				res.Raw = ronResp.Result.Raw
				if len(res.Raw) > 4096 {
					res.Raw = res.Raw[:4096] + "…"
				}
			}
			if c.KnownError.MatchesError(mysqlResp.Result, ronResp,
				canon.Options{Ordered: c.Ordered, Tolerance: o.tolerance}) {
				res.Status, res.Message = "KNOWN-ERROR", c.KnownError.Finding+": "+res.Message
			}
			return res
		}
	}
	return res
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "Caught exception:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Caught exception:"))
		}
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// .fs_verify [--all | --shape S1,S3 | --case ID] [--db D] [--sf 0.01] [--engines rdrs,mysql[,cli]]
//
//	[--threads N] [--timeout 30s] [--tolerance 1e-9] [--now TS] [--include-hazards]
//	[--allow-reject] [--relaxed-headers] [--dump-dir P] [--json P] [--quiet]
func (s *Shell) runFSVerify(args []string) error {
	a := parseFSArgs(args)
	if a.has("vectors") {
		return s.runFSVectors(a)
	}
	o, err := parseVerifyOpts(a)
	if err != nil {
		return err
	}
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. .fs_verify needs MySQL and RDRS.")
	}
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. .fs_verify needs MySQL and RDRS.")
	}
	now := data.FSNow
	if o.now != "" {
		if now, err = time.Parse(time.RFC3339, o.now); err != nil {
			return fmt.Errorf("invalid --now (RFC 3339 expected): %w", err)
		}
	}
	all, err := cases.Enumerate(cases.Config{DB: o.db, Scale: data.NewScale(o.sf), Now: now})
	if err != nil {
		return err
	}
	var selected []cases.Case
	for _, c := range all {
		if o.caseID != "" && c.ID != o.caseID {
			continue
		}
		if !c.MatchesShapes(o.shapes) {
			continue
		}
		selected = append(selected, c)
	}
	if len(selected) == 0 {
		return fmt.Errorf("no cases selected")
	}
	if !o.quiet {
		fmt.Println(ui.Info(fmt.Sprintf(".fs_verify: %d cases, db=%s sf=%g now=%s threads=%d timeout=%v tolerance=%g",
			len(selected), o.db, o.sf, now.UTC().Format(time.RFC3339), o.threads, o.timeout, o.tolerance)))
	}
	threads := o.threads
	if threads > len(selected) {
		threads = len(selected)
	}
	results := make([]caseResult, len(selected))
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
				results[i] = runCase(ctx, selected[i], my, rd, o)
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
	counts := map[string]int{}
	byShape := statusesByShape(results, o.shapes)
	for _, r := range results {
		line := fmt.Sprintf("CASE %s %s %s %s %.1f", r.ID, r.Shape, r.Mode, r.Status, r.LatencyMS)
		if r.Message != "" {
			line += " " + r.Message
		}
		if !o.quiet || isFailure(r.Status) {
			fmt.Println(line)
		}
		if r.Diff != "" && !o.quiet {
			fmt.Print(indent(r.Diff))
		}
		counts[r.Status]++
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
	fmt.Printf("SUMMARY cases=%d %s\n", len(results), strings.Join(parts, " "))
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
			if !isFailure(r.Status) && r.Status != "KNOWN-WRONG" && r.Status != "KNOWN-ERROR" && r.Status != "REJECT(allowed)" {
				continue
			}
			base := filepath.Join(o.dumpDir, r.ID)
			_ = os.WriteFile(base+".sql", []byte(r.Statement+"\n"), 0o644)
			body := r.Status + "\n" + r.Message + "\n" + r.Diff
			if r.Raw != "" {
				body += "-- raw RonSQL response --\n" + r.Raw + "\n"
			}
			_ = os.WriteFile(base+".txt", []byte(body), 0o644)
		}
	}
	if o.jsonPath != "" {
		raw, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(o.jsonPath, raw, 0o644); err != nil {
			return err
		}
	}
	if fails > 0 {
		return fmt.Errorf("%d case(s) failed", fails)
	}
	return nil
}

// statusesByShape includes edge evidence without changing global case counts.
// With --shape, report only requested shapes, not partially selected relatives.
func statusesByShape(results []caseResult, selected map[string]bool) map[string][]string {
	out := map[string][]string{}
	for _, r := range results {
		for _, shape := range r.Shapes {
			if selected == nil || selected[shape] {
				out[shape] = append(out[shape], r.Status)
			}
		}
	}
	return out
}

// shapeStatus reports SUPPORTED / UNSUPPORTED / FAILED per shape
// (framework_design.md §9, requirements report).
func shapeStatus(statuses []string) string {
	out := "SUPPORTED"
	for _, st := range statuses {
		switch {
		case isFailure(st):
			return "FAILED"
		case st == "REJECT(expected)" || st == "REJECT(allowed)" || st == "KNOWN-WRONG" || st == "KNOWN-ERROR" || st == "HAZARD-SKIPPED":
			out = "UNSUPPORTED"
		case st == "UNTESTED" && out == "SUPPORTED":
			out = "UNTESTED"
		}
	}
	return out
}

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("      | " + line + "\n")
	}
	return b.String()
}

// .fs_show [--case ID | --shape S] [--db D] [--sf 0.01]
func (s *Shell) runFSShow(args []string) error {
	a := parseFSArgs(args)
	sf, err := a.float("sf", 0.01)
	if err != nil {
		return err
	}
	all, err := cases.Enumerate(cases.Config{DB: a.str("db", "test"), Scale: data.NewScale(sf)})
	if err != nil {
		return err
	}
	id, shape := a.str("case", ""), a.str("shape", "")
	var shapes map[string]bool
	if shape != "" {
		shapes = map[string]bool{shape: true}
	}
	n := 0
	for _, c := range all {
		if (id != "" && c.ID != id) || !c.MatchesShapes(shapes) {
			continue
		}
		n++
		fmt.Printf("=== %s: %s %s [%s] — %s\n", c.ID, c.Shape, c.Mode, c.Origin, c.Note)
		if len(c.RelatedShapes) > 0 {
			fmt.Printf("    related shapes: %s\n", strings.Join(c.RelatedShapes, ", "))
		}
		if c.ExpectReject != nil {
			fmt.Printf("    expected rejection %s: %s\n", c.ExpectReject.Finding, c.ExpectReject.Pattern)
		}
		if c.KnownWrong != nil {
			fmt.Printf("    known wrong result %s: %s\n", c.KnownWrong.Finding, c.KnownWrong.Pattern)
		}
		if c.Hazard != "" {
			fmt.Printf("    HAZARD %s\n", c.Hazard)
		}
		for _, st := range c.Statements {
			fmt.Printf("    [%s] %s\n", st.Label, st.RonSQL)
		}
		for _, group := range c.Groups {
			twin := "<absent>"
			if group.MySQL != nil {
				twin = strings.ReplaceAll(*group.MySQL, "\n", " ")
			}
			fmt.Printf("    [production MySQL, DTO %d, %d RonSQL templates] %s\n",
				group.DTO.PreparedStatementIndex, len(group.Statements), twin)
		}
	}
	if n == 0 {
		return fmt.Errorf("no cases match")
	}
	if id == "" && shape == "" {
		fmt.Println(ui.Info(fmt.Sprintf("%d cases", n)))
	}
	return nil
}

// emitTemplatesTest writes t/ronsql_fs_templates.test next to the include dir.
func emitTemplatesTest(includeDir, db string, sf float64) (string, error) {
	all, err := cases.Enumerate(cases.Config{DB: db, Scale: data.NewScale(sf)})
	if err != nil {
		return "", err
	}
	testDir := filepath.Join(filepath.Dir(filepath.Clean(includeDir)), "t")
	if err := os.MkdirAll(testDir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(testDir, "ronsql_fs_templates.test")
	return path, os.WriteFile(path, []byte(mtr.RenderTemplatesTest(all, db)), 0o644)
}
