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

package cases

// The requirements manifest (framework_design.md §9, plan §3.1 A7, E8):
// a versioned map from the Hopsworks builder branches, gates, DTO
// contracts and the data_model.md §11 edge cases to mandatory evidence —
// case-matrix cases (L1), vector specs (L2) and captured Java fixtures
// (conformance).  `.fs_verify --requirements` resolves it against the
// enumerated cases and reports each requirement as SUPPORTED,
// UNSUPPORTED, HOPSWORKS-GATED, UNTESTED or FAILED.  Regression
// allowances (known rejections, known wrong results) never count as
// supported here: a requirement is SUPPORTED only when every piece of
// evidence passes.

import (
	"fmt"
	"sort"
	"strings"
)

// RequirementsVersion is recorded in every requirements report.
const RequirementsVersion = "req-v1"

// Requirement is one row of the manifest.
type Requirement struct {
	ID    string
	Title string
	// Branch names the Hopsworks builder branch / gate / DTO contract or the
	// data_model.md §11 edge group the requirement covers.
	Branch string
	// Emitted: Hopsworks emits this shape today, so it must be SUPPORTED
	// (or HOPSWORKS-GATED) for acceptance.  Framework-native shapes (S6b,
	// S8b) are reported but never satisfy an emitted requirement.
	Emitted bool
	// Shapes selects every enumerated case whose PRIMARY shape is listed
	// (edge probes relate to shapes through RelatedShapes for the regression
	// SHAPE view, but are claimed by their own requirement rows here, so one
	// probe's finding is attributed once); Cases adds mandatory case ids.  A
	// listed id that does not exist makes the requirement UNTESTED.
	Shapes []string
	Cases  []string
	// SpecShapes selects every vector spec (L2) of those shapes.
	SpecShapes []string
	// Fixtures are captured Java fixtures whose conformance is required.
	Fixtures []string
	// Gate names the Hopsworks gate the fixtures must reproduce; such a
	// requirement reports HOPSWORKS-GATED, never SUPPORTED.
	Gate string
}

// Requirements is the manifest.
func Requirements() []Requirement {
	return []Requirement{
		{ID: "R-S1", Title: "Aggregate, point lookup", Branch: "applyRonsqlAggregate (single)", Emitted: true,
			Shapes: []string{"S1"}, Cases: []string{"S1-k31", "S1-k16", "S1-k1001", "S1-avg-k31", "S1-decimal-k31"},
			SpecShapes: []string{"S1"}, Fixtures: []string{"aggregate_single", "aggregate_training_dataset"}},
		{ID: "R-S2", Title: "Aggregate, windowed", Branch: "applyRonsqlAggregate + window bound", Emitted: true,
			Shapes: []string{"S2"}, Cases: []string{"S2-k111-w604800s", "S2-k21-w2592000s", "S2-k1-w3600s", "S2-k111-w7776000s"},
			Fixtures: []string{"aggregate_window"}},
		{ID: "R-S3", Title: "Aggregate, batch IN list + GROUP BY", Branch: "applyRonsqlAggregate (batch, one entity key)", Emitted: true,
			Shapes: []string{"S3"}, Cases: []string{"S3-b10", "S3-b100", "S3-b100-w30d"},
			SpecShapes: []string{"S3"}, Fixtures: []string{"aggregate_batch"}},
		{ID: "R-S4", Title: "Aggregate with online filters", Branch: "renderRonsqlCondition (=, <>, <, <=, >, >=, LIKE)", Emitted: true,
			Shapes: []string{"S4"}, Cases: []string{"S4-eq-string", "S4-ge-numeric", "S4-ne", "S4-like", "S4-two-leaves", "S4-quote", "S4-window-filter-b10"},
			SpecShapes: []string{"S4"},
			Fixtures:   []string{"filter_equals", "filter_not_equals", "filter_greater_than", "filter_greater_than_or_equal", "filter_less_than", "filter_less_than_or_equal", "filter_like", "self_join_filter_scope"}},
		{ID: "R-S5", Title: "GREATEST/LEAST fold", Branch: "aggregateOutputs (greatest, least)", Emitted: true,
			Cases: []string{"S1-k31", "EDGE-null-greatest"}, Fixtures: []string{"aggregate_single"}},
		{ID: "R-S6", Title: "Collect last-N, Hopsworks CTE form", Branch: "buildRonsqlCollectTemplate", Emitted: true,
			Shapes: []string{"S6"}, Cases: []string{"S6-cte-k21", "S6-cte-k31", "S6-cte-k16"},
			SpecShapes: []string{"S6"}, Fixtures: []string{"collect_desc", "collect_asc", "collect_explicit_order"}},
		{ID: "R-S6b", Title: "Collect last-N, direct form (framework target shape)", Branch: "S6b (not emitted by Hopsworks)", Emitted: false,
			Shapes: []string{"S6b"}, Cases: []string{"S6b-k21", "S6b-k31", "S6b-k16", "S6b-k31-limit50"}, SpecShapes: []string{"S6b"}},
		{ID: "R-S7", Title: "Snowflake, all-INNER combined template", Branch: "buildSnowflakeStatement (INNER)", Emitted: true,
			Shapes: []string{"S7"}, Cases: []string{"S7-1hop-k21", "S7-2hop-k21", "S7-1hop-k13", "S7-2hop-k13", "S7-1hop-k29", "S7-2hop-k29", "S7-2hop-b10"},
			SpecShapes: []string{"S7"}, Fixtures: []string{"snowflake_inner", "snowflake_batch", "snowflake_helper_inference", "snowflake_helper_logging", "snowflake_helper_vector"}},
		{ID: "R-S8", Title: "Snowflake, all-LEFT per-chain templates", Branch: "buildRonsqlSnowflakeTemplates (LEFT)", Emitted: true,
			Shapes: []string{"S8"}, Cases: []string{"S8-chains-k21", "S8-chains-k13", "S8-chains-k29", "S8-chains-b10"},
			SpecShapes: []string{"S8"}, Fixtures: []string{"snowflake_left"}},
		{ID: "R-S9", Title: "Composite entity keys", Branch: "applyRonsqlAggregate (composite key, single)", Emitted: true,
			Shapes: []string{"S9"}, Cases: []string{"S9-a4-EUR-w90d"}, SpecShapes: []string{"S9"}, Fixtures: []string{"aggregate_composite_batch"}},
		{ID: "R-S10", Title: "String entity keys", Branch: "applyRonsqlAggregate (VARCHAR key)", Emitted: true,
			Shapes: []string{"S10"}, Cases: []string{"S10-b10"}, SpecShapes: []string{"S10"}, Fixtures: []string{"aggregate_string_key"}},
		{ID: "R-A2-binary", Title: "Emitted binary / complex snowflake projections", Branch: "review A2", Emitted: true,
			Cases: []string{"EDGE-comp-binary"}, Fixtures: []string{"snowflake_binary"}},
		{ID: "R-A3-null", Title: "Typed NULL and production aliases", Branch: "review A3", Emitted: true,
			Cases: []string{"EDGE-all-null", "EDGE-null-ints", "EDGE-null-decimal", "EDGE-null-avg", "EDGE-null-string-adjacent", "EDGE-str-null-vs-NULL"}},
		{ID: "R-A5-types", Title: "Deterministic type, literal and time-boundary fixtures", Branch: "review A5 / data_model.md §11", Emitted: true,
			Cases: []string{"EDGE-big-safe", "EDGE-big-limits", "EDGE-big-overflow", "EDGE-decimal-large", "EDGE-float-exact", "EDGE-float-rounding",
				"EDGE-str-collation", "EDGE-str-escapes", "EDGE-str-in-list", "EDGE-date-range", "EDGE-ts0-batch", "EDGE-ts3-cutoff", "EDGE-ts6-cutoff", "EDGE-seq-boundary", "EDGE-seq-order"}},
		{ID: "R-A5-composite", Title: "Composite child hops", Branch: "review A5 / composite hop", Emitted: true,
			Cases: []string{"EDGE-comp-hop", "EDGE-comp-batch", "EDGE-comp-swapped"}},
		{ID: "R-F1", Title: "String aggregate re-use (F1 hazard)", Branch: "data_model.md §11 F1", Emitted: true,
			Cases: []string{"EDGE-F1-string-reuse", "EDGE-F1-string-reuse-nonull"}},
		{ID: "R-GATE-filter", Title: "Online-filter gates", Branch: "COLLECT_UNSUPPORTED_ONLINE_FILTER", Emitted: true, Gate: "COLLECT_UNSUPPORTED_ONLINE_FILTER",
			Fixtures: []string{"filter_or_rejected", "filter_backslash_rejected", "filter_control_rejected"}},
		{ID: "R-GATE-join", Title: "Join-key gates", Branch: "JOIN_ON_PARTIAL_PRIMARY_KEY", Emitted: true, Gate: "JOIN_ON_PARTIAL_PRIMARY_KEY",
			Fixtures: []string{"snowflake_partial_key"}},
		{ID: "R-GATE-silent", Title: "Silent template suppression (mixed joins, cross-store, batch collect)", Branch: "buildRonsqlSnowflakeTemplates / buildDTO (no template)", Emitted: true, Gate: "no-template",
			Fixtures: []string{"snowflake_mixed_gated", "snowflake_cross_store_gated", "collect_batch_gated"}},
		{ID: "R-GATE-definition", Title: "Definition-time collect / aggregate gates", Branch: "ValidateDefinition", Emitted: true, Gate: "definition",
			Fixtures: []string{"definition_collect_valid", "definition_aggregate_valid", "definition_collect_zero", "definition_collect_over_max", "definition_collect_max",
				"definition_collect_width", "definition_collect_width_boundary", "definition_collect_order_unknown", "definition_collect_order_missing",
				"definition_collect_key_only", "definition_collect_complex", "definition_collect_bad_layout", "definition_collect_and_aggregate", "definition_aggregate_string_sum"}},
	}
}

// Resolved is a requirement with its evidence identifiers resolved
// against the enumerated cases and specs.
type Resolved struct {
	Requirement
	CaseIDs []string // every required case (shapes + explicit), sorted
	SpecIDs []string
	Missing []string // explicit ids / shapes / specs that resolve to nothing
}

// ResolveRequirements maps the manifest onto enumerated cases and specs.
func ResolveRequirements(all []Case, specs []Spec) []Resolved {
	byID := map[string]bool{}
	byShape := map[string][]string{}
	for _, c := range all {
		byID[c.ID] = true
		byShape[c.Shape] = append(byShape[c.Shape], c.ID)
	}
	specByShape := map[string][]string{}
	for _, sp := range specs {
		specByShape[sp.Shape] = append(specByShape[sp.Shape], sp.ID)
	}
	var out []Resolved
	for _, r := range Requirements() {
		res := Resolved{Requirement: r}
		ids := map[string]bool{}
		for _, sh := range r.Shapes {
			if len(byShape[sh]) == 0 {
				res.Missing = append(res.Missing, "shape "+sh)
			}
			for _, id := range byShape[sh] {
				ids[id] = true
			}
		}
		for _, id := range r.Cases {
			if !byID[id] {
				res.Missing = append(res.Missing, "case "+id)
				continue
			}
			ids[id] = true
		}
		for id := range ids {
			res.CaseIDs = append(res.CaseIDs, id)
		}
		sort.Strings(res.CaseIDs)
		for _, sh := range r.SpecShapes {
			if len(specByShape[sh]) == 0 {
				res.Missing = append(res.Missing, "spec shape "+sh)
			}
			res.SpecIDs = append(res.SpecIDs, specByShape[sh]...)
		}
		sort.Strings(res.SpecIDs)
		out = append(out, res)
	}
	return out
}

// Requirement statuses (framework_design.md §9).
const (
	ReqSupported   = "SUPPORTED"
	ReqUnsupported = "UNSUPPORTED"
	ReqGated       = "HOPSWORKS-GATED"
	ReqUntested    = "UNTESTED"
	ReqFailed      = "FAILED"
)

// EvidenceClass maps a runner status to its effect on a requirement:
// "pass", "unsupported" (a regression allowance), "untested" or "failed".
func EvidenceClass(status string) string {
	switch {
	case status == "PASS" || strings.HasPrefix(status, "PASS("):
		return "pass"
	case status == "REJECT(expected)" || status == "REJECT(allowed)" || status == "KNOWN-WRONG" || status == "KNOWN-ERROR" || status == "HAZARD-SKIPPED" || status == "HEADER-ONLY":
		return "unsupported"
	case status == "UNTESTED" || status == "SKIP" || status == "":
		return "untested"
	}
	return "failed"
}

// RequirementStatus folds evidence classes into the requirement status:
// FAILED > UNTESTED > UNSUPPORTED > (HOPSWORKS-GATED | SUPPORTED).
func RequirementStatus(r Requirement, classes []string, missing int) string {
	failed, untested, unsupported := false, missing > 0, false
	for _, c := range classes {
		switch c {
		case "failed":
			failed = true
		case "untested":
			untested = true
		case "unsupported":
			unsupported = true
		}
	}
	switch {
	case failed:
		return ReqFailed
	case untested:
		return ReqUntested
	case unsupported:
		return ReqUnsupported
	case r.Gate != "":
		return ReqGated
	}
	return ReqSupported
}

// Accepted reports whether a requirement status satisfies acceptance:
// emitted requirements must be SUPPORTED (gate requirements: HOPSWORKS-
// GATED); framework-native requirements never block acceptance.
func Accepted(r Requirement, status string) bool {
	if !r.Emitted {
		return true
	}
	if r.Gate != "" {
		return status == ReqGated
	}
	return status == ReqSupported
}

// ManifestSummary renders the manifest for .fs_show / docs.
func ManifestSummary() string {
	var b strings.Builder
	for _, r := range Requirements() {
		kind := "emitted"
		if !r.Emitted {
			kind = "framework"
		}
		if r.Gate != "" {
			kind = "gate"
		}
		fmt.Fprintf(&b, "%-18s %-9s %s — %s\n", r.ID, kind, r.Title, r.Branch)
	}
	return b.String()
}
