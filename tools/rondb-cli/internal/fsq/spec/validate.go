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

package spec

// Definition-time validators: a port of QueryController.convertCollect
// (QueryController.java:741-843) and QueryController.convertAggregate
// (:357-530), HOPSWORKS_REF f85a653bc.  Messages are verbatim because the
// golden fixtures compare them.

import (
	"encoding/json"
	"strings"
)

// DefinitionResult is what the validators write onto the internal Query:
// the normalized collect and aggregate settings (fixture "expected").
type DefinitionResult struct {
	CollectN         *int    `json:"collectN"`
	CollectOrderBy   *string `json:"collectOrderBy"`
	CollectAscending bool    `json:"collectAscending"`
	Aggregate        AggSpec `json:"aggregate"`
	AggregateWindow  *int64  `json:"aggregateWindow"`
}

// MaxAggregateWindowSeconds is 100 years (review X2-R16).
const MaxAggregateWindowSeconds int64 = 100 * 365 * 24 * 3600

var aggregateFunctions = map[string]bool{"count": true, "sum": true, "min": true, "max": true, "avg": true}
var aggregateNaryFunctions = map[string]bool{"greatest": true, "least": true}

// Java's HashSet iteration order for these two constant sets is not
// derivable from the source; the strings below are what a JDK 21 HashSet
// prints for them and are only used in two error messages that no fixture
// covers yet.
const (
	aggregateFunctionsText     = "[sum, min, max, avg, count]"
	aggregateNaryFunctionsText = "[least, greatest]"
)

// javaSplit mirrors String.split(","): trailing empty strings are removed.
func javaSplit(s, sep string) []string {
	parts := strings.Split(s, sep)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// ValidateDefinition runs convertCollect then convertAggregate for the
// view's Definition (the order the golden dumper uses).
func ValidateDefinition(v *View) (DefinitionResult, error) {
	var res DefinitionResult
	d := v.Definition
	if d == nil {
		return res, nil
	}
	fg, ok := v.FG(d.FG)
	if !ok {
		return res, Gate("INTERNAL", "unknown definition feature group %d", d.FG)
	}
	for _, name := range d.SourceFeatures {
		if _, ok := fg.Feature(name); !ok {
			return res, Gate("INTERNAL", "unknown source field %d.%s", d.FG, name)
		}
	}
	if err := convertCollect(v, d, fg, &res); err != nil {
		return res, err
	}
	if err := convertAggregate(v, d, fg, &res); err != nil {
		return res, err
	}
	return res, nil
}

func convertCollect(v *View, d *Definition, fg FeatureGroup, res *DefinitionResult) error {
	if d.CollectN == nil {
		return nil
	}
	collect := *d.CollectN
	if collect <= 0 {
		return Gate(CodeCollectInvalidN, "collect must be a positive integer, got: %d", collect)
	}
	if collect > v.MaxCollectN {
		return Gate(CodeCollectNTooLarge, "collect=%d exceeds the maximum allowed (%d)", collect, v.MaxCollectN)
	}
	selected := 0
	if d.SourceFeatures != nil {
		selected = len(d.SourceFeatures)
	}
	width := selected
	if width < 1 {
		width = 1
	}
	if int64(collect)*int64(width) > int64(v.MaxCollectCells) {
		return Gate(CodeCollectTooWide, "collect=%d over %d selected features = %d collected cells per row, exceeding the maximum (%d)",
			collect, selected, int64(collect)*int64(selected), v.MaxCollectCells)
	}
	orderBy := fg.EventTime
	if d.OrderBy != nil {
		orderBy = *d.OrderBy
	}
	if orderBy == "" {
		return Gate(CodeCollectNoOrderBy, "collect requires an order_by column, but feature group %s has no event_time and none was given", fg.Name)
	}
	if _, ok := fg.Feature(orderBy); !ok {
		return Gate(CodeCollectNoServableIndex, "collect order_by column '%s' is not a feature of feature group %s", orderBy, fg.Name)
	}
	primary := map[string]bool{}
	var primaryCols []string
	for _, f := range fg.Features {
		if f.Primary {
			primary[f.Name] = true
			primaryCols = append(primaryCols, f.Name)
		}
	}
	hasValueField := d.SourceFeatures == nil
	for _, name := range d.SourceFeatures {
		if !primary[name] && name != orderBy {
			hasValueField = true
		}
	}
	if !hasValueField {
		return Gate(CodeCollectInvalidN, "collect selects no value features on feature group %s (only keys and the order column '%s'): there is nothing to collect. Select at least one value feature or drop collect.",
			fg.Name, orderBy)
	}
	if fg.OnlineEnabled() && d.SourceFeatures != nil {
		for _, name := range d.SourceFeatures {
			if primary[name] || name == orderBy {
				continue
			}
			f, _ := fg.Feature(name)
			if IsComplexType(f.Type) {
				return Gate(CodeCollectInvalidN, "collect cannot serve the complex-typed feature '%s' (%s) online: its values would reach the collected structs undecoded. Select scalar features, or disable online for feature group %s",
					name, f.Type, fg.Name)
			}
		}
	}
	if fg.OnlineEnabled() {
		if len(primaryCols) < 2 || primaryCols[len(primaryCols)-1] != orderBy {
			return Gate(CodeCollectNoServableIndex, "online collect requires the order column '%s' to be the LAST primary-key column of feature group %s, preceded by the entity key column(s); found primary key (%s). Declare the primary key as (entity..., %s) or disable online for this feature group.",
				orderBy, fg.Name, strings.Join(primaryCols, ", "), orderBy)
		}
	}
	res.CollectN = &collect
	ob := orderBy
	res.CollectOrderBy = &ob
	res.CollectAscending = d.Ascending
	return nil
}

func convertAggregate(v *View, d *Definition, fg FeatureGroup, res *DefinitionResult) error {
	if len(d.Aggregate) == 0 {
		return nil
	}
	if res.CollectN != nil {
		return Gate(CodeAggregateWithCollect, "feature group: %s", fg.Name)
	}
	featureTypes := map[string]string{}
	for _, f := range fg.Features {
		if _, seen := featureTypes[f.Name]; !seen {
			featureTypes[f.Name] = f.Type
		}
	}
	outputNames := map[string]bool{}
	for _, entry := range d.Aggregate {
		key := entry.Key
		if len(entry.Fns) == 0 {
			return Gate(CodeAggregateInvalid, "no aggregation functions given for '%s'", key)
		}
		switch {
		case key == "*":
			for _, fn := range entry.Fns {
				if !strings.EqualFold(fn, "count") {
					return Gate(CodeAggregateInvalid, "the '*' key supports only 'count' (COUNT(*)), got '%s'", fn)
				}
			}
		case strings.Contains(key, ","):
			parts := javaSplit(key, ",")
			if len(parts) < 2 {
				return Gate(CodeAggregateInvalid, "a greatest/least key must list two or more features, got '%s'", key)
			}
			for _, part := range parts {
				if _, ok := featureTypes[part]; part == "" || !ok {
					return Gate(CodeAggregateInvalid, "aggregate feature '%s' is not a feature of feature group %s", part, fg.Name)
				}
				if !IsIntegerType(featureTypes[part]) {
					return Gate(CodeAggregateInvalid, "greatest/least operand '%s' has type '%s'; the online engine supports integer operands only", part, featureTypes[part])
				}
			}
			for _, fn := range entry.Fns {
				if !aggregateNaryFunctions[strings.ToLower(fn)] {
					return Gate(CodeAggregateInvalid, "multi-feature keys support only %s, got '%s'", aggregateNaryFunctionsText, fn)
				}
			}
		default:
			if _, ok := featureTypes[key]; !ok {
				return Gate(CodeAggregateInvalid, "aggregate feature '%s' is not a feature of feature group %s", key, fg.Name)
			}
			for _, fn := range entry.Fns {
				if !aggregateFunctions[strings.ToLower(fn)] {
					return Gate(CodeAggregateInvalid, "unsupported aggregation function '%s' for feature '%s'; allowed: %s", fn, key, aggregateFunctionsText)
				}
				typ := featureTypes[key]
				lower := strings.ToLower(fn)
				if (lower == "sum" || lower == "avg") && !IsNumericType(typ) {
					return Gate(CodeAggregateInvalid, "%s requires a numeric feature; '%s' has type '%s'", strings.ToUpper(lower), key, typ)
				}
				if (lower == "min" || lower == "max") && IsComplexType(typ) {
					return Gate(CodeAggregateInvalid, "%s does not support complex feature types; '%s' has type '%s'", strings.ToUpper(lower), key, typ)
				}
			}
		}
		for _, fn := range entry.Fns {
			outputName := "count"
			if key != "*" {
				outputName = strings.ReplaceAll(key, ",", "_") + "_" + strings.ToLower(fn)
			}
			if outputNames[outputName] {
				return Gate(CodeAggregateInvalid, "duplicate aggregate output feature '%s'", outputName)
			}
			outputNames[outputName] = true
		}
	}
	// review X14: the spec persists into a VARCHAR(2000) column.
	if raw, err := json.Marshal(d.Aggregate); err == nil && len(raw) > 2000 {
		return Gate(CodeAggregateInvalid, "the aggregation specification serializes to %d characters, above the persisted limit of 2000; split it across fewer functions or features", len(raw))
	}
	if d.Window != nil {
		window := *d.Window
		if window <= 0 {
			return Gate(CodeAggregateInvalid, "the aggregation window must be positive, got: %d", window)
		}
		if fg.EventTime == "" {
			return Gate(CodeAggregateInvalid, "a windowed aggregation requires the feature group to declare an event_time column")
		}
		eventTimeType := BaseType(featureTypes[fg.EventTime])
		if eventTimeType != "timestamp" {
			return Gate(CodeAggregateInvalid, "a windowed aggregation requires a TIMESTAMP event-time column; '%s' has type '%s'", fg.EventTime, featureTypes[fg.EventTime])
		}
		if window > MaxAggregateWindowSeconds {
			return Gate(CodeAggregateInvalid, "the aggregation window (%ds) exceeds the maximum of %ds (100 years)", window, MaxAggregateWindowSeconds)
		}
		if fg.TTLEnabled() && window > *fg.TTL {
			return Gate(CodeAggregateWindowExceedsTTL, "window=%ds exceeds the feature group TTL of %ds", window, *fg.TTL)
		}
	}
	if fg.OnlineEnabled() {
		if fg.EventTime == "" {
			return Gate(CodeAggregateInvalid, "online aggregation requires the feature group to declare an event_time column: the online table stores one row per primary key, so without event_time in the key there is no per-entity history to aggregate. Disable online for feature group %s or declare an event_time primary-key column", fg.Name)
		}
		var primaryCols []string
		for _, f := range fg.Features {
			if f.Primary {
				primaryCols = append(primaryCols, f.Name)
			}
		}
		if len(primaryCols) < 2 || primaryCols[len(primaryCols)-1] != fg.EventTime {
			return Gate(CodeAggregateInvalid, "online aggregation requires the event-time column '%s' to be the LAST primary-key column of feature group %s, preceded by the entity key column(s); found primary key (%s). Declare the primary key as (entity..., %s) or disable online for this feature group.",
				fg.EventTime, fg.Name, strings.Join(primaryCols, ", "), fg.EventTime)
		}
	}
	canonical := make(AggSpec, 0, len(d.Aggregate))
	for _, entry := range d.Aggregate {
		fns := make([]string, len(entry.Fns))
		for i, fn := range entry.Fns {
			fns[i] = strings.ToLower(fn)
		}
		canonical = append(canonical, AggEntry{Key: entry.Key, Fns: fns})
	}
	res.Aggregate = canonical
	res.AggregateWindow = d.Window
	return nil
}
