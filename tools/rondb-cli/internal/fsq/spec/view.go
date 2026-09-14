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

// Feature-view model of the framework: the input half of a Hopsworks golden
// fixture (HopsworksGoldenDump.CaseSpec and its nested classes), which
// mirrors the persisted entities the online-serving builder reads
// (TrainingDatasetJoin, TrainingDatasetFeature, TrainingDatasetFilter,
// TrainingDatasetFilterCondition).  Fixture JSON unmarshals into View
// directly; hand-written specs use the same shape.
//
// Design: storage/ndb/claude_files/fs_ronsql/framework_design.md §2.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// JoinType names follow org.apache.calcite.sql.JoinType; Ordinal returns
// the Calcite ordinal persisted in TrainingDatasetJoin.type (LEFT = 3 is
// the feature-view default).
type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinFull  JoinType = "FULL"
	JoinCross JoinType = "CROSS"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
)

// SqlCondition names follow the Hopsworks SqlCondition enum.
type SqlCondition string

const (
	CondLessThan           SqlCondition = "LESS_THAN"
	CondGreaterThan        SqlCondition = "GREATER_THAN"
	CondLessThanOrEqual    SqlCondition = "LESS_THAN_OR_EQUAL"
	CondGreaterThanOrEqual SqlCondition = "GREATER_THAN_OR_EQUAL"
	CondEquals             SqlCondition = "EQUALS"
	CondNotEquals          SqlCondition = "NOT_EQUALS"
	CondIn                 SqlCondition = "IN"
	CondLike               SqlCondition = "LIKE"
	CondIs                 SqlCondition = "IS"
)

// SqlFilterLogic names follow the Hopsworks SqlFilterLogic enum.
type SqlFilterLogic string

const (
	LogicSingle SqlFilterLogic = "SINGLE"
	LogicAnd    SqlFilterLogic = "AND"
	LogicOr     SqlFilterLogic = "OR"
)

// AggEntry is one (key, functions) pair of an aggregation spec.
type AggEntry struct {
	Key string
	Fns []string
}

// AggSpec is the persisted aggregation specification (TrainingDatasetJoin
// .aggSpec, a JSON object) with its insertion order preserved: the
// emitters iterate the keys in document order, which a Go map would lose.
type AggSpec []AggEntry

// UnmarshalJSON reads a JSON object preserving key order; JSON null is an
// empty spec.
func (a *AggSpec) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		*a = nil
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok != json.Delim('{') {
		return fmt.Errorf("aggregate: expected a JSON object")
	}
	var out AggSpec
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyTok.(string)
		if !ok {
			return fmt.Errorf("aggregate: non-string key")
		}
		var fns []string
		if err := dec.Decode(&fns); err != nil {
			return fmt.Errorf("aggregate %q: %w", key, err)
		}
		out = append(out, AggEntry{Key: key, Fns: fns})
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	*a = out
	return nil
}

// MarshalJSON writes the entries as a JSON object in order (null when empty).
func (a AggSpec) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte("null"), nil
	}
	var b bytes.Buffer
	b.WriteByte('{')
	for i, e := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(e.Key)
		v, _ := json.Marshal(e.Fns)
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// TDFeature mirrors TrainingDatasetFeature (HopsworksGoldenDump.Selection).
// Type carries the persisted type: for a collect feature that is the
// array<struct<...>> schema (review A1); nil means "the feature group's
// type", which the fixture loader resolves.
type TDFeature struct {
	Name            string  `json:"name"`
	Type            *string `json:"type"`
	Label           bool    `json:"label,omitempty"`
	InferenceHelper bool    `json:"inferenceHelper,omitempty"`
	TrainingHelper  bool    `json:"trainingHelper,omitempty"`
	// Index is the persisted feature index (selection order); the fixture
	// loader assigns it from the document order when absent.
	Index int `json:"index,omitempty"`
}

// Join mirrors TrainingDatasetJoin (HopsworksGoldenDump.Node).  Index 0 is
// the left/label feature group; Parent 0 is the star level.
type Join struct {
	Index     int         `json:"index"`
	Parent    int         `json:"parent"`
	FG        int         `json:"fg"`
	Type      JoinType    `json:"type"`
	Prefix    *string     `json:"prefix"`
	On        [][2]string `json:"on"`
	Features  []TDFeature `json:"features"`
	Aggregate AggSpec     `json:"aggregate"`
	Window    *int64      `json:"window"`
	CollectN  *int        `json:"collectN"`
	OrderBy   *string     `json:"orderBy"`
	Ascending bool        `json:"ascending"`
	LeftFGSet bool        `json:"-"`
	LeftFGID  int         `json:"-"`
}

// PrefixOrEmpty returns the join prefix, "" when null (the DTO convention).
func (j Join) PrefixOrEmpty() string {
	if j.Prefix == nil {
		return ""
	}
	return *j.Prefix
}

// Filter mirrors TrainingDatasetFilter + TrainingDatasetFilterCondition
// (HopsworksGoldenDump.Predicate): one node of the persisted filter tree.
// A node with Feature == "" carries no condition (a pure OR/AND node).
type Filter struct {
	FG                  int            `json:"fg"`
	JoinIndex           *int           `json:"joinIndex"`
	Logic               SqlFilterLogic `json:"logic"`
	Feature             string         `json:"feature"`
	Condition           *SqlCondition  `json:"condition"`
	Value               *string        `json:"value"`
	ValueFeatureGroupID *int           `json:"valueFeatureGroupId"`
}

// Options mirrors the builder entry-point flags.
type Options struct {
	Batch             bool `json:"batch"`
	InferenceHelpers  bool `json:"inferenceHelpers"`
	Logging           bool `json:"logging"`
	VectorWithHelpers bool `json:"vectorWithHelpers"`
}

// Definition is a feature-view creation request for one feature group
// (QueryDTO subset) checked by the definition-time validators.
type Definition struct {
	FG             int      `json:"fg"`
	SourceFeatures []string `json:"sourceFeatures"`
	CollectN       *int     `json:"collectN"`
	OrderBy        *string  `json:"orderBy"`
	Ascending      bool     `json:"ascending"`
	Aggregate      AggSpec  `json:"aggregate"`
	Window         *int64   `json:"window"`
}

// View is a feature view (or query-backed training dataset) as persisted,
// plus the builder options and the validation settings — the input half
// of a golden fixture.
type View struct {
	Name            string         `json:"name"`
	FGs             []FeatureGroup `json:"fgs"`
	Joins           []Join         `json:"joins"`
	Filters         []Filter       `json:"filters"`
	Options         Options        `json:"options"`
	Definition      *Definition    `json:"definition"`
	TrainingDataset bool           `json:"trainingDataset"`
	MaxCollectN     int            `json:"maxCollectN"`
	MaxCollectCells int            `json:"maxCollectCells"`
}

// FG returns the feature group with the id.
func (v *View) FG(id int) (FeatureGroup, bool) {
	for _, fg := range v.FGs {
		if fg.ID == id {
			return fg, true
		}
	}
	return FeatureGroup{}, false
}

// FGMap returns the feature groups keyed by id.
func (v *View) FGMap() map[int]FeatureGroup {
	m := map[int]FeatureGroup{}
	for _, fg := range v.FGs {
		m[fg.ID] = fg
	}
	return m
}

// Normalize resolves what the golden dumper resolves when it turns the
// input into entities: feature indexes from document order, feature types
// from the feature group when null, and the left feature group of every
// non-root join (its parent's feature group).  Idempotent.
func (v *View) Normalize() error {
	fgs := v.FGMap()
	// Feature indexes: the golden dumper numbers selections in document
	// order across all joins; a hand-written spec may set them explicitly.
	assignIndexes := true
	for _, j := range v.Joins {
		for _, f := range j.Features {
			if f.Index != 0 {
				assignIndexes = false
			}
		}
	}
	index := 0
	parents := map[int]int{}
	for i := range v.Joins {
		parents[v.Joins[i].Index] = v.Joins[i].FG
	}
	for i := range v.Joins {
		j := &v.Joins[i]
		fg, ok := fgs[j.FG]
		if !ok {
			return fmt.Errorf("join %d: unknown feature group %d", j.Index, j.FG)
		}
		if j.Type == "" {
			j.Type = JoinLeft
		}
		for k := range j.Features {
			f := &j.Features[k]
			if assignIndexes {
				f.Index = index
				index++
			}
			if f.Type == nil {
				col, ok := fg.Feature(f.Name)
				if !ok {
					return fmt.Errorf("join %d: unknown source field %d.%s", j.Index, j.FG, f.Name)
				}
				t := col.Type
				f.Type = &t
			}
		}
		if j.Index != 0 {
			pfg, ok := parents[j.Parent]
			if !ok {
				return fmt.Errorf("join %d: unknown parent index %d", j.Index, j.Parent)
			}
			j.LeftFGSet, j.LeftFGID = true, pfg
		}
	}
	return nil
}

// JoinsSorted returns the joins ordered by index
// (TrainingDatasetController.getJoinsSorted).
func (v *View) JoinsSorted() []Join {
	out := append([]Join(nil), v.Joins...)
	sort.SliceStable(out, func(a, b int) bool { return out[a].Index < out[b].Index })
	return out
}

// GateError is a Hopsworks FeaturestoreException: the RESTCodes name and
// the user message, compared verbatim against the golden fixtures.
type GateError struct {
	Code    string
	Message string
}

func (e *GateError) Error() string { return e.Code + ": " + e.Message }

// Gate constructs a GateError.
func Gate(code, format string, args ...interface{}) *GateError {
	return &GateError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Hopsworks RESTCodes.FeaturestoreErrorCode names used by the port.
const (
	CodeCollectInvalidN              = "COLLECT_INVALID_N"
	CodeCollectNTooLarge             = "COLLECT_N_TOO_LARGE"
	CodeCollectTooWide               = "COLLECT_TOO_WIDE"
	CodeCollectNoOrderBy             = "COLLECT_NO_ORDER_BY"
	CodeCollectNoServableIndex       = "COLLECT_NO_SERVABLE_INDEX"
	CodeAggregateWithCollect         = "AGGREGATE_WITH_COLLECT"
	CodeAggregateInvalid             = "AGGREGATE_INVALID"
	CodeAggregateWindowExceedsTTL    = "AGGREGATE_WINDOW_EXCEEDS_TTL"
	CodeCollectUnsupportedOnlineFilt = "COLLECT_UNSUPPORTED_ONLINE_FILTER"
	CodeFeaturestoreOnlineNotEnabled = "FEATURESTORE_ONLINE_NOT_ENABLED"
	CodePrimaryKeyRequired           = "PRIMARY_KEY_REQUIRED"
	CodeTrainingDatasetNoQuery       = "TRAINING_DATASET_NO_QUERY"
	CodeQueryFailedFGDeleted         = "QUERY_FAILED_FG_DELETED"
	CodeJoinOnPartialPrimaryKey      = "JOIN_ON_PARTIAL_PRIMARY_KEY"
	CodeForeignKeyNotPrimaryKey      = "FOREIGN_KEY_NOT_PRIMARY_KEY"
	CodeFeatureDoesNotExist          = "FEATURE_DOES_NOT_EXIST"
	CodeNestedJoinsRecursionExceeded = "NESTED_JOINS_RECURSION_LIMIT_EXCEEDED"
)
