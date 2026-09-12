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

package emit

// Statement is the ServingPreparedStatementDTO as the golden dumper
// snapshots it: every declared field, JSON names as in the fixtures, null
// (nil) where the Java field is null.  The fixture comparison marshals both
// sides, so nil-ness must match the Java object exactly.

// Param is PreparedStatementParameterDTO.
type Param struct {
	Name  string `json:"name"`
	Index int    `json:"index"`
}

// CollectFilter is ServingPreparedStatementDTO.CollectFilterDTO: a
// /scan-expressible feature-view filter carried structured on the DTO.
type CollectFilter struct {
	Feature   string `json:"feature"`
	Condition string `json:"condition"`
	Value     string `json:"value"`
	Type      string `json:"type"`
}

// Statement mirrors ServingPreparedStatementDTO.
type Statement struct {
	AggregateFeatureNames       []string        `json:"aggregateFeatureNames"`
	AggregateWindow             *int64          `json:"aggregateWindow"`
	CollectAscending            *bool           `json:"collectAscending"`
	CollectFeatureName          *string         `json:"collectFeatureName"`
	CollectFilterApplied        *bool           `json:"collectFilterApplied"`
	CollectFilters              []CollectFilter `json:"collectFilters"`
	CollectN                    *int            `json:"collectN"`
	CollectOrderBy              *string         `json:"collectOrderBy"`
	CollectSourceFeatures       []string        `json:"collectSourceFeatures"`
	FeatureGroupID              int             `json:"featureGroupId"`
	Prefix                      *string         `json:"prefix"`
	PreparedStatementIndex      int             `json:"preparedStatementIndex"`
	PreparedStatementParameters []Param         `json:"preparedStatementParameters"`
	QueryOnline                 *string         `json:"queryOnline"`
	QueryOnlineScan             *string         `json:"queryOnlineScan"`
	QueryRonsql                 *string         `json:"queryRonsql"`
	RonsqlDatabase              *string         `json:"ronsqlDatabase"`
	SnowflakeTemplate           *bool           `json:"snowflakeTemplate"`
	SnowflakeTemplates          []string        `json:"snowflakeTemplates"`
}

// TemplateCount is the number of RonSQL templates the statement carries
// (queryRonsql plus snowflake templates), the golden dumper's
// expectedTemplates accounting.
func (s Statement) TemplateCount() int {
	n := 0
	if s.QueryRonsql != nil {
		n++
	}
	n += len(s.SnowflakeTemplates)
	return n
}

func strp(s string) *string { return &s }
func boolp(b bool) *bool    { return &b }
