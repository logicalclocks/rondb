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

// Package spec holds the feature-store model of the RonSQL feature-store
// test framework (RONDB-1121): feature groups and their features, mirroring
// the Hopsworks entities the online-serving builders read (Featuregroup,
// FeatureGroupFeatureDTO, OnlineConfigDTO).  The feature-view / join model
// used by the statement emitters is added in execution phase E2.
//
// Design: storage/ndb/claude_files/fs_ronsql/framework_design.md §2 and
// data_model.md §9.
package spec

import (
	"fmt"
	"strings"
)

// Feature is one feature of a feature group (FeatureGroupFeatureDTO).
//
// Type is the OFFLINE (Hive/Spark) type a Hopsworks user declares, e.g.
// "bigint", "string", "timestamp", "decimal(12,2)", "array<string>".  It
// drives both the online DDL type mapping (package ddl) and the
// definition-time gates.  OnlineType, when set, overrides the mapping
// exactly like FeatureGroupFeatureDTO.onlineType.
type Feature struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	Primary      bool    `json:"primary,omitempty"`
	OnlineType   string  `json:"onlineType,omitempty"`
	DefaultValue *string `json:"defaultValue,omitempty"`
	OfflineOnly  bool    `json:"offlineOnly,omitempty"`
	// Complex is a framework-only hint: the feature maps to VARBINARY
	// online and must never be selected by the spec generator (E6); it
	// exists so E7 can probe the engine with it deliberately.
	Complex bool `json:"complex,omitempty"`
}

// OnlineConfig mirrors OnlineConfigDTO: the knobs that shape the online
// CREATE TABLE statement.
type OnlineConfig struct {
	// PrimaryKeyIndexType is "HASH" (PRIMARY KEY ... USING HASH), "ORDERED"
	// or "" (both mean the NDB default: hash + ordered index).
	PrimaryKeyIndexType string     `json:"primaryKeyIndexType,omitempty"`
	SecondaryIndexes    [][]string `json:"secondaryIndexes,omitempty"`
	Comments            []string   `json:"onlineComments,omitempty"`
	TableSpace          string     `json:"tableSpace,omitempty"`
}

// FeatureGroup mirrors Featuregroup plus its feature list.  The JSON
// shape is the "fgs" entry of a Hopsworks golden fixture
// (HopsworksGoldenDump.Group), so fixtures unmarshal directly.
type FeatureGroup struct {
	// ID is the Featuregroup id (fixture "id"); joins and filters refer to it.
	ID      int    `json:"id,omitempty"`
	Name    string `json:"name"`
	Version int    `json:"version"`
	// FeaturestoreID identifies the feature store (project); snowflake
	// templates require every feature group of a subtree to share it.
	FeaturestoreID int `json:"featurestoreId,omitempty"`
	// Online is Featuregroup.onlineEnabled; nil means true, so hand-written
	// specs can omit it.
	Online    *bool  `json:"online,omitempty"`
	EventTime string `json:"eventTime,omitempty"`
	// TTL in seconds; nil or 0 means TTL disabled.
	TTL *int64 `json:"ttl,omitempty"`
	// OnlineConfig is the OnlineConfigDTO used by the DDL port.
	OnlineConfig OnlineConfig `json:"onlineConfig,omitempty"`
	// OnlineDB overrides the online database name (the real database the
	// statements run against); empty means the golden-dumper convention.
	OnlineDB string    `json:"onlineDatabase,omitempty"`
	Features []Feature `json:"features"`
}

// TableName is the online table name: <name>_<version>
// (Utils.getFeaturegroupName).
func (fg FeatureGroup) TableName() string {
	return fmt.Sprintf("%s_%d", fg.Name, fg.Version)
}

// OnlineEnabled reports Featuregroup.isOnlineEnabled().
func (fg FeatureGroup) OnlineEnabled() bool { return fg.Online == nil || *fg.Online }

// OnlineDatabase is the online feature-store database of the feature
// group's project as the golden dumper mocks it: "golden_<featurestoreId>_fs".
// Real deployments use the project name; the test framework only needs
// consistency between the spec and the emitted templates.
func (fg FeatureGroup) OnlineDatabase() string {
	if fg.OnlineDB != "" {
		return fg.OnlineDB
	}
	return fmt.Sprintf("golden_%d_fs", fg.FeaturestoreID)
}

// TTLEnabled reports Featuregroup.isTtlEnabled().
func (fg FeatureGroup) TTLEnabled() bool { return fg.TTL != nil && *fg.TTL > 0 }

// PrimaryKeys returns the primary-key features in declared order (the
// order becomes the prepared-statement parameter order, review X4).
func (fg FeatureGroup) PrimaryKeys() []Feature {
	var pks []Feature
	for _, f := range fg.Features {
		if f.Primary {
			pks = append(pks, f)
		}
	}
	return pks
}

// Feature looks a feature up by name.
func (fg FeatureGroup) Feature(name string) (Feature, bool) {
	for _, f := range fg.Features {
		if f.Name == name {
			return f, true
		}
	}
	return Feature{}, false
}

// FeatureNames returns the feature names in declared order.
func (fg FeatureGroup) FeatureNames() []string {
	names := make([]string, 0, len(fg.Features))
	for _, f := range fg.Features {
		names = append(names, f.Name)
	}
	return names
}

// Type classification, ported from QueryController.java:330-352.

var integerTypes = map[string]bool{
	"int": true, "integer": true, "bigint": true, "smallint": true, "tinyint": true, "long": true,
}

var numericTypes = map[string]bool{
	"int": true, "integer": true, "bigint": true, "smallint": true, "tinyint": true, "long": true,
	"float": true, "double": true, "decimal": true,
}

// BaseType lower-cases the type and strips a parameter list:
// "DECIMAL(12,2)" -> "decimal", "array<string>" -> "array<string>".
func BaseType(t string) string {
	return strings.TrimSpace(strings.SplitN(strings.ToLower(t), "(", 2)[0])
}

// IsIntegerType reports whether the offline type is an integer type
// (GREATEST/LEAST operands must be).
func IsIntegerType(t string) bool { return integerTypes[BaseType(t)] }

// IsNumericType reports whether the offline type is numeric (SUM/AVG).
func IsNumericType(t string) bool { return numericTypes[BaseType(t)] }

// IsComplexType reports whether the offline type is an array, map,
// struct or binary type (MIN/MAX refused).
func IsComplexType(t string) bool {
	base := BaseType(t)
	return strings.HasPrefix(base, "array") || strings.HasPrefix(base, "map") ||
		strings.HasPrefix(base, "struct") || base == "binary"
}
