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

// Package ddl is a port of the Hopsworks online table builder
// (hopsworks-common/.../featuregroup/online/OnlineFeaturegroupController.java,
// HOPSWORKS_REF f85a653bc): buildCreateStatement, getOnlineType,
// resolvePkIndexClause, addTtlCommentToConfig and ensureReadBackupComment.
//
// The framework creates its feature-group tables with exactly the DDL text
// Hopsworks would produce, so the RonSQL test data is indistinguishable
// from a real online feature store (data_model.md §1).
package ddl

import (
	"fmt"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// OnlineFeaturegroupController constants.
const (
	varbinaryDefault = "VARBINARY(100)"
	varcharDefault   = "VARCHAR(100)"
	ndbTablePrefix   = "NDB_TABLE="
	indexTypeHash    = "HASH"
	indexTypeOrdered = "ORDERED"
)

// SUPPORTED_MYSQL_TYPES: offline types that are used as-is online.
var supportedMySQLTypes = []string{
	"INT", "TINYINT", "SMALLINT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIMESTAMP",
}

// NON_INDEXABLE_ONLINE_TYPES: types that cannot appear in a secondary index.
var nonIndexableOnlineTypes = map[string]bool{
	"TEXT": true, "TINYTEXT": true, "MEDIUMTEXT": true, "LONGTEXT": true,
	"BLOB": true, "TINYBLOB": true, "MEDIUMBLOB": true, "LONGBLOB": true,
}

// OnlineType ports getOnlineType (:667-689): an explicit online type wins
// (lower-cased); supported MySQL type prefixes pass the offline type
// through unchanged (so "decimal(12,2)" and "timestamp" survive as typed);
// boolean -> tinyint; string -> VARCHAR(100); everything else ->
// VARBINARY(100).
func OnlineType(f spec.Feature) string {
	if f.OnlineType != "" {
		return strings.ToLower(f.OnlineType)
	}
	upper := strings.ToUpper(f.Type)
	for _, t := range supportedMySQLTypes {
		if strings.HasPrefix(upper, t) {
			return f.Type
		}
	}
	switch strings.ToLower(f.Type) {
	case "boolean":
		return "tinyint"
	case "string":
		return varcharDefault
	default:
		return varbinaryDefault
	}
}

// pkIndexClause ports resolvePkIndexClause (:564-583).
func pkIndexClause(cfg spec.OnlineConfig) (string, error) {
	requested := strings.TrimSpace(cfg.PrimaryKeyIndexType)
	if requested == "" {
		return "", nil
	}
	switch strings.ToUpper(requested) {
	case indexTypeHash:
		return " USING HASH", nil
	case indexTypeOrdered:
		return "", nil // no modifier: RonDB creates both hash and ordered indexes
	default:
		return "", fmt.Errorf("invalid primaryKeyIndexType %q; allowed values: %s, %s",
			requested, indexTypeHash, indexTypeOrdered)
	}
}

// ttlComment ports buildTtlComment (:888-893).
func ttlComment(fg spec.FeatureGroup) string {
	if fg.TTLEnabled() {
		return fmt.Sprintf("TTL=%d@%s", *fg.TTL, fg.EventTime)
	}
	return "TTL=OFF"
}

// Comments ports the comment pipeline that runs before buildCreateStatement:
// addTtlCommentToConfig (:901-912, only for TTL-enabled feature groups)
// followed by ensureReadBackupComment (:920-942).  The result is what
// buildCreateStatement joins with "," into COMMENT='...'.
func Comments(fg spec.FeatureGroup) []string {
	comments := append([]string(nil), fg.OnlineConfig.Comments...)
	contains := func(sub string) bool {
		for _, c := range comments {
			if strings.Contains(c, sub) {
				return true
			}
		}
		return false
	}
	if fg.TTLEnabled() {
		if contains(ndbTablePrefix) {
			comments = append(comments, ttlComment(fg))
		} else {
			comments = append(comments, ndbTablePrefix+ttlComment(fg))
		}
	}
	const readBackup = "READ_BACKUP="
	if !contains(readBackup) {
		if contains(ndbTablePrefix) {
			comments = append(comments, readBackup+"1")
		} else {
			comments = append(comments, ndbTablePrefix+readBackup+"1")
		}
	}
	return comments
}

// validateNoOfflineOnlyKeyColumns ports :510-540.
func validateNoOfflineOnlyKeyColumns(fg spec.FeatureGroup) error {
	offlineOnly := map[string]bool{}
	for _, f := range fg.Features {
		if f.OfflineOnly {
			offlineOnly[f.Name] = true
		}
	}
	if len(offlineOnly) == 0 {
		return nil
	}
	for _, f := range fg.Features {
		if f.Primary && offlineOnly[f.Name] {
			return fmt.Errorf("primary key column %q is offline_only", f.Name)
		}
	}
	if fg.EventTime != "" && offlineOnly[fg.EventTime] {
		return fmt.Errorf("event_time column %q is offline_only", fg.EventTime)
	}
	for _, idx := range fg.OnlineConfig.SecondaryIndexes {
		for _, col := range idx {
			if offlineOnly[col] {
				return fmt.Errorf("secondary index column %q is offline_only", col)
			}
		}
	}
	return nil
}

// validateSecondaryIndexes ports validateSecondaryIndexes: every column must
// exist and must not be a TEXT/BLOB type.
func validateSecondaryIndexes(fg spec.FeatureGroup, onlineTypes map[string]string) error {
	for _, idx := range fg.OnlineConfig.SecondaryIndexes {
		if len(idx) == 0 {
			return fmt.Errorf("empty secondary index")
		}
		for _, col := range idx {
			t, ok := onlineTypes[col]
			if !ok {
				return fmt.Errorf("secondary index column %q is not a feature", col)
			}
			if nonIndexableOnlineTypes[strings.ToUpper(spec.BaseType(t))] {
				return fmt.Errorf("secondary index column %q has non-indexable type %s", col, t)
			}
		}
	}
	return nil
}

// BuildCreateStatement ports buildCreateStatement (:410-508) byte for
// byte, including its spacing quirks ("`db`.`t`(" and ")ENGINE=ndbcluster").
func BuildCreateStatement(db string, fg spec.FeatureGroup) (string, error) {
	if err := validateNoOfflineOnlyKeyColumns(fg); err != nil {
		return "", err
	}
	pkClause, err := pkIndexClause(fg.OnlineConfig)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString("`" + db + "`")
	b.WriteString(".")
	b.WriteString("`" + fg.TableName() + "`")
	b.WriteString("(")

	var columns []string
	onlineTypes := map[string]string{}
	for _, f := range fg.Features {
		onlineTypes[f.Name] = OnlineType(f)
		if f.OfflineOnly {
			continue
		}
		var col strings.Builder
		col.WriteString("`" + f.Name + "`")
		col.WriteString(" ")
		col.WriteString(OnlineType(f))
		if f.DefaultValue != nil {
			col.WriteString(" NOT NULL DEFAULT ")
			if strings.EqualFold(f.Type, "string") {
				col.WriteString("'" + *f.DefaultValue + "'")
			} else {
				col.WriteString(*f.DefaultValue)
			}
		}
		if fg.EventTime != "" && fg.EventTime == f.Name {
			// TTL columns must stay in memory even on a disk tablespace.
			col.WriteString(" STORAGE MEMORY")
		}
		columns = append(columns, col.String())
	}
	b.WriteString(strings.Join(columns, ", "))

	var pkNames []string
	for _, f := range fg.Features {
		if f.Primary {
			pkNames = append(pkNames, f.Name)
		}
	}
	if len(pkNames) > 0 {
		b.WriteString(", PRIMARY KEY (`")
		b.WriteString(strings.Join(pkNames, "`,`"))
		b.WriteString("`)")
		b.WriteString(pkClause)
	}

	if fg.TTLEnabled() {
		b.WriteString(", KEY `ttl_index`(`" + fg.EventTime + "`)")
	}

	if len(fg.OnlineConfig.SecondaryIndexes) > 0 {
		if err := validateSecondaryIndexes(fg, onlineTypes); err != nil {
			return "", err
		}
		for _, idx := range fg.OnlineConfig.SecondaryIndexes {
			b.WriteString(", KEY `idx_" + strings.Join(idx, "_") + "`(`")
			b.WriteString(strings.Join(idx, "`,`"))
			b.WriteString("`)")
		}
	}

	b.WriteString(")")
	b.WriteString("ENGINE=ndbcluster")
	b.WriteString(" ")
	b.WriteString("COMMENT='" + strings.Join(Comments(fg), ",") + "'")
	if fg.OnlineConfig.TableSpace != "" {
		b.WriteString("/*!50100 TABLESPACE `" + fg.OnlineConfig.TableSpace + "` STORAGE DISK */")
	}
	return b.String(), nil
}

// CreateDatabase returns the statement the online feature store facade
// runs for a new project database (OnlineFeaturestoreFacade.java:127).
func CreateDatabase(db string) string {
	return "CREATE DATABASE IF NOT EXISTS `" + db + "`;"
}

// DropTable returns a DROP TABLE IF EXISTS statement for the feature group.
func DropTable(db string, fg spec.FeatureGroup) string {
	return "DROP TABLE IF EXISTS `" + db + "`.`" + fg.TableName() + "`;"
}

// DropDatabase returns a DROP DATABASE IF EXISTS statement.
func DropDatabase(db string) string {
	return "DROP DATABASE IF EXISTS `" + db + "`;"
}
