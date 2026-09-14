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

// Package mysqltwin renders the MySQL serving statements that the Hopsworks
// builder produces through Calcite (ConstructorController.generateSQL,
// wrapOnlineCollect, wrapOnlineScan, and the nested-join query of
// TrainingDatasetController.getQuery) in the exact text the golden fixtures
// captured: MysqlSqlDialect output with backticked identifiers, `fg<n>`
// table aliases, newline-separated clauses, `IN ?` for batch keys.
//
// The rendering is a re-implementation of the observed output, not a port
// of Calcite; the golden fixtures pin it, and shapes no fixture covers are
// marked below.
package mysqltwin

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// Table is a table reference with its Calcite alias.
type Table struct {
	DB, Name, Alias string
}

func (t Table) String() string {
	return "`" + t.DB + "`.`" + t.Name + "` AS `" + t.Alias + "`"
}

// Col is a projected column: `alias`.`name` AS `out`.
type Col struct {
	Alias, Name, Out string
	Type             string
	DefaultValue     *string
}

// Expression mirrors ConstructorController.generateFeature/caseWhenDefault.
// Defaults are SQL expressions except for the exact offline type "string".
func (c Col) Expression() string {
	ref := "`" + c.Alias + "`.`" + c.Name + "`"
	if c.DefaultValue == nil {
		return ref
	}
	value := *c.DefaultValue
	if strings.EqualFold(c.Type, "string") {
		value = quote(value)
	}
	return "CASE WHEN " + ref + " IS NULL THEN " + value + " ELSE " + ref + " END"
}

func (c Col) String() string {
	return c.Expression() + " AS `" + c.Out + "`"
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func cols(cs []Col) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = c.String()
	}
	return strings.Join(parts, ", ")
}

// KeyWhere renders the entity-key predicates, including default expressions.
// buildDTO has already changed bind-key types to "parameter", as Java does.
func KeyWhere(keys []Col, batch bool) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k.Expression()
		if !batch {
			parts[i] += " = ?"
		}
	}
	if !batch {
		return strings.Join(parts, " AND ")
	}
	if len(parts) == 1 {
		return parts[0] + " IN ?"
	}
	return "(" + strings.Join(parts, ", ") + ") IN ?"
}

// These match the DateString/TimestampString constructors in the pinned
// Calcite 1.32.1 jar, not Go time parsing (which has different validation).
var dateLiteral = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
var timestampLiteral = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]*[1-9])?$`)

// Literal mirrors FilterController.getSQLNode and the MySQL dialect's
// date/time rendering. Collect's queryOnline and queryOnlineScan use it;
// applyMysqlAggregate deliberately uses the separately rendered RonSQL filter.
func Literal(value, featureType string) (string, error) {
	switch strings.ToLower(featureType) {
	case "string":
		return quote(value), nil
	case "date":
		if !dateLiteral.MatchString(value) || value[:4] == "0000" ||
			value[5:7] < "01" || value[5:7] > "12" || value[8:10] < "01" || value[8:10] > "31" {
			return "", fmt.Errorf("invalid Calcite DATE literal %q", value)
		}
		return "DATE " + quote(value), nil
	case "timestamp":
		if !timestampLiteral.MatchString(value) {
			return "", fmt.Errorf("invalid Calcite TIMESTAMP literal %q", value)
		}
		// SqlTimestampLiteral(precision=3) truncates, never rounds up, and
		// pads the fractional part to exactly three digits.
		fraction := ""
		if len(value) > 19 {
			fraction = value[20:]
		}
		fraction = (fraction + "000")[:3]
		return "TIMESTAMP " + quote(value[:19]+"."+fraction), nil
	default:
		// Numeric and prepared-statement types become bare SqlIdentifiers.
		return value, nil
	}
}

// Operator maps a SqlCondition to its SQL operator.
func Operator(c spec.SqlCondition) string {
	switch c {
	case spec.CondEquals:
		return "="
	case spec.CondNotEquals:
		return "<>"
	case spec.CondGreaterThan:
		return ">"
	case spec.CondGreaterThanOrEqual:
		return ">="
	case spec.CondLessThan:
		return "<"
	case spec.CondLessThanOrEqual:
		return "<="
	case spec.CondLike:
		return "LIKE"
	}
	return string(c)
}

// PointRead renders a single-table read (generateSQL over one feature group).
func PointRead(cs []Col, t Table, where string) string {
	return "SELECT " + cols(cs) + "\nFROM " + t.String() + "\nWHERE " + where
}

// CollectWindow renders wrapOnlineCollect: the ROW_NUMBER() top-N per entity.
func CollectWindow(cs []Col, t Table, where string, partition []string, order Col, ascending bool) string {
	parts := make([]string, len(partition))
	for i, k := range partition {
		parts[i] = "`" + t.Alias + "`.`" + k + "`"
	}
	outer := "ORDER BY hopsworks_collect_rank"
	if ascending {
		outer += " DESC"
	}
	return "SELECT *\nFROM (SELECT " + cols(cs) + ", ROW_NUMBER() OVER (PARTITION BY " + strings.Join(parts, ", ") +
		" ORDER BY `" + order.Alias + "`.`" + order.Name + "` DESC) AS hopsworks_collect_rank\nFROM " + t.String() +
		"\nWHERE " + where + ") AS hopsworks_collect_src\nWHERE hopsworks_collect_rank <= ?\n" + outer
}

// CollectScan renders wrapOnlineScan: the direct ORDER BY ... LIMIT ? read.
func CollectScan(cs []Col, t Table, where string, order Col) string {
	return "SELECT " + cols(cs) + "\nFROM " + t.String() + "\nWHERE " + where +
		"\nORDER BY `" + order.Alias + "`.`" + order.Name + "` DESC\nLIMIT ?"
}

// JoinClause is one nested join: `<TYPE> JOIN <table> ON parent.left = child.right [AND ...]`.
type JoinClause struct {
	Type   spec.JoinType
	Table  Table
	Parent string // parent alias
	On     [][2]string
}

func joinKeyword(t spec.JoinType) string {
	switch t {
	case spec.JoinLeft:
		return "LEFT JOIN"
	case spec.JoinRight:
		return "RIGHT JOIN"
	case spec.JoinFull:
		return "FULL JOIN"
	case spec.JoinCross:
		return "CROSS JOIN"
	default:
		return "INNER JOIN"
	}
}

// Nested renders the snowflake subtree query of getQuery.
func Nested(cs []Col, root Table, joins []JoinClause, where string) string {
	var b strings.Builder
	b.WriteString("SELECT " + cols(cs) + "\nFROM " + root.String())
	for _, j := range joins {
		conds := make([]string, len(j.On))
		for i, on := range j.On {
			conds[i] = "`" + j.Parent + "`.`" + on[0] + "` = `" + j.Table.Alias + "`.`" + on[1] + "`"
		}
		b.WriteString("\n" + joinKeyword(j.Type) + " " + j.Table.String() + " ON " + strings.Join(conds, " AND "))
	}
	b.WriteString("\nWHERE " + where)
	return b.String()
}
