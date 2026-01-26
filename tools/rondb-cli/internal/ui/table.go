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

package ui

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
)

// RenderTable renders a table with headers and rows, returning a formatted string
// with Unicode box drawing and row count footer.
func RenderTable(headers []string, rows [][]string) string {
	buf := &bytes.Buffer{}
	table := tablewriter.NewWriter(buf)

	table.SetAutoFormatHeaders(false)
	table.SetHeader(headers)
	table.AppendBulk(rows)

	table.SetBorders(tablewriter.Border{
		Left:   true,
		Right:  true,
		Top:    true,
		Bottom: true,
	})
	table.SetCenterSeparator("┼")
	table.SetColumnSeparator("│")
	table.SetRowSeparator("─")

	table.Render()

	output := buf.String()
	rowCount := len(rows)
	footer := fmt.Sprintf("%d row", rowCount)
	if rowCount != 1 {
		footer += "s"
	}

	return strings.TrimSpace(output) + "\n" + footer + "\n"
}

// RenderSQLResult renders a SQL result set with columns and rows, returning a formatted string
// with Unicode box drawing and row count footer. It converts interface{} values to strings.
func RenderSQLResult(columns []string, rows [][]interface{}) string {
	buf := &bytes.Buffer{}
	table := tablewriter.NewWriter(buf)

	table.SetAutoFormatHeaders(false)
	table.SetHeader(columns)

	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		stringRow := make([]string, len(row))
		for j, cell := range row {
			stringRow[j] = formatCell(cell)
		}
		stringRows[i] = stringRow
	}

	table.AppendBulk(stringRows)

	table.SetBorders(tablewriter.Border{
		Left:   true,
		Right:  true,
		Top:    true,
		Bottom: true,
	})
	table.SetCenterSeparator("┼")
	table.SetColumnSeparator("│")
	table.SetRowSeparator("─")

	table.Render()

	output := buf.String()
	rowCount := len(rows)
	footer := fmt.Sprintf("%d row", rowCount)
	if rowCount != 1 {
		footer += "s"
	}

	return strings.TrimSpace(output) + "\n" + footer + "\n"
}

// RenderTableWithDuration renders a table with a custom duration in the footer.
func RenderTableWithDuration(headers []string, rows [][]string, duration time.Duration) string {
	buf := &bytes.Buffer{}
	table := tablewriter.NewWriter(buf)

	table.SetAutoFormatHeaders(false)
	table.SetHeader(headers)
	table.AppendBulk(rows)

	table.SetBorders(tablewriter.Border{
		Left:   true,
		Right:  true,
		Top:    true,
		Bottom: true,
	})
	table.SetCenterSeparator("┼")
	table.SetColumnSeparator("│")
	table.SetRowSeparator("─")

	table.Render()

	output := buf.String()
	rowCount := len(rows)
	footer := fmt.Sprintf("%d row", rowCount)
	if rowCount != 1 {
		footer += "s"
	}

	ms := duration.Milliseconds()
	return strings.TrimSpace(output) + "\n" + fmt.Sprintf("%s (%.1fms)\n", footer, float64(ms))
}

// formatCell converts a cell value to a displayable string
func formatCell(cell interface{}) string {
	if cell == nil {
		return "NULL"
	}
	switch v := cell.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// RenderSQLResultWithDuration renders a SQL result set with a custom duration in the footer.
func RenderSQLResultWithDuration(columns []string, rows [][]interface{}, duration time.Duration) string {
	buf := &bytes.Buffer{}
	table := tablewriter.NewWriter(buf)

	table.SetAutoFormatHeaders(false)
	table.SetHeader(columns)

	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		stringRow := make([]string, len(row))
		for j, cell := range row {
			stringRow[j] = formatCell(cell)
		}
		stringRows[i] = stringRow
	}

	table.AppendBulk(stringRows)

	table.SetBorders(tablewriter.Border{
		Left:   true,
		Right:  true,
		Top:    true,
		Bottom: true,
	})
	table.SetCenterSeparator("┼")
	table.SetColumnSeparator("│")
	table.SetRowSeparator("─")

	table.Render()

	output := buf.String()
	rowCount := len(rows)
	footer := fmt.Sprintf("%d row", rowCount)
	if rowCount != 1 {
		footer += "s"
	}

	ms := duration.Milliseconds()
	return strings.TrimSpace(output) + "\n" + fmt.Sprintf("%s (%.1fms)\n", footer, float64(ms))
}
