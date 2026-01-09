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
