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

package dsl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Filter represents a column filter in a pk-read request
type Filter struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
}

// ReadColumn represents a column to read with its return type
type ReadColumn struct {
	Column         string `json:"column"`
	DataReturnType string `json:"dataReturnType"`
}

// PkReadRequest represents a single pk-read request body
type PkReadRequest struct {
	Filters     []Filter     `json:"filters"`
	ReadColumns []ReadColumn `json:"readColumns,omitempty"`
	OperationID string       `json:"operationId,omitempty"`
}

// BatchOperation represents a single operation in a batch request
type BatchOperation struct {
	Method      string        `json:"method"`
	RelativeURL string        `json:"relative-url"`
	Body        PkReadRequest `json:"body"`
}

// BatchRequest represents a batch of operations
type BatchRequest struct {
	Operations []BatchOperation `json:"operations"`
}

// ParseSingleRead parses a single READ command
// Format: READ db.table [col0, col1] FILTER id0=0, id1=0
func ParseSingleRead(line string) (database, table string, req *PkReadRequest, err error) {
	line = strings.TrimSpace(line)

	// Remove READ prefix (case-insensitive)
	if !strings.HasPrefix(strings.ToLower(line), "read ") {
		return "", "", nil, fmt.Errorf("command must start with READ")
	}
	line = strings.TrimSpace(line[5:])

	// Split by FILTER keyword
	parts := splitByKeyword(line, "FILTER")
	if len(parts) != 2 {
		return "", "", nil, fmt.Errorf("FILTER clause is required")
	}

	beforeFilter := strings.TrimSpace(parts[0])
	filterStr := strings.TrimSpace(parts[1])

	// Parse the part before FILTER: "db.table [col0, col1]"
	// First token is db.table
	tokens := strings.Fields(beforeFilter)
	if len(tokens) == 0 {
		return "", "", nil, fmt.Errorf("database.table is required")
	}

	dbTable := tokens[0]
	database, table, err = parseDbTable(dbTable)
	if err != nil {
		return "", "", nil, err
	}

	// Remaining tokens are read columns (if any)
	var readColumns []ReadColumn
	if len(tokens) > 1 {
		colStr := strings.Join(tokens[1:], " ")
		readColumns, err = parseReadColumns(colStr)
		if err != nil {
			return "", "", nil, fmt.Errorf("invalid read columns: %w", err)
		}
	}

	// Parse filters
	filters, err := parseFilters(filterStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid filter: %w", err)
	}

	req = &PkReadRequest{
		Filters:     filters,
		ReadColumns: readColumns,
	}

	return database, table, req, nil
}

// ParseBatch parses BATCH input (single-line or multi-line)
// Syntax 1: BATCH READ db.table [cols] FILTER conditions [OP id] [READ ...]
// Syntax 2: BATCH db.table: [cols] READ FILTER conditions; READ FILTER conditions; ...
func ParseBatch(lines []string) (*BatchRequest, error) {
	// Join all input into single string
	input := strings.Join(lines, " ")
	input = strings.TrimSpace(input)

	// Check for header syntax: "db.table: cols READ FILTER ..."
	if isHeaderSyntax(input) {
		return parseHeaderBatch(input)
	}

	// Otherwise use per-operation syntax
	chunks := tokenizeBatchInput(lines)

	var operations []BatchOperation

	for _, chunk := range chunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" || strings.ToLower(chunk) == "batch" || chunk == ";" {
			continue
		}

		// Each chunk should be a READ statement: "READ db.table [cols] FILTER conditions [OP id]"
		database, table, req, err := parseBatchRead(chunk)
		if err != nil {
			return nil, err
		}

		op := BatchOperation{
			Method:      "POST",
			RelativeURL: fmt.Sprintf("%s/%s/pk-read", database, table),
			Body:        *req,
		}
		operations = append(operations, op)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations defined in batch")
	}

	return &BatchRequest{Operations: operations}, nil
}

// isHeaderSyntax checks if input uses the header syntax: "db.table cols READ FILTER ..."
// Header syntax starts with db.table (not READ)
func isHeaderSyntax(input string) bool {
	// If it starts with READ, it's per-operation syntax
	if strings.HasPrefix(strings.ToLower(input), "read ") {
		return false
	}
	// Check if it starts with db.table pattern
	headerPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b`)
	return headerPattern.MatchString(input)
}

// parseHeaderBatch parses: "db.table [cols] READ FILTER a=1, READ FILTER a=2"
func parseHeaderBatch(input string) (*BatchRequest, error) {
	// Find db.table at the start
	tablePattern := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*:?\s*`)
	match := tablePattern.FindStringSubmatch(input)
	if match == nil {
		return nil, fmt.Errorf("invalid header syntax: expected db.table")
	}

	// Parse db.table
	dbTableStr := match[1]
	database, table, err := parseDbTable(dbTableStr)
	if err != nil {
		return nil, fmt.Errorf("invalid table reference: %w", err)
	}

	rest := strings.TrimSpace(input[len(match[0]):])

	// Find first READ keyword to separate columns from operations
	readPattern := regexp.MustCompile(`(?i)\bREAD\b`)
	readLoc := readPattern.FindStringIndex(rest)
	if readLoc == nil {
		return nil, fmt.Errorf("no READ operations found")
	}

	// Parse columns (between : and first READ)
	colStr := strings.TrimSpace(rest[:readLoc[0]])
	var readColumns []ReadColumn
	if colStr != "" {
		readColumns, err = parseReadColumns(colStr)
		if err != nil {
			return nil, fmt.Errorf("invalid columns: %w", err)
		}
	}

	// Split remaining by "READ" to get individual operations
	opsStr := rest[readLoc[0]:]
	// Split by ", READ" or ",READ" to separate operations
	opChunks := splitReadOperations(opsStr)

	var operations []BatchOperation
	for i, chunk := range opChunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}

		// Remove leading "READ" if present
		lower := strings.ToLower(chunk)
		if strings.HasPrefix(lower, "read") {
			chunk = strings.TrimSpace(chunk[4:])
		}

		// Parse FILTER and optional OP
		req, err := parseFilterOp(chunk)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i+1, err)
		}

		// Apply shared columns
		req.ReadColumns = readColumns

		op := BatchOperation{
			Method:      "POST",
			RelativeURL: fmt.Sprintf("%s/%s/pk-read", database, table),
			Body:        *req,
		}
		operations = append(operations, op)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations defined in batch")
	}

	return &BatchRequest{Operations: operations}, nil
}

// splitReadOperations splits "READ FILTER a=1, READ FILTER a=2" into chunks
// Splits by ", READ" or "; READ" pattern to separate operations
func splitReadOperations(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	// Split by READ keyword (each READ starts a new operation)
	// Support both ", READ" and "; READ" as separators
	readPattern := regexp.MustCompile(`(?i)[,;]\s*READ\b`)
	parts := readPattern.Split(s, -1)

	var result []string
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// For parts after the first one, we removed "READ" when splitting, so add it back
		if i > 0 {
			p = "READ " + p
		}
		result = append(result, p)
	}
	return result
}

// parseFilterOp parses "FILTER a=1, b=2 [OP opid]"
func parseFilterOp(s string) (*PkReadRequest, error) {
	s = strings.TrimSpace(s)

	// Check for OP at the end
	var opID string
	opParts := splitByKeyword(s, "OP")
	if len(opParts) == 2 {
		s = strings.TrimSpace(opParts[0])
		opID = strings.TrimSpace(opParts[1])
	}

	// Must start with FILTER
	lower := strings.ToLower(s)
	if !strings.HasPrefix(lower, "filter") {
		return nil, fmt.Errorf("expected FILTER keyword")
	}
	filterStr := strings.TrimSpace(s[6:])

	filters, err := parseFilters(filterStr)
	if err != nil {
		return nil, err
	}

	return &PkReadRequest{
		Filters:     filters,
		OperationID: opID,
	}, nil
}

// parseBatchRead parses a single READ operation for batch
// Format: READ db.table [col1, col2] FILTER conditions [OP id]
func parseBatchRead(line string) (database, table string, req *PkReadRequest, err error) {
	line = strings.TrimSpace(line)

	// Remove READ prefix (case-insensitive)
	lower := strings.ToLower(line)
	if !strings.HasPrefix(lower, "read ") {
		return "", "", nil, fmt.Errorf("batch operation must start with READ")
	}
	line = strings.TrimSpace(line[5:])

	// Split by OP keyword first (optional, at the end)
	var opID string
	opParts := splitByKeyword(line, "OP")
	if len(opParts) == 2 {
		line = strings.TrimSpace(opParts[0])
		opID = strings.TrimSpace(opParts[1])
	}

	// Split by FILTER keyword
	parts := splitByKeyword(line, "FILTER")
	if len(parts) != 2 {
		return "", "", nil, fmt.Errorf("FILTER clause is required")
	}

	beforeFilter := strings.TrimSpace(parts[0])
	filterStr := strings.TrimSpace(parts[1])

	// Parse the part before FILTER: "db.table [col1, col2]"
	tokens := strings.Fields(beforeFilter)
	if len(tokens) == 0 {
		return "", "", nil, fmt.Errorf("database.table is required")
	}

	dbTable := tokens[0]
	database, table, err = parseDbTable(dbTable)
	if err != nil {
		return "", "", nil, err
	}

	// Remaining tokens are read columns (if any)
	var readColumns []ReadColumn
	if len(tokens) > 1 {
		colStr := strings.Join(tokens[1:], " ")
		readColumns, err = parseReadColumns(colStr)
		if err != nil {
			return "", "", nil, fmt.Errorf("invalid read columns: %w", err)
		}
	}

	// Parse filters
	filters, err := parseFilters(filterStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid filter: %w", err)
	}

	req = &PkReadRequest{
		Filters:     filters,
		ReadColumns: readColumns,
		OperationID: opID,
	}

	return database, table, req, nil
}

// parseDbTable parses "database.table" format
func parseDbTable(s string) (database, table string, err error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected format: database.table")
	}
	database = strings.TrimSpace(parts[0])
	table = strings.TrimSpace(parts[1])
	if database == "" || table == "" {
		return "", "", fmt.Errorf("database and table names cannot be empty")
	}
	return database, table, nil
}

// parseReadColumns parses "col0, col1, col2" format
func parseReadColumns(s string) ([]ReadColumn, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	var columns []ReadColumn
	parts := strings.Split(s, ",")
	for _, part := range parts {
		col := strings.TrimSpace(part)
		if col == "" {
			continue
		}
		columns = append(columns, ReadColumn{
			Column:         col,
			DataReturnType: "default",
		})
	}
	return columns, nil
}

// parseFilters parses "col0=val0, col1=val1" format
func parseFilters(s string) ([]Filter, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("filter expression is required")
	}

	var filters []Filter
	// Split by comma, but be careful with quoted strings
	parts := splitFilterParts(s)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split by = sign
		eqIdx := strings.Index(part, "=")
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid filter format '%s', expected col=value", part)
		}

		column := strings.TrimSpace(part[:eqIdx])
		valueStr := strings.TrimSpace(part[eqIdx+1:])

		if column == "" {
			return nil, fmt.Errorf("column name cannot be empty")
		}

		value := parseValue(valueStr)
		filters = append(filters, Filter{
			Column: column,
			Value:  value,
		})
	}

	if len(filters) == 0 {
		return nil, fmt.Errorf("at least one filter is required")
	}

	return filters, nil
}

// splitFilterParts splits filter expression by commas, respecting quotes
func splitFilterParts(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range s {
		if !inQuote && (r == '"' || r == '\'') {
			inQuote = true
			quoteChar = r
			current.WriteRune(r)
		} else if inQuote && r == quoteChar {
			inQuote = false
			quoteChar = 0
			current.WriteRune(r)
		} else if !inQuote && r == ',' {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// parseValue converts a string value to appropriate type (int, float, or string)
func parseValue(s string) interface{} {
	s = strings.TrimSpace(s)

	// Remove surrounding quotes if present
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}

	// Try to parse as integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}

	// Try to parse as float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	// Return as string
	return s
}

// splitByKeyword splits a string by a keyword (case-insensitive)
func splitByKeyword(s string, keyword string) []string {
	// Create case-insensitive regex for the keyword
	pattern := regexp.MustCompile(`(?i)\s+` + regexp.QuoteMeta(keyword) + `\s+`)
	loc := pattern.FindStringIndex(s)
	if loc == nil {
		return []string{s}
	}
	return []string{s[:loc[0]], s[loc[1]:]}
}

// tokenizeBatchInput converts batch input into logical operation chunks
// New syntax: "READ redis_0.x b FILTER a=1 READ redis_0.y c FILTER d=2"
// Each READ starts a new operation
func tokenizeBatchInput(lines []string) []string {
	// Join all input into single string
	input := strings.Join(lines, " ")
	input = strings.TrimSpace(input)

	// Remove trailing semicolon if present
	input = strings.TrimSuffix(input, ";")
	input = strings.TrimSpace(input)

	if input == "" {
		return nil
	}

	// Split by READ keyword (each READ starts a new operation)
	// Pattern: READ db.table [cols] FILTER ... [OP ...]
	readPattern := regexp.MustCompile(`(?i)\bREAD\b`)
	locs := readPattern.FindAllStringIndex(input, -1)

	if len(locs) == 0 {
		return lines
	}

	var result []string
	for i, loc := range locs {
		var end int
		if i+1 < len(locs) {
			end = locs[i+1][0]
		} else {
			end = len(input)
		}
		chunk := strings.TrimSpace(input[loc[0]:end])
		if chunk != "" {
			result = append(result, chunk)
		}
	}

	return result
}
