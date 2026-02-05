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

// WriteColumn represents a column to write with its value
type WriteColumn struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
}

// PkWriteRequest represents a single pk-write request body
type PkWriteRequest struct {
	Filters      []Filter      `json:"filters"`
	WriteColumns []WriteColumn `json:"writeColumns"`
	OperationID  string        `json:"operationId,omitempty"`
}

// BatchOperation represents a single operation in a batch request
type BatchOperation struct {
	Method      string        `json:"method"`
	RelativeURL string        `json:"relative-url"`
	Body        PkReadRequest `json:"body"`
}

// BatchWriteOperation represents a single write operation in a batch request
type BatchWriteOperation struct {
	Method      string         `json:"method"`
	RelativeURL string         `json:"relative-url"`
	Body        PkWriteRequest `json:"body"`
}

// BatchRequest represents a batch of operations
type BatchRequest struct {
	Operations []BatchOperation `json:"operations"`
}

// BatchWriteRequest represents a batch of write operations
type BatchWriteRequest struct {
	Operations []BatchWriteOperation `json:"operations"`
}

// OpType indicates the type of batch operation
type OpType int

const (
	OpTypeRead OpType = iota
	OpTypeDelete
	OpTypeWrite // covers WRITE, UPDATE, INSERT (all use batchwrite endpoint)
)

// WriteOpType indicates the specific write operation (used with OpTypeWrite)
type WriteOpType string

const (
	WriteOpWrite  WriteOpType = "pk-write"
	WriteOpUpdate WriteOpType = "pk-update"
	WriteOpInsert WriteOpType = "pk-insert"
)

// BatchResult contains the parsed batch request and detected operation type
type BatchResult struct {
	Request      *BatchRequest
	WriteRequest *BatchWriteRequest
	OpType       OpType
	WriteOpType  WriteOpType // set when OpType is OpTypeWrite
	IsDelete     bool        // deprecated: use OpType instead
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

// ParseBatchUnified parses BATCH input and auto-detects READ, DELETE, or WRITE operations
// Syntax 1 (per-op): BATCH READ/DELETE/WRITE db.table ... FILTER ... [READ/DELETE/WRITE ...]
// Syntax 2 (header): BATCH db.table READ/DELETE/WRITE/UPDATE/INSERT ... FILTER ...
func ParseBatchUnified(lines []string) (*BatchResult, error) {
	input := strings.Join(lines, " ")
	input = strings.TrimSpace(input)

	// Detect operation type by looking for READ, DELETE, WRITE, UPDATE, INSERT keywords
	lower := strings.ToLower(input)

	// Check if it starts with READ, DELETE, WRITE, UPDATE, or INSERT (per-operation syntax)
	if strings.HasPrefix(lower, "read ") {
		req, err := ParseBatch(lines)
		if err != nil {
			return nil, err
		}
		return &BatchResult{Request: req, OpType: OpTypeRead, IsDelete: false}, nil
	}
	if strings.HasPrefix(lower, "delete ") {
		req, err := ParseBatchDelete(lines)
		if err != nil {
			return nil, err
		}
		return &BatchResult{Request: req, OpType: OpTypeDelete, IsDelete: true}, nil
	}
	if strings.HasPrefix(lower, "write ") {
		req, err := ParseBatchWriteWithType(lines, WriteOpWrite, "WRITE")
		if err != nil {
			return nil, err
		}
		return &BatchResult{WriteRequest: req, OpType: OpTypeWrite, WriteOpType: WriteOpWrite, IsDelete: false}, nil
	}
	if strings.HasPrefix(lower, "update ") {
		req, err := ParseBatchWriteWithType(lines, WriteOpUpdate, "UPDATE")
		if err != nil {
			return nil, err
		}
		return &BatchResult{WriteRequest: req, OpType: OpTypeWrite, WriteOpType: WriteOpUpdate, IsDelete: false}, nil
	}
	if strings.HasPrefix(lower, "insert ") {
		req, err := ParseBatchWriteWithType(lines, WriteOpInsert, "INSERT")
		if err != nil {
			return nil, err
		}
		return &BatchResult{WriteRequest: req, OpType: OpTypeWrite, WriteOpType: WriteOpInsert, IsDelete: false}, nil
	}

	// Header syntax - detect by finding first READ, DELETE, WRITE, UPDATE, or INSERT keyword after db.table
	// Pattern: db.table READ/DELETE/WRITE/UPDATE/INSERT ... FILTER ...
	readPattern := regexp.MustCompile(`(?i)\bREAD\b`)
	deletePattern := regexp.MustCompile(`(?i)\bDELETE\b`)
	writePattern := regexp.MustCompile(`(?i)\bWRITE\b`)
	updatePattern := regexp.MustCompile(`(?i)\bUPDATE\b`)
	insertPattern := regexp.MustCompile(`(?i)\bINSERT\b`)

	readLoc := readPattern.FindStringIndex(input)
	deleteLoc := deletePattern.FindStringIndex(input)
	writeLoc := writePattern.FindStringIndex(input)
	updateLoc := updatePattern.FindStringIndex(input)
	insertLoc := insertPattern.FindStringIndex(input)

	// Find which keyword comes first
	type locInfo struct {
		loc      []int
		opType   OpType
		writeOp  WriteOpType
		keyword  string
	}

	candidates := []locInfo{
		{readLoc, OpTypeRead, "", "READ"},
		{deleteLoc, OpTypeDelete, "", "DELETE"},
		{writeLoc, OpTypeWrite, WriteOpWrite, "WRITE"},
		{updateLoc, OpTypeWrite, WriteOpUpdate, "UPDATE"},
		{insertLoc, OpTypeWrite, WriteOpInsert, "INSERT"},
	}

	var first *locInfo
	for i := range candidates {
		if candidates[i].loc != nil {
			if first == nil || candidates[i].loc[0] < first.loc[0] {
				first = &candidates[i]
			}
		}
	}

	if first == nil {
		return nil, fmt.Errorf("no READ, DELETE, WRITE, UPDATE, or INSERT operations found in batch")
	}

	switch first.opType {
	case OpTypeRead:
		req, err := ParseBatch(lines)
		if err != nil {
			return nil, err
		}
		return &BatchResult{Request: req, OpType: OpTypeRead, IsDelete: false}, nil
	case OpTypeDelete:
		req, err := ParseBatchDelete(lines)
		if err != nil {
			return nil, err
		}
		return &BatchResult{Request: req, OpType: OpTypeDelete, IsDelete: true}, nil
	case OpTypeWrite:
		req, err := ParseBatchWriteWithType(lines, first.writeOp, first.keyword)
		if err != nil {
			return nil, err
		}
		return &BatchResult{WriteRequest: req, OpType: OpTypeWrite, WriteOpType: first.writeOp, IsDelete: false}, nil
	}

	return nil, fmt.Errorf("no READ, DELETE, WRITE, UPDATE, or INSERT operations found in batch")
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
	return tokenizeBatchInputByKeyword(lines, "READ")
}

// tokenizeBatchInputByKeyword converts batch input into logical operation chunks
// splitting by the specified keyword (READ or DELETE)
func tokenizeBatchInputByKeyword(lines []string, keyword string) []string {
	// Join all input into single string
	input := strings.Join(lines, " ")
	input = strings.TrimSpace(input)

	// Remove trailing semicolon if present
	input = strings.TrimSuffix(input, ";")
	input = strings.TrimSpace(input)

	if input == "" {
		return nil
	}

	// Split by keyword (each keyword starts a new operation)
	// Pattern: KEYWORD db.table [cols] FILTER ... [OP ...]
	keywordPattern := regexp.MustCompile(`(?i)\b` + keyword + `\b`)
	locs := keywordPattern.FindAllStringIndex(input, -1)

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

// ParseBatchDelete parses BATCH DELETE input (single-line or multi-line)
// Syntax 1: BATCH DELETE db.table [cols] FILTER conditions [OP id] [DELETE ...]
// Syntax 2: BATCH DELETE db.table: [cols] DELETE FILTER conditions, DELETE FILTER conditions
func ParseBatchDelete(lines []string) (*BatchRequest, error) {
	// Join all input into single string
	input := strings.Join(lines, " ")
	input = strings.TrimSpace(input)

	// Check for header syntax: "db.table: cols DELETE FILTER ..."
	if isDeleteHeaderSyntax(input) {
		return parseHeaderBatchDelete(input)
	}

	// Otherwise use per-operation syntax
	chunks := tokenizeBatchInputByKeyword(lines, "DELETE")

	var operations []BatchOperation

	for _, chunk := range chunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" || chunk == ";" {
			continue
		}

		// Each chunk should be a DELETE statement: "DELETE db.table [cols] FILTER conditions [OP id]"
		database, table, req, err := parseBatchDeleteOp(chunk)
		if err != nil {
			return nil, err
		}

		op := BatchOperation{
			Method:      "POST",
			RelativeURL: fmt.Sprintf("%s/%s/pk-delete", database, table),
			Body:        *req,
		}
		operations = append(operations, op)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations defined in batch delete")
	}

	return &BatchRequest{Operations: operations}, nil
}

// isDeleteHeaderSyntax checks if input uses the header syntax: "db.table cols DELETE FILTER ..."
// Header syntax starts with db.table (not DELETE)
func isDeleteHeaderSyntax(input string) bool {
	// If it starts with DELETE, it's per-operation syntax
	if strings.HasPrefix(strings.ToLower(input), "delete ") {
		return false
	}
	// Check if it starts with db.table pattern
	headerPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b`)
	return headerPattern.MatchString(input)
}

// parseHeaderBatchDelete parses: "db.table [cols] DELETE FILTER a=1, DELETE FILTER a=2"
func parseHeaderBatchDelete(input string) (*BatchRequest, error) {
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

	// Find first DELETE keyword to separate columns from operations
	deletePattern := regexp.MustCompile(`(?i)\bDELETE\b`)
	deleteLoc := deletePattern.FindStringIndex(rest)
	if deleteLoc == nil {
		return nil, fmt.Errorf("no DELETE operations found")
	}

	// Parse columns (between : and first DELETE) - can be empty
	colStr := strings.TrimSpace(rest[:deleteLoc[0]])
	var readColumns []ReadColumn
	if colStr != "" {
		readColumns, err = parseReadColumns(colStr)
		if err != nil {
			return nil, fmt.Errorf("invalid columns: %w", err)
		}
	}

	// Split remaining by "DELETE" to get individual operations
	opsStr := rest[deleteLoc[0]:]
	// Split by ", DELETE" or ",DELETE" to separate operations
	opChunks := splitDeleteOperations(opsStr)

	var operations []BatchOperation
	for i, chunk := range opChunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}

		// Remove leading "DELETE" if present
		lower := strings.ToLower(chunk)
		if strings.HasPrefix(lower, "delete") {
			chunk = strings.TrimSpace(chunk[6:])
		}

		// Parse FILTER and optional OP
		req, err := parseFilterOp(chunk)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i+1, err)
		}

		// Apply shared columns (can be empty for delete)
		req.ReadColumns = readColumns

		op := BatchOperation{
			Method:      "POST",
			RelativeURL: fmt.Sprintf("%s/%s/pk-delete", database, table),
			Body:        *req,
		}
		operations = append(operations, op)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations defined in batch delete")
	}

	return &BatchRequest{Operations: operations}, nil
}

// splitDeleteOperations splits "DELETE FILTER a=1, DELETE FILTER a=2" into chunks
// Splits by ", DELETE" or "; DELETE" pattern to separate operations
func splitDeleteOperations(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	// Split by DELETE keyword (each DELETE starts a new operation)
	// Support both ", DELETE" and "; DELETE" as separators
	deletePattern := regexp.MustCompile(`(?i)[,;]\s*DELETE\b`)
	parts := deletePattern.Split(s, -1)

	var result []string
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// For parts after the first one, we removed "DELETE" when splitting, so add it back
		if i > 0 {
			p = "DELETE " + p
		}
		result = append(result, p)
	}
	return result
}

// parseBatchDeleteOp parses a single DELETE operation for batch
// Format: DELETE db.table [col1, col2] FILTER conditions [OP id]
func parseBatchDeleteOp(line string) (database, table string, req *PkReadRequest, err error) {
	line = strings.TrimSpace(line)

	// Remove DELETE prefix (case-insensitive)
	lower := strings.ToLower(line)
	if !strings.HasPrefix(lower, "delete ") {
		return "", "", nil, fmt.Errorf("batch operation must start with DELETE")
	}
	line = strings.TrimSpace(line[7:])

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

	// Remaining tokens are read columns (if any - optional for delete)
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

// ParseBatchWrite parses BATCH WRITE input (single-line or multi-line)
// Syntax 1: BATCH WRITE db.table col1=val1, col2=val2 FILTER pk1=1, pk2=2 [OP id] [WRITE ...]
// Syntax 2: BATCH db.table WRITE col1=val1, col2=val2 FILTER pk1=1, pk2=2, WRITE col1=val3 FILTER pk1=3, pk2=4
func ParseBatchWrite(lines []string) (*BatchWriteRequest, error) {
	return ParseBatchWriteWithType(lines, WriteOpWrite, "WRITE")
}

// ParseBatchWriteWithType parses batch write/update/insert input with specified write type and keyword
// keyword is WRITE, UPDATE, or INSERT
// writeOp determines the relative URL suffix (pk-write, pk-update, pk-insert)
func ParseBatchWriteWithType(lines []string, writeOp WriteOpType, keyword string) (*BatchWriteRequest, error) {
	// Join all input into single string
	input := strings.Join(lines, " ")
	input = strings.TrimSpace(input)

	// Check for header syntax: "db.table WRITE/UPDATE/INSERT col=val FILTER ..."
	if isWriteHeaderSyntaxWithKeyword(input, keyword) {
		return parseHeaderBatchWriteWithType(input, writeOp, keyword)
	}

	// Otherwise use per-operation syntax
	chunks := tokenizeBatchInputByKeyword(lines, keyword)

	var operations []BatchWriteOperation

	for _, chunk := range chunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" || chunk == ";" {
			continue
		}

		// Each chunk should be a WRITE/UPDATE/INSERT statement
		database, table, req, err := parseBatchWriteOpWithKeyword(chunk, keyword)
		if err != nil {
			return nil, err
		}

		op := BatchWriteOperation{
			Method:      "POST",
			RelativeURL: fmt.Sprintf("%s/%s/%s", database, table, writeOp),
			Body:        *req,
		}
		operations = append(operations, op)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations defined in batch %s", strings.ToLower(keyword))
	}

	return &BatchWriteRequest{Operations: operations}, nil
}

// isWriteHeaderSyntax checks if input uses the header syntax: "db.table WRITE col=val FILTER ..."
// Header syntax starts with db.table (not WRITE)
func isWriteHeaderSyntax(input string) bool {
	return isWriteHeaderSyntaxWithKeyword(input, "WRITE")
}

// isWriteHeaderSyntaxWithKeyword checks if input uses header syntax for given keyword (WRITE/UPDATE/INSERT)
func isWriteHeaderSyntaxWithKeyword(input string, keyword string) bool {
	// If it starts with the keyword, it's per-operation syntax
	if strings.HasPrefix(strings.ToLower(input), strings.ToLower(keyword)+" ") {
		return false
	}
	// Check if it starts with db.table pattern
	headerPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b`)
	return headerPattern.MatchString(input)
}

// parseHeaderBatchWrite parses: "db.table WRITE col=val FILTER a=1, WRITE col=val2 FILTER a=2"
func parseHeaderBatchWrite(input string) (*BatchWriteRequest, error) {
	return parseHeaderBatchWriteWithType(input, WriteOpWrite, "WRITE")
}

// parseHeaderBatchWriteWithType parses header syntax for WRITE/UPDATE/INSERT operations
func parseHeaderBatchWriteWithType(input string, writeOp WriteOpType, keyword string) (*BatchWriteRequest, error) {
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

	// Find first keyword (WRITE/UPDATE/INSERT)
	keywordPattern := regexp.MustCompile(`(?i)\b` + keyword + `\b`)
	keywordLoc := keywordPattern.FindStringIndex(rest)
	if keywordLoc == nil {
		return nil, fmt.Errorf("no %s operations found", keyword)
	}

	// Split remaining by keyword to get individual operations
	opsStr := rest[keywordLoc[0]:]
	opChunks := splitWriteOperationsWithKeyword(opsStr, keyword)

	var operations []BatchWriteOperation
	for i, chunk := range opChunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}

		// Remove leading keyword if present
		lower := strings.ToLower(chunk)
		lowerKeyword := strings.ToLower(keyword)
		if strings.HasPrefix(lower, lowerKeyword) {
			chunk = strings.TrimSpace(chunk[len(keyword):])
		}

		// Parse write columns and FILTER
		req, err := parseWriteColsFilterOp(chunk)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i+1, err)
		}

		op := BatchWriteOperation{
			Method:      "POST",
			RelativeURL: fmt.Sprintf("%s/%s/%s", database, table, writeOp),
			Body:        *req,
		}
		operations = append(operations, op)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations defined in batch %s", strings.ToLower(keyword))
	}

	return &BatchWriteRequest{Operations: operations}, nil
}

// splitWriteOperations splits "WRITE col=val FILTER a=1, WRITE col=val2 FILTER a=2" into chunks
func splitWriteOperations(s string) []string {
	return splitWriteOperationsWithKeyword(s, "WRITE")
}

// splitWriteOperationsWithKeyword splits operations by the given keyword (WRITE/UPDATE/INSERT)
func splitWriteOperationsWithKeyword(s string, keyword string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	// Split by keyword (each keyword starts a new operation)
	pattern := regexp.MustCompile(`(?i)[,;]\s*` + keyword + `\b`)
	parts := pattern.Split(s, -1)

	var result []string
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// For parts after the first one, we removed the keyword when splitting, so add it back
		if i > 0 {
			p = keyword + " " + p
		}
		result = append(result, p)
	}
	return result
}

// parseWriteColsFilterOp parses "col1=val1, col2=val2 FILTER a=1, b=2 [OP opid]"
func parseWriteColsFilterOp(s string) (*PkWriteRequest, error) {
	s = strings.TrimSpace(s)

	// Check for OP at the end
	var opID string
	opParts := splitByKeyword(s, "OP")
	if len(opParts) == 2 {
		s = strings.TrimSpace(opParts[0])
		opID = strings.TrimSpace(opParts[1])
	}

	// Split by FILTER
	parts := splitByKeyword(s, "FILTER")
	if len(parts) != 2 {
		return nil, fmt.Errorf("FILTER clause is required")
	}

	writeColsStr := strings.TrimSpace(parts[0])
	filterStr := strings.TrimSpace(parts[1])

	// Parse write columns (col=val format)
	writeColumns, err := parseWriteColumns(writeColsStr)
	if err != nil {
		return nil, fmt.Errorf("invalid write columns: %w", err)
	}

	// Parse filters
	filters, err := parseFilters(filterStr)
	if err != nil {
		return nil, err
	}

	return &PkWriteRequest{
		Filters:      filters,
		WriteColumns: writeColumns,
		OperationID:  opID,
	}, nil
}

// parseBatchWriteOp parses a single WRITE operation for batch
// Format: WRITE db.table col1=val1, col2=val2 FILTER conditions [OP id]
func parseBatchWriteOp(line string) (database, table string, req *PkWriteRequest, err error) {
	return parseBatchWriteOpWithKeyword(line, "WRITE")
}

// parseBatchWriteOpWithKeyword parses a single WRITE/UPDATE/INSERT operation for batch
// Format: KEYWORD db.table col1=val1, col2=val2 FILTER conditions [OP id]
func parseBatchWriteOpWithKeyword(line string, keyword string) (database, table string, req *PkWriteRequest, err error) {
	line = strings.TrimSpace(line)

	// Remove keyword prefix (case-insensitive)
	lower := strings.ToLower(line)
	lowerKeyword := strings.ToLower(keyword)
	if !strings.HasPrefix(lower, lowerKeyword+" ") {
		return "", "", nil, fmt.Errorf("batch operation must start with %s", keyword)
	}
	line = strings.TrimSpace(line[len(keyword):])

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

	// Parse the part before FILTER: "db.table col1=val1, col2=val2"
	// First token is db.table, rest is write columns
	spaceIdx := strings.Index(beforeFilter, " ")
	var dbTable, writeColsStr string
	if spaceIdx == -1 {
		dbTable = beforeFilter
		writeColsStr = ""
	} else {
		dbTable = beforeFilter[:spaceIdx]
		writeColsStr = strings.TrimSpace(beforeFilter[spaceIdx+1:])
	}

	database, table, err = parseDbTable(dbTable)
	if err != nil {
		return "", "", nil, err
	}

	// Parse write columns (col=val format)
	writeColumns, err := parseWriteColumns(writeColsStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid write columns: %w", err)
	}

	// Parse filters
	filters, err := parseFilters(filterStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid filter: %w", err)
	}

	req = &PkWriteRequest{
		Filters:      filters,
		WriteColumns: writeColumns,
		OperationID:  opID,
	}

	return database, table, req, nil
}

// parseWriteColumns parses "col1=val1, col2=val2" format into WriteColumn slice
func parseWriteColumns(s string) ([]WriteColumn, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	var columns []WriteColumn
	// Split by comma, respecting quotes
	parts := splitFilterParts(s)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split by = sign
		eqIdx := strings.Index(part, "=")
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid write column format '%s', expected col=value", part)
		}

		column := strings.TrimSpace(part[:eqIdx])
		valueStr := strings.TrimSpace(part[eqIdx+1:])

		if column == "" {
			return nil, fmt.Errorf("column name cannot be empty")
		}

		value := parseValue(valueStr)
		columns = append(columns, WriteColumn{
			Column: column,
			Value:  value,
		})
	}

	return columns, nil
}
