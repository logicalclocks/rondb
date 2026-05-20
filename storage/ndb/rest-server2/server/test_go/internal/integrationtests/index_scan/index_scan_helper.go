/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package index_scan

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/version"
)

// EMPTY_STRING is used when no error message is expected in the response
const EMPTY_STRING = ""

// Constants for row order comparison in CompareResults
const (
	ROWS_ORDER_MUST_MATCH              = true
	ROWS_ORDER_MAY_NOT_MATCH           = false
	DATA_NEEDS_BINARY_ENCODING         = true
	DATA_DOES_NOT_NEED_BINARY_ENCODING = false
)

// ConverJSONtToSQL converts an IndexScanQuery to a SQL SELECT statement
func ConverJSONtToSQL(database string, table string, query *api.IndexScanQuery, isBinaryData bool) (string, error) {
	var sqlBuilder strings.Builder

	sqlBuilder.WriteString("SELECT ")
	if query.ReadColumns != nil && len(*query.ReadColumns) > 0 {
		columns := make([]string, 0, len(*query.ReadColumns))
		for _, col := range *query.ReadColumns {
			if col.Column != nil {
				columns = append(columns, *col.Column)
			}
		}
		sqlBuilder.WriteString(strings.Join(columns, ", "))
	} else {
		sqlBuilder.WriteString("*")
	}

	sqlBuilder.WriteString(fmt.Sprintf(" FROM %s.%s", database, table))

	// Build WHERE clause from filters and/or index ranges
	var whereClauses []string

	if query.Filters != nil {
		filterClause, err := convertFilterToSQL(query.Filters, isBinaryData)
		if err != nil {
			return "", err
		}
		whereClauses = append(whereClauses, "("+filterClause+")")
	}

	// Convert index ranges to WHERE clause conditions
	if query.Index != nil && len(query.Index.Ranges) > 0 {
		rangeClause, err := convertIndexRangesToSQL(query.Index, isBinaryData)
		if err != nil {
			return "", err
		}
		if rangeClause != "" {
			whereClauses = append(whereClauses, rangeClause)
		}
	}

	if len(whereClauses) > 0 {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(strings.Join(whereClauses, " AND "))
	}

	if query.Index != nil && query.Index.Order != "" {
		// SQL `ORDER BY a, b DESC` means `a ASC, b DESC` — DESC only binds to
		// the immediately preceding column. NDB's SF_Descending applies to
		// every key column, so we must repeat the direction per column to
		// stay consistent.
		direction := strings.ToUpper(query.Index.Order)
		clauses := make([]string, len(query.Index.KeyColumns))
		for i, col := range query.Index.KeyColumns {
			clauses[i] = col + " " + direction
		}
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(strings.Join(clauses, ", "))
	}

	if query.Limit >= 0 {
		sqlBuilder.WriteString(fmt.Sprintf(" LIMIT %d", query.Limit))
	}

	sqlBuilder.WriteString(";")
	return sqlBuilder.String(), nil
}

// convertFilterToSQL recursively converts FilterScan to SQL WHERE clause
func convertFilterToSQL(filter *api.ScanFilter, isBinaryData bool) (string, error) {
	switch filter.Op {
	case "AND", "OR", "NAND", "NOR":
		if filter.Args == nil || len(filter.Args) == 0 {
			return "", fmt.Errorf("logical operator %s requires args", filter.Op)
		}

		subClauses := make([]string, 0, len(filter.Args))
		for _, arg := range filter.Args {
			subClause, err := convertFilterToSQL(arg, isBinaryData)
			if err != nil {
				return "", err
			}
			subClauses = append(subClauses, fmt.Sprintf("(%s)", subClause))
		}

		var operator string
		switch filter.Op {
		case "AND":
			operator = " AND "
		case "OR":
			operator = " OR "
		case "NAND":
			return fmt.Sprintf("NOT (%s)", strings.Join(subClauses, " AND ")), nil
		case "NOR":
			return fmt.Sprintf("NOT (%s)", strings.Join(subClauses, " OR ")), nil
		}
		return strings.Join(subClauses, operator), nil

	case "CMP":
		if filter.Column == "" {
			return "", fmt.Errorf("CMP operator requires column")
		}
		if filter.Cond == "" {
			return "", fmt.Errorf("CMP operator requires cond")
		}

		var sqlOperator string
		switch filter.Cond {
		case "EQ":
			sqlOperator = "="
		case "NE":
			sqlOperator = "!="
		case "GT":
			sqlOperator = ">"
		case "GE":
			sqlOperator = ">="
		case "LT":
			sqlOperator = "<"
		case "LE":
			sqlOperator = "<="
		default:
			return "", fmt.Errorf("unknown condition: %s", filter.Cond)
		}

		value := formatValue(filter.Value, isBinaryData)
		return fmt.Sprintf("%s %s %s", filter.Column, sqlOperator, value), nil

	case "ISNOTNULL":
		if filter.Column == "" {
			return "", fmt.Errorf("ISNOTNULL operator requires column")
		}
		return fmt.Sprintf("%s IS NOT NULL", filter.Column), nil

	case "ISNULL":
		if filter.Column == "" {
			return "", fmt.Errorf("ISNULL operator requires column")
		}
		return fmt.Sprintf("%s IS NULL", filter.Column), nil

	default:
		return "", fmt.Errorf("unknown operator: %s", filter.Op)
	}
}

// formatValue formats a value for SQL
// If isBinaryData is true, wraps the value with from_base64() for binary column comparison
func formatValue(value interface{}, isBinaryData bool) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		// Escape single quotes in strings
		escaped := strings.ReplaceAll(v, "'", "''")
		if isBinaryData {
			// For binary data, the value is base64 encoded, use from_base64() to decode
			return fmt.Sprintf("from_base64('%s')", escaped)
		}
		return fmt.Sprintf("'%s'", escaped)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		// For other types, convert to string and quote
		if isBinaryData {
			return fmt.Sprintf("from_base64('%v')", v)
		}
		return fmt.Sprintf("'%v'", v)
	}
}

// convertIndexRangesToSQL converts index ranges to SQL WHERE clause conditions
// Note: Only works correctly for single-column index ranges. Multi-column composite
// index ranges have different semantics in NDB that can't be easily translated to SQL.
func convertIndexRangesToSQL(index *api.IndexScan, isBinaryData bool) (string, error) {
	if index == nil || len(index.Ranges) == 0 {
		return "", nil
	}

	// Only generate WHERE clause for single-column indexes
	// Multi-column composite index ranges have complex semantics that don't
	// translate directly to simple SQL comparisons
	if len(index.KeyColumns) != 1 {
		return "", nil
	}

	col := index.KeyColumns[0]
	var rangeClauses []string

	for _, r := range index.Ranges {
		var conditions []string

		// Process lower bound
		if len(r.Lower.Values) > 0 && r.Lower.Values[0] != nil {
			formattedVal := formatValue(r.Lower.Values[0], isBinaryData)
			if r.Lower.Inclusive {
				conditions = append(conditions, fmt.Sprintf("%s >= %s", col, formattedVal))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s > %s", col, formattedVal))
			}
		}

		// Process upper bound
		if len(r.Upper.Values) > 0 && r.Upper.Values[0] != nil {
			formattedVal := formatValue(r.Upper.Values[0], isBinaryData)
			if r.Upper.Inclusive {
				conditions = append(conditions, fmt.Sprintf("%s <= %s", col, formattedVal))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s < %s", col, formattedVal))
			}
		}

		if len(conditions) > 0 {
			rangeClauses = append(rangeClauses, "("+strings.Join(conditions, " AND ")+")")
		}
	}

	if len(rangeClauses) == 0 {
		return "", nil
	}

	// Multiple ranges are OR'd together
	if len(rangeClauses) == 1 {
		return rangeClauses[0], nil
	}
	return "(" + strings.Join(rangeClauses, " OR ") + ")", nil
}

// GetSampleData executes a SQL query and returns the result rows
// Returns: rows ([][]interface{}), column names ([]string), column types ([]string), error
// If isBinaryData is true, []byte values are base64 encoded
func GetSampleData(db *sql.DB, sqlQuery string, isBinaryData bool) ([][]interface{}, []string, []string, error) {
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get column types: %w", err)
	}

	colTypeNames := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		colTypeNames[i] = ct.DatabaseTypeName()
	}

	// Fetch all rows
	var resultRows [][]interface{}
	for rows.Next() {
		// Create slice to hold column values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row into value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert sql.RawBytes to appropriate types
		row := make([]interface{}, len(columns))
		for i, val := range values {
			if val == nil {
				row[i] = nil
			} else {
				// Convert []byte to string or base64 based on column type
				switch v := val.(type) {
				case []byte:
					// Only base64 encode actual BINARY/VARBINARY/BIT/BLOB columns
					// Other columns (like INT returned as []byte) should be converted to string
					colType := strings.ToUpper(colTypeNames[i])
					if isBinaryData && (strings.Contains(colType, "BINARY") || strings.Contains(colType, "BLOB") || strings.Contains(colType, "BIT")) {
						row[i] = base64.StdEncoding.EncodeToString(v)
					} else {
						row[i] = string(v)
					}
				default:
					row[i] = v
				}
			}
		}

		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return resultRows, columns, colTypeNames, nil
}

// GetSampleDataWithQuery executes an IndexScanQuery and returns the result rows
func GetSampleDataWithQuery(db *sql.DB, database string, table string, query *api.IndexScanQuery, isBinaryData bool) ([][]interface{}, []string, []string, error) {
	sqlQuery, err := ConverJSONtToSQL(database, table, query, isBinaryData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to convert query to SQL: %w", err)
	}

	return GetSampleData(db, sqlQuery, isBinaryData)
}

// ExecuteUsingMySQLServer is a helper function to execute query and print results for testing
// Returns: rows ([][]interface{}), column names ([]string), error
func ExecuteUsingMySQLServer(t *testing.T, database string, table string, query *api.IndexScanQuery, isBinaryData bool) ([][]interface{}, []string, error) {
	jsonBytes, err := json.MarshalIndent(query, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal query: %w", err)
	}
	log.Debugf("JSON:\n%s\n", string(jsonBytes))

	sql, err := ConverJSONtToSQL(database, table, query, isBinaryData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert to SQL: %w", err)
	}
	log.Infof("Request SQL:\n%s\n", sql)

	conn, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create DB connection: %w", err)
	}
	defer conn.Close()

	rows, columns, colTypes, err := GetSampleData(conn, sql, isBinaryData)
	if err != nil {
		log.Infof("Query returned no data or error: %v", err)
		return nil, nil, err
	}

	log.Infof("Columns: %v (Types: %v)", columns, colTypes)
	log.Infof("Sample data (%d rows):", len(rows))
	for i, row := range rows {
		log.Infof("  Row %d: %v", i, row)
	}
	log.Infof("\n" + strings.Repeat("=", 80) + "\n")

	return rows, columns, nil
}

// NewIndexScanURL creates a URL for the index scan endpoint
func NewIndexScanURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s/%s/scan",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		db,
		table,
	)
	if conf.Security.TLS.EnableTLS {
		url = fmt.Sprintf("https://%s", url)
	} else {
		url = fmt.Sprintf("http://%s", url)
	}
	return url
}

// ExecuteUsingRESTServer is a helper function to execute query via REST endpoint and print results
// Returns: rows ([][]any), column names ([]string), response code (int), error
func ExecuteUsingRESTServer(t *testing.T, database string, table string, query *api.IndexScanQuery,
	expectedErrMsg string, expectedRespCode int) ([][]any, []string, int, error) {
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to marshal query: %w", err)
	}

	url := NewIndexScanURL(database, table)

	log.Infof("JSON Request. URL: %s. Body:\n%s\n", url, string(jsonBytes))

	respCode, respBody := testclient.SendHttpRequest(
		t,
		http.MethodPost,
		url,
		string(jsonBytes),
		expectedErrMsg,
		expectedRespCode,
	)

	log.Infof("Response Code: %d. Body: %s \n", respCode, string(respBody))

	// Validate response code
	if respCode != expectedRespCode {
		t.Fatalf("Expected response code %d, got %d. Body: %s", expectedRespCode, respCode, string(respBody))
	}

	// Validate error message if expected and response is not OK
	if expectedErrMsg != "" && respCode != http.StatusOK {
		if !strings.Contains(string(respBody), expectedErrMsg) {
			t.Fatalf("Expected error message containing '%s', got: %s", expectedErrMsg, string(respBody))
		}
	}

	// Don't try to unmarshal if response is not 200 OK
	if respCode != http.StatusOK {
		return nil, nil, respCode, nil
	}

	// Use json.Decoder with UseNumber() to preserve numeric precision for large int64/uint64 values
	var scanResp api.IndexScanResponse
	decoder := json.NewDecoder(strings.NewReader(string(respBody)))
	decoder.UseNumber()
	err = decoder.Decode(&scanResp)
	if err != nil {
		return nil, nil, respCode, fmt.Errorf("failed to unmarshal response body: %w", err)
	}

	if len(scanResp.Data) == 0 {
		return [][]any{}, []string{}, respCode, nil
	}

	// Extract column names in order from raw JSON (map iteration doesn't preserve order)
	columnNames, err := extractColumnNamesInOrder(respBody)
	if err != nil {
		return nil, nil, respCode, fmt.Errorf("failed to extract column names: %w", err)
	}

	rows := make([][]any, len(scanResp.Data))
	for i, rowMap := range scanResp.Data {
		row := make([]any, len(columnNames))
		for j, colName := range columnNames {
			// Convert json.Number to string for consistent comparison
			if num, ok := rowMap[colName].(json.Number); ok {
				row[j] = num.String()
			} else {
				row[j] = rowMap[colName]
			}
		}
		rows[i] = row
	}

	log.Infof("Parsed data (%d rows):", len(rows))
	log.Infof("Column order: %v", columnNames)
	for i, row := range rows {
		log.Infof("  Row %d: %v", i, row)
	}
	log.Infof("\n" + strings.Repeat("=", 80) + "\n")

	return rows, columnNames, respCode, nil
}

// CompareResults compares MySQL and REST server results
// If rowOrder is true, rows must match in the same order
// If rowOrder is false, rows must match in count and data but order doesn't matter
func CompareResults(t *testing.T, mysqlRows [][]interface{}, mysqlCols []string,
	restRows [][]any, restCols []string, rowOrder bool) {

	// When both return 0 rows, column comparison is skipped because
	// REST API doesn't return column metadata when there's no data
	if len(mysqlRows) == 0 && len(restRows) == 0 {
		return
	}

	if len(mysqlCols) != len(restCols) {
		t.Fatalf("Column count mismatch: MySQL=%d, REST=%d", len(mysqlCols), len(restCols))
	}

	for i := range mysqlCols {
		if mysqlCols[i] != restCols[i] {
			t.Fatalf("Column name mismatch at index %d: MySQL=%s, REST=%s",
				i, mysqlCols[i], restCols[i])
		}
	}

	if len(mysqlRows) != len(restRows) {
		t.Fatalf("Row count mismatch: MySQL=%d, REST=%d", len(mysqlRows), len(restRows))
	}

	if rowOrder {
		// Compare rows in order
		for i := range mysqlRows {
			for j := range mysqlRows[i] {
				if !valuesEqual(mysqlRows[i][j], restRows[i][j]) {
					t.Errorf("Value mismatch at row %d, col %d (%s): MySQL=%v, REST=%v",
						i, j, mysqlCols[j], mysqlRows[i][j], restRows[i][j])
				}
			}
		}
	} else {
		// Compare rows without considering order
		// Convert rows to string representation for comparison
		mysqlRowSet := make(map[string]int)
		for _, row := range mysqlRows {
			rowStr := rowToString(row)
			mysqlRowSet[rowStr]++
		}

		restRowSet := make(map[string]int)
		for _, row := range restRows {
			rowStr := rowToString(row)
			restRowSet[rowStr]++
		}

		// Check that all MySQL rows exist in REST results
		for rowStr, count := range mysqlRowSet {
			if restRowSet[rowStr] != count {
				t.Errorf("Row mismatch: MySQL has %d occurrence(s) of row %s, REST has %d",
					count, rowStr, restRowSet[rowStr])
			}
		}

		// Check that all REST rows exist in MySQL results
		for rowStr, count := range restRowSet {
			if mysqlRowSet[rowStr] != count {
				t.Errorf("Row mismatch: REST has %d occurrence(s) of row %s, MySQL has %d",
					count, rowStr, mysqlRowSet[rowStr])
			}
		}
	}
}

// rowToString converts a row to a string representation for comparison
func rowToString(row []any) string {
	parts := make([]string, len(row))
	for i, val := range row {
		parts[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(parts, "|")
}

// valuesEqual compares two values, using float tolerance for numeric types
// This handles the case where MySQL and REST server format floats differently
func valuesEqual(mysqlVal, restVal any) bool {
	// Handle nil values
	if mysqlVal == nil && restVal == nil {
		return true
	}
	if mysqlVal == nil || restVal == nil {
		return false
	}

	// Try to convert both to float64 for numeric comparison
	mysqlFloat, mysqlOk := toFloat64(mysqlVal)
	restFloat, restOk := toFloat64(restVal)

	if mysqlOk && restOk {
		// Both are numeric - compare with relative tolerance
		// Use float32 precision tolerance since MySQL FLOAT is 32-bit
		return floatsEqual(mysqlFloat, restFloat, 1e-6)
	}

	// Fall back to string comparison
	return fmt.Sprintf("%v", mysqlVal) == fmt.Sprintf("%v", restVal)
}

// toFloat64 attempts to convert a value to float64
func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case string:
		// Try to parse string as float64 (MySQL driver returns floats as strings)
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// floatsEqual compares two float64 values with relative tolerance
func floatsEqual(a, b, tolerance float64) bool {
	if a == b {
		return true
	}
	diff := math.Abs(a - b)
	// Use relative tolerance based on the larger absolute value
	maxAbs := math.Max(math.Abs(a), math.Abs(b))
	if maxAbs == 0 {
		return diff < tolerance
	}
	return diff/maxAbs < tolerance
}

// extractColumnNamesInOrder extracts column names from JSON response preserving order
// Uses json.Decoder to read tokens in order since map iteration doesn't preserve key order
func extractColumnNamesInOrder(respBody []byte) ([]string, error) {
	decoder := json.NewDecoder(strings.NewReader(string(respBody)))

	// Navigate to the first object in "data" array: {"data":[{...},...]}
	// Skip opening brace of root object
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("failed to read opening brace: %w", err)
	}

	// Find "data" key
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("failed to read token: %w", err)
		}

		key, ok := token.(string)
		if !ok {
			continue
		}

		if key == "data" {
			// Skip opening bracket of data array
			if _, err := decoder.Token(); err != nil {
				return nil, fmt.Errorf("failed to read data array opening: %w", err)
			}

			// Check if array has elements
			if !decoder.More() {
				return []string{}, nil
			}

			// Skip opening brace of first object
			if _, err := decoder.Token(); err != nil {
				return nil, fmt.Errorf("failed to read first object opening: %w", err)
			}

			// Extract column names in order
			var columnNames []string
			for decoder.More() {
				token, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("failed to read column name: %w", err)
				}

				colName, ok := token.(string)
				if !ok {
					return nil, fmt.Errorf("expected string key, got %T", token)
				}
				columnNames = append(columnNames, colName)

				// Skip the value
				var value interface{}
				if err := decoder.Decode(&value); err != nil {
					return nil, fmt.Errorf("failed to skip value: %w", err)
				}
			}

			return columnNames, nil
		}
	}

	return nil, fmt.Errorf("data field not found in response")
}

func indexScanTestMultiple(t *testing.T, tests map[string]api.IndexTestInfo, isBinaryData bool) {

	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			indexScanTest(t, testInfo, isBinaryData)
		})
	}

}

func indexScanTest(t *testing.T, testInfo api.IndexTestInfo, isBinaryData bool) {
	restRows, restCols, _, err := ExecuteUsingRESTServer(t, testInfo.DB, testInfo.Table, &testInfo.IndexScanReq, testInfo.BodyContains, testInfo.ExpectedHttpCode)
	if err != nil {
		t.Fatalf("ExecuteUsingRESTServer failed: %v", err)
	}

	// Skip MySQL validation if explicitly requested or if expected response is an error
	if testInfo.SkipMySQLValidation || testInfo.ExpectedHttpCode != http.StatusOK {
		return
	}

	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, testInfo.DB, testInfo.Table, &testInfo.IndexScanReq, isBinaryData)
	if err != nil {
		t.Fatalf("ExecuteUsingMySQLServer failed: %v", err)
	}

	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, testInfo.RowsOrder)
}
