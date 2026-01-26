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

package client

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type MySQLClient struct {
	db *sql.DB
}

// MySQLOptions holds connection options
type MySQLOptions struct {
	Host     string
	Port     int
	User     string
	Password string
	TLS      bool // Enable TLS
}

// sanitizeIdentifier escapes MySQL identifiers to prevent SQL injection.
// Identifiers (database/table names) can't use parameterized queries,
// so we escape backticks by doubling them.
func sanitizeIdentifier(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

// NewMySQLClient creates a new MySQL client connection
func NewMySQLClient(host string, port int, user, password string) (*MySQLClient, error) {
	return NewMySQLClientWithOptions(MySQLOptions{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	})
}

// NewMySQLClientWithOptions creates a new MySQL client with extended options
func NewMySQLClientWithOptions(opts MySQLOptions) (*MySQLClient, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", opts.User, opts.Password, opts.Host, opts.Port)

	// Register TLS config if enabled
	if opts.TLS {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
		mysql.RegisterTLSConfig("custom", tlsConfig)
		dsn += "?tls=custom"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &MySQLClient{db: db}, nil
}

// Query executes a SELECT query and returns columns, rows, and duration
func (c *MySQLClient) Query(sql string) (columns []string, rows [][]interface{}, duration time.Duration, err error) {
	start := time.Now()
	defer func() { duration = time.Since(start) }()

	rows_result, err := c.db.Query(sql)
	if err != nil {
		return nil, nil, duration, fmt.Errorf("query failed: %w", err)
	}
	defer rows_result.Close()

	columns, err = rows_result.Columns()
	if err != nil {
		return nil, nil, duration, fmt.Errorf("failed to get columns: %w", err)
	}

	var data [][]interface{}
	for rows_result.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows_result.Scan(valuePtrs...); err != nil {
			return nil, nil, duration, fmt.Errorf("failed to scan row: %w", err)
		}

		data = append(data, values)
	}

	if err := rows_result.Err(); err != nil {
		return nil, nil, duration, fmt.Errorf("error iterating rows: %w", err)
	}

	return columns, data, duration, nil
}

// Execute runs INSERT, UPDATE, or DELETE and returns affected rows and duration
func (c *MySQLClient) Execute(sql string) (affected int64, duration time.Duration, err error) {
	start := time.Now()
	defer func() { duration = time.Since(start) }()

	result, err := c.db.Exec(sql)
	if err != nil {
		return 0, duration, fmt.Errorf("execute failed: %w", err)
	}

	affected, err = result.RowsAffected()
	if err != nil {
		return 0, duration, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return affected, duration, nil
}

// ListTables returns all tables in the current database
func (c *MySQLClient) ListTables() ([]string, error) {
	rows, err := c.db.Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// Ping checks the database connection
func (c *MySQLClient) Ping() error {
	if err := c.db.Ping(); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

// Close closes the database connection
func (c *MySQLClient) Close() error {
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	return nil
}

// ListDatabases returns all databases
func (c *MySQLClient) ListDatabases() ([]string, error) {
	rows, err := c.db.Query("SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %w", err)
		}
		// Skip system databases for cleaner view
		if dbName != "information_schema" && dbName != "performance_schema" && dbName != "mysql" && dbName != "sys" {
			databases = append(databases, dbName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating databases: %w", err)
	}

	return databases, nil
}

// ListTablesInDB returns all tables in a specific database
func (c *MySQLClient) ListTablesInDB(database string) ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM `%s`", sanitizeIdentifier(database))
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// ColumnInfo holds column metadata for TUI
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable string
	Key      string
	Default  string
}

// TableData holds query results for TUI
type TableData struct {
	Columns []string
	Rows    [][]string
	Total   int
}

// QueryTableData returns sample data from a table (limited rows for TUI)
func (c *MySQLClient) QueryTableData(database, table string, limit int) (*TableData, error) {
	db := sanitizeIdentifier(database)
	tbl := sanitizeIdentifier(table)

	// Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", db, tbl)
	var total int
	if err := c.db.QueryRow(countQuery).Scan(&total); err != nil {
		total = -1 // Unknown
	}

	// Get data
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", db, tbl, limit)
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var data [][]string
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make([]string, len(columns))
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
			} else if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		data = append(data, row)
	}

	return &TableData{
		Columns: columns,
		Rows:    data,
		Total:   total,
	}, nil
}

// DescribeTable returns column info for a table
func (c *MySQLClient) DescribeTable(database, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", sanitizeIdentifier(database), sanitizeIdentifier(table))
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var field, colType, null, key string
		var defaultVal, extra sql.NullString

		if err := rows.Scan(&field, &colType, &null, &key, &defaultVal, &extra); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		col := ColumnInfo{
			Name:     field,
			Type:     colType,
			Nullable: null,
			Key:      key,
		}
		if defaultVal.Valid {
			col.Default = defaultVal.String
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return columns, nil
}
