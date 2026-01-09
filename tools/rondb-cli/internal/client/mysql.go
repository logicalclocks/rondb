package client

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLClient struct {
	db *sql.DB
}

// NewMySQLClient creates a new MySQL client connection
func NewMySQLClient(host string, port int, user, password string) (*MySQLClient, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)

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
