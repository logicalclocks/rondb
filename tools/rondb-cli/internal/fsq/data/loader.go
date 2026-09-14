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

package data

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/ddl"
)

// Target is a MySQL server the loader connects to.
type Target struct {
	Host     string
	Port     int
	User     string
	Password string
	TLS      bool
}

// LoadOptions controls a bench-scale load (data_model.md §8.3).
type LoadOptions struct {
	DB       string
	Scale    Scale
	Threads  int
	Batch    int // rows per INSERT statement
	HashTwin bool
	Log      func(msg string)
}

// LoadStats summarises a load.
type LoadStats struct {
	Rows     int64
	Tables   int
	Skipped  int
	Duration time.Duration
}

var tlsRegistered sync.Once

func dsn(t Target, db string) string {
	if t.TLS {
		tlsRegistered.Do(func() {
			_ = mysql.RegisterTLSConfig("fsq", &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})
		})
	}
	params := url.Values{}
	// Every connection stores TIMESTAMP literals as UTC (RonSQL is UTC-only).
	params.Set("time_zone", "'+00:00'")
	params.Set("maxAllowedPacket", "67108864")
	if t.TLS {
		params.Set("tls", "fsq")
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", t.User, t.Password, t.Host, t.Port, db, params.Encode())
}

func open(t Target, db string) (*sql.DB, error) {
	conn, err := sql.Open("mysql", dsn(t, db))
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1)
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// CreateSchema creates the database and every table (idempotent DDL).
func CreateSchema(ctx context.Context, t Target, db string, hashTwin bool) error {
	conn, err := open(t, "")
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, ddl.CreateDatabase(db)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	for _, tg := range Tables() {
		if tg.HashTwin && !hashTwin {
			continue
		}
		stmt, err := ddl.BuildCreateStatement(db, tg.FG)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create %s: %w\n%s", tg.Table, err, stmt)
		}
	}
	return nil
}

// Drop drops the database.
func Drop(ctx context.Context, t Target, db string) error {
	conn, err := open(t, "")
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, ddl.DropDatabase(db))
	return err
}

// Load creates the schema and loads every table with multi-row INSERTs,
// Threads workers per table over disjoint entity ranges.  A table whose row
// count already equals the expected count is skipped (idempotent reload);
// a table with a different non-zero count is an error.
func Load(ctx context.Context, t Target, o LoadOptions) (LoadStats, error) {
	if o.Threads <= 0 {
		o.Threads = 4
	}
	if o.Batch <= 0 {
		o.Batch = 500
	}
	if o.Log == nil {
		o.Log = func(string) {}
	}
	start := time.Now()
	var stats LoadStats
	if err := CreateSchema(ctx, t, o.DB, o.HashTwin); err != nil {
		return stats, err
	}
	expected := map[string]int64{}
	for _, c := range Checksums(o.Scale, o.HashTwin) {
		expected[c.Table] = c.Count
	}
	ctl, err := open(t, o.DB)
	if err != nil {
		return stats, err
	}
	defer ctl.Close()

	for _, tg := range Tables() {
		if tg.HashTwin && !o.HashTwin {
			continue
		}
		var have int64
		if err := ctl.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", o.DB, tg.Table)).Scan(&have); err != nil {
			return stats, fmt.Errorf("count %s: %w", tg.Table, err)
		}
		want := expected[tg.Table]
		if have == want {
			o.Log(fmt.Sprintf("   %s: %d rows already loaded, skipping", tg.Table, have))
			stats.Skipped++
			continue
		}
		if have != 0 {
			return stats, fmt.Errorf("%s has %d rows, expected 0 or %d: run .fs_drop first", tg.Table, have, want)
		}
		o.Log(fmt.Sprintf("Loading %s (%d rows)...", tg.Table, want))
		tableStart := time.Now()
		rows, err := loadTable(ctx, t, o, tg)
		if err != nil {
			return stats, fmt.Errorf("load %s: %w", tg.Table, err)
		}
		if rows != want {
			return stats, fmt.Errorf("%s: loaded %d rows, expected %d", tg.Table, rows, want)
		}
		d := time.Since(tableStart)
		o.Log(fmt.Sprintf("   Loaded %d rows in %v (%.0f rows/sec)", rows, d.Round(time.Millisecond),
			float64(rows)/d.Seconds()))
		stats.Rows += rows
		stats.Tables++
	}
	stats.Duration = time.Since(start)
	return stats, nil
}

func loadTable(ctx context.Context, t Target, o LoadOptions, tg TableGen) (int64, error) {
	n := tg.Entities(o.Scale)
	threads := o.Threads
	if int64(threads) > n {
		threads = int(n)
	}
	per := (n + int64(threads) - 1) / int64(threads)
	var loaded int64
	var wg sync.WaitGroup
	errCh := make(chan error, threads)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Progress reporting.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.Log(fmt.Sprintf("   Progress: %d rows", atomic.LoadInt64(&loaded)))
			case <-done:
				return
			}
		}
	}()
	defer close(done)

	prefix := fmt.Sprintf("INSERT INTO `%s`.`%s` %s VALUES ", o.DB, tg.Table, tg.ColumnList())
	for w := 0; w < threads; w++ {
		lo := int64(w)*per + 1
		hi := lo + per - 1
		if hi > n {
			hi = n
		}
		if lo > hi {
			continue
		}
		wg.Add(1)
		go func(lo, hi int64) {
			defer wg.Done()
			conn, err := open(t, o.DB)
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			defer conn.Close()
			var batch []string
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				_, err := conn.ExecContext(ctx, prefix+strings.Join(batch, ",\n"))
				if err != nil {
					return err
				}
				atomic.AddInt64(&loaded, int64(len(batch)))
				batch = batch[:0]
				return nil
			}
			for e := lo; e <= hi; e++ {
				if ctx.Err() != nil {
					return
				}
				tg.Rows(o.Scale, e, func(tuple string) { batch = append(batch, tuple) })
				if len(batch) >= o.Batch {
					if err := flush(); err != nil {
						errCh <- err
						cancel()
						return
					}
				}
			}
			if err := flush(); err != nil {
				errCh <- err
				cancel()
			}
		}(lo, hi)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return atomic.LoadInt64(&loaded), err
		}
	}
	return atomic.LoadInt64(&loaded), nil
}

// Verify runs the checksum queries against the loaded database and returns
// a description of every mismatch (empty when all tables match).
func Verify(ctx context.Context, t Target, db string, sc Scale, hashTwin bool) ([]string, error) {
	conn, err := open(t, db)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var mismatches []string
	for _, c := range Checksums(sc, hashTwin) {
		var count int64
		var sum, cnt sql.NullString
		if err := conn.QueryRowContext(ctx, c.Query(db)).Scan(&count, &sum, &cnt); err != nil {
			return nil, fmt.Errorf("%s: %w", c.Table, err)
		}
		got := fmt.Sprintf("%d,%s,%s", count, nullStr(sum), nullStr(cnt))
		want := fmt.Sprintf("%d,%d,%d", c.Count, c.Sum, c.CntValue)
		if got != want {
			mismatches = append(mismatches, fmt.Sprintf("%s: got %s want %s", c.Table, got, want))
		}
	}
	return mismatches, nil
}

func nullStr(s sql.NullString) string {
	if !s.Valid {
		return "0"
	}
	// SUM() of an integer column is a DECIMAL: strip a ".00" suffix if any.
	return strings.TrimSuffix(s.String, ".00")
}
