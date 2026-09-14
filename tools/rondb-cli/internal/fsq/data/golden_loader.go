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
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/ddl"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// GoldenLoad owns only databases whose CREATE succeeded in this load.
// A fixture must finish using them before Cleanup; overlapping fixture
// loads are not supported. Do not copy the handle or externally replace
// its databases. This handle is not safe for concurrent use.
type GoldenLoad struct {
	target Target
	owned  []string
}

// Databases returns a copy of the database names still owned by this load.
func (l *GoldenLoad) Databases() []string {
	return append([]string(nil), l.owned...)
}

// LoadGolden creates and populates one fixture's captured database/table
// names, without rewriting SQL or using the E4 data set. Existing databases
// are an error, never reused. All input is validated before opening MySQL.
// When the returned handle is non-nil, the caller must arrange Cleanup even
// if err is non-nil: a failed setup may already own some databases.
// A failed CREATE is never claimed or dropped; a transport error may leave
// its outcome uncertain and require manual inspection.
func LoadGolden(ctx context.Context, target Target, fgs []spec.FeatureGroup) (*GoldenLoad, error) {
	plan, err := planGoldenLoad(fgs)
	if err != nil {
		return nil, err
	}
	conn, err := openGolden(ctx, target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	load := &GoldenLoad{target: target}
	err = load.populate(ctx, conn, plan)
	return load, err
}

// Cleanup drops only successfully created databases, in reverse order.
// Use a fresh context if the run context was cancelled. Successful drops
// are forgotten; failed drops remain owned so Cleanup can be retried.
// Do not start the next fixture until cleanup succeeds. It does not
// discover databases by prefix or adopt a failed CREATE.
func (l *GoldenLoad) Cleanup(ctx context.Context) error {
	if len(l.owned) == 0 {
		return nil
	}
	conn, err := openGolden(ctx, l.target)
	if err != nil {
		return err
	}
	defer conn.Close()
	return l.cleanup(ctx, conn)
}

func openGolden(ctx context.Context, target Target) (*sql.DB, error) {
	params := url.Values{}
	params.Set("charset", "utf8mb4")
	params.Set("sql_mode", "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'")
	params.Set("autocommit", "1")
	// dsn also sets UTC on every connection, including driver reconnects.
	conn, err := sql.Open("mysql", dsn(target, "")+"&"+params.Encode())
	if err != nil {
		return nil, fmt.Errorf("A6 open MySQL: %w", err)
	}
	conn.SetMaxOpenConns(1)
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("A6 connect MySQL: %w", err)
	}
	return conn, nil
}

type goldenSQL interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type goldenStep struct {
	sql  string
	rows int64 // -1 for DDL; otherwise expected INSERT rows
}

type goldenLoadPlan struct {
	databases []string
	steps     []goldenStep
}

func planGoldenLoad(fgs []spec.FeatureGroup) (goldenLoadPlan, error) {
	var plan goldenLoadPlan
	if len(fgs) == 0 {
		return plan, fmt.Errorf("A6 load: no feature groups")
	}
	dbs, tables, ids := map[string]bool{}, map[string]bool{}, map[int]bool{}
	for _, fg := range fgs {
		if fg.ID <= 0 || fg.FeaturestoreID <= 0 || fg.Version <= 0 || ids[fg.ID] {
			return goldenLoadPlan{}, fmt.Errorf("A6 load: invalid or duplicate feature-group identity")
		}
		ids[fg.ID] = true
		db := fmt.Sprintf("golden_%d_fs", fg.FeaturestoreID)
		if fg.OnlineDatabase() != db || !fg.OnlineEnabled() || fg.TTLEnabled() {
			return goldenLoadPlan{}, fmt.Errorf("A6 load: %s needs its captured database, online enabled and TTL disabled", fg.TableName())
		}
		name := db + "." + fg.TableName()
		if tables[name] {
			return goldenLoadPlan{}, fmt.Errorf("A6 load: duplicate table %s", name)
		}
		tables[name], dbs[db] = true, true
		rows, err := GoldenRows(fg)
		if err != nil {
			return goldenLoadPlan{}, err
		}
		create, err := ddl.BuildCreateStatement(db, fg)
		if err != nil {
			return goldenLoadPlan{}, fmt.Errorf("A6 load %s: %w", name, err)
		}
		// Preserve the Hopsworks table definition, but never reuse a table.
		create = strings.Replace(create, "CREATE TABLE IF NOT EXISTS ", "CREATE TABLE ", 1)
		plan.steps = append(plan.steps, goldenStep{sql: create, rows: -1})
		var columns, tuples []string
		for _, f := range fg.Features {
			columns = append(columns, "`"+f.Name+"`")
		}
		for _, row := range rows {
			if len(row) != len(columns) {
				return goldenLoadPlan{}, fmt.Errorf("A6 load %s: row width differs from schema", name)
			}
			tuples = append(tuples, tuple(row...))
		}
		if len(tuples) == 0 {
			return goldenLoadPlan{}, fmt.Errorf("A6 load %s: no fixture rows", name)
		}
		insert := "INSERT INTO `" + db + "`.`" + fg.TableName() + "` (" +
			strings.Join(columns, ", ") + ") VALUES " + strings.Join(tuples, ", ") + ";"
		plan.steps = append(plan.steps, goldenStep{sql: insert, rows: int64(len(rows))})
	}
	for db := range dbs {
		plan.databases = append(plan.databases, db)
	}
	sort.Strings(plan.databases)
	return plan, nil
}

func (l *GoldenLoad) populate(ctx context.Context, conn goldenSQL, plan goldenLoadPlan) error {
	for _, db := range plan.databases {
		stmt := "CREATE DATABASE `" + db + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("A6 create %s (must not already exist; failed CREATE is not owned): %w", db, err)
		}
		l.owned = append(l.owned, db)
	}
	for i, step := range plan.steps {
		result, err := conn.ExecContext(ctx, step.sql)
		if err != nil {
			return fmt.Errorf("A6 setup step %d: %w", i+1, err)
		}
		if step.rows >= 0 {
			if result == nil {
				return fmt.Errorf("A6 setup step %d: missing INSERT result", i+1)
			}
			n, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("A6 setup step %d: INSERT row count: %w", i+1, err)
			}
			if n != step.rows {
				return fmt.Errorf("A6 setup step %d: inserted %d rows, want %d", i+1, n, step.rows)
			}
		}
	}
	return nil
}

func (l *GoldenLoad) cleanup(ctx context.Context, conn goldenSQL) error {
	var failures []error
	for i := len(l.owned) - 1; i >= 0; i-- {
		db := l.owned[i]
		if _, err := conn.ExecContext(ctx, ddl.DropDatabase(db)); err != nil {
			failures = append(failures, fmt.Errorf("A6 cleanup %s: %w", db, err))
			continue
		}
		l.owned = append(l.owned[:i], l.owned[i+1:]...)
	}
	return errors.Join(failures...)
}
