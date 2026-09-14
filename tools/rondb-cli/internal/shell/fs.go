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

package shell

// .fs_* commands of the RonSQL feature-store test framework (RONDB-1121).
// Execution phase E1: schema, data loaders, MTR include emission.
// Design: storage/ndb/claude_files/fs_ronsql/framework_design.md §10.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// fsArgs is the parsed form of ".fs_x pos1 pos2 --flag value --switch".
type fsArgs struct {
	pos   []string
	flags map[string]string
}

func parseFSArgs(args []string) fsArgs {
	a := fsArgs{flags: map[string]string{}}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			name := strings.TrimPrefix(arg, "--")
			if eq := strings.IndexByte(name, '='); eq >= 0 {
				a.flags[name[:eq]] = name[eq+1:]
				continue
			}
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") && !isFSSwitch(name) {
				a.flags[name] = args[i+1]
				i++
			} else {
				a.flags[name] = "true"
			}
			continue
		}
		a.pos = append(a.pos, arg)
	}
	return a
}

// Switches never take a value.
func isFSSwitch(name string) bool {
	switch name {
	case "hash-twin", "no-hash-twin", "verify", "quiet", "cases", "all", "vectors",
		"include-hazards", "allow-reject", "relaxed-headers":
		return true
	}
	return false
}

func (a fsArgs) has(name string) bool { _, ok := a.flags[name]; return ok }

func (a fsArgs) str(name, def string) string {
	if v, ok := a.flags[name]; ok {
		return v
	}
	return def
}

func (a fsArgs) float(name string, def float64) (float64, error) {
	v, ok := a.flags[name]
	if !ok {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("invalid --%s: %s (use a positive number)", name, v)
	}
	return f, nil
}

// executeFS dispatches the .fs_* commands.
func (s *Shell) executeFS(cmd string, args []string) error {
	switch cmd {
	case "fs_load":
		return s.runFSLoad(args)
	case "fs_drop":
		return s.runFSDrop(args)
	case "fs_emit_mtr":
		return s.runFSEmitMTR(args)
	case "fs_schema":
		return s.runFSSchema(args)
	case "fs_verify":
		return s.runFSVerify(args)
	case "fs_show":
		return s.runFSShow(args)
	case "fs_fuzz":
		return s.runFSFuzz(args)
	}
	return fmt.Errorf("unknown command .%s", cmd)
}

func (s *Shell) fsTarget() (data.Target, error) {
	if s.mysqlClient == nil {
		return data.Target{}, fmt.Errorf("MySQL not connected. The .fs_load/.fs_drop commands need the MySQL connection.")
	}
	host := s.config.MySQLHost
	if host == "" {
		host = s.config.Host
	}
	return data.Target{
		Host: host, Port: s.config.MySQLPort,
		User: s.mysqlUser, Password: s.mysqlPass, TLS: s.config.TLS,
	}, nil
}

// hashTwinDefault: the hash-only twin doubles the largest table, so it is
// loaded by default only at small scale factors.
func hashTwinDefault(a fsArgs, sf float64) bool {
	if a.has("no-hash-twin") {
		return false
	}
	if a.has("hash-twin") {
		return true
	}
	return sf <= 0.1
}

// .fs_load <sf> [threads] [batch] [--db fs_bench] [--hash-twin|--no-hash-twin]
func (s *Shell) runFSLoad(args []string) error {
	a := parseFSArgs(args)
	target, err := s.fsTarget()
	if err != nil {
		return err
	}
	sf := 1.0
	if len(a.pos) > 0 {
		f, err := strconv.ParseFloat(a.pos[0], 64)
		if err != nil || f <= 0 {
			return fmt.Errorf("invalid scale factor: %s (use a positive number, e.g. 0.01, 0.1, 1)", a.pos[0])
		}
		sf = f
	}
	threads, batch := 8, 500
	if len(a.pos) > 1 {
		if threads, err = strconv.Atoi(a.pos[1]); err != nil || threads <= 0 {
			return fmt.Errorf("invalid thread count: %s", a.pos[1])
		}
	}
	if len(a.pos) > 2 {
		if batch, err = strconv.Atoi(a.pos[2]); err != nil || batch <= 0 {
			return fmt.Errorf("invalid batch size: %s", a.pos[2])
		}
	}
	db := a.str("db", "fs_bench")
	sc := data.NewScale(sf)
	hashTwin := hashTwinDefault(a, sf)

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Feature-store load: sf=%g (E=%d customers, M=%d merchants, A=%d accounts) into `%s`, %d threads, %d rows/statement, hash twin=%v",
		sf, sc.E, sc.M, sc.A, db, threads, batch, hashTwin)))
	fmt.Println(ui.Info(fmt.Sprintf("Reference clock FS_NOW = %s UTC; every connection uses time_zone='+00:00'", data.FSNowString)))
	fmt.Println()

	stats, err := data.Load(context.Background(), target, data.LoadOptions{
		DB: db, Scale: sc, Threads: threads, Batch: batch, HashTwin: hashTwin,
		Log: func(msg string) { fmt.Println(msg) },
	})
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println(ui.Success(fmt.Sprintf("Load complete: %d rows in %d tables (%d already loaded) in %v",
		stats.Rows, stats.Tables, stats.Skipped, stats.Duration.Round(time.Millisecond))))

	fmt.Println(ui.Info("Verifying checksums..."))
	mismatches, err := data.Verify(context.Background(), target, db, sc, hashTwin)
	if err != nil {
		return err
	}
	if len(mismatches) > 0 {
		for _, m := range mismatches {
			fmt.Println(ui.Error("   " + m))
		}
		return fmt.Errorf("%d table(s) differ from the data formulas", len(mismatches))
	}
	fmt.Println(ui.Success("All checksums match the data formulas"))
	fmt.Println()
	return nil
}

// .fs_drop [--db fs_bench]
func (s *Shell) runFSDrop(args []string) error {
	a := parseFSArgs(args)
	target, err := s.fsTarget()
	if err != nil {
		return err
	}
	db := a.str("db", "fs_bench")
	if err := data.Drop(context.Background(), target, db); err != nil {
		return err
	}
	fmt.Println(ui.Success(fmt.Sprintf("Dropped database `%s`", db)))
	return nil
}

// .fs_emit_mtr <dir> [--sf 0.01] [--db test] [--no-hash-twin]
func (s *Shell) runFSEmitMTR(args []string) error {
	a := parseFSArgs(args)
	if len(a.pos) < 1 {
		return fmt.Errorf("usage: .fs_emit_mtr <dir> [--sf 0.01] [--db test] [--no-hash-twin]")
	}
	dir := a.pos[0]
	sf, err := a.float("sf", 0.01)
	if err != nil {
		return err
	}
	o := data.MTROptions{DB: a.str("db", "test"), Scale: data.NewScale(sf), HashTwin: hashTwinDefault(a, sf)}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	schema, err := data.RenderSchemaInc(o)
	if err != nil {
		return err
	}
	dataInc, err := data.RenderDataInc(o)
	if err != nil {
		return err
	}
	files := map[string]string{
		"fs_schema.inc": schema,
		"fs_data.inc":   dataInc,
		"fs_drop.inc":   data.RenderDropInc(o),
	}
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return err
		}
		fmt.Println(ui.Success(fmt.Sprintf("wrote %s (%d bytes)", path, len(content))))
	}
	var rows int64
	for _, c := range data.Checksums(o.Scale, o.HashTwin) {
		rows += c.Count
	}
	fmt.Println(ui.Info(fmt.Sprintf("sf=%g: E=%d customers, %d rows in total, hash twin=%v, database `%s`",
		sf, o.Scale.E, rows, o.HashTwin, o.DB)))
	if a.has("cases") {
		path, err := emitTemplatesTest(dir, o.DB, sf)
		if err != nil {
			return err
		}
		fmt.Println(ui.Success("wrote " + path))
	}
	return nil
}

// .fs_schema [--db fs_bench] [--hash-twin]
func (s *Shell) runFSSchema(args []string) error {
	a := parseFSArgs(args)
	sql, err := data.RenderSchemaSQL(a.str("db", "fs_bench"), a.has("hash-twin"))
	if err != nil {
		return err
	}
	fmt.Print(sql)
	return nil
}
