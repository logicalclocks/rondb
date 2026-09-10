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

package exec

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestParseJSONData(t *testing.T) {
	body := []byte("{\"data\":\n[{\"c\":null,\"max(sint16)\":32566,\"d\":1.50,\"s\":\"O'Brien\"}\n,{\"c\":\"x\",\"max(sint16)\":-1,\"d\":0.5000000000,\"s\":\"\"}\n]\n}")
	cols, rows, err := ParseJSONData(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 || cols[1] != "max(sint16)" || len(rows) != 2 {
		t.Fatalf("cols %v rows %d", cols, len(rows))
	}
	if !rows[0][0].Null || rows[0][2].Text != "1.50" || rows[1][2].Text != "0.5000000000" || rows[1][3].Text != "" || rows[1][3].Null {
		t.Errorf("cells: %+v", rows)
	}
	if _, _, err := ParseJSONData([]byte(`[{"a":1,"a":2}]`)); err == nil {
		t.Error("duplicate output names must be rejected")
	}
	cols, rows, err = ParseJSONData([]byte("{\"data\":\n[]\n}"))
	if err != nil || cols != nil || len(rows) != 0 {
		t.Errorf("empty data: cols %v rows %v err %v", cols, rows, err)
	}
	if _, _, err := ParseJSONData([]byte(`[{"a":1}]`)); err != nil {
		t.Errorf("bare array (ronsql_cli): %v", err)
	}
	f9 := []byte(`{"data":[{"cnt":3,"d_min":1970-01-01}]}`)
	if _, _, err := ParseJSONData(f9); err == nil {
		t.Error("unquoted temporal aggregate (F9) must be a syntax error")
	} else if d := jsonErrDetail(f9, err); len(d) <= len(err.Error()) || d[len(d)-1] != '"' {
		t.Errorf("detail must quote the offending text: %s", d)
	}
}

func TestParseJSONDataEnvelope(t *testing.T) {
	for _, body := range []string{
		`{"data":[{"n":1}]`,
		`{"data":[]} garbage`,
		`[{"n":1}] {}`,
		`{"data":[],"data":[{"n":1}]}`,
		`{"metadata":{"data":[]}}`,
		`{"metadata":"data"}`,
		`{"data":null}`,
		`{"data":{}}`,
	} {
		t.Run(body, func(t *testing.T) {
			if _, _, err := ParseJSONData([]byte(body)); err == nil {
				t.Fatal("invalid result envelope must be rejected")
			}
		})
	}
	for _, body := range []string{
		`{"metadata":"data","data":[{"n":9007199254740993}],"extra":true}`,
		`{"metadata":{"data":[]},"data":[{"n":9007199254740993}]}`,
		` [{"n":9007199254740993}] `,
	} {
		t.Run(body, func(t *testing.T) {
			cols, rows, err := ParseJSONData([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if len(cols) != 1 || cols[0] != "n" || len(rows) != 1 ||
				len(rows[0]) != 1 || rows[0][0].Null || rows[0][0].Text != "9007199254740993" {
				t.Fatalf("numeric token changed: cols=%v rows=%+v", cols, rows)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	if o, _ := Classify(http.StatusOK, ""); o != OK {
		t.Error("200")
	}
	if o, _ := Classify(500, "CTE 't' ...\nCaught exception: Non-aggregating CTE body is not a single-row key lookup.\nError handling: RPE"); o != CleanReject {
		t.Error("permanent")
	}
	if o, _ := Classify(500, "Caught RonSQLRetryableError after 10 attempts: x"); o != Retryable {
		t.Error("retryable")
	}
	if o, _ := Classify(400, "bad database"); o != Error {
		t.Error("400")
	}
	if o, _ := Classify(429, "rate limited"); o != Retryable {
		t.Error("429")
	}
	if got := ParsePhases("parse=12,analyze=3,rows=5,attempts=1"); got["rows"] != 5 || got["parse"] != 12 {
		t.Errorf("phases %v", got)
	}
	if h := ParseTextHeader("a\tb\n1\t2\n"); len(h) != 2 || h[1] != "b" {
		t.Errorf("header %v", h)
	}
}

func testCLI(t *testing.T, script string, timeout time.Duration) *CLI {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("CLI subprocess fixtures require a POSIX shell")
	}
	path := filepath.Join(t.TempDir(), "ronsql_cli")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return NewCLI(path, "", "test", timeout)
}

func TestCLIQueryDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cliTimeout  time.Duration
		parentLimit time.Duration
	}{
		{"internal", 100 * time.Millisecond, 0},
		{"parent", 30 * time.Second, 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// exec replaces the shell, leaving no descendant holding pipes
			// open after CommandContext kills the process.
			cli := testCLI(t, "exec sleep 30", tc.cliTimeout)
			ctx := context.Background()
			if tc.parentLimit > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.parentLimit)
				defer cancel()
			}
			r := cli.Query(ctx, "SELECT 1;")
			if r.Outcome != Timeout || !strings.Contains(r.Message, context.DeadlineExceeded.Error()) {
				t.Fatalf("deadline must report TIMEOUT, got %+v", r)
			}
			if r.Result == nil || r.Result.Latency <= 0 {
				t.Fatal("timeout must preserve result diagnostics")
			}
			if tc.parentLimit == 0 && ctx.Err() != nil {
				t.Fatal("the internal deadline must not cancel the caller context")
			}
		})
	}
}

func TestCLIQueryCancellation(t *testing.T) {
	cli := testCLI(t, "exec sleep 30", 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := cli.Query(ctx, "SELECT 1;")
	if r.Outcome != Error || !strings.Contains(r.Message, context.Canceled.Error()) {
		t.Fatalf("cancellation must not be classified as CRASH or TIMEOUT: %+v", r)
	}
}

func TestCLIQueryExitClassification(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		want         Outcome
	}{
		{"success", `printf '[{"n":1}]'`, OK},
		{"reject", "exit 1", CleanReject},
		{"retryable", "exit 3", Retryable},
		{"other-error", "exit 2", Error},
		{"signal", "kill -KILL $$", Crash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := testCLI(t, tc.script, 30*time.Second)
			if r := cli.Query(context.Background(), "SELECT 1;"); r.Outcome != tc.want {
				t.Fatalf("outcome=%s, want %s: %s", r.Outcome, tc.want, r.Message)
			}
		})
	}
}

func TestMySQLConnectionSettings(t *testing.T) {
	cfg := MySQLConfig{Host: "oracle", Port: 3307, User: "tester", Password: "pw", Database: "golden_1_fs"}
	for _, explicit := range []bool{false, true} {
		if explicit {
			cfg.Charset, cfg.SQLMode = "utf8mb4", "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION"
		}
		dsn := mysqlDSN(cfg)
		if !strings.HasPrefix(dsn, "tester:pw@tcp(oracle:3307)/golden_1_fs?") {
			t.Fatal("connection target changed")
		}
		params, err := url.ParseQuery(strings.SplitN(dsn, "?", 2)[1])
		if err != nil || params.Get("time_zone") != "'+00:00'" {
			t.Fatalf("missing UTC session setting: %v", err)
		}
		if explicit {
			if params.Get("charset") != "utf8mb4" ||
				params.Get("sql_mode") != "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'" {
				t.Fatal("explicit session settings lost")
			}
		} else if params.Has("charset") || params.Has("sql_mode") {
			t.Fatal("legacy connection defaults changed")
		}
	}
}

func TestOpenMySQLAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// OpenMySQL must return before parsing a DSN or attempting a connection.
	engine, err := OpenMySQL(ctx, MySQLConfig{Host: "not-used.invalid", Port: 1})
	if engine != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("engine=%v error=%v", engine, err)
	}
}
