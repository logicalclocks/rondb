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

// Package exec runs statements on the engines the framework compares —
// RonSQL through RDRS (/ronsql, JSON output), RonSQL through ronsql_cli,
// and MySQL — and classifies the outcome (framework_design.md §6,
// random_generator.md §3).  Results keep explicit nullness and the exact
// value tokens; no float64 intermediate touches an integer or decimal.
package exec

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	oexec "os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Cell is one result value.
type Cell struct {
	Null bool
	Text string
}

// Result is a decoded result set.
type Result struct {
	Columns []string
	Rows    [][]Cell
	// Types holds MySQL's DatabaseTypeName per column (empty for RonSQL).
	Types []string
	Raw   string
	// Latency is the wall-clock time of the request.
	Latency time.Duration
	// Phases is the parsed x-ronsql-phases header (RDRS only).
	Phases map[string]int64
	// HTTPStatus is the RDRS status code (0 elsewhere).
	HTTPStatus int
}

// Outcome classifies a statement execution.
type Outcome int

const (
	// OK: the engine returned a result set.
	OK Outcome = iota
	// CleanReject: RonSQL refused the statement with a permanent error.
	CleanReject
	// Retryable: RonSQL reported a retryable error after its retries (or RDRS rate-limited).
	Retryable
	// Timeout: the request exceeded the deadline and the engine still answers probes.
	Timeout
	// Crash: the engine stopped answering (transport failure + failed probe).
	Crash
	// Error: any other failure (MySQL error, HTTP 400, malformed output).
	Error
)

func (o Outcome) String() string {
	switch o {
	case OK:
		return "OK"
	case CleanReject:
		return "CLEAN-REJECT"
	case Retryable:
		return "RETRY-EXHAUSTED"
	case Timeout:
		return "TIMEOUT"
	case Crash:
		return "CRASH"
	}
	return "ERROR"
}

// Response is the outcome of one statement.
type Response struct {
	Result  *Result
	Outcome Outcome
	Message string
}

// Engine is one execution target.
type Engine interface {
	Name() string
	Query(ctx context.Context, sql string) Response
	// Header returns the output column names of a statement that may
	// return no rows (JSON carries none): a TEXT request on RDRS/CLI, the
	// column metadata on MySQL.
	Header(ctx context.Context, sql string) ([]string, error)
	Explain(ctx context.Context, sql string) (string, error)
	Probe(ctx context.Context) error
	Close() error
}

// ---- RDRS ---------------------------------------------------------------------

// RDRS runs statements through POST /<version>/ronsql.
type RDRS struct {
	base     string
	apiKey   string
	database string
	client   *http.Client
	version  string
	probeSQL string
}

// NewRDRS creates an RDRS engine; probeSQL is a cheap statement used to
// tell a crash from a timeout (e.g. "SELECT COUNT(*) FROM countries_1;").
func NewRDRS(host string, port int, useTLS bool, apiKey, database, apiVersion, probeSQL string, timeout time.Duration) *RDRS {
	scheme := "http"
	transport := &http.Transport{}
	if useTLS {
		scheme = "https"
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	}
	if apiVersion == "" {
		apiVersion = "0.2.0"
	}
	return &RDRS{
		base: fmt.Sprintf("%s://%s:%d", scheme, host, port), apiKey: apiKey, database: database,
		client: &http.Client{Transport: transport, Timeout: timeout}, version: apiVersion, probeSQL: probeSQL,
	}
}

func (r *RDRS) Name() string { return "rdrs" }

type ronsqlRequest struct {
	Query        string `json:"query"`
	Database     string `json:"database,omitempty"`
	ExplainMode  string `json:"explainMode,omitempty"`
	OutputFormat string `json:"outputFormat,omitempty"`
}

func (r *RDRS) post(ctx context.Context, req ronsqlRequest) (status int, body []byte, phases string, latency time.Duration, err error) {
	payload, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+"/"+r.version+"/ronsql", bytes.NewReader(payload))
	if err != nil {
		return 0, nil, "", 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if r.apiKey != "" {
		httpReq.Header.Set("X-API-Key", r.apiKey)
	}
	start := time.Now()
	resp, err := r.client.Do(httpReq)
	latency = time.Since(start)
	if err != nil {
		return 0, nil, "", latency, err
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	return resp.StatusCode, body, resp.Header.Get("x-ronsql-phases"), latency, err
}

// ParsePhases parses "parse=12,analyze=3,...,rows=5,attempts=1".
func ParsePhases(header string) map[string]int64 {
	if header == "" {
		return nil
	}
	out := map[string]int64{}
	for _, kv := range strings.Split(header, ",") {
		if i := strings.IndexByte(kv, '='); i > 0 {
			if v, err := strconv.ParseInt(strings.TrimSpace(kv[i+1:]), 10, 64); err == nil {
				out[strings.TrimSpace(kv[:i])] = v
			}
		}
	}
	return out
}

// transientNDBErrors are NDB dictionary conditions RDRS reports as a
// permanent RonSQL error although they clear on the next attempt: right
// after DDL the RDRS NDB API dictionary cache can lag (MTR runs create the
// tables moments before the first request; E8 flake control).
var transientNDBErrors = []string{
	"Schema cache for table not up to date",
	"Invalid schema object version",
	"Table definition has changed",
}

// Classify maps an RDRS response to an outcome (ronsql_ctrl.cpp /
// ronsql_operation.cpp: every RonSQL error is HTTP 500 with a text body;
// "Caught exception:" = permanent, "RonSQLRetryableError" = retryable;
// 429 = rate limited; 400 = request validation).  Transient NDB dictionary
// errors classify as Retryable whatever the body's framing.
func Classify(status int, body string) (Outcome, string) {
	msg := strings.TrimSpace(body)
	switch {
	case status == http.StatusOK:
		return OK, ""
	case status == http.StatusTooManyRequests:
		return Retryable, msg
	case status == http.StatusInternalServerError && strings.Contains(msg, "RonSQLRetryableError"):
		return Retryable, msg
	case status == http.StatusInternalServerError && isTransientNDB(msg):
		return Retryable, msg
	case status == http.StatusInternalServerError && strings.Contains(msg, "Caught exception:"):
		return CleanReject, msg
	default:
		return Error, fmt.Sprintf("HTTP %d: %s", status, msg)
	}
}

func isTransientNDB(msg string) bool {
	for _, p := range transientNDBErrors {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

func (r *RDRS) Query(ctx context.Context, sqlText string) Response {
	const attempts = 4
	var last Response
	for i := 0; i < attempts; i++ {
		status, body, phases, latency, err := r.post(ctx, ronsqlRequest{Query: sqlText, Database: r.database, ExplainMode: "ALLOW", OutputFormat: "JSON"})
		if err != nil {
			out := Timeout
			if !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err) {
				out = Error
			}
			if r.Probe(context.Background()) != nil {
				out = Crash
			}
			return Response{Outcome: out, Message: err.Error(), Result: &Result{Latency: latency}}
		}
		outcome, msg := Classify(status, string(body))
		res := &Result{Raw: string(body), Latency: latency, Phases: ParsePhases(phases), HTTPStatus: status}
		if outcome == OK {
			cols, rows, perr := ParseJSONData(body)
			if perr != nil {
				return Response{Outcome: Error, Message: "malformed JSON result: " + jsonErrDetail([]byte(res.Raw), perr), Result: res}
			}
			res.Columns, res.Rows = cols, rows
			return Response{Outcome: OK, Result: res}
		}
		last = Response{Outcome: outcome, Message: msg, Result: res}
		if outcome != Retryable {
			return last
		}
		// back off longer for a dictionary refresh than for a plain retry
		delay := 50 * time.Millisecond
		if isTransientNDB(msg) {
			delay = 250 * time.Millisecond * time.Duration(i+1)
		}
		time.Sleep(delay)
	}
	return last
}

func isTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	return errors.As(err, &ne) && ne.Timeout()
}

// Header runs the statement with TEXT output and returns the header line.
func (r *RDRS) Header(ctx context.Context, sqlText string) ([]string, error) {
	status, body, _, _, err := r.post(ctx, ronsqlRequest{Query: sqlText, Database: r.database, ExplainMode: "ALLOW", OutputFormat: "TEXT"})
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	return ParseTextHeader(string(body)), nil
}

// Explain returns the RonSQL EXPLAIN output (explainMode FORCE).
func (r *RDRS) Explain(ctx context.Context, sqlText string) (string, error) {
	status, body, _, _, err := r.post(ctx, ronsqlRequest{Query: sqlText, Database: r.database, ExplainMode: "FORCE", OutputFormat: "TEXT"})
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// Probe runs the probe statement with a short deadline.
func (r *RDRS) Probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	status, body, _, _, err := r.post(ctx, ronsqlRequest{Query: r.probeSQL, Database: r.database, ExplainMode: "ALLOW", OutputFormat: "TEXT"})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("probe HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (r *RDRS) Close() error { r.client.CloseIdleConnections(); return nil }

// ParseTextHeader returns the tab-separated first line of a TEXT result.
func ParseTextHeader(text string) []string {
	line := text
	if i := strings.IndexByte(text, '\n'); i >= 0 {
		line = text[:i]
	}
	if strings.TrimSpace(line) == "" {
		return nil
	}
	return strings.Split(line, "\t")
}

// ParseJSONData decodes {"data":[{...},...]} (RDRS) or a bare [...] (CLI),
// keeping the output-name order of the first row, exact numeric tokens
// and explicit nulls.  Duplicate keys in a row object are an error (a
// duplicate output alias would silently overwrite a value).
// jsonErrDetail appends the text around a syntax error's offset so an
// engine-side formatting bug (F9: an unquoted temporal MIN/MAX in JSON
// output) is visible in the CASE line.
func jsonErrDetail(body []byte, err error) string {
	var se *json.SyntaxError
	if !errors.As(err, &se) {
		return err.Error()
	}
	lo, hi := int(se.Offset)-40, int(se.Offset)+40
	if lo < 0 {
		lo = 0
	}
	if hi > len(body) {
		hi = len(body)
	}
	return fmt.Sprintf("%s near %q", err.Error(), body[lo:hi])
}

func ParseJSONData(body []byte) ([]string, [][]Cell, error) {
	// Validate the whole document before extracting rows. Token decoding
	// alone can otherwise accept a truncated wrapper or trailing garbage.
	// RawMessage preserves numeric tokens and SyntaxError offsets (F9).
	var document json.RawMessage
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(document))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, err
	}
	if tok == json.Delim('{') {
		// Only a top-level data member carries rows; metadata may contain
		// the string "data" or nested members with that name.
		var data json.RawMessage
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, nil, err
			}
			var value json.RawMessage
			if err := dec.Decode(&value); err != nil {
				return nil, nil, err
			}
			if key == "data" {
				if data != nil {
					return nil, nil, fmt.Errorf("duplicate data member")
				}
				data = value
			}
		}
		if _, err := dec.Token(); err != nil { // '}'
			return nil, nil, err
		}
		if data == nil {
			return nil, nil, fmt.Errorf("missing top-level data member")
		}
		dec = json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		tok, err = dec.Token()
		if err != nil {
			return nil, nil, err
		}
	}
	if tok != json.Delim('[') {
		return nil, nil, fmt.Errorf("expected an array of rows")
	}
	var columns []string
	var rows [][]Cell
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, nil, err
		}
		if t != json.Delim('{') {
			return nil, nil, fmt.Errorf("expected a row object")
		}
		var names []string
		var cells []Cell
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return nil, nil, err
			}
			key := kt.(string)
			if seen[key] {
				return nil, nil, fmt.Errorf("duplicate output name %q in a row", key)
			}
			seen[key] = true
			vt, err := dec.Token()
			if err != nil {
				return nil, nil, err
			}
			cell := Cell{}
			switch v := vt.(type) {
			case nil:
				cell.Null = true
			case json.Number:
				cell.Text = v.String()
			case string:
				cell.Text = v
			case bool:
				if v {
					cell.Text = "1"
				} else {
					cell.Text = "0"
				}
			default:
				return nil, nil, fmt.Errorf("unsupported JSON value for %q", key)
			}
			names = append(names, key)
			cells = append(cells, cell)
		}
		if _, err := dec.Token(); err != nil { // '}'
			return nil, nil, err
		}
		if columns == nil {
			columns = names
		} else if strings.Join(columns, "\x00") != strings.Join(names, "\x00") {
			return nil, nil, fmt.Errorf("row output names differ from the first row")
		}
		rows = append(rows, cells)
	}
	if _, err := dec.Token(); err != nil { // ']'
		return nil, nil, err
	}
	return columns, rows, nil
}

// ---- MySQL --------------------------------------------------------------------

// MySQL runs statements on mysqld (the oracle).
type MySQL struct {
	db       *sql.DB
	database string
}

var tlsOnce sync.Once

// MySQLConfig specifies one oracle connection. Empty Charset/SQLMode retain
// server/driver defaults; time_zone is always UTC, including on reconnect.
type MySQLConfig struct {
	Host, User, Password string
	Port                 int
	TLS                  bool
	Database             string
	Charset, SQLMode     string
}

// NewMySQL preserves the existing constructor and its connection defaults.
func NewMySQL(host string, port int, user, password string, useTLS bool, database string) (*MySQL, error) {
	return OpenMySQL(context.Background(), MySQLConfig{
		Host: host, Port: port, User: user, Password: password, TLS: useTLS, Database: database,
	})
}

// OpenMySQL honors cancellation during connection setup. Session settings
// belong in the DSN so a replacement connection receives the same settings.
func OpenMySQL(ctx context.Context, cfg MySQLConfig) (*MySQL, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", mysqlDSN(cfg))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &MySQL{db: db, database: cfg.Database}, nil
}

func mysqlDSN(cfg MySQLConfig) string {
	params := url.Values{}
	params.Set("time_zone", "'+00:00'")
	if cfg.Charset != "" {
		params.Set("charset", cfg.Charset)
	}
	if cfg.SQLMode != "" {
		params.Set("sql_mode", "'"+strings.ReplaceAll(cfg.SQLMode, "'", "''")+"'")
	}
	if cfg.TLS {
		tlsOnce.Do(func() {
			_ = mysql.RegisterTLSConfig("fsqexec", &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})
		})
		params.Set("tls", "fsqexec")
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, params.Encode())
}

func (m *MySQL) Name() string { return "mysql" }

func (m *MySQL) Query(ctx context.Context, sqlText string) Response {
	start := time.Now()
	rows, err := m.db.QueryContext(ctx, sqlText)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return Response{Outcome: Timeout, Message: err.Error()}
		}
		return Response{Outcome: Error, Message: err.Error()}
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return Response{Outcome: Error, Message: err.Error()}
	}
	types, _ := rows.ColumnTypes()
	res := &Result{Columns: cols}
	for _, t := range types {
		res.Types = append(res.Types, t.DatabaseTypeName())
	}
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Response{Outcome: Error, Message: err.Error()}
		}
		row := make([]Cell, len(cols))
		for i, b := range raw {
			if b == nil {
				row[i] = Cell{Null: true}
			} else {
				row[i] = Cell{Text: string(b)}
			}
		}
		res.Rows = append(res.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return Response{Outcome: Error, Message: err.Error()}
	}
	res.Latency = time.Since(start)
	return Response{Outcome: OK, Result: res}
}

func (m *MySQL) Header(ctx context.Context, sqlText string) ([]string, error) {
	r := m.Query(ctx, sqlText)
	if r.Outcome != OK {
		return nil, errors.New(r.Message)
	}
	return r.Result.Columns, nil
}

func (m *MySQL) Explain(ctx context.Context, sqlText string) (string, error) {
	r := m.Query(ctx, "EXPLAIN FORMAT=TREE "+strings.TrimSuffix(strings.TrimSpace(sqlText), ";"))
	if r.Outcome != OK {
		return "", errors.New(r.Message)
	}
	var b strings.Builder
	for _, row := range r.Result.Rows {
		for _, c := range row {
			b.WriteString(c.Text)
		}
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func (m *MySQL) Probe(ctx context.Context) error { return m.db.PingContext(ctx) }
func (m *MySQL) Close() error                    { return m.db.Close() }

// ---- ronsql_cli ----------------------------------------------------------------

// CLI runs statements through ronsql_cli, one process per statement (a
// crash kills only that process).
type CLI struct {
	path, connectString, database string
	timeout                       time.Duration
}

// NewCLI creates a ronsql_cli engine.
func NewCLI(path, connectString, database string, timeout time.Duration) *CLI {
	return &CLI{path: path, connectString: connectString, database: database, timeout: timeout}
}

func (c *CLI) Name() string { return "cli" }

func (c *CLI) run(ctx context.Context, sqlText, format, explain string) (stdout, stderr string, exit int, signaled bool, latency time.Duration, err error) {
	f, err := os.CreateTemp("", "fsq-cli-*.sql")
	if err != nil {
		return "", "", 0, false, 0, err
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(sqlText); err != nil {
		f.Close()
		return "", "", 0, false, 0, err
	}
	f.Close()
	args := []string{"--connect-string", c.connectString, "-D", c.database, "--output-format", format, "--execute-file", f.Name()}
	if explain != "" {
		args = append(args, "--explain-mode", explain)
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	cmd := oexec.CommandContext(ctx, c.path, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	start := time.Now()
	runErr := cmd.Run()
	latency = time.Since(start)
	if runErr != nil {
		// CommandContext kills the child on cancellation. Preserve the
		// inner context error before interpreting that signal as a crash.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out.String(), errb.String(), -1, false, latency, ctxErr
		}
		var ee *oexec.ExitError
		if errors.As(runErr, &ee) {
			if ee.ProcessState != nil && ee.ProcessState.ExitCode() < 0 {
				return out.String(), errb.String(), -1, true, latency, nil
			}
			return out.String(), errb.String(), ee.ExitCode(), false, latency, nil
		}
		return out.String(), errb.String(), -1, false, latency, runErr
	}
	return out.String(), errb.String(), 0, false, latency, nil
}

func (c *CLI) Query(ctx context.Context, sqlText string) Response {
	stdout, stderr, exit, signaled, latency, err := c.run(ctx, sqlText, "JSON", "")
	res := &Result{Raw: stdout, Latency: latency}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return Response{Outcome: Timeout, Message: err.Error(), Result: res}
		}
		return Response{Outcome: Error, Message: err.Error(), Result: res}
	}
	switch {
	case signaled:
		return Response{Outcome: Crash, Message: strings.TrimSpace(stdout + "\n" + stderr), Result: res}
	case exit == 1:
		return Response{Outcome: CleanReject, Message: strings.TrimSpace(stdout + "\n" + stderr), Result: res}
	case exit == 3:
		return Response{Outcome: Retryable, Message: strings.TrimSpace(stdout + "\n" + stderr), Result: res}
	case exit != 0:
		return Response{Outcome: Error, Message: fmt.Sprintf("exit %d: %s", exit, strings.TrimSpace(stdout+"\n"+stderr)), Result: res}
	}
	cols, rows, perr := ParseJSONData([]byte(stdout))
	if perr != nil {
		return Response{Outcome: Error, Message: "malformed JSON result: " + jsonErrDetail([]byte(res.Raw), perr), Result: res}
	}
	res.Columns, res.Rows = cols, rows
	return Response{Outcome: OK, Result: res}
}

func (c *CLI) Header(ctx context.Context, sqlText string) ([]string, error) {
	stdout, stderr, exit, _, _, err := c.run(ctx, sqlText, "TEXT", "")
	if err != nil {
		return nil, err
	}
	if exit != 0 {
		return nil, fmt.Errorf("exit %d: %s", exit, strings.TrimSpace(stdout+"\n"+stderr))
	}
	return ParseTextHeader(stdout), nil
}

func (c *CLI) Explain(ctx context.Context, sqlText string) (string, error) {
	stdout, stderr, exit, _, _, err := c.run(ctx, sqlText, "TEXT", "FORCE")
	if err != nil {
		return "", err
	}
	if exit != 0 {
		return "", fmt.Errorf("exit %d: %s", exit, strings.TrimSpace(stdout+"\n"+stderr))
	}
	return stdout, nil
}

func (c *CLI) Probe(ctx context.Context) error {
	if _, err := os.Stat(c.path); err != nil {
		return err
	}
	return nil
}

func (c *CLI) Close() error { return nil }
