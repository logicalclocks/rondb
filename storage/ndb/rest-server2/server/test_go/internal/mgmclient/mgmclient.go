/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// Package mgmclient speaks the RonDB management server text protocol
// (the same protocol the mgmapi C library uses) over a plain TCP
// connection. It implements only the USER rate limit commands
// (RONDB-978): "set user", "alter user", "drop user", "get user".
//
// Wire format per command:
//
//	<command>\n
//	<arg>: <value>\n
//	...
//	\n
//
// Reply:
//
//	<command> reply\n
//	result: Ok\n            (or an error message)
//	...
//	\n
//
// "get user" additionally returns "num_rows: N" and, after the blank line,
// N rows of the form "key = value"; see GetUser.
package mgmclient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// CallTimeout bounds one management-server command. It has to cover a full
// schema transaction: "set user"/"alter user"/"drop user" run an
// ALTER DATABASE through DBDICT, and DICT is single-threaded for schema
// transactions, so a command issued while the cluster is still working off an
// overload burst (as the rate limit tests deliberately create) can take tens
// of seconds even though it eventually succeeds. Timing out and retrying only
// adds another schema transaction to the queue, so wait generously instead.
//
// The management server itself (MgmtSrvr::startSchemaTrans) keeps retrying
// for up to 120 s while DICT reports the schema transaction lock busy, and
// only then runs the transaction. The client deadline sits above that so
// that a timeout here means something is broken, not merely slow.
//
// A command that hits this deadline is NOT cancelled by it: the management
// server keeps waiting for the lock and applies the change whenever DICT gets
// to it. A caller that sees a transport error therefore does not know the
// entity's state and must read it back with GetUser before acting on it (see
// testutils.setRateLimit), and must not reuse the connection, whose late
// reply would otherwise be taken for the reply of the next command.
const CallTimeout = 180 * time.Second

// connectTimeout bounds establishing the TCP connection to the management
// server, which does not depend on any cluster-side work.
const connectTimeout = 10 * time.Second

type Client struct {
	conn net.Conn
	rd   *bufio.Reader
	// broken is set after a transport error. The reply of the failed
	// command may still arrive on the connection, so it cannot be used for
	// another command.
	broken bool
}

// CommandError is a reply whose result is not Ok. It is deliberately distinct
// from a transport error (deadline exceeded, connection reset): a
// CommandError means the management server has finished the command and
// rejected it, whereas after a transport error the command may still be
// running server side (see CallTimeout).
type CommandError struct {
	Cmd       string
	Result    string
	ErrorCode int
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("mgmclient: %q failed: result=%q error_code=%d",
		e.Cmd, e.Result, e.ErrorCode)
}

// errNoSuchUser is the error_code "get user" reports for a USER entity that
// does not exist (DropTableRef::NoSuchTable).
const errNoSuchUser = 709

// IsNoSuchUser reports whether err is the "get user" reply for a USER entity
// that does not exist.
func IsNoSuchUser(err error) bool {
	var cmdErr *CommandError
	return errors.As(err, &cmdErr) && cmdErr.Cmd == "get user" &&
		cmdErr.ErrorCode == errNoSuchUser
}

// UserInfo is what "get user" reports for a USER entity. The management
// server does not report MaxParallelComplexQueries for users, so that limit
// is always 0 here.
type UserInfo struct {
	UserId uint32
	Limits UserLimits
}

// UserLimits mirrors the ndb_mgm_set_user/ndb_mgm_alter_user arguments.
// A value of 0 means no limit for that dimension.
type UserLimits struct {
	RatePerSec                uint32
	MaxTransactionSize        uint32
	MaxParallelTransactions   uint32
	MaxParallelComplexQueries uint32
}

// Connect opens a connection to the management server, e.g. "localhost:13000".
func Connect(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("mgmclient: connect to %s: %w", addr, err)
	}
	return newClient(conn), nil
}

func newClient(conn net.Conn) *Client {
	return &Client{conn: conn, rd: bufio.NewReader(conn)}
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type kv struct {
	key   string
	value string
}

// call sends one command and parses its reply into a key/value map. A
// transport error marks the connection unusable, see Client.broken.
func (c *Client) call(cmd string, args []kv) (map[string]string, error) {
	if c.broken {
		return nil, fmt.Errorf("mgmclient: cannot send %q: connection unusable "+
			"after an earlier transport error", cmd)
	}
	reply, err := c.exchange(cmd, args)
	if err != nil {
		c.broken = true
	}
	return reply, err
}

func (c *Client) exchange(cmd string, args []kv) (map[string]string, error) {
	deadline := time.Now().Add(CallTimeout)
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	var sb strings.Builder
	sb.WriteString(cmd)
	sb.WriteString("\n")
	for _, a := range args {
		sb.WriteString(a.key)
		sb.WriteString(": ")
		sb.WriteString(a.value)
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	if _, err := c.conn.Write([]byte(sb.String())); err != nil {
		return nil, fmt.Errorf("mgmclient: send %q: %w", cmd, err)
	}

	// First reply line must be "<command> reply"
	header, err := c.rd.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("mgmclient: read reply of %q: %w", cmd, err)
	}
	header = strings.TrimRight(header, "\n")
	expected := cmd + " reply"
	if header != expected {
		return nil, fmt.Errorf("mgmclient: unexpected reply header %q for %q", header, cmd)
	}

	reply := make(map[string]string)
	for {
		line, err := c.rd.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("mgmclient: read reply of %q: %w", cmd, err)
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			break
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			return nil, fmt.Errorf("mgmclient: malformed reply line %q for %q", line, cmd)
		}
		reply[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return reply, nil
}

// callChecked runs call() and verifies result == Ok.
func (c *Client) callChecked(cmd string, args []kv) error {
	reply, err := c.call(cmd, args)
	if err != nil {
		return err
	}
	return checkResult(cmd, reply)
}

// checkResult turns a reply whose result is not Ok into a CommandError.
func checkResult(cmd string, reply map[string]string) error {
	if reply["result"] == "Ok" {
		return nil
	}
	code, _ := strconv.Atoi(reply["error_code"])
	return &CommandError{Cmd: cmd, Result: reply["result"], ErrorCode: code}
}

func limitArgs(username string, limits UserLimits) []kv {
	return []kv{
		{"username", username},
		{"rate_per_sec", fmt.Sprintf("%d", limits.RatePerSec)},
		{"max_transaction_size", fmt.Sprintf("%d", limits.MaxTransactionSize)},
		{"max_parallel_transactions", fmt.Sprintf("%d", limits.MaxParallelTransactions)},
		{"max_parallel_complex_queries", fmt.Sprintf("%d", limits.MaxParallelComplexQueries)},
	}
}

// SetUser creates a USER rate limit entity (kernel stores it $-prefixed).
func (c *Client) SetUser(username string, limits UserLimits) error {
	return c.callChecked("set user", limitArgs(username, limits))
}

// AlterUser changes the limits of an existing USER entity.
func (c *Client) AlterUser(username string, limits UserLimits) error {
	return c.callChecked("alter user", limitArgs(username, limits))
}

// DropUser removes a USER entity.
func (c *Client) DropUser(username string) error {
	return c.callChecked("drop user", []kv{{"username", username}})
}

// GetUser reads a USER entity back from DICT. It is a plain lookup, not a
// schema transaction, so it is answered even while a set/alter/drop is queued
// behind the schema transaction lock. IsNoSuchUser(err) identifies a missing
// entity.
//
// The reply carries "num_rows: N" followed by a blank line and then exactly N
// rows of the form "key = value" (the first row is a free-text heading) with
// no terminating blank line. The generic reply parser stops at the blank
// line, so the rows are read here; reading fewer than N would leave them in
// the buffered reader and corrupt the next command on the connection.
func (c *Client) GetUser(username string) (*UserInfo, error) {
	const cmd = "get user"
	reply, err := c.call(cmd, []kv{{"username", username}})
	if err != nil {
		return nil, err
	}
	if err := checkResult(cmd, reply); err != nil {
		return nil, err
	}
	numRows, err := strconv.Atoi(reply["num_rows"])
	if err != nil {
		c.broken = true
		return nil, fmt.Errorf("mgmclient: %q reply without num_rows: %v",
			cmd, reply)
	}
	rows := make(map[string]string, numRows)
	for i := 0; i < numRows; i++ {
		line, err := c.rd.ReadString('\n')
		if err != nil {
			c.broken = true
			return nil, fmt.Errorf("mgmclient: read row %d of %q: %w",
				i, cmd, err)
		}
		key, value, found := strings.Cut(strings.TrimRight(line, "\n"), "=")
		if !found {
			continue // heading: "User rate limits for <name>"
		}
		rows[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	info := &UserInfo{}
	for _, f := range []struct {
		key string
		dst *uint32
	}{
		{"userId", &info.UserId},
		{"RatePerSec", &info.Limits.RatePerSec},
		{"MaxTransactionSize", &info.Limits.MaxTransactionSize},
		{"MaxParallelTransactions", &info.Limits.MaxParallelTransactions},
	} {
		v, err := strconv.ParseUint(rows[f.key], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("mgmclient: %q reply: bad %s %q: %w",
				cmd, f.key, rows[f.key], err)
		}
		*f.dst = uint32(v)
	}
	return info, nil
}
