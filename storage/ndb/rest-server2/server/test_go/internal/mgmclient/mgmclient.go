/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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
package mgmclient

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

const callTimeout = 10 * time.Second

type Client struct {
	conn net.Conn
	rd   *bufio.Reader
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
	conn, err := net.DialTimeout("tcp", addr, callTimeout)
	if err != nil {
		return nil, fmt.Errorf("mgmclient: connect to %s: %w", addr, err)
	}
	return &Client{conn: conn, rd: bufio.NewReader(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type kv struct {
	key   string
	value string
}

// call sends one command and parses its reply into a key/value map.
func (c *Client) call(cmd string, args []kv) (map[string]string, error) {
	deadline := time.Now().Add(callTimeout)
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
	if result, ok := reply["result"]; !ok || result != "Ok" {
		return fmt.Errorf("mgmclient: %q failed: result=%q error_code=%q",
			cmd, reply["result"], reply["error_code"])
	}
	return nil
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

// GetUser returns the reply map of "get user" for inspection.
func (c *Client) GetUser(username string) (map[string]string, error) {
	return c.call("get user", []kv{{"username", username}})
}
