/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

package mgmclient

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"testing"
)

// fakeMgmd answers each command on the server end of a pipe with the next
// canned reply, and records the commands it received.
type fakeMgmd struct {
	conn    net.Conn
	replies []string
	got     chan string
}

func startFakeMgmd(t *testing.T, replies ...string) (*Client, *fakeMgmd) {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	f := &fakeMgmd{conn: serverEnd, replies: replies,
		got: make(chan string, len(replies))}
	go f.serve()
	t.Cleanup(func() { clientEnd.Close(); serverEnd.Close() })
	return newClient(clientEnd), f
}

func (f *fakeMgmd) serve() {
	rd := bufio.NewReader(f.conn)
	for _, reply := range f.replies {
		var cmd strings.Builder
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			cmd.WriteString(line)
			if line == "\n" {
				break
			}
		}
		f.got <- cmd.String()
		if _, err := f.conn.Write([]byte(reply)); err != nil {
			return
		}
	}
	f.conn.Close()
}

const getUserOkReply = "get user reply\n" +
	"result: Ok\n" +
	"num_rows: 6\n" +
	"\n" +
	"User rate limits for $abcdef\n" +
	"userId = 5\n" +
	"userVersion = 5\n" +
	"RatePerSec = 1000000\n" +
	"MaxTransactionSize = 7\n" +
	"MaxParallelTransactions = 3\n"

// The rows of a "get user" reply follow the blank line that ends the generic
// reply and must be consumed, or they are taken for the next command's reply.
func TestGetUserConsumesRowsSoNextCommandWorks(t *testing.T) {
	c, f := startFakeMgmd(t, getUserOkReply,
		"drop user reply\nresult: Ok\n\n")

	info, err := c.GetUser("$abcdef")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got := <-f.got; got != "get user\nusername: $abcdef\n\n" {
		t.Fatalf("unexpected command sent: %q", got)
	}
	want := UserInfo{UserId: 5, Limits: UserLimits{RatePerSec: 1000000,
		MaxTransactionSize: 7, MaxParallelTransactions: 3}}
	if *info != want {
		t.Fatalf("GetUser parsed %+v, want %+v", *info, want)
	}

	if err := c.DropUser("$abcdef"); err != nil {
		t.Fatalf("DropUser after GetUser on the same connection: %v", err)
	}
}

func TestGetUserNoSuchUserIsCommandError(t *testing.T) {
	c, _ := startFakeMgmd(t,
		"get user reply\nresult: No such user exists\nerror_code: 709\n\n")

	_, err := c.GetUser("$nobody")
	if !IsNoSuchUser(err) {
		t.Fatalf("expected no-such-user error, got %v", err)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) || cmdErr.Result != "No such user exists" {
		t.Fatalf("expected CommandError with the server's result, got %v", err)
	}
}

func TestRejectedCommandIsCommandErrorNotTransportError(t *testing.T) {
	c, _ := startFakeMgmd(t,
		"alter user reply\nresult: Alter User failed\nerror_code: 709\n\n",
		"set user reply\nresult: Ok\n\n")

	err := c.AlterUser("$abcdef", UserLimits{RatePerSec: 1})
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) || cmdErr.Cmd != "alter user" ||
		cmdErr.ErrorCode != 709 {
		t.Fatalf("expected CommandError for alter user, got %v", err)
	}
	// The server answered, so the connection is still usable.
	if err := c.SetUser("$abcdef", UserLimits{RatePerSec: 1}); err != nil {
		t.Fatalf("SetUser after a rejected AlterUser: %v", err)
	}
}

// After a transport error the reply of the failed command may still arrive,
// so the connection must refuse further commands rather than hand that reply
// to the next one.
func TestTransportErrorMarksConnectionUnusable(t *testing.T) {
	c, _ := startFakeMgmd(t) // closes without answering anything

	err := c.AlterUser("$abcdef", UserLimits{RatePerSec: 1})
	var cmdErr *CommandError
	if err == nil || errors.As(err, &cmdErr) {
		t.Fatalf("expected a transport error, got %v", err)
	}
	if _, err := c.GetUser("$abcdef"); err == nil ||
		!strings.Contains(err.Error(), "unusable") {
		t.Fatalf("expected the connection to be refused, got %v", err)
	}
}
