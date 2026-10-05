/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package ping

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/version"
)

// REST.MaxKeepaliveRequests: after N requests on one keep-alive connection
// the server closes it, so a Kubernetes Service (which balances per TCP
// connection) can re-balance the client onto another pod. Runs against the
// mtr rdrs.4.1 instance, whose template sets MaxKeepaliveRequests=5:
//   - requests 1..4 on one connection are answered and the connection stays
//     open,
//   - request 5 is answered with "connection: close" and the server then
//     closes the socket,
//   - a fresh connection serves again (the limit is per connection, not a
//     rate limit).
func TestMaxKeepaliveRequestsClosesConnection(t *testing.T) {
	const cap = 5 // must match MaxKeepaliveRequests in the auth template
	cfgPath := os.Getenv("RDRS_CONFIG_FILE_MAINPORT_AUTH")
	if cfgPath == "" {
		t.Skip("RDRS_CONFIG_FILE_MAINPORT_AUTH not set " +
			"(run via mtr --suite=rdrs2-golang)")
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", cfgPath, err)
	}
	var conf config.AllConfigs
	if err := json.Unmarshal(raw, &conf); err != nil {
		t.Fatalf("cannot parse %s: %v", cfgPath, err)
	}
	if conf.REST.MaxKeepaliveRequests != cap {
		t.Fatalf("instance has MaxKeepaliveRequests=%d, test expects %d",
			conf.REST.MaxKeepaliveRequests, cap)
	}

	addr := net.JoinHostPort(testutils.ConnectHost(conf.REST.ServerIP),
		fmt.Sprintf("%d", conf.REST.ServerPort))
	request := fmt.Sprintf(
		"GET /%s/%s HTTP/1.1\r\nHost: %s\r\n%s: %s\r\n\r\n",
		version.API_VERSION, config.PING_OPERATION, addr,
		config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)

	// readResponse consumes one response (the ping body is empty, so headers
	// suffice) and reports its status code and whether the server announced
	// it will close the connection.
	readResponse := func(r *bufio.Reader) (status int, closing bool, err error) {
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, false, err
		}
		if _, err := fmt.Sscanf(line, "HTTP/1.1 %d", &status); err != nil {
			return 0, false, fmt.Errorf("bad status line %q", line)
		}
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return 0, false, err
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				return status, closing, nil
			}
			if strings.EqualFold(line, "connection: close") {
				closing = true
			}
		}
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)

	for i := 1; i <= cap; i++ {
		if _, err := conn.Write([]byte(request)); err != nil {
			t.Fatalf("request %d: write: %v", i, err)
		}
		status, closing, err := readResponse(reader)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if status != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200", i, status)
		}
		if i < cap && closing {
			t.Errorf("request %d of %d already announced connection: close",
				i, cap)
		}
		if i == cap && !closing {
			t.Errorf("request %d (the cap) did not announce connection: close",
				cap)
		}
	}

	// The server must actually close after the capped response.
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Errorf("connection not closed after %d requests (read: %v)", cap, err)
	}

	// And a fresh connection is served normally: a per-connection limit,
	// not a rate limit.
	fresh, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("fresh dial: %v", err)
	}
	defer fresh.Close()
	fresh.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := fresh.Write([]byte(request)); err != nil {
		t.Fatalf("fresh write: %v", err)
	}
	status, _, err := readResponse(bufio.NewReader(fresh))
	if err != nil {
		t.Fatalf("fresh response: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("fresh connection: status %d, want 200", status)
	}

	// Pipelined batches straddling the cap. The cap is per-request, not
	// per-batch: a batch whose requests span the boundary must have every
	// request UP TO the cap answered (only the capped one announcing
	// close), the connection closed after it, and anything pipelined past
	// the cap discarded - per Connection: close semantics the client
	// re-issues those on a new connection.
	for _, tc := range []struct {
		name     string
		preSend  int // sequential requests before the pipelined batch
		batch    int // requests written in ONE write
		expected int // answered responses within the batch
	}{
		// 3 sequential, then 4 and 5 together: both answered, close on 5.
		{"batch ends exactly at the cap", 3, 2, 2},
		// 3 sequential, then 4, 5 and 6 together: 4 and 5 answered (close
		// on 5), 6 discarded.
		{"batch crosses the cap", 3, 3, 2},
		// everything in one batch: 1..5 answered, 6 and 7 discarded.
		{"whole lifetime in one batch", 0, 7, cap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(30 * time.Second))
			reader := bufio.NewReader(conn)

			for i := 1; i <= tc.preSend; i++ {
				if _, err := conn.Write([]byte(request)); err != nil {
					t.Fatalf("pre-send %d: %v", i, err)
				}
				if _, _, err := readResponse(reader); err != nil {
					t.Fatalf("pre-send %d response: %v", i, err)
				}
			}

			if _, err := conn.Write(
				[]byte(strings.Repeat(request, tc.batch))); err != nil {
				t.Fatalf("pipelined write: %v", err)
			}
			for i := 1; i <= tc.expected; i++ {
				ordinal := tc.preSend + i
				status, closing, err := readResponse(reader)
				if err != nil {
					t.Fatalf("pipelined response %d (ordinal %d): %v",
						i, ordinal, err)
				}
				if status != http.StatusOK {
					t.Errorf("ordinal %d: status %d, want 200",
						ordinal, status)
				}
				if closing != (ordinal == cap) {
					t.Errorf("ordinal %d: connection-close announced=%v, "+
						"want %v", ordinal, closing, ordinal == cap)
				}
			}
			// Nothing more than the capped response may arrive; then EOF.
			if _, err := reader.ReadByte(); err != io.EOF {
				t.Errorf("expected EOF after the capped response, got: %v",
					err)
			}
		})
	}
}
