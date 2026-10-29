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
package testutils

import "testing"

// The parser against a literal scrape; needs no server.
func TestParseMetricsText(t *testing.T) {
	text := `# HELP rdrs_buffer_pool Request/response buffer pool state, in buffers
# TYPE rdrs_buffer_pool gauge
rdrs_buffer_pool{pool="request",state="in_use"} 3
rdrs_buffer_pool{pool="request",state="free"} 5
rdrs_buffer_pool{pool="response",state="in_use"} 1.000000
rdrs_buffer_pool{pool="response",state="allocated"} 8
rdrs_buffer_pool_events{pool="request",event="rejected"} 2
rdrs_limits{name="max_columns"} 512
rdrs_limits{name="memory_limit_bytes"} 38654705664
http_request_duration_seconds_bucket{le="0.005"} 10
not a sample line
`
	m := parseMetricsText(text)
	if got := m.Get(t, "rdrs_buffer_pool", map[string]string{"pool": "request", "state": "in_use"}); got != 3 {
		t.Fatalf("request in_use: got %d", got)
	}
	if got := m.Get(t, "rdrs_buffer_pool", map[string]string{"pool": "response", "state": "in_use"}); got != 1 {
		t.Fatalf("response in_use: got %d", got)
	}
	if got := m.Get(t, "rdrs_buffer_pool_events", map[string]string{"pool": "request", "event": "rejected"}); got != 2 {
		t.Fatalf("rejected: got %d", got)
	}
	if got := m.Get(t, "rdrs_limits", map[string]string{"name": "memory_limit_bytes"}); got != 38654705664 {
		t.Fatalf("memory limit: got %d", got)
	}
	if m.Has("rdrs_buffer_pool", map[string]string{"pool": "response", "state": "free"}) {
		t.Fatalf("response free should be absent")
	}
	if !m.Has("http_request_duration_seconds_bucket", map[string]string{"le": "0.005"}) {
		t.Fatalf("histogram bucket should parse")
	}
}
