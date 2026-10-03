/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package ping

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
)

func TestPing(t *testing.T) {
	// HTTP
	if config.GetAll().REST.Enable {
		sendRestPingRequest(t)
	}
}

// Both-port invariant: /ping answers 200 with an EMPTY body on the main
// port and on the dedicated probe port, byte-identically, under both API
// versions.
//
// The empty-body assertion is deliberate on the main port too: it pins that
// PingCtrl serves /ping (empty 200, honours PingRequiresAuth). Historically
// a second controller (BaseCtrl, "Hello, World!", no auth) registered the
// same route and which one won was decided by unspecified static-init
// order; BaseCtrl was also the only handler of /0.2.0/ping. BaseCtrl is
// removed, and this test keeps both routes pinned to PingCtrl.
func TestPingBothPorts(t *testing.T) {
	conf := config.GetAll()
	if !conf.REST.Enable {
		t.Skip("REST disabled")
	}
	client := testutils.SetupHttpClient(t)

	urls := map[string]string{
		"main":    testutils.NewPingURL(),
		"main v2": testutils.NewPingURLV2(),
	}
	if conf.REST.ProbeEnable {
		urls["probe"] = testutils.NewProbePingURL()
		urls["probe v2"] = testutils.NewProbePingURLV2()
	}
	for name, url := range urls {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if conf.Security.APIKey.UseHopsworksAPIKeys &&
			strings.HasPrefix(name, "main") {
			// Only the main port authenticates; the probe port never does.
			req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s port ping (%s): %v", name, url, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%s port ping body: %v", name, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s port ping: status %d, want 200", name, resp.StatusCode)
		}
		if len(body) != 0 {
			t.Fatalf("%s port ping: body %q, want empty", name, string(body))
		}
	}
}
