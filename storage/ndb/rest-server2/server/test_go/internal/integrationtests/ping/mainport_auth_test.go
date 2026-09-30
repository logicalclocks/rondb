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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"os"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/version"
)

// The AUTHENTICATED main-port contract, against the mtr rdrs.4.1 instance
// (PingRequiresAuth + HealthRequiresAuth, ProbeEnable=false - the only valid
// way to have authenticated ping/health, since the never-authenticating
// probe port cannot be enabled alongside those flags):
//   - no API key at all: 400 (a missing key is a malformed credential),
//   - well-formed but unknown key: 401,
//   - the hopsworks test key: 200.
//
// This pins that the main-port endpoints still honour the auth flags exactly
// as before the probe port existed.
func TestMainPortAuthenticatedPingAndHealth(t *testing.T) {
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
	if conf.REST.ProbeEnable {
		t.Fatalf("the main-port-auth instance must have ProbeEnable=false")
	}

	client := testutils.SetupHttpClient(t)
	for _, endpoint := range []string{config.PING_OPERATION, config.HEALTH_OPERATION} {
		url := fmt.Sprintf("http://%s/%s/%s",
			net.JoinHostPort(testutils.ConnectHost(conf.REST.ServerIP),
				strconv.Itoa(int(conf.REST.ServerPort))),
			version.API_VERSION, endpoint)
		for _, tc := range []struct {
			name   string
			apiKey string
			want   int
		}{
			{"without key", "", http.StatusBadRequest},
			{"well-formed unknown key",
				strings.Repeat("X", 16) + "." + strings.Repeat("x", 64),
				http.StatusUnauthorized},
			{"with key", testutils.HOPSWORKS_TEST_API_KEY, http.StatusOK},
		} {
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.apiKey != "" {
				req.Header.Set(config.API_KEY_NAME, tc.apiKey)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", endpoint, tc.name, err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Errorf("%s %s: status %d, want %d",
					endpoint, tc.name, resp.StatusCode, tc.want)
			}
		}
	}
}
