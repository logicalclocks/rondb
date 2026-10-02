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

package health

import (
	"net/http"
	"strconv"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

func TestHealth(t *testing.T) {

	if config.GetAll().REST.Enable {
		healthHttp := getHealthHttp(t)
		if healthHttp.RonDBHealth != 1 {
			t.Fatalf("Unexpected RonDB health status. Expected: 1 Got: %v", healthHttp.RonDBHealth)
		}
	}

}

// Both-port invariant: /health answers 200 with the byte-identical body "1"
// on the main port and on the dedicated probe port, under both API
// versions. The two read the same wait-free state, so on a healthy cluster
// they must agree.
func TestHealthBothPorts(t *testing.T) {
	conf := config.GetAll()
	if !conf.REST.Enable {
		t.Skip("REST disabled")
	}
	urls := map[string]string{
		"main":    testutils.NewHealthURL(),
		"main v2": testutils.NewHealthURLV2(),
	}
	if conf.REST.ProbeEnable {
		urls["probe"] = testutils.NewProbeHealthURL()
		urls["probe v2"] = testutils.NewProbeHealthURLV2()
	}
	for name, url := range urls {
		_, respBody := testclient.SendHttpRequest(t, config.HEALTH_HTTP_VERB,
			url, "", "", http.StatusOK)
		if string(respBody) != "1" {
			t.Fatalf("%s port health: body %q, want \"1\"", name, string(respBody))
		}
	}
}

func getHealthHttp(t *testing.T) *api.HealthResponse {
	body := ""
	url := testutils.NewHealthURL()
	_, respBody := testclient.SendHttpRequest(t, config.HEALTH_HTTP_VERB, url, string(body),
		"", http.StatusOK)

	var health api.HealthResponse
	healthInt, err := strconv.Atoi(string(respBody))
	if err != nil {
		t.Fatalf("%v", err)
	}
	health.RonDBHealth = healthInt
	return &health
}
