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
