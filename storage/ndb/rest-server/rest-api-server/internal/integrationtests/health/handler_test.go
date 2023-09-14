/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package health

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func TestHealth(t *testing.T) {

	if config.GetAll().REST.Enable {
		healthHttp := getHealthHttp(t)
		if healthHttp.RonDBHealth != 0 {
			t.Fatalf("Unexpected RonDB health status. Expected: 0 Got: %v", healthHttp.RonDBHealth)
		}
	}

	if config.GetAll().GRPC.Enable {
		healthGRPC := sendGRPCHealthRequest(t)
		if healthGRPC.RonDBHealth != 0 {
			t.Fatalf("Unexpected RonDB health status. Expected: 0 Got: %v", healthGRPC.RonDBHealth)
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

func sendGRPCHealthRequest(t *testing.T) *api.HealthResponse {
	// Create gRPC client
	conn, err := testclient.InitGRPCConnction()
	if err != nil {
		t.Fatal(err.Error())
	}
	client := api.NewRonDBRESTClient(conn)

	// Create Request
	healthRequest := api.HealthRequest{}

	reqProto := api.ConvertHealthRequest(&healthRequest)

	expectedStatus := http.StatusOK
	respCode := 200
	var errStr string
	respProto, err := client.Health(context.Background(), reqProto)
	if err != nil {
		respCode = testclient.GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Received unexpected status; Expected: %d, Got: %d; Complete Error Message: %v ", expectedStatus, respCode, errStr)
	}

	return api.ConvertHealthResponseProto(respProto)
}
