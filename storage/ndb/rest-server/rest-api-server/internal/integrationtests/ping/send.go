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
package ping

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func sendGrpcPingRequest(t testing.TB) {
	conn, err := testclient.InitGRPCConnction()
	if err != nil {
		t.Fatal(err.Error())
	}
	sendGrpcPingRequestWithConnection(t, conn)
}

func sendGrpcPingRequestWithConnection(t testing.TB, connection *grpc.ClientConn) {
	grpcClient := api.NewRonDBRESTClient(connection)
	_, err := grpcClient.Ping(context.Background(), &api.Empty{})
	if err != nil {
		t.Fatal(err)
	}
}

func sendRestPingRequest(t testing.TB) {
	conf := config.GetAll()

	httpClient := testutils.SetupHttpClient(t)

	url := testutils.NewPingURL()
	req, err := http.NewRequest(http.MethodGet, url, nil)

	if err != nil {
		t.Fatal(err)
	}

	if conf.Security.APIKeyParameters.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	respCode := resp.StatusCode
	if respCode != http.StatusOK {
		t.Fatalf("Status code is %d", respCode)
	}
}
