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
	"context"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

func sendGrpcPingRequestWithConnection(t testing.TB, connection *grpc.ClientConn) {
	grpcClient := api.NewRonDBRESTClient(connection)
	_, err := grpcClient.Ping(context.Background(), &api.Empty{})
	if err != nil {
		t.Fatal(err)
	}
}

func sendRestPingRequest(
	t testing.TB,
) {
	client := testutils.SetupHttpClient(t)
	sendRestPingRequestWithClient(t, client)
}

func sendRestPingRequestWithClient(t testing.TB, client *http.Client) {
	conf := config.GetAll()

	url := testutils.NewPingURL()
	req, err := http.NewRequest(http.MethodGet, url, nil)

	if err != nil {
		t.Fatal(err)
		fmt.Printf("Err: NewPingURL: %s, err: %s\n", url, err)
	}

	if conf.Security.APIKey.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
		fmt.Println("Failed Send Ping")
	}
	defer resp.Body.Close()

	respCode := resp.StatusCode
	if respCode != http.StatusOK {
		t.Fatalf("Status code is %d", respCode)
		fmt.Println("Failed Recv Ping")
	}
}
