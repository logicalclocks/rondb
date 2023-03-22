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

package servers

import (
	"context"
	"net/http"
	"os"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

/*
This test does not affect any handler in the C layer and just checks whether
our servers are up and running.
*/
func TestPing(t *testing.T) {
	conf := config.GetAll()
	log.InitLogger(conf.Log)

	cleanupTLSCerts := func() {}
	var err error
	if conf.Security.EnableTLS {
		// The server will need this when starting up
		cleanupTLSCerts, err = testutils.CreateAllTLSCerts()
		if err != nil {
			t.Fatal(err)
		}
	}
	defer cleanupTLSCerts()

	newHeap, releaseBuffers, err := heap.New()
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBuffers()

	quit := make(chan os.Signal)
	err, cleanupServers := CreateAndStartDefaultServers(newHeap, quit)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupServers()

	//////////////////////
	//////// gRPC ////////
	//////////////////////

	connection, err := testutils.CreateGrpcConn(&testing.B{}, conf.Security.UseHopsworksAPIKeys, conf.Security.EnableTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	state := connection.GetState()
	t.Log(state.String())

	client := api.NewRonDBRESTClient(connection)

	_, err = client.Ping(context.Background(), &api.Empty{})
	if err != nil {
		t.Fatal(err)
	}

	//////////////////////
	//////// HTTP ////////
	//////////////////////

	httpClient := testutils.SetupHttpClient(t)

	url := testutils.NewPingURL()
	req, err := http.NewRequest(http.MethodGet, url, nil)

	if err != nil {
		t.Fatal(err)
	}

	if conf.Security.UseHopsworksAPIKeys {
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
