/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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

package stat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestStat(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"

	ch := make(chan int)

	numOps := uint32(5)
	expectedAllocations := numOps * 2

	conf := config.GetAll()

	preAllocatedBuffers := conf.Internal.PreAllocatedBuffers
	if preAllocatedBuffers > numOps {
		expectedAllocations = preAllocatedBuffers
	}

	integrationtests.WithDBs(t, []string{db},
		func(tc testutils.TlsContext) {
			for i := uint32(0); i < numOps; i++ {
				go performPkOp(t, tc, db, table, ch)
			}
			for i := uint32(0); i < numOps; i++ {
				<-ch
			}

			// get stats
			statsHttp := getStatsHttp(t, tc)
			compare(t, statsHttp, int64(expectedAllocations), int64(numOps))

			statsGRPC := getStatsGRPC(t, tc)
			compare(t, statsGRPC, int64(expectedAllocations), int64(numOps))
		})
}

func compare(t *testing.T, stats *api.StatResponse, expectedAllocations int64, numOps int64) {
	if stats.MemoryStats.AllocationsCount != expectedAllocations ||
		stats.MemoryStats.BuffersCount != expectedAllocations ||
		stats.MemoryStats.FreeBuffers != expectedAllocations {
		t.Fatalf("Native buffer stats do not match Got: %v", stats)
	}

	if stats.RonDBStats.NdbObjectsCreationCount != numOps ||
		stats.RonDBStats.NdbObjectsTotalCount != numOps ||
		stats.RonDBStats.NdbObjectsFreeCount != numOps {
		t.Fatalf("RonDB stats do not match. %#v", stats.RonDBStats)
	}
}

func performPkOp(t *testing.T, tc testutils.TlsContext, db string, table string, ch chan int) {
	param := api.PKReadBody{
		Filters:     integrationtests.NewFiltersKVs("id0", 0, "id1", 0),
		ReadColumns: integrationtests.NewReadColumn("col0"),
	}
	body, _ := json.MarshalIndent(param, "", "\t")

	url := testutils.NewPKReadURL(db, table)
	integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusOK, "")

	ch <- 0
}

func getStatsHttp(t *testing.T, tc testutils.TlsContext) *api.StatResponse {
	body := ""
	url := testutils.NewStatURL()
	_, respBody := integrationtests.SendHttpRequest(t, tc, config.STAT_HTTP_VERB, url, string(body), http.StatusOK, "")

	var stats api.StatResponse
	err := json.Unmarshal([]byte(respBody), &stats)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return &stats
}

func getStatsGRPC(t *testing.T, tc testutils.TlsContext) *api.StatResponse {
	stats := sendGRPCStatRequest(t, tc)
	return stats
}

func sendGRPCStatRequest(t *testing.T, tc testutils.TlsContext) *api.StatResponse {
	// Create gRPC client
	conf := config.GetAll()
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d",
		conf.GRPC.ServerIP,
		conf.GRPC.ServerPort),
		grpc.WithTransportCredentials(credentials.NewTLS(testutils.GetClientTLSConfig(t, tc))))
	defer conn.Close()

	if err != nil {
		t.Fatalf("Failed to connect to server %v", err)
	}
	client := api.NewRonDBRESTClient(conn)

	// Create Request
	statRequest := api.StatRequest{}

	reqProto := api.ConvertStatRequest(&statRequest)

	expectedStatus := http.StatusOK
	respCode := 200
	var errStr string
	respProto, err := client.Stat(context.Background(), reqProto)
	if err != nil {
		respCode = integrationtests.GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Received unexpected status; Expected: %d, Got: %d; Complete Error Message: %v ", expectedStatus, respCode, errStr)
	}

	return api.ConvertStatResponseProto(respProto)
}
