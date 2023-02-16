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
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	tu "hopsworks.ai/rdrs/internal/handlers/utils"
	"hopsworks.ai/rdrs/pkg/api"
)

func TestStat(t *testing.T) {

	db := "db004"
	table := "int_table"

	ch := make(chan int)

	numOps := uint32(5)
	expectedAllocations := numOps * 2

	if config.Configuration().RestServer.PreAllocatedBuffers > numOps {
		expectedAllocations = config.Configuration().RestServer.PreAllocatedBuffers
	}

	tu.WithDBs(t, []string{db},
		getStatHandlers(), func(tc common.TestContext) {
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

func performPkOp(t *testing.T, tc common.TestContext, db string, table string, ch chan int) {
	param := api.PKReadBody{
		Filters:     tu.NewFiltersKVs("id0", 0, "id1", 0),
		ReadColumns: tu.NewReadColumn("col0"),
	}
	body, _ := json.MarshalIndent(param, "", "\t")

	url := tu.NewPKReadURL(db, table)
	tu.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusOK, "")

	ch <- 0
}

func getStatsHttp(t *testing.T, tc common.TestContext) *api.StatResponse {
	body := ""
	url := tu.NewStatURL()
	_, respBody := tu.SendHttpRequest(t, tc, config.STAT_HTTP_VERB, url, string(body), http.StatusOK, "")

	var stats api.StatResponse
	err := json.Unmarshal([]byte(respBody), &stats)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return &stats
}

func getStatsGRPC(t *testing.T, tc common.TestContext) *api.StatResponse {
	stats := sendGRPCStatRequest(t, tc)
	return stats
}

func sendGRPCStatRequest(t *testing.T, tc common.TestContext) *api.StatResponse {
	// Create gRPC client
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d",
		config.Configuration().RestServer.GRPCServerIP,
		config.Configuration().RestServer.GRPCServerPort),
		grpc.WithTransportCredentials(credentials.NewTLS(tu.GetClientTLSConfig(t, tc))))
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
		respCode = tu.GetStatusCodeFromError(t, err)
		errStr = fmt.Sprintf("%v", err)
	}

	if respCode != expectedStatus {
		t.Fatalf("Test failed. Expected: %d, Got: %d. Complete Error Message: %v ", expectedStatus, respCode, errStr)
	}

	return api.ConvertStatResponseProto(respProto)
}

func getStatHandlers() *handlers.AllHandlers {
	return &handlers.AllHandlers{
		Stater:   GetStater(),
		Batcher:  nil,
		PKReader: pkread.GetPKReader(),
	}
}
