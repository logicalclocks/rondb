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

package batchpkread

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// The HTTP server keeps request bodies up to its in-memory threshold (64KiB)
// in RAM and buffers anything larger in files under <REST.UploadPath>/tmp/.
// Production rdrs ran with an unwritable working directory and no UploadPath,
// so every body in the 64KiB..1MiB range was silently read as EMPTY and large
// batch requests failed with parse errors. This test drives the file-buffered
// path end to end: the MTR config points UploadPath at a writable directory
// (@uploadpath), and a valid batch request padded past the in-memory
// threshold must reach the handler intact and be answered from the database.
//
// The padding is whitespace inside the JSON document: legal JSON, counted by
// the HTTP layer towards the body size, invisible to the parser. This keeps
// the request within BatchMaxSize while making the BODY arbitrarily large.
func TestBatchBodyLargerThanMemoryThreshold(t *testing.T) {
	op := api.BatchSubOp{
		Method: &[]string{config.PK_HTTP_VERB}[0],
		RelativeURL: &[]string{
			string(testdbs.DB004 + "/int_table/" + config.PK_DB_OPERATION)}[0],
		Body: &api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
			ReadColumns: testclient.NewReadColumns("col", 2),
			OperationID: testclient.NewOperationID(64),
		},
	}
	subOps := []api.BatchSubOp{op}
	batch := api.BatchOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	// Pad well past Drogon's 64KiB in-memory threshold (and stay under the
	// 1MiB request cap): 256KiB of spaces after the opening brace.
	const padding = 256 * 1024
	if body[0] != '{' {
		t.Fatalf("expected JSON object body, got %q...", body[0])
	}
	padded := "{" + strings.Repeat(" ", padding) + string(body[1:])
	if len(padded) <= 64*1024 {
		t.Fatalf("padded body is not larger than the in-memory threshold")
	}

	client := testutils.SetupHttpClient(t)
	httpCode, response := testclient.SendHttpRequestWithClient(
		t,
		client,
		config.BATCH_HTTP_VERB,
		testutils.NewBatchReadURL(),
		padded,
		"",
		http.StatusOK,
	)
	if httpCode != http.StatusOK {
		t.Fatalf("large body request failed: code %d, response %s",
			httpCode, string(response))
	}
	// The body reached the handler intact iff the response echoes the
	// operation id and carries the read columns.
	resp := string(response)
	if !strings.Contains(resp, *op.Body.OperationID) {
		t.Fatalf("response does not echo the operation id; body was "+
			"truncated or emptied: %s", resp)
	}
	for _, col := range []string{"col0", "col1"} {
		if !strings.Contains(resp, col) {
			t.Fatalf("response missing column %q: %s", col, resp)
		}
	}
}
