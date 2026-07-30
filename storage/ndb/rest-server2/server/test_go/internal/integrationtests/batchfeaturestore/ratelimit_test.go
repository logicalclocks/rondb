/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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

package batchfeaturestore

import (
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// TestRateLimit verifies that the batch feature store endpoint is subject to
// API key rate limiting (RONDB-978). Each entry in the batch becomes an NDB
// transaction tagged with the request identity. It reuses the batch request
// builder against the simple fsdb001/sample_1 feature view (PK id1).
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)

	entries := [][]interface{}{
		{[]byte("1")}, {[]byte("1")}, {[]byte("1")}, {[]byte("1")}, {[]byte("1")},
	}
	req := CreateBatchFeatureStoreRequest(testdbs.FSDB001, "sample_1", 1,
		[]string{"id1"}, entries, nil, nil)
	reqBody := req.String()
	url := testutils.NewBatchFeatureStoreURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client,
			config.FEATURE_STORE_HTTP_VERB, url, reqBody, "",
			http.StatusOK, http.StatusTooManyRequests)
		return code
	})
}
