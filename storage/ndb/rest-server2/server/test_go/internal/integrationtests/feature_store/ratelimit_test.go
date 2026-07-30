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

package feature_store

import (
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
)

// TestRateLimit verifies that the feature store endpoint is subject to API
// key rate limiting (RONDB-978). It reuses the package's known-good simple
// feature view (fsdb001/sample_1, PK id1) and request builder.
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)

	req := CreateFeatureStoreRequest(fsNameSimple, fvNameSimple, fvVersionSimple,
		[]string{"id1"}, []interface{}{[]byte("1")}, nil, nil)
	reqBody := req.String()
	url := testutils.NewFeatureStoreURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client,
			config.FEATURE_STORE_HTTP_VERB, url, reqBody, "",
			http.StatusOK, http.StatusTooManyRequests)
		return code
	})
}
