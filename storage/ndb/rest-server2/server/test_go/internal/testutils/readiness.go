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

package testutils

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
)

// WaitForAPIKeyAuthReady polls the given endpoint with the default test API
// key until it answers 200, so tests only start once the server's API key
// cache has converged after (re-)seeding the hopsworks database. The cache
// is maintained asynchronously (preload, NDB events, periodic refresh);
// around a drop/re-seed of hopsworks an entry can transiently be cached as
// invalid or with incomplete grants, which heals within a refresh cycle.
// Tests must not race that window - the same way a production client retries
// right after its key is provisioned.
func WaitForAPIKeyAuthReady(url string, body string,
	timeout time.Duration) error {
	tlsConfig, err := GetClientTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to get TLS config: %w", err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   tlsConfig,
			ForceAttemptHTTP2: true,
		}}
	deadline := time.Now().Add(timeout)
	lastResult := "no request sent"
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodPost, url,
			strings.NewReader(body))
		if err != nil {
			return fmt.Errorf("failed to create readiness request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(config.API_KEY_NAME, HOPSWORKS_TEST_API_KEY)
		resp, err := client.Do(req)
		if err != nil {
			lastResult = err.Error()
		} else {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastResult = resp.Status
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("API key auth not ready within %v; last result: %s",
		timeout, lastResult)
}
