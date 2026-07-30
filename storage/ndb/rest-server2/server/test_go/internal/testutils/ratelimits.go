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

package testutils

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/mgmclient"
)

// HIGH_RATE_LIMIT is the rate limit provisioned for the test API keys by
// default: high enough that ordinary test traffic never trips it, so all
// tests exercise the identity tagging path without being throttled.
// Rate limit tests lower it temporarily with SetOrAlterRateLimitUser.
const HIGH_RATE_LIMIT = 1000000

// APIKeyPrefix returns the public prefix of a Hopsworks API key (the part
// before the '.'). With RateLimitIdentity=apikey and RateLimitFullAPIKey
// false (the defaults), this is the rate limit identity RDRS tags
// transactions with.
func APIKeyPrefix(apiKey string) string {
	prefix, _, _ := strings.Cut(apiKey, ".")
	return prefix
}

func connectMgmd() (*mgmclient.Client, error) {
	conf := config.GetAll()
	return mgmclient.Connect(conf.RonDB.GenerateMgmdConnectString())
}

// SetOrAlterRateLimitUser creates the USER rate limit entity for the given
// identity, or updates it if it already exists (e.g. from a previous test
// run against the same cluster).
func SetOrAlterRateLimitUser(identity string, ratePerSec uint32) error {
	client, err := connectMgmd()
	if err != nil {
		return err
	}
	defer client.Close()
	limits := mgmclient.UserLimits{RatePerSec: ratePerSec}
	if setErr := client.SetUser(identity, limits); setErr != nil {
		if alterErr := client.AlterUser(identity, limits); alterErr != nil {
			return fmt.Errorf("set user failed: %v; alter user failed: %w",
				setErr, alterErr)
		}
	}
	return nil
}

// ProvisionDefaultRateLimitUsers gives the default test API key a high
// rate limit so that every test tags its transactions with a resolvable
// identity. Idempotent; called from InitialiseTesting.
func ProvisionDefaultRateLimitUsers() error {
	return SetOrAlterRateLimitUser(APIKeyPrefix(HOPSWORKS_TEST_API_KEY),
		HIGH_RATE_LIMIT)
}

// ---- Rate limit test harness (RONDB-978) --------------------------------
//
// Each endpoint's rate limit test lives in that endpoint's own package and
// reuses that package's request builders; the shared burst/assert machinery
// below is defined once here so it is not duplicated per endpoint. An
// endpoint supplies a sendOne closure that performs one request with the
// given client and returns its HTTP status (200 or 429); this package owns
// the identity, the mgm limit changes, the concurrent burst and the
// assertions. The closure indirection also keeps testutils free of a
// testclient import (testclient already imports testutils).

const (
	// rateLimitLowRate is a deliberately tiny per-second budget so that a
	// burst is guaranteed to exhaust it and trip the overload rejection.
	rateLimitLowRate = 1
	// rateLimitBurstParallel concurrent senders apply sustained pressure.
	rateLimitBurstParallel = 32
	// rateLimitBurstMaxDuration bounds the throttled burst. The data nodes
	// reject only once the queueing delay computed from usage accumulated
	// over 100ms ticks crosses the read-abort threshold (~30ms); how fast
	// it climbs depends on machine speed and per-request cost, so the
	// burst is bounded by time and first-429, never by request count (a
	// fixed-count burst on a fast machine finishes before the delay can
	// climb, and is merely slowed down instead of rejected).
	rateLimitBurstMaxDuration = 30 * time.Second
	// rateLimitZeroBurstDuration sizes the burst asserting the absence of
	// throttling for rate 0; a couple of seconds of full pressure is far
	// beyond the point where a limited identity starts seeing 429s.
	rateLimitZeroBurstDuration = 3 * time.Second
)

// rateLimitSettleTime waits out the ~1s client-side overload backoff plus the
// data node flag recomputation (100ms ticks) after a limit change, so each
// phase of a test starts from a known state.
const rateLimitSettleTime = 2500 * time.Millisecond

func rateLimitIdentity() string {
	return APIKeyPrefix(HOPSWORKS_TEST_API_KEY)
}

// SkipIfRateLimitsDisabled skips t unless the server has API key rate limits
// enabled and a RonDB backend to enforce them.
func SkipIfRateLimitsDisabled(t *testing.T) {
	t.Helper()
	conf := config.GetAll()
	if !conf.REST.Enable {
		t.Skip("Skipping test as REST is disabled")
	}
	if !conf.REST.UserRateLimits || conf.REST.RateLimitIdentity != "apikey" {
		t.Skip("Skipping test as API key rate limits are disabled")
	}
	if !*WithRonDB {
		t.Skip("Skipping test as it requires a running RonDB instance")
	}
}

func setRateLimit(t *testing.T, ratePerSec uint32) {
	t.Helper()
	if err := SetOrAlterRateLimitUser(rateLimitIdentity(), ratePerSec); err != nil {
		t.Fatalf("failed to set rate limit to %d: %v", ratePerSec, err)
	}
}

// setupBurstHttpClient returns an HTTP client like SetupHttpClient but with
// an idle connection pool sized for the burst, so every worker reuses its
// connection. The default pool (2 idle conns per host) closes almost every
// burst connection after one response, and the sustained burst then runs the
// OS out of ephemeral ports (thousands of sockets in TIME_WAIT).
func setupBurstHttpClient(t *testing.T) *http.Client {
	t.Helper()
	tlsConfig, err := GetClientTLSConfig()
	if err != nil {
		t.Fatalf("failed to get TLS config for HTTP client. Error: %v", err)
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:     tlsConfig,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        rateLimitBurstParallel,
			MaxIdleConnsPerHost: rateLimitBurstParallel,
		}}
}

// rateLimitBurst applies sustained pressure: rateLimitBurstParallel workers
// (all sharing one client so the burst reuses pooled connections) send
// requests in a loop until maxDuration elapses — or, when stopOn429 is set,
// until some request has been rejected with 429 (each worker still finishes
// its in-flight request). Returns how many were served vs rate limited.
func rateLimitBurst(client *http.Client, sendOne func(*http.Client) int,
	stopOn429 bool, maxDuration time.Duration) (numOk, numRateLimited int) {
	deadline := time.Now().Add(maxDuration)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < rateLimitBurstParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				code := sendOne(client)
				mu.Lock()
				if code == http.StatusOK {
					numOk++
				} else {
					numRateLimited++
				}
				stop := stopOn429 && numRateLimited > 0
				mu.Unlock()
				if stop {
					return
				}
			}
		}()
	}
	wg.Wait()
	return
}

// restoreHighRateLimit raises the limit back to HIGH_RATE_LIMIT and waits out
// the throttled window so later tests in the same package run unthrottled.
func restoreHighRateLimit(t *testing.T) {
	setRateLimit(t, HIGH_RATE_LIMIT)
	time.Sleep(rateLimitSettleTime)
}

// RunEndpointRateLimitTest verifies that the endpoint driven by sendOne is
// subject to API key rate limiting: under a low limit a concurrent burst is
// throttled with HTTP 429, and raising the limit restores service. sendOne
// performs one request with the supplied client and returns its HTTP status,
// which must be http.StatusOK or http.StatusTooManyRequests.
//
// Rejection is deliberately not instantaneous in the kernel: per-operation
// code only consults per-user queueing/abort flags, which the data nodes
// recompute from accounted usage as it flows in and decay on 100ms ticks.
// Below the abort thresholds an over-budget user is only *delayed*, so the
// burst applies sustained pressure until the computed delay crosses the
// read-abort threshold and the first 429 arrives (see
// rateLimitBurstMaxDuration); from the first kernel rejection the
// client-side backoff cascades 429s anyway.
func RunEndpointRateLimitTest(t *testing.T, sendOne func(*http.Client) int) {
	t.Helper()
	client := setupBurstHttpClient(t)
	defer restoreHighRateLimit(t)

	setRateLimit(t, rateLimitLowRate)
	time.Sleep(rateLimitSettleTime) // start from a fresh window

	numOk, numRateLimited := rateLimitBurst(client, sendOne, true,
		rateLimitBurstMaxDuration)
	t.Logf("sustained burst at rate %d/sec -> %d ok, %d rate limited",
		rateLimitLowRate, numOk, numRateLimited)
	if numRateLimited == 0 {
		t.Fatalf("expected some requests rejected with 429, all %d succeeded", numOk)
	}

	// Recovery: raising the limit collapses the overload immediately.
	setRateLimit(t, HIGH_RATE_LIMIT)
	time.Sleep(rateLimitSettleTime)
	if code := sendOne(client); code != http.StatusOK {
		t.Fatalf("expected recovery to 200 after raising limit, got %d", code)
	}
}

// RunZeroRateIsUnlimitedTest verifies the "no limit" path: a USER entity with
// rate_per_sec == 0 means unlimited (the data nodes skip all quota queueing),
// so even a large concurrent burst is never throttled. sendOne is as in
// RunEndpointRateLimitTest. This is kernel behaviour independent of the
// endpoint, so it is exercised once (via pk-read).
func RunZeroRateIsUnlimitedTest(t *testing.T, sendOne func(*http.Client) int) {
	t.Helper()
	client := setupBurstHttpClient(t)
	defer restoreHighRateLimit(t)

	setRateLimit(t, 0)
	time.Sleep(rateLimitSettleTime)

	numOk, numRateLimited := rateLimitBurst(client, sendOne, false,
		rateLimitZeroBurstDuration)
	if numRateLimited != 0 {
		t.Fatalf("expected no rate limiting at rate 0, got %d/%d rejected",
			numRateLimited, numOk+numRateLimited)
	}
}
