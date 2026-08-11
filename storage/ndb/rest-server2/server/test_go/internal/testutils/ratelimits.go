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

// HOPSWORKS_TEST_USERNAME is users.username of the fixture user (uid
// 10000) that owns HOPSWORKS_TEST_API_KEY.
const HOPSWORKS_TEST_USERNAME = "macho"

// rateLimitIdentityMaxLen mirrors RDRS's RATE_LIMIT_IDENTITY_MAX_LEN,
// reproducing Hopsworks' online-FS MySQL account name clip
// (OnlineFeaturestoreController.onlineDbUsername): names longer than 32
// are cut to 31; a 32-char name is kept as is.
const rateLimitIdentityMaxLen = 32

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
// identity (apikey mode; in username mode identities are per project and
// only provisioned by the rate limit tests themselves - unprovisioned
// identities run unmetered). Idempotent; called from InitialiseTesting.
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
	// rateLimitPushBurstMaxDuration bounds the burst that must observe the
	// first 429 after a USER entity is created while the server is running.
	// It must stay well below ClusterMgr's 60s periodic re-LIST backstop
	// (USER_ID_RELIST_INTERVAL_MS): a 429 inside this window can only mean
	// the identity reached the user-id caches through the
	// CREATE_DATABASE_REP push announcement, which is what the push-path
	// test pins down. With a working push the first 429 arrives within a
	// few seconds; 15s is margin for slow machines.
	rateLimitPushBurstMaxDuration = 15 * time.Second
)

// rateLimitSettleTime waits out the ~1s client-side overload backoff plus the
// data node flag recomputation (100ms ticks) after a limit change, so each
// phase of a test starts from a known state.
const rateLimitSettleTime = 2500 * time.Millisecond

// rateLimitIdentity returns the identity the server under test tags the
// test API key's transactions with when they are billed to billingDb -
// the API key prefix in apikey mode, or the project-user of the fixture
// user acting in billingDb's project in username mode (matching RDRS's
// rate_limit.hpp and the online-FS MySQL account convention; billingDb
// is the project name, which equals both the online database name and
// the feature store name). NOTE: the server builds the identity from
// project.projectname with its ORIGINAL case, so billingDb must be the
// project name as spelled in the fixture - a test billing to an
// upper-case project (e.g. FSDB003) must pass "FSDB003", not the
// lowercased database name, or the provisioned entity will not match.
func rateLimitIdentity(billingDb string) string {
	if config.GetAll().REST.RateLimitIdentity == "username" {
		identity := billingDb + "_" + HOPSWORKS_TEST_USERNAME
		if len(identity) > rateLimitIdentityMaxLen {
			identity = identity[:rateLimitIdentityMaxLen-1]
		}
		return identity
	}
	return APIKeyPrefix(HOPSWORKS_TEST_API_KEY)
}

// SkipIfRateLimitsDisabled skips t unless the server has user rate limits
// enabled (in either identity mode) and a RonDB backend to enforce them.
func SkipIfRateLimitsDisabled(t *testing.T) {
	t.Helper()
	conf := config.GetAll()
	if !conf.REST.Enable {
		t.Skip("Skipping test as REST is disabled")
	}
	if !conf.REST.UserRateLimits ||
		(conf.REST.RateLimitIdentity != "apikey" &&
			conf.REST.RateLimitIdentity != "username") {
		t.Skip("Skipping test as user rate limits are disabled")
	}
	if !*WithRonDB {
		t.Skip("Skipping test as it requires a running RonDB instance")
	}
}

func setRateLimit(t *testing.T, identity string, ratePerSec uint32) {
	t.Helper()
	if err := SetOrAlterRateLimitUser(identity, ratePerSec); err != nil {
		t.Fatalf("failed to set rate limit of %q to %d: %v",
			identity, ratePerSec, err)
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
func restoreHighRateLimit(t *testing.T, identity string) {
	setRateLimit(t, identity, HIGH_RATE_LIMIT)
	time.Sleep(rateLimitSettleTime)
}

// RunEndpointRateLimitTest verifies that the endpoint driven by sendOne is
// subject to user rate limiting: under a low limit a concurrent burst is
// throttled with HTTP 429, and raising the limit restores service. sendOne
// performs one request with the supplied client and returns its HTTP status,
// which must be http.StatusOK or http.StatusTooManyRequests. billingDb is
// the database the endpoint's requests are billed to (the pk-read/scan URL
// db, the batch's first operation db, or the feature store name); it
// selects the identity to throttle in username mode and is ignored in
// apikey mode.
//
// Rejection is deliberately not instantaneous in the kernel: per-operation
// code only consults per-user queueing/abort flags, which the data nodes
// recompute from accounted usage as it flows in and decay on 100ms ticks.
// Below the abort thresholds an over-budget user is only *delayed*, so the
// burst applies sustained pressure until the computed delay crosses the
// read-abort threshold and the first 429 arrives (see
// rateLimitBurstMaxDuration); from the first kernel rejection the
// client-side backoff cascades 429s anyway.
func RunEndpointRateLimitTest(t *testing.T, sendOne func(*http.Client) int,
	billingDb string) {
	t.Helper()
	client := setupBurstHttpClient(t)
	identity := rateLimitIdentity(billingDb)
	defer restoreHighRateLimit(t, identity)

	setRateLimit(t, identity, rateLimitLowRate)
	time.Sleep(rateLimitSettleTime) // start from a fresh window

	numOk, numRateLimited := rateLimitBurst(client, sendOne, true,
		rateLimitBurstMaxDuration)
	t.Logf("sustained burst of %q at rate %d/sec -> %d ok, %d rate limited",
		identity, rateLimitLowRate, numOk, numRateLimited)
	if numRateLimited == 0 {
		t.Fatalf("expected some requests rejected with 429, all %d succeeded", numOk)
	}

	// Recovery: raising the limit collapses the overload immediately.
	setRateLimit(t, identity, HIGH_RATE_LIMIT)
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
func RunZeroRateIsUnlimitedTest(t *testing.T, sendOne func(*http.Client) int,
	billingDb string) {
	t.Helper()
	client := setupBurstHttpClient(t)
	identity := rateLimitIdentity(billingDb)
	defer restoreHighRateLimit(t, identity)

	setRateLimit(t, identity, 0)
	time.Sleep(rateLimitSettleTime)

	numOk, numRateLimited := rateLimitBurst(client, sendOne, false,
		rateLimitZeroBurstDuration)
	if numRateLimited != 0 {
		t.Fatalf("expected no rate limiting at rate 0, got %d/%d rejected",
			numRateLimited, numOk+numRateLimited)
	}
}

// RunUserCreatedAfterStartRateLimitTest verifies that a USER entity created
// while the server is already running (its user-id caches long initialised)
// starts being enforced via the CREATE_DATABASE_REP push announcement alone.
// The authoritative user-id cache never probes DICT per transaction: an
// unknown identity runs unmetered until a push announcement (or the 60s
// re-LIST backstop) delivers it. The test drops the provisioned user, shows
// the identity is then unmetered, re-creates it with a tiny rate — a
// genuinely new DICT entity with a new user id, so a stale cache entry from
// before the drop cannot satisfy the lookup — and asserts a burst is
// throttled well before the re-LIST backstop could have healed a missed
// announcement (see rateLimitPushBurstMaxDuration).
func RunUserCreatedAfterStartRateLimitTest(t *testing.T,
	sendOne func(*http.Client) int, billingDb string) {
	t.Helper()
	client := setupBurstHttpClient(t)
	identity := rateLimitIdentity(billingDb)
	defer restoreHighRateLimit(t, identity)

	mgm, err := connectMgmd()
	if err != nil {
		t.Fatalf("failed to connect to mgmd: %v", err)
	}
	defer mgm.Close()

	if err := mgm.DropUser(identity); err != nil {
		t.Fatalf("failed to drop user %q: %v", identity, err)
	}
	time.Sleep(rateLimitSettleTime) // DROP_DATABASE_REP propagation

	// The dropped identity is unknown to every user-id cache and must run
	// unmetered (and in particular must not be rejected or probed).
	if code := sendOne(client); code != http.StatusOK {
		t.Fatalf("expected unmetered 200 after user drop, got %d", code)
	}

	// Re-create the user with a tiny rate. SetUser (not alter) must succeed:
	// the entity was just dropped, so this is a fresh create, announced to
	// the running server only by CREATE_DATABASE_REP.
	limits := mgmclient.UserLimits{RatePerSec: rateLimitLowRate}
	if err := mgm.SetUser(identity, limits); err != nil {
		t.Fatalf("failed to re-create user %q: %v", identity, err)
	}
	created := time.Now()
	time.Sleep(rateLimitSettleTime)

	numOk, numRateLimited := rateLimitBurst(client, sendOne, true,
		rateLimitPushBurstMaxDuration)
	elapsed := time.Since(created)
	t.Logf("user created after server start -> %d ok, %d rate limited, %v "+
		"after create", numOk, numRateLimited, elapsed)
	if numRateLimited == 0 {
		t.Fatalf("no 429 within %v of creating the user: the "+
			"CREATE_DATABASE_REP push did not reach the user-id caches "+
			"(only the 60s re-LIST backstop would eventually heal this)",
			elapsed)
	}
}
