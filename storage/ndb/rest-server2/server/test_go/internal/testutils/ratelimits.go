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
	"errors"
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
// before the '.'). With .RateLimit.Identity=apikey and .RateLimit.FullAPIKey
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

// withMgmd runs fn on a fresh management server connection. One connection
// per command: a command that hits the client deadline leaves its late reply
// on the connection, which then cannot carry another command.
func withMgmd(fn func(*mgmclient.Client) error) error {
	mgm, err := connectMgmd()
	if err != nil {
		return err
	}
	defer mgm.Close()
	return fn(mgm)
}

// SetOrAlterRateLimitUser updates the USER rate limit entity for the given
// identity, or creates it if it does not exist yet.
//
// Alter is tried first even though "set or alter" reads the other way round:
// every mgm command here is a schema transaction, DICT runs one of those at a
// time, and by the time any test changes a limit the entity has already been
// provisioned by ProvisionDefaultRateLimitUsers. Creating first would
// therefore mean a doomed CREATE (error 721, object already exists) plus an
// ALTER on every single change - two serialised schema transactions where one
// suffices, which is what makes these calls slow while the cluster is busy.
func SetOrAlterRateLimitUser(identity string, ratePerSec uint32) error {
	client, err := connectMgmd()
	if err != nil {
		return err
	}
	defer client.Close()
	limits := mgmclient.UserLimits{RatePerSec: ratePerSec}
	alterErr := client.AlterUser(identity, limits)
	if alterErr == nil {
		return nil
	}
	// Only fall back to a create when the management server actually
	// rejected the alter. After a transport error (deadline exceeded) the
	// alter may still be queued server side, and a create issued on top of
	// it would just add another schema transaction to that queue.
	var cmdErr *mgmclient.CommandError
	if !errors.As(alterErr, &cmdErr) {
		return alterErr
	}
	if setErr := client.SetUser(identity, limits); setErr != nil {
		return fmt.Errorf("alter user failed: %v; set user failed: %w",
			alterErr, setErr)
	}
	return nil
}

// ProvisionDefaultRateLimitUsers gives the default test API key a high
// rate limit so that every test tags its transactions with a resolvable
// identity. Idempotent; called from InitialiseTesting. Like setRateLimit it
// only returns once the limit reads back, so a slow cluster at startup
// cannot leave the provisioning in flight under the first tests.
func ProvisionDefaultRateLimitUsers() error {
	identity := APIKeyPrefix(HOPSWORKS_TEST_API_KEY)
	changeErr := SetOrAlterRateLimitUser(identity, HIGH_RATE_LIMIT)
	if err := waitForRateLimit(identity, HIGH_RATE_LIMIT); err != nil {
		return errors.Join(changeErr, err)
	}
	return nil
}

// rateLimitLandingTimeout bounds how long the helpers wait for a limit
// change to become visible through "get user". A command that hit the
// client deadline may still be queued in the management server, which keeps
// retrying for the DICT schema transaction lock for up to 120 s before it
// gives up; mgmclient.CallTimeout already exceeds that, so a second window
// of the same length only has to absorb a transaction whose execution phase
// is itself pathologically slow.
const rateLimitLandingTimeout = mgmclient.CallTimeout

const rateLimitPollInterval = 500 * time.Millisecond

// pollRateLimitUser calls check with the identity's current USER entity (or
// the "get user" error) every rateLimitPollInterval until check returns nil
// or rateLimitLandingTimeout elapses. The polling connection is separate
// from the one that issued the change: that one may have timed out and must
// not be reused, and "get user" is a plain DICT lookup that is answered
// while schema transactions are queued.
func pollRateLimitUser(identity string,
	check func(*mgmclient.UserInfo, error) error) error {
	client, err := connectMgmd()
	if err != nil {
		return err
	}
	defer client.Close()
	deadline := time.Now().Add(rateLimitLandingTimeout)
	for {
		info, getErr := client.GetUser(identity)
		var cmdErr *mgmclient.CommandError
		if getErr != nil && !errors.As(getErr, &cmdErr) {
			return getErr // transport error: the connection is gone
		}
		err := check(info, getErr)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("after %v: %w", rateLimitLandingTimeout, err)
		}
		time.Sleep(rateLimitPollInterval)
	}
}

// waitForRateLimit waits until the identity's USER entity reads ratePerSec.
func waitForRateLimit(identity string, ratePerSec uint32) error {
	err := pollRateLimitUser(identity,
		func(info *mgmclient.UserInfo, getErr error) error {
			if getErr != nil {
				return getErr
			}
			if info.Limits.RatePerSec != ratePerSec {
				return fmt.Errorf("rate limit is %d, want %d",
					info.Limits.RatePerSec, ratePerSec)
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("rate limit of %q did not reach %d: %w",
			identity, ratePerSec, err)
	}
	return nil
}

// waitForRateLimitUserGone waits until "get user" reports no such user.
func waitForRateLimitUserGone(identity string) error {
	err := pollRateLimitUser(identity,
		func(info *mgmclient.UserInfo, getErr error) error {
			if getErr == nil {
				return fmt.Errorf("user still exists with rate limit %d",
					info.Limits.RatePerSec)
			}
			if !mgmclient.IsNoSuchUser(getErr) {
				return getErr
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("user %q was not dropped: %w", identity, err)
	}
	return nil
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
	if !conf.RateLimit.Enable || conf.RateLimit.Identity != "apikey" {
		t.Skip("Skipping test as API key rate limits are disabled")
	}
	if !*WithRonDB {
		t.Skip("Skipping test as it requires a running RonDB instance")
	}
}

// setRateLimit changes the identity's rate limit and returns once the
// management server reports the new value back, so that no limit change is
// left in flight when the helper returns.
//
// A mgm command that hits the client deadline is not cancelled by it: the
// management server keeps waiting for the DICT schema transaction lock and
// applies the change whenever DICT gets to it. Treating that timeout as a
// failure would leave the change in flight, where it could land after the
// deferred restoreHighRateLimit and leave the identity throttled for every
// later test in the package, or, the other way round, a stale restore could
// overwrite the next test's low limit. So on an error the helper waits for
// the requested value to land, bounded by rateLimitLandingTimeout, and only
// a value that never arrives fails the test.
//
// The read-back cannot tell a still-queued command apart from an earlier one
// that set the same value. With mgmclient.CallTimeout above the management
// server's 120 s lock wait, a timed-out command has by then either been
// applied or abandoned unless its execution phase alone took over a minute,
// which the polling window then absorbs.
func setRateLimit(t *testing.T, ratePerSec uint32) {
	t.Helper()
	identity := rateLimitIdentity()
	changeErr := SetOrAlterRateLimitUser(identity, ratePerSec)
	if changeErr != nil {
		t.Logf("rate limit change to %d returned %v; waiting for it to land",
			ratePerSec, changeErr)
	}
	if err := waitForRateLimit(identity, ratePerSec); err != nil {
		t.Fatalf("failed to set rate limit to %d: %v", ratePerSec,
			errors.Join(changeErr, err))
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
	sendOne func(*http.Client) int) {
	t.Helper()
	client := setupBurstHttpClient(t)
	defer restoreHighRateLimit(t)

	// As in setRateLimit, each mgm command is confirmed through "get user"
	// before the test goes on: a command that hits the client deadline is
	// still applied later by the management server, and only the confirmed
	// state can be reasoned about.
	identity := rateLimitIdentity()
	dropErr := withMgmd(func(mgm *mgmclient.Client) error {
		return mgm.DropUser(identity)
	})
	if dropErr != nil {
		t.Logf("drop user %q returned %v; waiting for it to land",
			identity, dropErr)
	}
	if err := waitForRateLimitUserGone(identity); err != nil {
		t.Fatalf("failed to drop user %q: %v", identity,
			errors.Join(dropErr, err))
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
	createErr := withMgmd(func(mgm *mgmclient.Client) error {
		return mgm.SetUser(identity, limits)
	})
	var cmdErr *mgmclient.CommandError
	if errors.As(createErr, &cmdErr) {
		t.Fatalf("failed to re-create user %q: %v", identity, createErr)
	}
	if createErr != nil {
		t.Logf("set user %q returned %v; waiting for it to land",
			identity, createErr)
	}
	if err := waitForRateLimit(identity, rateLimitLowRate); err != nil {
		t.Fatalf("failed to re-create user %q: %v", identity,
			errors.Join(createErr, err))
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
