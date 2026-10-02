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

// Package starvation measures whether the REST server's /ping endpoint (the
// Kubernetes liveness probe) stays responsive while a data node restarts
// under constant pk-read load, on BOTH listeners in one run:
//   - the main port, where /ping shares the Drogon event loops with the
//     data path and starves when the loops block (the incident);
//   - the dedicated probe port, whose /ping must keep answering in every
//     mode, including a silent-but-connected node (SIGSTOP).
//
// The probe port is an ASSERTING regression gate (see the gates at the
// end); the main port additionally asserts mode-dependent bounds in the
// announced-departure modes, and stays reporting-only under SIGSTOP - its
// continued blackout there is the proof the harness still bites. Set
// STARV_ASSERT=0 to demote everything to reporting. Findings go to
// $STARV_REPORT.
//
// It is driven by mysql-test/suite/rdrs2-golang/t/rdrs2-golang_ping_starvation.test
// and self-skips unless RDRS_STARVATION=1, so the normal `go test ./...`
// pass over all packages does not run it.
package starvation

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/testutils"
)

// The shipped rondb-helm liveness probe for rdrs: httpGet /0.1.0/ping,
// timeoutSeconds=2, periodSeconds=5, failureThreshold=4.
const (
	probeTimeout    = 2 * time.Second
	probePeriod     = 5 * time.Second
	probeThreshold  = 4
	pingInterval    = 200 * time.Millisecond
	pingHTTPTimeout = 10 * time.Second
	opHTTPTimeout   = 30 * time.Second
)

type sample struct {
	start   time.Time
	latency time.Duration
	status  int
	err     error
}

// ok reports whether this ping, had it been a kubelet probe, would have
// passed: answered 200 within the probe timeout.
func (s sample) ok() bool {
	return s.err == nil && s.status == http.StatusOK && s.latency <= probeTimeout
}

type recorder struct {
	mu      sync.Mutex
	samples []sample
}

func (r *recorder) add(s sample) {
	r.mu.Lock()
	r.samples = append(r.samples, s)
	r.mu.Unlock()
}

func (r *recorder) sorted() []sample {
	r.mu.Lock()
	out := make([]sample, len(r.samples))
	copy(out, r.samples)
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].start.Before(out[j].start) })
	return out
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envIntOr(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// runTool runs an MTR-provided tool command line ("<exe> --defaults-file=...")
// with extra args appended, returning combined output.
func runTool(t *testing.T, cmdline string, extra ...string) string {
	t.Helper()
	fields := strings.Fields(cmdline)
	if len(fields) == 0 {
		t.Fatalf("empty tool command line")
	}
	args := append(fields[1:], extra...)
	out, err := exec.Command(fields[0], args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", fields[0], strings.Join(args, " "), err, out)
	}
	return string(out)
}

// ndbmtdChildPid finds the running ndbmtd kernel process of the given node by
// parsing the angel's startup line in the MTR-managed ndbd log. The last
// occurrence wins (the node may have been restarted during the MTR session).
func ndbmtdChildPid(t *testing.T, nodeID string) int {
	t.Helper()
	vardir := os.Getenv("MYSQLTEST_VARDIR")
	if vardir == "" {
		t.Fatalf("MYSQLTEST_VARDIR must be set for freeze mode")
	}
	logPath := fmt.Sprintf("%s/mysql_cluster.1/ndbd.%s/ndbd.log", vardir, nodeID)
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", logPath, err)
	}
	re := regexp.MustCompile(`Angel pid: \d+ started child: (\d+)`)
	matches := re.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		t.Fatalf("no 'Angel pid ... started child' line in %s", logPath)
	}
	pid, err := strconv.Atoi(string(matches[len(matches)-1][1]))
	if err != nil {
		t.Fatalf("bad pid in %s: %v", logPath, err)
	}
	return pid
}

func percentile(durs []time.Duration, p float64) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durs))
	copy(sorted, durs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

type phase struct {
	name  string
	from  time.Time
	until time.Time
}

func phaseStats(name string, samples []sample, from, until time.Time) string {
	var lat []time.Duration
	failed := 0
	// Failure breakdown: a failed sample is a transport error, a non-200, or
	// an over-timeout success - which one matters when attributing blame.
	reasons := map[string]int{}
	for _, s := range samples {
		if s.start.Before(from) || !s.start.Before(until) {
			continue
		}
		if s.ok() {
			lat = append(lat, s.latency)
		} else {
			failed++
			switch {
			case s.err != nil:
				msg := s.err.Error()
				// Strip the variable prefix ("Get http://...: ") so the
				// same cause aggregates.
				if idx := strings.LastIndex(msg, ": "); idx != -1 {
					msg = msg[idx+2:]
				}
				reasons[msg]++
			case s.status != http.StatusOK:
				reasons[fmt.Sprintf("status %d", s.status)]++
			default:
				reasons[fmt.Sprintf("ok but > %v", probeTimeout)]++
			}
		}
	}
	if len(lat) == 0 && failed == 0 {
		return fmt.Sprintf("%-12s no samples", name)
	}
	var max time.Duration
	for _, l := range lat {
		if l > max {
			max = l
		}
	}
	line := fmt.Sprintf("%-12s pings=%d failed=%d p50=%v p99=%v max_ok=%v",
		name, len(lat)+failed, failed, percentile(lat, 0.50),
		percentile(lat, 0.99), max)
	for reason, count := range reasons {
		line += fmt.Sprintf("\n%-14s failure: %dx %s", "", count, reason)
	}
	return line
}

// blackout returns the longest wall-clock gap between completions of
// probe-passing pings within [from, until).
func blackout(samples []sample, from, until time.Time) time.Duration {
	last := from
	var worst time.Duration
	for _, s := range samples {
		if s.start.Before(from) || !s.start.Before(until) {
			continue
		}
		if s.ok() {
			end := s.start.Add(s.latency)
			if gap := end.Sub(last); gap > worst {
				worst = gap
			}
			if end.After(last) {
				last = end
			}
		}
	}
	if gap := until.Sub(last); gap > worst {
		worst = gap
	}
	return worst
}

// kubeletVerdict simulates the shipped liveness probe over the recorded
// samples: probes fire every probePeriod; the probe at time T is approximated
// by the recorded ping launched closest to T. Returns the maximum run of
// consecutive failed probes.
func kubeletVerdict(samples []sample, from, until time.Time) int {
	worst, run := 0, 0
	for T := from; T.Before(until); T = T.Add(probePeriod) {
		var nearest *sample
		var best time.Duration
		for i := range samples {
			d := samples[i].start.Sub(T)
			if d < 0 {
				d = -d
			}
			if nearest == nil || d < best {
				nearest = &samples[i]
				best = d
			}
		}
		if nearest == nil || nearest.ok() {
			run = 0
			continue
		}
		run++
		if run > worst {
			worst = run
		}
	}
	return worst
}

func TestPingDuringNodeRestart(t *testing.T) {
	if os.Getenv("RDRS_STARVATION") != "1" {
		t.Skip("set RDRS_STARVATION=1 to run the ping-starvation driver")
	}

	workers := envIntOr("STARV_WORKERS", 32)
	rows := envIntOr("STARV_ROWS", 2048)
	baselineS := envIntOr("STARV_BASELINE_S", 15)
	recoveryS := envIntOr("STARV_RECOVERY_S", 15)
	restartMode := envOr("STARV_RESTART_MODE", "graceful")
	db := envOr("STARV_DB", "starv_db")
	table := envOr("STARV_TABLE", "t1")
	reportPath := os.Getenv("STARV_REPORT")

	mgm := os.Getenv("NDB_MGM")
	waiter := os.Getenv("NDB_WAITER")
	nodeID := os.Getenv("NDB_NDBD_2_NODEID")
	if mgm == "" || waiter == "" || nodeID == "" {
		t.Fatalf("NDB_MGM, NDB_WAITER and NDB_NDBD_2_NODEID must be set " +
			"(run via mtr --suite=rdrs2-golang rdrs2-golang_ping_starvation)")
	}

	pingURL := testutils.NewPingURL()
	probePingURL := testutils.NewProbePingURL()
	healthURL := testutils.NewHealthURL()
	probeHealthURL := testutils.NewProbeHealthURL()
	pkReadURL := testutils.NewPKReadURL(db, table)

	// Sanity: the server must answer /ping before we measure anything.
	sanity := &http.Client{Timeout: pingHTTPTimeout}
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := sanity.Get(pingURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("rdrs2 did not answer /ping at %s: %v", pingURL, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	pings := &recorder{}
	probePings := &recorder{}
	healths := &recorder{}
	probeHealths := &recorder{}
	ops := &recorder{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Pingers: fresh TCP connection per probe (the kubelet does the same, and
	// Drogon assigns each new connection to an event loop - the exact surface
	// that starves). Concurrent fire-and-record so a hung probe does not
	// lower the sampling rate. One pinger per listener: the main port (shares
	// the Drogon loops with the data path) and the dedicated probe port,
	// giving the A/B comparison within a single run.
	startPinger := func(url string, rec *recorder) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{
				Timeout:   pingHTTPTimeout,
				Transport: &http.Transport{DisableKeepAlives: true},
			}
			ticker := time.NewTicker(pingInterval)
			defer ticker.Stop()
			var pwg sync.WaitGroup
			for {
				select {
				case <-stop:
					pwg.Wait()
					return
				case <-ticker.C:
					pwg.Add(1)
					go func() {
						defer pwg.Done()
						s := sample{start: time.Now()}
						resp, err := client.Get(url)
						s.latency = time.Since(s.start)
						s.err = err
						if err == nil {
							s.status = resp.StatusCode
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
						}
						rec.add(s)
					}()
				}
			}
		}()
	}
	startPinger(pingURL, pings)
	startPinger(probePingURL, probePings)
	/* /health sampled the same way on both ports. In every mode of this
	 * harness one data node of the two stays up, so a correct /health is 200
	 * throughout and the same pass-criterion as /ping applies. */
	startPinger(healthURL, healths)
	startPinger(probeHealthURL, probeHealths)

	// Load workers: persistent clients (production traffic is long-lived
	// connections) doing single pk-reads as fast as they are answered.
	// Each worker gets its OWN transport pinned to one connection: a nil
	// Transport would share http.DefaultTransport, whose
	// MaxIdleConnsPerHost=2 closes all but two of the 64 concurrent
	// connections after every response - thousands of redials per second,
	// ephemeral-port exhaustion (EADDRNOTAVAIL), and the pingers start
	// failing for a reason that has nothing to do with the server.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			client := &http.Client{
				Timeout: opHTTPTimeout,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 1,
					MaxConnsPerHost:     1,
				},
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				body := fmt.Sprintf(`{"filters":[{"column":"id","value":%d}]}`,
					rng.Intn(rows)+1)
				s := sample{start: time.Now()}
				resp, err := client.Post(pkReadURL, "application/json",
					bytes.NewReader([]byte(body)))
				s.latency = time.Since(s.start)
				s.err = err
				if err == nil {
					s.status = resp.StatusCode
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				ops.add(s)
			}
		}(int64(w))
	}

	marks := map[string]time.Time{}
	marks["start"] = time.Now()

	time.Sleep(time.Duration(baselineS) * time.Second)

	if restartMode == "sigkill" {
		// A true `kill -9` of the kernel process: it dies instantly and the
		// OS closes its sockets, so peers should get a TCP reset rather than
		// having to wait out heartbeats.
		//
		// This mode CANNOT be run under a normal `mtr` invocation:
		// mysql-test-run monitors the data-node processes it started and
		// aborts the test the moment one dies ("Server [ndbd.2.1 ... exit:
		// 512] failed during test run"). Run it against a cluster started
		// with `mtr --start-and-exit`, where MTR has exited and is no
		// longer watching. The node stays down afterwards (StopOnError),
		// so there is no recovery phase to wait for.
		pid := ndbmtdChildPid(t, nodeID)
		marks["restart_issued"] = time.Now()
		marks["node_stopped"] = marks["restart_issued"]
		t.Logf("SIGKILL ndbmtd child pid %d (node %s)", pid, nodeID)
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatalf("SIGKILL %d: %v", pid, err)
		}
		observeS := envIntOr("STARV_FREEZE_S", 45)
		time.Sleep(time.Duration(observeS) * time.Second)
		marks["node_started"] = time.Now()
		t.Logf("observed %ds after the kill (node stays down)", observeS)
	} else if restartMode == "freeze" {
		// Reproduce the production incident precisely: the data node goes
		// SILENT but its TCP connections stay open (in production: a node
		// wedged by memory pressure before the OOM kill; the API nodes
		// logged missed heartbeats for ~20s before expelling it). SIGSTOP
		// freezes the kernel process without closing sockets, so nothing
		// fails over: operations touching the node just block until the
		// heartbeat/TDDT machinery notices.
		freezeS := envIntOr("STARV_FREEZE_S", 45)
		pid := ndbmtdChildPid(t, nodeID)
		marks["restart_issued"] = time.Now()
		marks["node_stopped"] = marks["restart_issued"]
		t.Logf("freezing ndbmtd child pid %d (node %s) for %ds", pid, nodeID, freezeS)
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			t.Fatalf("SIGSTOP %d: %v", pid, err)
		}
		time.Sleep(time.Duration(freezeS) * time.Second)
		if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
			t.Fatalf("SIGCONT %d: %v", pid, err)
		}
		marks["node_started"] = time.Now()
		t.Logf("node %s thawed after %v", nodeID,
			marks["node_started"].Sub(marks["restart_issued"]))
		// If the freeze outlived the heartbeat budget the node was expelled;
		// wait until the whole cluster reports started again either way.
		runTool(t, waiter, "--timeout=300")
	} else {
		restartArgs := nodeID + " restart -n"
		if restartMode == "abort" {
			restartArgs = nodeID + " restart -a -n"
		}
		marks["restart_issued"] = time.Now()
		t.Logf("issuing: %s", restartArgs)
		runTool(t, mgm, "-e", restartArgs)
		runTool(t, waiter, "--wait-nodes="+nodeID, "--not-started", "--timeout=120")
		marks["node_stopped"] = time.Now()
		t.Logf("node %s stopped after %v", nodeID,
			marks["node_stopped"].Sub(marks["restart_issued"]))

		runTool(t, mgm, "-e", nodeID+" start")
		runTool(t, waiter, "--wait-nodes="+nodeID, "--timeout=300")
		marks["node_started"] = time.Now()
		t.Logf("node %s started after %v", nodeID,
			marks["node_started"].Sub(marks["restart_issued"]))
	}

	time.Sleep(time.Duration(recoveryS) * time.Second)
	marks["end"] = time.Now()
	close(stop)
	wg.Wait()

	pingSamples := pings.sorted()
	probePingSamples := probePings.sorted()
	healthSamples := healths.sorted()
	probeHealthSamples := probeHealths.sorted()
	opSamples := ops.sorted()

	phases := []phase{
		{"baseline", marks["start"], marks["restart_issued"]},
		{"disturbance", marks["restart_issued"], marks["node_started"]},
		{"recovery", marks["node_started"], marks["end"]},
	}

	var rep strings.Builder
	fmt.Fprintf(&rep, "ping starvation report (%s)\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(&rep, "config: workers=%d rows=%d restart_mode=%s "+
		"num_threads=%s ping_url=%s\n",
		workers, rows, restartMode, envOr("RDRS_STARV_NUM_THREADS", "?"), pingURL)
	fmt.Fprintf(&rep, "timeline (relative to start):\n")
	for _, k := range []string{"start", "restart_issued", "node_stopped",
		"node_started", "end"} {
		fmt.Fprintf(&rep, "  %-15s +%v\n", k, marks[k].Sub(marks["start"]).Round(time.Millisecond))
	}

	fmt.Fprintf(&rep, "\nMAIN PORT /ping (shares the Drogon loops; probe-eye "+
		"view, fresh connection each, fail = err, non-200 or >%v):\n", probeTimeout)
	for _, ph := range phases {
		fmt.Fprintf(&rep, "  %s\n", phaseStats(ph.name, pingSamples, ph.from, ph.until))
	}

	fmt.Fprintf(&rep, "\nPROBE PORT /ping (dedicated thread; same probe-eye "+
		"view):\n")
	for _, ph := range phases {
		fmt.Fprintf(&rep, "  %s\n", phaseStats(ph.name, probePingSamples, ph.from, ph.until))
	}

	fmt.Fprintf(&rep, "\nMAIN PORT /health (same pass-criterion: one data node "+
		"stays up in every mode, so a correct answer is a fast 200):\n")
	for _, ph := range phases {
		fmt.Fprintf(&rep, "  %s\n", phaseStats(ph.name, healthSamples, ph.from, ph.until))
	}

	fmt.Fprintf(&rep, "\nPROBE PORT /health:\n")
	for _, ph := range phases {
		fmt.Fprintf(&rep, "  %s\n", phaseStats(ph.name, probeHealthSamples, ph.from, ph.until))
	}

	fmt.Fprintf(&rep, "\npk-read load:\n")
	for _, ph := range phases {
		fmt.Fprintf(&rep, "  %s\n", phaseStats(ph.name, opSamples, ph.from, ph.until))
	}

	type portVerdict struct {
		name        string
		samples     []sample
		wholeBlack  time.Duration
		distBlack   time.Duration
		consecFails int
	}
	verdicts := []portVerdict{
		{name: "main port", samples: pingSamples},
		{name: "probe port", samples: probePingSamples},
		{name: "probe port health", samples: probeHealthSamples},
	}
	for i := range verdicts {
		v := &verdicts[i]
		v.wholeBlack = blackout(v.samples, marks["start"], marks["end"])
		v.distBlack = blackout(v.samples, marks["restart_issued"], marks["node_started"])
		v.consecFails = kubeletVerdict(v.samples, marks["start"], marks["end"])
		fmt.Fprintf(&rep, "\n%s blackout (longest gap without a probe-passing ping):\n", v.name)
		fmt.Fprintf(&rep, "  whole run:    %v\n", v.wholeBlack.Round(time.Millisecond))
		fmt.Fprintf(&rep, "  disturbance:  %v\n", v.distBlack.Round(time.Millisecond))
		fmt.Fprintf(&rep, "%s simulated kubelet liveness (timeout %v, period %v, "+
			"threshold %d): max consecutive probe failures = %d\n",
			v.name, probeTimeout, probePeriod, probeThreshold, v.consecFails)
		if v.consecFails >= probeThreshold {
			fmt.Fprintf(&rep, "%s VERDICT: rdrs2 WOULD HAVE BEEN LIVENESS-KILLED\n", v.name)
		} else {
			fmt.Fprintf(&rep, "%s VERDICT: no liveness kill under these settings\n", v.name)
		}
	}

	t.Log("\n" + rep.String())
	if reportPath != "" {
		if err := os.WriteFile(reportPath, []byte(rep.String()), 0644); err != nil {
			t.Fatalf("cannot write report %s: %v", reportPath, err)
		}
	}

	// Regression gates (STARV_ASSERT=0 demotes everything to reporting).
	//
	// Probe port, EVERY mode including the silent-node freeze: this is the
	// fix's contract - the dedicated thread never runs data-path work, so a
	// probe there never times out. Zero simulated probe failures, and the
	// blackout stays far below the ~20s liveness budget.
	//
	// Main port: asserted only in the announced-departure modes at the
	// measured-plus-margin bounds (graceful 0.21s, abort 1.02s, kill 1.05s
	// measured); under freeze it stays reporting-only - its ~45s blackout is
	// the harness's own liveness check, proving the starvation still exists
	// to be measured.
	if envOr("STARV_ASSERT", "1") == "1" {
		/* Both probe-port endpoints are held to the gate: ping (liveness)
		 * and health (readiness) must answer fast and correct in EVERY
		 * mode, including the silent-node freeze. */
		for i := 1; i <= 2; i++ {
			probe := &verdicts[i]
			if probe.consecFails != 0 {
				t.Errorf("%s: %d consecutive simulated probe failures, want 0",
					probe.name, probe.consecFails)
			}
			if probe.distBlack > probeTimeout {
				t.Errorf("%s: disturbance blackout %v exceeds the probe timeout %v",
					probe.name, probe.distBlack.Round(time.Millisecond), probeTimeout)
			}
		}
		mainBound := map[string]time.Duration{
			"graceful": 500 * time.Millisecond,
			"abort":    2 * time.Second,
			"sigkill":  2 * time.Second,
		}
		if bound, ok := mainBound[restartMode]; ok {
			main := &verdicts[0]
			if main.distBlack > bound {
				t.Errorf("main port: disturbance blackout %v exceeds the %s-mode bound %v",
					main.distBlack.Round(time.Millisecond), restartMode, bound)
			}
			if main.consecFails >= probeThreshold {
				t.Errorf("main port: %d consecutive simulated probe failures reaches "+
					"the liveness threshold %d in %s mode",
					main.consecFails, probeThreshold, restartMode)
			}
		}
	}
}
