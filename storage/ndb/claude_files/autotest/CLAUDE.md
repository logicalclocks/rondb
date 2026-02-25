# Autotest (ATRT) Failure Analysis Guide

## Overview

Autotest (ndb_atrt) is the NDB Cluster automated test framework. It runs a
sequence of test cases against a cluster configuration, saving results for
each test. Test results are stored in directories named by date under a
suite/architecture path:

```
result-<suite>--<arch>/<YYYY-MM-DD>/
```

## Top-Level Directory Structure

```
result-daily-basic--05--x86_64_Linux/2026-02-09/
├── log.txt          # Main ATRT log - start here
├── report.txt       # Semicolon-delimited summary of all tests
├── info.txt         # Run metadata (date, suite, hosts, atrt command)
├── my.cnf           # Cluster configuration used for the run
├── INFO_BIN.0       # Build information (cmake flags, build date)
├── INFO_SRC.0       # Source information
├── result.9/        # Saved artifacts for failed test #9
└── result.10/       # Saved artifacts for failed test #10
```

Only failed tests have `result.N` directories saved.

## Step 1: Read log.txt — Identify Failures

The main `log.txt` is the starting point. Each test produces lines like:

```
2026-02-09 17:36:45 [ndb_atrt] INFO     -- #1 OK(0)
2026-02-09 17:38:14 [ndb_atrt] INFO     -- #9 FAILED(101)
2026-02-09 17:38:35 [ndb_atrt] INFO     -- #10 FAILED(256)
2026-02-09 17:39:28 [ndb_atrt] INFO     -- #13 SKIPPED(1024)
```

### Result Codes

| Code | Meaning | Action |
|------|---------|--------|
| **0** | OK — test passed | No action needed |
| **101** | Data node crash — at least one ndbd died | Check error logs and trace files in `result.N/ndbd.*/` |
| **103** | Test timeout — test exceeded time limit | Check test program log for hangs; check data node logs for slow operations |
| **256** | Test failure — no crashes but test reported failure | Check test program log `result.N/ndb_api.1/log.out` for error details |
| **1024** | Skipped — test not applicable to this configuration | No action needed (e.g., test requires more nodes than configured) |

### Reading log.txt for Context

Before and after the `FAILED` line, look for CRITICAL messages that provide
immediate context:

```
2026-02-09 17:38:13 [ndb_atrt] CRITICAL -- ndbd #1 not running on beanbag1
2026-02-09 17:38:13 [ndb_atrt] CRITICAL -- ndbd #2 not running on beanbag1
2026-02-09 17:38:13 [ndb_atrt] CRITICAL -- ndbd #3 not running on beanbag1
2026-02-09 17:38:13 [ndb_atrt] CRITICAL -- Failed to get updated status for all processes
```

The test name and arguments are logged when the test starts:

```
2026-02-09 17:38:07 [ndb_atrt] INFO     -- #9 - testNdbApi -n MaxGetValue T1 T6 T13
```

This tells you: test program `testNdbApi`, test case `MaxGetValue`, run with
table types T1, T6, T13.

## Step 2: Read report.txt — Quick Summary

`report.txt` provides a machine-parseable summary with semicolon-delimited fields:

```
testNdbApi -n MaxGetValue T1 T6 T13 ; 9 ; 101 ; 5 ; 1
testBlobs -bug 36756 -skip p ; 10 ; 256 ; 5 ; 1
```

Format: `<test command> ; <test#> ; <result_code> ; <duration_secs> ; <attempts>`

This is useful for scripted analysis or quickly scanning which tests failed.

## Step 3: Examine the result.N Directory

For each failed test, a `result.N` directory contains the saved state:

```
result.9/
├── my.cnf           # Cluster config used for this test
├── ndb_mgmd.1/      # Management server
│   ├── log.out      # ndb_mgmd program output
│   ├── ndb_1_cluster.log  # Cluster event log
│   └── env.sh       # Environment variables and start command
├── ndbd.1/          # Data node 1 (node id typically = directory + 1)
│   ├── log.out      # Data node program output (stdout/stderr + stack traces)
│   ├── ndb_N_error.log        # Crash summary — START HERE for crashes
│   ├── ndb_N_trace.log.M      # Combined trace file
│   ├── ndb_N_trace.log.M_tT   # Per-thread trace file
│   ├── ndb_N_signal.log       # Signal log
│   └── env.sh       # Environment (includes CMD with start command)
├── ndbd.2/          # Data node 2
├── ndbd.3/          # Data node 3
├── ndb_api.1/       # Test program (main — check this for test output)
│   ├── log.out      # Test program output — LOOK HERE for test failures
│   └── env.sh
├── ndb_api.2/       # Additional API nodes (usually empty except env.sh)
└── ndb_api.3/
```

**Important note on naming:** The directory `ndbd.1` does NOT mean node id 1.
The management server is typically node 1. Data node directories ndbd.1,
ndbd.2, ndbd.3 correspond to node ids 2, 3, 4 respectively. The error log
filenames confirm this: `ndbd.1/ndb_2_error.log` means node id 2.

## Step 4: Analyse by Failure Type

### FAILED(101) — Data Node Crash

This is the most complex failure type. All data nodes typically crash because
one crash triggers the others (loss of heartbeat / node failure handling).

**4a. Find the first crash — check ndb_N_error.log in each ndbd directory:**

```
Time: Monday 9 February 2026 - 17:38:11
Status: Temporary error, restart node
Message: Internal program error (failed ndbrequire)
Error: 2341
Error data: DbtupExecQuery.cpp
Error object: DBTUP (Line: 916) 0x00000006 Check sectionPtr.sz < 24000 failed
Program: ndbmtd
Pid: 649658 thr: 4
Trace file name: ndb_2_trace.log.1_t4
```

Compare timestamps across all ndbd.*/ndb_*_error.log files. The earliest
timestamp is the node that crashed first — the others are typically
consequential failures.

Key fields:
- **Error data**: Source file where the crash occurred
- **Error object**: Block name, line number, and failed condition
- **thr: N**: Thread that crashed
- **Trace file name**: The specific per-thread trace file to examine

**4b. Read the program output log (log.out):**

The `log.out` file in each ndbd directory contains stdout/stderr from ndbmtd.
For segfaults, this includes native stack traces that show the C++ call stack.
See the trace file analysis guide for details on interpreting stack traces.

Note: In autotest the program output log is called `log.out` (not `ndbd.log`
as in MTR).

**4c. Read the per-thread trace file:**

The trace file named in the error log (e.g., `ndb_2_trace.log.1_t4`) contains
the signal and jam trace for the crashing thread. Signals are listed NEWEST
first; jam content within each signal is OLDEST first.

Example from a crash in DBTUP:
```
    DbtupExecQuery.cpp                 00907  00916
    SimulatedBlock.cpp                 02328
```

The last jam entries (00907, 00916) show the code path leading to the crash.
Line 916 is where the ndbrequire failed.

For detailed trace file analysis, see:
`storage/ndb/claude_files/pushdown_join_aggregation/trace_file_analysis.md`

**4d. Check the test program log:**

Also check `ndb_api.1/log.out` to understand what the test was doing when
the crash happened. Look for the last operations before node disconnection:

```
24000 getValues called
ERROR: 4010 Node failure caused abort of transaction
```

### FAILED(256) — Test Failure (No Crash)

The test program itself reported failure. There are no data node crashes,
so focus entirely on the test program log.

**Check ndb_api.1/log.out:**

```
testBlobs -bug 36756 -skip p
line 642 FAIL g_dic->createIndex(idx) == 0
dic: 4714: Index stats system tables do not exist
line 4189 FAIL createTable(storage) == 0

NDBT_ProgramExit: 1 - Failed
```

The `FAIL` lines show:
- The source file line number where the assertion failed
- The condition that was not met
- NDB error messages providing context

For FAILED(256), the ndbd directories typically won't have error logs or
trace files since no data nodes crashed. They will have `log.out` (normal
startup output) and `ndb_N_signal.log`.

### FAILED(103) — Test Timeout

The test exceeded its time limit. Check:
1. `ndb_api.1/log.out` — look at the last output to see where the test was stuck
2. `ndbd.*/log.out` — check if data nodes were in a stuck state
3. Consider whether the test is simply too slow for the hardware/config

### SKIPPED(1024) — Not a Failure

The test was skipped, typically because the cluster configuration doesn't
match the test requirements (e.g., test needs 4 nodes but only 3 configured,
or test needs specific error injection support). No action needed.

## Step 5: Cross-Reference

### Identifying the test source code

The test name from log.txt maps to source files:
```
storage/ndb/test/ndbapi/testNdbApi.cpp      # testNdbApi
storage/ndb/test/ndbapi/testBasic.cpp       # testBasic
storage/ndb/test/ndbapi/testScan.cpp        # testScan
storage/ndb/test/ndbapi/testDict.cpp        # testDict
storage/ndb/test/ndbapi/testBlobs.cpp       # testBlobs
storage/ndb/test/ndbapi/testNodeRestart.cpp # testNodeRestart
storage/ndb/test/ndbapi/testSystemRestart.cpp # testSystemRestart
storage/ndb/test/ndbapi/testIndex.cpp       # testIndex
storage/ndb/test/ndbapi/test_event.cpp      # test_event
```

The `-n TestCaseName` argument identifies the specific test function within
the file.

### Cluster configuration

`my.cnf` in the result directory shows which cluster template was used.
The `[atrt]` section's `clusters` value (e.g., `.3node_6cpus`) identifies
the template. Key parameters to check:
- `NoOfReplicas` — number of data replicas
- `NumCPUs` — affects thread configuration
- `AutomaticThreadConfig` / `AutomaticMemoryConfig` — auto-configuration
- `ndbd = host,host,host` — number of data nodes

### Environment

`env.sh` in each process directory contains the full start command (`CMD`
variable) and environment. Useful for reproducing issues locally.

## Quick Checklist for Analyzing a Failed Run

1. Open `log.txt`, search for `FAILED` to find all failures
2. Note the test numbers and result codes
3. For each failure:
   - **101 (crash)**: Check `ndbd.*/ndb_*_error.log` across all nodes, find earliest timestamp, read the trace file, check `ndb_api.1/log.out`
   - **256 (test fail)**: Read `ndb_api.1/log.out` for the FAIL lines
   - **103 (timeout)**: Read `ndb_api.1/log.out` for last activity
4. Cross-reference with source code using file/line from error messages
5. Check `report.txt` for a quick overview of the entire run

## Example: Analyzing the 2026-02-09 Run

This run had 30 tests: 25 passed, 2 failed, 3 skipped.

**Test #9: testNdbApi -n MaxGetValue — FAILED(101)**

All three data nodes crashed with the same ndbrequire failure:
```
DBTUP (Line: 916) Check sectionPtr.sz < 24000 failed
```
The test calls `getValue()` in a loop up to `1000*m` times (m=1..99). At
m=24, 24000 attrinfo words hit the `ndbrequire(sectionPtr.sz < ZATTR_BUFFER_SIZE)`
check in `copyAttrinfo()` (DbtupExecQuery.cpp:916) where ZATTR_BUFFER_SIZE
was 24000. The test expects graceful errors (880, 823, 4257, 4002) but gets
a node crash instead.

Root cause: The attrinfo size check pipeline had a gap:
- NDB API allows up to 262143 words (MaxTotalAttrInfo)
- DBTUP stored proc path checks gracefully (error 874)
- DBTUP interpreted path checks gracefully (error 882)
- DBTUP copyAttrinfo for key ops used ndbrequire → crash

Fix (branch autotest_fixes):
1. Changed `copyAttrinfo()` to return error 823 (ZTOO_MUCH_ATTRINFO_ERROR)
   instead of crashing with ndbrequire
2. DblqhMain.cpp now checks the return and sends TUPKEYREF on failure
3. Increased ZATTR_BUFFER_SIZE from 24000 to 32768 for 4096 column support
   (interpreted ops need ~30K+ words: 5 header + 4096 initialRead +
   22096 finalUpdate + 4096 finalRead + program)

**Test #10: testBlobs -bug 36756 — FAILED(256)**

No crashes. The test program failed because:
```
dic: 4714: Index stats system tables do not exist
```
This is NOT a consequence of the test #9 crash — it's an independent issue.
The index stats system tables (`ndb_index_stat_head`, `ndb_index_stat_sample`)
are only created by the MySQL plugin (ha_ndbcluster_binlog.cc). In this ATRT
configuration there is no mysqld, so these tables never exist. When testBlobs
creates an ordered index, DBDICT triggers an index stats update via TRIX, which
fails with error 4714 when it can't find the stats tables. The error path in
`alterIndex_fromIndexStat()` (Dbdict.cpp) aborted the entire createIndex.

Earlier tests (T1, T6, D1, D2) pass because those table types don't have
ordered indexes, so the index stats path is never triggered.

Root cause: `Dbdict::alterIndex_fromIndexStat()` treated all IndexStatRef
errors as fatal, including 4714 which is expected without mysqld.

Fix (branch autotest_fixes):
1. In `alterIndex_fromIndexStat()`, when error 4714 (NoSysTables) is
   received, treat it as non-fatal: skip the stats operation by setting
   `m_sub_index_stat_dml = true` and `m_sub_index_stat_mon = true`, then
   continue with `createSubOps()` instead of `abortSubOps()`.

## Example: Analyzing the 2026-02-09 Run (daily-basic--07)

This run had 32 tests: 29 passed, 1 failed, 2 skipped.

**Test #12: testBasic -n Bug27756 — FAILED(256)**

No crashes. The test reported "Memleak detected" for 9 out of 15 table types.
The test runs 5 iterations of insert → interpretedUpdateTuple →
getValue(COPY_ROWID) → delete → commit/rollback, then checks that all
COPY_ROWID values are identical. If they differ, it reports a memory leak.

Results by table type:
- PASS (all 5 addresses same): T3, T6, T13, T17, D1, I3
- FAIL (addresses change at iteration 4): T1, T2, T4, T14, T15, T16, D2, I1, I2

Root cause: Copy tuples are now allocated via `lc_ndbd_pool_malloc` (Dbtup.hpp)
instead of the old page-based `c_undo_buffer`. The old allocator reused the
same page locations deterministically, so COPY_ROWID stayed constant. The new
pool allocator does not guarantee address reuse — freed memory may be returned
at different addresses. This is NOT a real memory leak, but the test's
detection method (address comparison) is incompatible with the new allocator.

All table types are well below the 10KB row size limit mentioned in the test
comment (largest is T3 at ~5500 bytes), so this is not a row-size issue.

Fix approach (branch autotest_fixes):
Since `lc_ndbd_pool_malloc` has no automatic leak discovery, add a copy tuple
allocation counter under `#ifdef ERROR_INSERT` (autotest always runs with
ERROR_INSERT enabled):

1. Add `m_copy_tuple_alloc_count` counter to Dbtup (under `#ifdef ERROR_INSERT`)
   - Incremented in `alloc_copy_tuple()`, decremented in `free_copy_tuple()`
2. Add `m_copy_tuple_saved_count` to save a snapshot of the counter
3. Add two DumpStateOrd handlers in DBTUP (error insert range 4000-4999):
   - DUMP code A: saves current counter (`m_copy_tuple_saved_count =
     m_copy_tuple_alloc_count`)
   - DUMP code B: checks counter matches saved value, crashes (ndbrequire)
     if they differ — this converts a silent leak into a FAILED(101)
4. Update `runBug27756` in testBasic.cpp:
   - Before loop: `restarter.dumpStateAllNodes({DUMP_A})` to save counter
   - Run the existing insert/update/delete loop (remove COPY_ROWID read
     and address comparison)
   - After loop: `restarter.dumpStateAllNodes({DUMP_B})` to verify counter
   - If any node crashes, the test fails with FAILED(101) indicating a real
     copy tuple leak

Key code locations:
- `Dbtup::alloc_copy_tuple()` — Dbtup.hpp:4055 (lc_ndbd_pool_malloc)
- `Dbtup::free_copy_tuple()` — Dbtup.hpp:4079 (lc_ndbd_pool_free)
- `COPY_ROWID` handler — DbtupRoutines.cpp:3450
- `m_copy_tuple_location` — now `Uint32*` (was `Local_key`), Dbtup.hpp:1057
- Error insert range for DBTUP: 4000-4999 (Cmvmi.cpp:1135-1140)
- DumpStateOrd handler: DbtupGen.cpp (execDUMP_STATE_ORD)
