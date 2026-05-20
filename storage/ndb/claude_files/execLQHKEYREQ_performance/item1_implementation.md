# Item 1 implementation plan: outline error-return epilogues

Companion to `item1_case.md`. This file lists every concrete edit, in
order, with the exact files and line numbers in the current tree.

## Scope of changes

Two source files modified, no public headers, no signal-format
changes, no schema changes.

| File | Edits |
|---|---|
| `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp` | 3 new private declarations |
| `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` | 3 new helper definitions + 20 call-site rewrites in `execLQHKEYREQ` |

No changes to: signal definitions, MTR result files, on-wire protocol,
configuration parameters, public NDB API.

## Inventory of call sites to rewrite

All sites are inside `Dblqh::execLQHKEYREQ` (DblqhMain.cpp:8915–9978).

### `earlyKeyReqAbort` sites (9 total)

| Source line | Pattern | Helper to call |
|---|---|---|
| 8933–8937 | `jam(); releaseSections(handle); earlyKeyReqAbort(…); return;` | `earlyKeyReqAbort_releasing` |
| 8973–8977 | `jam(); releaseSections(handle); earlyKeyReqAbort(…); return;` | `earlyKeyReqAbort_releasing` |
| 9032–9035 | `jam(); releaseSections(handle); earlyKeyReqAbort(…); return;` | `earlyKeyReqAbort_releasing` |
| 9107–9110 | `releaseSections(handle); earlyKeyReqAbort(…); return;` (jam at 9102) | `earlyKeyReqAbort_releasing` |
| 9125–9128 | same | `earlyKeyReqAbort_releasing` |
| 9193–9197 | `jam(); releaseSections(handle); earlyKeyReqAbort(…); return;` | `earlyKeyReqAbort_releasing` |
| 9376–9378 | `earlyKeyReqAbort(…); return;` (handle.clear() at 9375) | `earlyKeyReqAbort_simple` |
| 9388–9392 | `jam(); ndbassert(…); earlyKeyReqAbort(…); return;` | `earlyKeyReqAbort_simple` |
| 9434–9437 | `jam(); earlyKeyReqAbort(…); return;` | `earlyKeyReqAbort_simple` |

### `LQHKEY_abort` sites (5 total)

All are non-releasing (handle already cleared or never opened by this
point).

| Source line | Pattern | Helper |
|---|---|---|
| 9544 | `jam(); LQHKEY_abort(signal, 5, tcConnectptr); return;` | `LQHKEY_abort_cold` |
| 9557 | `LQHKEY_abort(signal, 5, tcConnectptr); return;` | `LQHKEY_abort_cold` |
| 9565 | `LQHKEY_abort(signal, 6, tcConnectptr); return;` | `LQHKEY_abort_cold` |
| 9842 | `LQHKEY_abort(signal, 0, tcConnectptr); return;` | `LQHKEY_abort_cold` |
| 9939 | `LQHKEY_abort(signal, 7, tcConnectptr); return;` | `LQHKEY_abort_cold` |

### `LQHKEY_error` sites (6 total)

| Source line | Pattern | Helper |
|---|---|---|
| 9402 | `LQHKEY_error(signal, 3, tcConnectptr); return;` | `LQHKEY_error_cold` |
| 9461 | `g_eventLogger->info(…); LQHKEY_error(signal, 2, tcConnectptr); return;` | keep info(), call helper |
| 9475 | `LQHKEY_error(signal, 0, tcConnectptr); return;` | `LQHKEY_error_cold` |
| 9479 | `LQHKEY_error(signal, 4, tcConnectptr); return;` | `LQHKEY_error_cold` |
| 9551 | `LQHKEY_error(signal, 5, tcConnectptr); return;` | `LQHKEY_error_cold` |
| 9845 | `LQHKEY_error(signal, 1, tcConnectptr); return;` | `LQHKEY_error_cold` |

**Total: 20 call-site rewrites.**

## Step 1 — Add the helper declarations to `Dblqh.hpp`

Insert immediately after the existing `earlyKeyReqAbort` declaration
at line 3927:

```cpp
  void earlyKeyReqAbort(Signal *signal, const class LqhKeyReq *lqhKeyReq,
                        Uint32 errorCode, TcConnectionrecPtr);

  // -- Outlined cold helpers used by execLQHKEYREQ. Each performs
  //    jamLine(callerLine) so the jam-ring entry shows the original
  //    call site, not this helper. Marked cold/noinline so the
  //    compiler emits them in .text.cold.* and keeps them out of
  //    execLQHKEYREQ's main body. See claude_files/
  //    execLQHKEYREQ_performance/item1_case.md.
  void earlyKeyReqAbort_releasing(Signal *signal,
                                  const class LqhKeyReq *lqhKeyReq,
                                  Uint32 errCode,
                                  Uint16 callerLine,
                                  SectionHandle &handle,
                                  TcConnectionrecPtr tcConnectptr)
      __attribute__((cold, noinline));
  void earlyKeyReqAbort_simple(Signal *signal,
                               const class LqhKeyReq *lqhKeyReq,
                               Uint32 errCode,
                               Uint16 callerLine,
                               TcConnectionrecPtr tcConnectptr)
      __attribute__((cold, noinline));
```

And immediately after the existing `LQHKEY_abort`/`LQHKEY_error`
declarations at lines 3563–3564:

```cpp
  void LQHKEY_abort(Signal* signal, int errortype, TcConnectionrecPtr);
  void LQHKEY_error(Signal* signal, int errortype, TcConnectionrecPtr);

  void LQHKEY_abort_cold(Signal *signal, int errortype,
                         Uint16 callerLine,
                         TcConnectionrecPtr tcConnectptr)
      __attribute__((cold, noinline));
  void LQHKEY_error_cold(Signal *signal, int errortype,
                         Uint16 callerLine,
                         TcConnectionrecPtr tcConnectptr)
      __attribute__((cold, noinline));
```

Use the GCC attribute spelling (`__attribute__((cold, noinline))`)
rather than the C++11 `[[gnu::cold]]` form to match existing NDB
style — most of the kernel uses `__attribute__` already.

## Step 2 — Add the helper definitions to `DblqhMain.cpp`

Insert immediately after `Dblqh::LQHKEY_error` (around DblqhMain.cpp:5694,
before `execLQHKEYREQF` at 5696):

```cpp
void Dblqh::earlyKeyReqAbort_releasing(Signal *signal,
                                       const LqhKeyReq *lqhKeyReq,
                                       Uint32 errCode,
                                       Uint16 callerLine,
                                       SectionHandle &handle,
                                       TcConnectionrecPtr tcConnectptr) {
  jamLine(callerLine);
  releaseSections(handle);
  earlyKeyReqAbort(signal, lqhKeyReq, errCode, tcConnectptr);
}

void Dblqh::earlyKeyReqAbort_simple(Signal *signal,
                                    const LqhKeyReq *lqhKeyReq,
                                    Uint32 errCode,
                                    Uint16 callerLine,
                                    TcConnectionrecPtr tcConnectptr) {
  jamLine(callerLine);
  earlyKeyReqAbort(signal, lqhKeyReq, errCode, tcConnectptr);
}

void Dblqh::LQHKEY_abort_cold(Signal *signal, int errortype,
                              Uint16 callerLine,
                              TcConnectionrecPtr tcConnectptr) {
  jamLine(callerLine);
  LQHKEY_abort(signal, errortype, tcConnectptr);
}

void Dblqh::LQHKEY_error_cold(Signal *signal, int errortype,
                              Uint16 callerLine,
                              TcConnectionrecPtr tcConnectptr) {
  jamLine(callerLine);
  LQHKEY_error(signal, errortype, tcConnectptr);
}
```

Note that `jamLine(line)` already takes a `Uint16` data payload and
records it in the jam ring under the helper's own `JAM_FILE_ID`. The
ring viewer will show two consecutive entries: the original
`callerLine` recorded as data, then the file/line of the helper's
own entry inside `earlyKeyReqAbort`. Both are visible in
`ndb_dump_jam_buffer` output, so call-site information is preserved.

## Step 3 — Rewrite call sites in `execLQHKEYREQ`

Each call site collapses to a single helper call + `return`. Below are
all 20 rewrites with the new code. **Source line numbers are pre-edit**
— line numbers will shift after the first edit, so I'll do the edits
top-down and update them as I go.

### Site 1 — DblqhMain.cpp:8932–8938

Before:
```cpp
      if (checkTransporterOverloaded(signal, all, lqhKeyReq)) {
        /* Overloaded, reject new work */
        jam();
        releaseSections(handle);
        earlyKeyReqAbort(signal, lqhKeyReq, ZTRANSPORTER_OVERLOADED_ERROR,
                         tcConnectptr);
        return;
      }
```

After:
```cpp
      if (checkTransporterOverloaded(signal, all, lqhKeyReq)) {
        /* Overloaded, reject new work */
        earlyKeyReqAbort_releasing(signal, lqhKeyReq,
                                   ZTRANSPORTER_OVERLOADED_ERROR,
                                   __LINE__, handle, tcConnectptr);
        return;
      }
```

### Site 2 — DblqhMain.cpp:8972–8978

Before:
```cpp
      ERROR_INSERTED(5098)) {
    jam();
    releaseSections(handle);
    earlyKeyReqAbort(signal, lqhKeyReq, ZTRANSPORTER_OVERLOADED_ERROR,
                     tcConnectptr);
    return;
  }
```

After:
```cpp
      ERROR_INSERTED(5098)) {
    earlyKeyReqAbort_releasing(signal, lqhKeyReq,
                               ZTRANSPORTER_OVERLOADED_ERROR,
                               __LINE__, handle, tcConnectptr);
    return;
  }
```

### Site 3 — DblqhMain.cpp:9030–9036

Before:
```cpp
    if (unlikely(!succ))
    {
      jam();
      releaseSections(handle);
      earlyKeyReqAbort(signal, lqhKeyReq, ZNO_TC_CONNECT_ERROR, tcConnectptr);
      return;
    }
```

After:
```cpp
    if (unlikely(!succ))
    {
      earlyKeyReqAbort_releasing(signal, lqhKeyReq,
                                 ZNO_TC_CONNECT_ERROR,
                                 __LINE__, handle, tcConnectptr);
      return;
    }
```

### Site 4 — DblqhMain.cpp:9100–9111

Before:
```cpp
  if (ERROR_INSERTED(5080) ||
      (unlikely((op == ZREAD || op == ZREAD_EX) && !getAllowRead()))) {
    jam();
    if (ERROR_INSERTED(5080))
    {
      g_eventLogger->info("Error due to ERROR_INSERT 5080");
    }
    releaseSections(handle);
    earlyKeyReqAbort(signal, lqhKeyReq, ZNODE_SHUTDOWN_IN_PROGRESS,
                     tcConnectptr);
    return;
  }
```

After:
```cpp
  if (ERROR_INSERTED(5080) ||
      (unlikely((op == ZREAD || op == ZREAD_EX) && !getAllowRead()))) {
    if (ERROR_INSERTED(5080))
    {
      g_eventLogger->info("Error due to ERROR_INSERT 5080");
    }
    earlyKeyReqAbort_releasing(signal, lqhKeyReq,
                               ZNODE_SHUTDOWN_IN_PROGRESS,
                               __LINE__, handle, tcConnectptr);
    return;
  }
```

### Site 5 — DblqhMain.cpp:9113–9129

Before:
```cpp
  if (ERROR_INSERTED(5081) ||
      unlikely(get_node_status(refToNode(tcRef)) != ZNODE_UP
#if defined(VM_TRACE) || defined(ERROR_INSERT)
               && !m_skip_tc_node_check
#endif
               ))
  {
    jam();
    if (ERROR_INSERTED(5081))
    {
      g_eventLogger->info("Error due to ERROR_INSERT 5081");
    }
    releaseSections(handle);
    earlyKeyReqAbort(signal, lqhKeyReq, ZNODE_SHUTDOWN_IN_PROGRESS,
                     tcConnectptr);
    return;
  }
```

After:
```cpp
  if (ERROR_INSERTED(5081) ||
      unlikely(get_node_status(refToNode(tcRef)) != ZNODE_UP
#if defined(VM_TRACE) || defined(ERROR_INSERT)
               && !m_skip_tc_node_check
#endif
               ))
  {
    if (ERROR_INSERTED(5081))
    {
      g_eventLogger->info("Error due to ERROR_INSERT 5081");
    }
    earlyKeyReqAbort_releasing(signal, lqhKeyReq,
                               ZNODE_SHUTDOWN_IN_PROGRESS,
                               __LINE__, handle, tcConnectptr);
    return;
  }
```

### Site 6 — DblqhMain.cpp:9191–9198

Before:
```cpp
      if (ERROR_INSERTED(5082) ||
          unlikely(!m_commitAckMarkerPool.seize(markerPtr))) {
        jam();
        releaseSections(handle);
        earlyKeyReqAbort(signal, lqhKeyReq, ZNO_FREE_MARKER_RECORDS_ERROR,
                         tcConnectptr);
        return;
      }
```

After:
```cpp
      if (ERROR_INSERTED(5082) ||
          unlikely(!m_commitAckMarkerPool.seize(markerPtr))) {
        earlyKeyReqAbort_releasing(signal, lqhKeyReq,
                                   ZNO_FREE_MARKER_RECORDS_ERROR,
                                   __LINE__, handle, tcConnectptr);
        return;
      }
```

### Site 7 — DblqhMain.cpp:9376–9379

Before:
```cpp
    handle.clear();
    if (totalAttrInfoLen > ZATTR_BUFFER_SIZE) {
      earlyKeyReqAbort(signal, lqhKeyReq, ZATTRINFO_TOO_LARGE, tcConnectptr);
      return;
    }
```

After:
```cpp
    handle.clear();
    if (totalAttrInfoLen > ZATTR_BUFFER_SIZE) {
      earlyKeyReqAbort_simple(signal, lqhKeyReq, ZATTRINFO_TOO_LARGE,
                              __LINE__, tcConnectptr);
      return;
    }
```

### Site 8 — DblqhMain.cpp:9387–9392

Before:
```cpp
    if (refToMain(senderRef) == DBSPJ) {
      jam();
      ndbassert(!LqhKeyReq::getNrCopyFlag(Treqinfo));
      /* Reply with NO_TUPLE_FOUND */
      earlyKeyReqAbort(signal, lqhKeyReq, ZNO_TUPLE_FOUND, tcConnectptr);
      return;
    }
```

After:
```cpp
    if (refToMain(senderRef) == DBSPJ) {
      ndbassert(!LqhKeyReq::getNrCopyFlag(Treqinfo));
      /* Reply with NO_TUPLE_FOUND */
      earlyKeyReqAbort_simple(signal, lqhKeyReq, ZNO_TUPLE_FOUND,
                              __LINE__, tcConnectptr);
      return;
    }
```

### Site 9 — DblqhMain.cpp:9432–9438

Before:
```cpp
    if (aggState != nullptr &&
        !getNodeInfo(refToNode(aggState->m_senderRef)).m_connected) {
      jam();
      earlyKeyReqAbort(signal, lqhKeyReq,
                        ZNODEFAIL_BEFORE_COMMIT, tcConnectptr);
      return;
    }
```

After:
```cpp
    if (aggState != nullptr &&
        !getNodeInfo(refToNode(aggState->m_senderRef)).m_connected) {
      earlyKeyReqAbort_simple(signal, lqhKeyReq,
                              ZNODEFAIL_BEFORE_COMMIT,
                              __LINE__, tcConnectptr);
      return;
    }
```

### Sites 10–14 — `LQHKEY_abort` rewrites

Pattern: replace
```cpp
LQHKEY_abort(signal, N, tcConnectptr);
return;
```
with
```cpp
LQHKEY_abort_cold(signal, N, __LINE__, tcConnectptr);
return;
```

Lines: 9544, 9557, 9565, 9842, 9939. Drop the preceding `jam()` if it
exists *only* to instrument the abort (sites 9543–9544); keep `jam()`
when it's documenting a non-abort branch decision earlier in the
basic block.

For site 9544 specifically:
```cpp
    if (unlikely(instanceNo == RNIL)) {
      LQHKEY_abort_cold(signal, 5, __LINE__, tcConnectptr);
      return;
    }
```

### Sites 15–20 — `LQHKEY_error` rewrites

Same pattern as the `LQHKEY_abort` rewrites. Lines: 9402, 9461, 9475,
9479, 9551, 9845.

For site 9461 (the one with the `g_eventLogger->info` call), keep the
info call inline:
```cpp
  if (unlikely((LqhKeyReq::FixedSignalLength + nextPos) !=
               signal->length())) {
    g_eventLogger->info("nextPos: %u, siglen: %u", nextPos, signal->length());
    LQHKEY_error_cold(signal, 2, __LINE__, tcConnectptr);
    return;
  }
```

## Step 4 — Build and verify the helpers are outlined

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/prod_build
make -j$(sysctl -n hw.ncpu) ndbmtd

# Confirm the four helper symbols exist
nm -n bin/ndbmtd | grep -E \
  "earlyKeyReqAbort_releasing|earlyKeyReqAbort_simple|LQHKEY_abort_cold|LQHKEY_error_cold"

# Confirm execLQHKEYREQ shrank
nm -n bin/ndbmtd | grep -A1 "execLQHKEYREQEP6Signal"
# Compute size delta vs the baseline 5960 bytes
```

Expected outcome:

- Four new symbols, all in the same text section (Mach-O on macOS
  doesn't separate `.text.cold` by default but the compiler still
  honours `noinline` so the bytes are no longer in the caller).
- `execLQHKEYREQ` size: target ≤ 5300 bytes (down from 5960).

If a helper was inlined despite `noinline`, check the attribute
spelling in the disassembly. With clang, occasionally `__attribute__`
on member functions needs to be on the declaration *and* the
definition.

## Step 5 — Functional test

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/mysql-test

# Tests that exercise the abort/error paths
./mtr ndb_basic ndb_dd_basic ndb_short_signal_format
./mtr --suite=rondis rondis_basic rondis_advanced
./mtr ndb_lock_basic
```

All must pass with no diff. The `ndb_lock_basic` and `ndb_dd_basic`
suites exercise transaction-abort paths that go through
`earlyKeyReqAbort` and `LQHKEY_abort`, so they're the most relevant.

## Step 6 — Crash-trail check

Trigger a deliberate abort and confirm the jam ring shows the original
caller's line.

Easiest path: `ERROR_INSERTED(5080)` (line 9100) forces the
"node-shutdown" early abort. Build a debug binary with
`WITH_ERROR_INSERT=ON`, set the error code, send any LQHKEYREQ, and
inspect the error log:

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/debug_build
make -j$(sysctl -n hw.ncpu) ndbmtd
# Run cluster, set error insert 5080 in DBLQH, do any read, look at
# the jam buffer in the trace file
```

The jam ring entry immediately preceding the crash should record line
**9100** (or whatever the post-edit line is for site 4) as the data
payload. If it shows the helper's internal line instead, the
`jamLine(callerLine)` call needs to move earlier in the helper.

## Step 7 — Performance measurement

```bash
# From the debug_build (testJoinAgg etc. live there)
cd /Users/mikael/mysql_trees/rondb_1051_performance/debug_build
make -j$(sysctl -n hw.ncpu) benchJoinAgg bench_q12_dbtc

# Run benchmarks 5 times each, take median
storage/ndb/block_unit_test/benchJoinAgg \
  -c "<connect_string>" -m <mysql_port>
storage/ndb/block_unit_test/bench_q12_dbtc \
  -c "<connect_string>" -m <mysql_port>
```

Record results in `claude_files/execLQHKEYREQ_performance/results.md`.
The expected outcome is **no measurable throughput delta** — Item 1's
value is structural (smaller body, cleaner baseline), not directly
performance-visible. A throughput regression would be a signal that
something went wrong (e.g. the helpers were not actually outlined and
the body grew, or the `noinline` is being ignored).

## Step 8 — Commit

Single commit per the repo's RONDB-1051 convention:

```
RONDB-1051: Outline execLQHKEYREQ error-return epilogues

The 9 earlyKeyReqAbort + 5 LQHKEY_abort + 6 LQHKEY_error call sites
inside Dblqh::execLQHKEYREQ each inlined a 5-25 instruction epilogue
(jam preamble, optional releaseSections, argument marshalling, the
abort/error call itself, and a branch back to the common return).
Total: 744 bytes / 186 instructions (12.5%) of the function body
spent on cold error plumbing interleaved with hot code.

Move the epilogues into four cold/noinline helpers that take the
caller's __LINE__ as an argument so the jam ring still identifies
the original site exactly as before.

Function size before: 5960 bytes (1490 instructions).
Function size after: <fill in> bytes.
```

## Effort estimate

- Step 1 (declarations): 10 minutes.
- Step 2 (definitions): 10 minutes.
- Step 3 (20 call-site rewrites): 60 minutes including
  re-checking each post-edit line number.
- Steps 4–6 (build, MTR, crash check): 60 minutes.
- Step 7 (benchmarks): 30 minutes.
- Total: ~3 hours including measurement.

## Things to flag during review

- The four helpers all funnel through existing `earlyKeyReqAbort` /
  `LQHKEY_abort` / `LQHKEY_error` — those bodies are unchanged, so
  reviewers should focus on the helpers themselves and the call-site
  rewrites.
- Argument order for the helpers puts `callerLine` after the existing
  arguments and before `tcConnectptr`. This keeps the diff-with-old
  clear at each site.
- One borderline case: site 9461 has a `g_eventLogger->info(…)` call
  before the error. We keep that inline (still in the hot path) — its
  cost is one branch on the never-taken side, so it doesn't warrant
  outlining and moving the info call into the helper would lose the
  caller's local variables.
- `__attribute__((cold, noinline))` must appear on both the
  declaration and the definition to be reliable across clang
  versions. The repo already uses this pattern in a few places — grep
  for `__attribute__((cold))` to confirm.
