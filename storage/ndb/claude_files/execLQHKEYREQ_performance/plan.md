# execLQHKEYREQ hot-path optimisation plan (RONDB-1051)

Starting point measurements, from `prod_build/bin/ndbmtd` (arm64,
`-O2 -g -DNDEBUG`, `WITH_ERROR_INSERT=OFF`, `WITH_NDB_DEBUG=OFF`):

| Metric | Value |
|---|--:|
| `Dblqh::execLQHKEYREQ` size | 5 960 bytes / 1 490 insns |
| Stack frame | 384 bytes |
| `bl` calls | 72 |
| Conditional branches | 142 |
| Unconditional branches | 76 |
| Inlined `jam()` sequences | 19 |
| Inlined `__ZTW18NDB_THREAD_TLS_JAM` stubs | 9 |
| Branches into tail region (+0x1300..+0x1748) | 43 |

Each `jam()` expands to a 7-instruction sequence that updates
`m_jamBuffer`'s ring (`ldr`, `ldr`, `add`, `add`, `str`, `add`, `and`,
`str`). Each `jamNoBlock()` is a real call via a TLS trampoline.

This plan is ordered by expected impact-to-effort. Each item has a
decision at the top: **Do**, **Investigate**, or **Skip** (with why).

---

## 0. ERROR_INSERT status — already resolved, just verify

**Decision:** Skip — no code change needed, but add a one-time
verification.

**What the analysis originally said.** Earlier notes claimed the
`ERROR_INSERTED(5047) || ERROR_INSERTED_CLEAR(5108) || …` disjunction at
`DblqhMain.cpp:8966` produced live code in prod. That was incorrect —
I misidentified the `NodeBitmask` iteration inside
`globalTransporterRegistry.get_status_overloaded()` at
`DblqhMain.cpp:8929` as an ERROR_INSERT probe.

**Actual state.** `storage/ndb/src/kernel/vm/pc.hpp:249–296` gates every
`ERROR_INSERTED(x)` macro on `#ifdef ERROR_INSERT`. With
`WITH_ERROR_INSERT=OFF`, each expands to the literal `false` and the
compiler eliminates the whole block.

**Verification step.** One sanity check so we don't need to re-argue
this. In `DblqhMain.cpp:8944` insert a temporary `#error` under
`#ifdef ERROR_INSERT` around the guarded blocks and confirm a prod build
still compiles. Also grep the disassembly for the literal constants the
guarded blocks set (`0x5079`, `5079`, `0x1fb3`) — they must not appear.
Remove the `#error` after the check.

**If a future build surfaces ERROR_INSERT bytes in the hot path.** The
right fix is then to wrap the whole multi-code guard at
`DblqhMain.cpp:8966–8978` in `#ifdef ERROR_INSERT` at source level (not
rely on macro expansion to `false`), and to move the one-off
`ERROR_INSERTED(5078) && …` block at `DblqhMain.cpp:8944–8964` into a
`[[gnu::cold, gnu::noinline]]` helper guarded by the same macro.

---

## 1. Outline the error-return epilogues

**Decision:** Do — highest impact, low risk.

**What.** Four distinct `earlyKeyReqAbort` return sequences, three
`LQHKEY_abort` sites and three `LQHKEY_error` sites are inlined mid-body.
Each carries its own `releaseSections(handle)` + `jam()` preamble
(~15 insns) before the call. Total: roughly 120 instructions of error
epilogue interleaved with hot code, and 22 `b 0x…+0x1734` landing-pad
branches at the tail for C++ exception unwind (never taken in prod).

**Concrete change.**

1. Add three `[[gnu::cold, gnu::noinline]]` helpers, private to
   `DblqhMain.cpp`:
   ```cpp
   NEVER_INLINE void Dblqh::earlyKeyReqAbort_cold(
       Signal* signal, const LqhKeyReq* req, Uint32 errCode,
       SectionHandle& handle, TcConnectionrecPtr tcConnectptr);
   NEVER_INLINE void Dblqh::LQHKEY_abort_cold(
       Signal* signal, int errCode, SectionHandle& handle,
       TcConnectionrecPtr tcConnectptr);
   NEVER_INLINE void Dblqh::LQHKEY_error_cold(
       Signal* signal, int errCode, SectionHandle& handle,
       TcConnectionrecPtr tcConnectptr);
   ```
   Each helper does `jam(); releaseSections(handle);` then calls the
   existing function. That moves the preamble out of the caller.

2. Replace the ~10 inlined `{ jam(); releaseSections(handle);
   earlyKeyReqAbort(...); return; }` blocks in `execLQHKEYREQ` (lines
   8933–8937, 8973–8977, 9107–9110, 9125–9128, 9193–9198, 9433–9437,
   9376–9378, 9391–9392, 9435–9437, etc.) with a single call to the
   matching cold helper + `return`.

3. Check the attribute combination actually keeps the callee out of
   the main body. Clang honours `__attribute__((cold))` by placing the
   function in a separate section; verify with `objdump -d` that the
   section for `execLQHKEYREQ` shrinks and the helpers are emitted
   adjacent to other cold functions.

**Expected saving.** ~80–120 bytes removed from the main body per
outlined site × ~10 sites = ~1000 bytes. Perhaps more important is
that the main-body fall-through is no longer punctuated by
cold-epilogue clusters, which reduces decoded-uop cache pressure and
front-end fetch stalls.

**Risk.** The `earlyKeyReqAbort` path currently runs with `jam()`
inlined into the caller's `JAM_FILE_ID`; if we move it into a helper,
the jam records will report the helper's file/line, not the original
caller. For crash forensics this matters. Mitigation: pass the caller's
`__LINE__` as a parameter and have the helper emit `jamLine(callerLine)`
before the abort call, so the jam ring still shows the original site.

**Verification.** Build debug + prod. Compare `size` of
`__ZN5Dblqh13execLQHKEYREQEP6Signal` in both. Run
`testJoinAgg`, `testJoinAggSpj`, `rondis_basic`, and the `ndb_*` MTR
suites — no behavioural change expected.

---

## 2. Outline `ndbrequire` failure arms

**Decision:** Do — medium impact, very low risk.

**What.** `ndbrequire(cond)` (pc.hpp:378–385) expands to
```cpp
if (likely(cond)) {} else { jamNoBlock(); progError(...); }
```
The `else` arm — a `jamNoBlock()` + `progError()` — is inlined at every
call site. In `execLQHKEYREQ` that produces 10 `progError` call
sequences in the tail (each ~10 insns of `adrp`/`add`/`mov` to set up
the file/line arguments), plus a hot-path compare+branch at each
check site. The branch is harmless; the inlined failure arm is the
bytes we want to reclaim.

**Concrete change.** Add a noreturn cold helper:
```cpp
[[noreturn, gnu::cold, gnu::noinline]]
static void ndbrequire_fail(int line, int code, const char* file,
                            const char* check);
```
Redefine the macro in a new `pc_opt.hpp` (or inside `pc.hpp` under a
build switch):
```cpp
#define ndbrequire(check) do { \
  if (unlikely(!(check))) ndbrequire_fail(__LINE__, NDBD_EXIT_NDBREQUIRE, \
                                          __FILE__, #check); \
} while (0)
```
This keeps the fast path (compare + predicted-not-taken conditional
branch) but moves the entire failure sequence to a single cold
function. The `jamNoBlock` call inside the helper is fine because
`progError` is already unreachable-return.

**Expected saving.** ~20 insns per `ndbrequire` call site in the main
body × ~10 sites in `execLQHKEYREQ` alone = ~200 insns reclaimed here,
plus a proportionally large effect across the whole kernel (hundreds
of blocks).

**Risk.** The `__FILE__` and `__LINE__` arguments still expand at the
caller, so the helper receives them — same crash logs as today.
Moving `jamNoBlock` into the helper means the TLS jam trampoline
(`__ZTW18NDB_THREAD_TLS_JAM`) is called from one place instead of
many; no semantic change.

**Verification.** Pick a block with an intentional ndbrequire failure
(there are existing MTR tests that trigger `NDBD_EXIT_NDBREQUIRE`) and
confirm the crash report still shows the right file/line. Diff the
disassembly of `execLQHKEYREQ` before/after.

---

## 3. Collapse the Treqinfo bit-slice flag expansion

**Decision:** Do — small savings but obvious and contained.

**What.** Between `DblqhMain.cpp:9253–9343` the function reads ~12
feature flags from `Treqinfo` (`getReplicaApplierFlag`, `getNoWaitFlag`,
`getDeferredConstraints`, `getDisableFkConstraints`,
`getNormalProtocolFlag`, `getNoTriggersFlag`, …) and `or`s each one
into `regTcPtr->m_flags`. In the disassembly this becomes a cluster of
~15 individual `tbz`/`tbnz` + conditional `orr`-into-m_flags sequences
(visible at +0x87c onward in the dump).

**Concrete change.** Replace the per-bit expansion with a mask-and-shift
build of the `m_flags` contribution:
```cpp
constexpr Uint32 TREQ_DEFERRED = LqhKeyReq::DeferredConstraintsBit;    // example
constexpr Uint32 TREQ_DISABLE_FK = LqhKeyReq::DisableFkBit;
constexpr Uint32 TREQ_NORMAL_PROTO = LqhKeyReq::NormalProtocolBit;
constexpr Uint32 TREQ_NO_TRIGGERS = LqhKeyReq::NoTriggersBit;
// … etc …

const Uint32 m = Treqinfo;
Uint32 addFlags = 0;
addFlags |= (m & TREQ_DEFERRED)   ? TcConnectionrec::OP_DEFERRED_CONSTRAINTS : 0;
addFlags |= (m & TREQ_DISABLE_FK) ? TcConnectionrec::OP_DISABLE_FK           : 0;
addFlags |= (m & TREQ_NORMAL_PROTO) ? TcConnectionrec::OP_NORMAL_PROTOCOL    : 0;
addFlags |= (m & TREQ_NO_TRIGGERS) ? TcConnectionrec::OP_NO_TRIGGERS         : 0;
regTcPtr->m_flags |= addFlags;
```
If the bit positions in `Treqinfo` can be chosen to match the bit
positions in `m_flags` (or are an affine transform), the compiler will
fold this into a single shift-and-or. Where that's not possible (e.g.
`ReplicaApplier` and `NoWait` depend on `senderVersion`), keep those
two as separate conditional `or`s.

**Expected saving.** ~15 insns out of the hot path. Minor, but removes
decode-width pressure where the body is dense with loads.

**Risk.** Low. All affected `get*Flag()` helpers are `static
constexpr` inline extractors — easy to audit against the new mask
layout. Test by bit-fuzzing: set each bit alone, observe that the
resulting `m_flags` matches the original code's output.

**Verification.** A unit test in `block_unit_test/` that constructs a
`LqhKeyReq`, sets each flag permutation, calls `execLQHKEYREQ` or a
wrapper, and asserts on the resulting `m_flags`. Alternatively, a
static assert that the new constants match the old extractor results.

---

## 4. Short-circuit `prepare_tab_pointers` on repeated same fragment

**Decision:** Investigate — promising, need to confirm cache-line
behaviour under batching.

**What.** `DblqhMain.cpp:9695–9696` unconditionally calls:
```cpp
c_tup->prepare_tab_pointers(fragptr.p->tupFragptr);
c_acc->prepare_tab_pointers(fragptr.p->accFragptr);
```
Function sizes: `Dbtup::prepare_tab_pointers` is 216 bytes,
`Dbacc::prepare_tab_pointers` is 84 bytes. They set
`m_curr_tup`/`m_curr_acc` tablerec pointers on the block. When
`execLQHKEYREQ` fires repeatedly for the same fragment (common in
key-batched traffic from a single TC), the work is redundant.

**Concrete change.** Cache the most-recently-prepared `tupFragptr` on
`Dbtup` (and similarly on `Dbacc`) and early-out inside the callee:
```cpp
void Dbtup::prepare_tab_pointers(Uint64 tupFragptrI) {
  if (likely(tupFragptrI == m_prepared_tup_fragptr_i)) return;
  m_prepared_tup_fragptr_i = tupFragptrI;
  // … existing body …
}
```
Make `m_prepared_tup_fragptr_i` a per-block (not per-signal) member so
the hit rate survives across signals.

**Investigation needed.**

- Does some other call path (scan, commit, abort) rely on
  `prepare_tab_pointers` running its body every time? If so, the cache
  must be invalidated in those paths.
- Is the cached pointer stable across fragment drops / table redefines?
  Likely we need to invalidate on `DROP_FRAG_REQ`/`ALTER_TAB_REQ` and
  on block restart.

**Expected saving.** Two `bl`s plus their prologue/epilogue skipped —
~40 insns and two I-cache line pulls on repeat requests. Proportional
to batch depth.

**Risk.** Medium — wrong invalidation could cause stale table
metadata. Needs careful audit of all callers.

**Verification.** MTR NDB suite, plus a targeted test that alternates
operations on two fragments and on one fragment, comparing cycle
counts.

---

## 5. Reduce unconditional `jam()` coverage

**Decision:** Investigate — potentially large win (up to ~150 insns in
this one function), but high behavioural risk if done wrong.

**What.** 19 inlined `jam()` sequences + 9 TLS `jamNoBlock()`
trampolines in `execLQHKEYREQ`. Across the whole kernel the total is
substantial. `jam()` is not a debug aid: it is always compiled in, and
the jam ring is the primary crash-forensics tool for NDB data nodes.

**Options, from least to most invasive.**

a) **Keep as-is.** Jam traces are load-bearing in production crash
   investigations. Don't touch without operational sign-off.

b) **Coalesce adjacent `jam()` calls.** Many `if (cond) { jam(); … }`
   pairs sit in tight succession. A single `jam()` at the top of each
   basic block would keep crash-site resolution at the "which branch
   did we take" level and still be unambiguous in the ring buffer
   when cross-referenced with the trap PC. This is a hand-edit pass,
   not a macro change. Expected saving: ~30–50 % of jam sites (~75
   insns in this function).

c) **Sparse jam with PC-based crash decoding.** Replace per-branch
   `jam()` with a single entry `jam()` per signal handler and rely on
   the PC at the crash to reconstruct the branch history. This
   requires a matching change in the crash-dump reader and is a big
   ecosystem change.

d) **Compile-time jam-level switch.** Add `WITH_NDB_JAM=FULL|SPARSE|OFF`
   to the build. Default to `FULL` for debug/dev, `SPARSE` for perf
   benchmarks, `OFF` for the absolute fastest path (no jam at all, no
   ring buffer maintenance). This gives us a dial without committing
   operationally.

**Recommendation.** Start with (b) — coalesce obvious redundant `jam()`
pairs in `execLQHKEYREQ`, `prepareContinueAfterBlockedLab`,
`handleUserUnlockRequest`, `exec_ACCKEYREQ`/`TUPKEYREQ`. Measure.
Decide on (c) or (d) based on the numbers.

**Risk.** High if we go too far. `jam()` trails are the primary way
field support teams reason about NDB crashes.

**Verification.** Trigger a forced `ndbabort()` in a MTR test and
confirm the jam trail produced by the changed code still identifies
the call site to sufficient granularity. A regression in crash
forensics is not acceptable.

---

## 6. Gate usage-stat and TTL-trace work

**Decision:** Do for TTL_RONDB_TRACE (verify compile-out); Investigate
for usage-stats.

**What.**

- `TTL_RONDB_TRACE` is used twice in `execLQHKEYREQ` (9241, 9272).
  Verify that in prod it expands to `(void)0`. If not, fix the macro.
- The usage-stats switch at `DblqhMain.cpp:9634–9660` runs on every
  request that is not NR-copy and not from RESTORE: a 5-branch
  `switch (op)` + increments on `readKeyReqCount` /
  `updKeyReqCount` / `insKeyReqCount` / `writeKeyReqCount` /
  `delKeyReqCount`, plus `m_keyReqAttrWords` and `m_keyReqKeyWords`
  accumulators. If the interpretation flag is set, a `getSection` +
  `getProgramWordCount` call is added (9663–9673).

**Concrete change.**

1. Confirm `TTL_RONDB_TRACE` is empty in prod with
   `objdump -d` around the two call sites. If not, rewrite the macro
   to `do {} while (0)` under `#ifndef DBUG_OFF` or equivalent.

2. For usage-stats: these feed management/monitoring views. We can't
   simply delete them. Options:
   - Move the full block into a single `[[gnu::cold]]` helper called
     via a single branch on a per-block `m_usage_stats_enabled` flag
     (manager-toggled).
   - Fold the 5 increments into one indexed increment — the `op`
     switch has 5 cases selecting 5 `Uint32` fields, which could be a
     single array index:
     ```cpp
     useStat.counters[op_to_stat_idx(op)]++;
     ```
     with `op_to_stat_idx` a small lookup table returning 0…4 (or
     `SIZE` meaning "skip"). That removes the 4-way cmp+branch.
   - Skip the interpreted-program word count when
     `totReclenAi == 0` (already short-circuited, but verify).

**Expected saving.** ~15–25 insns on the hot path when stats remain
enabled; more when disabled.

**Risk.** Low if we keep counts identical. Management views depend on
these fields.

**Verification.** Record stats output via `ndb_mgm -e "ALL REPORT …"`
before and after; counters must match bit-for-bit.

---

## 7. Cache the transporter-overload status

**Decision:** Investigate — small per-request saving, simple design.

**What.** `DblqhMain.cpp:8929`:
```cpp
const NodeBitmask &all = globalTransporterRegistry.get_status_overloaded();
if (unlikely(!all.isclear())) { … }
```
In the disassembly (+0x108..+0x138) this is a 64-word `Bitmask<64>`
iteration that loads up to 63 × 4-byte words until it finds one
non-zero. Common case (no node overloaded): first word is zero, one
load and one compare exit. Still, one load per request hitting a line
that is also written by the transporter thread = coherence traffic.

**Concrete change.** Maintain a `std::atomic<bool> m_any_overloaded`
updated whenever the transporter's overload mask becomes non-empty /
empties. The hot path reads `m_any_overloaded.load(relaxed)` — one
byte, likely in a predictable cache line.
```cpp
if (unlikely(globalTransporterRegistry.any_overloaded())) {
  const NodeBitmask &all = globalTransporterRegistry.get_status_overloaded();
  if (checkTransporterOverloaded(signal, all, lqhKeyReq)) { … }
}
```

**Expected saving.** Two cache-line-crossing loads replaced by a one-
byte atomic. Minor per-request, but nice under load.

**Risk.** Low, but the cache has to be updated anywhere the underlying
bitmask changes. Search `setOverloaded` / `clearOverloaded` call sites.

**Verification.** Force a node-overloaded condition (existing NDB
tests do this) and confirm `execLQHKEYREQ` still rejects new work.

---

## 8. Layout & register pressure in the function body

**Decision:** Skip for now — symptomatic, addressed by items 1–3.

**What.** 384-byte stack frame with visible spill slots at `[sp,
#0x30]` (ctransidHash pointer), `[sp, #0x4c]` (prefetched word),
`[sp, #0x58]`, `[sp, #0x5c]`, `[sp, #0x60]`, `[sp, #0x78]`, `[sp,
#0xb8]`. The compiler is at the edge of its register budget.

**Why skip.** Items 1–3 (outlining error epilogues, outlining
`ndbrequire` arms, collapsing flag bit-slices) all reduce live-range
pressure by removing instructions and constants from the main body.
Revisit frame size after those are merged — it should shrink without
further work.

**If it doesn't shrink.** Split `execLQHKEYREQ` at the natural "end
of local setup, start of hash insert" boundary (DblqhMain.cpp:9494)
into two functions, with the second marked `noinline`. That would
halve each function's register pressure.

---

## 9. Block-member layout tuning

**Decision:** Investigate once items 1–3 are in, before touching
layout.

**What.** `execLQHKEYREQ` touches `this` at offsets 0x5c58, 0x60c0,
0x61b0, 0x61c0, 0x6418, 0x6438, 0x6c58, 0x6c60, 0x6c68, 0x6ca8, 0x6cb0,
0x6cb8, 0x6d18, 0x6d28, 0x7098. That's ~15 distinct locations, packing
into roughly 2–3 actual 128-byte cache lines (Apple M-series) or 4–5
64-byte lines (Intel). Access pattern looks already-clustered, but
worth confirming.

**Concrete step.** After the other items land, dump actual
load/store addresses from a perf run and measure L1-D miss rate at
this function. Only if misses are material, rearrange members of
`Dblqh` so all hot-path reads fit in two cache lines.

**Risk.** Member reordering can break ABI assumptions elsewhere (e.g.
`#ifdef VM_TRACE` fields that shift). Very mechanical change but
needs full rebuild and test.

---

## Execution sequence

1. **Item 0** — verify ERROR_INSERT compile-out (one build test).
2. **Item 1** — outline error epilogues. Measure function size.
3. **Item 2** — outline `ndbrequire` failure arm. Measure again.
4. **Item 3** — collapse Treqinfo bit-slice.
5. **Item 6** — verify TTL_RONDB_TRACE compile-out; fold usage-stat
   increments into indexed form.
6. **Item 4** — `prepare_tab_pointers` short-circuit (requires
   fragment-cache audit across blocks).
7. **Item 7** — transporter-overload cached flag.
8. **Item 5** — jam-coverage reduction (requires operational
   sign-off).
9. **Item 8 & 9** — only if still needed after the above.

## Measurement protocol

Each step should be accompanied by:

- `objdump` size of `__ZN5Dblqh13execLQHKEYREQEP6Signal` before/after.
- Instruction count (`count_text_insns.py` or `size` + ARM64 4-byte
  divisor).
- `benchJoinAgg` and `bench_q12_dbtc` throughput, median of 5 runs.
- MTR `ndb_*` and `rondis_*` suites must pass.

A running table in `results.md` beside this plan will track each
step's delta.
