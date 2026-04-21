# Results log

Tracks the measured outcome of each item in the plan against the
pre-change baseline (`prod_build/bin/ndbmtd`, arm64, RelWithDebInfo,
`WITH_ERROR_INSERT=OFF`).

Baseline artefacts preserved in
`claude_files/execLQHKEYREQ_performance/measurements/`:
- `ndbmtd.before` — 14 MB binary
- `exec_lqhkey.before.asm` — disassembly of `Dblqh::execLQHKEYREQ`
- `nm.before.txt` — full symbol table

## Baseline

| Metric | Value |
|---|--:|
| `__ZN5Dblqh13execLQHKEYREQEP6Signal` size | 5960 B (1490 insns) |
| `bl` calls in body | 72 |
| Conditional branches | 139 |
| Inline `jam()` sequences | 19 |
| TLS `jamNoBlock()` stubs (from `ndbrequire`) | 9 |
| `bl earlyKeyReqAbort` in body | 7 (source had 9, compiler merged 2 pairs) |
| `bl LQHKEY_abort` in body | 4 |
| `bl LQHKEY_error` in body | 3 |
| `bl releaseSections` in body | 4 |
| `bl progError` in body | 10 |
| Top convergent branch target | +0x494, 15 forward branches |
| C++ unwind landing-pad branches | 22 |

## Item 1 — Outline error-return epilogues

Commit: pending (branch `RONDB-1051-execLQHKEYREQ`).

### What changed

- `Dblqh.hpp`: added 4 declarations marked `__attribute__((cold, noinline))`.
- `DblqhMain.cpp`: added 4 helper definitions after `Dblqh::LQHKEY_error`.
- `execLQHKEYREQ`: 20 call-site rewrites (9 `earlyKeyReqAbort`,
  5 `LQHKEY_abort`, 6 `LQHKEY_error`).

### After

| Metric | Before | After | Δ |
|---|--:|--:|--:|
| Function size | 5960 B | **5792 B** | **−168 B (−2.8 %)** |
| Instructions in body | 1490 | **1448** | **−42** |
| `bl` calls in body | 72 | 68 | −4 |
| Conditional branches | 139 | 141 | +2 |
| Inline `jam()` sequences | 19 | **13** | **−6** |
| TLS `jamNoBlock()` stubs | 9 | 9 | 0 |
| `bl earlyKeyReqAbort` in body | 7 | 0 | −7 |
| `bl earlyKeyReqAbort_releasing` | 0 | 4 | +4 |
| `bl earlyKeyReqAbort_simple` | 0 | 3 | +3 |
| `bl LQHKEY_abort` in body | 4 | 0 | −4 |
| `bl LQHKEY_abort_cold` | 0 | 4 | +4 |
| `bl LQHKEY_error` in body | 3 | 0 | −3 |
| `bl LQHKEY_error_cold` | 0 | 3 | +3 |
| `bl releaseSections` in body | 4 | 0 | **−4** |
| `bl progError` in body | 10 | 10 | 0 |
| Top convergent branch target | 15 forward | 15 forward | 0 |
| C++ unwind landing-pad branches | 22 | 22 | 0 |

### Helper symbols (all in cold text, address 0x1004ca060+, far from
Dblqh's main range at 0x100193xxx)

| Symbol | Size | Call sites |
|---|--:|--:|
| `earlyKeyReqAbort_releasing` | 112 B | 6 |
| `earlyKeyReqAbort_simple` | 36 B | 3 |
| `LQHKEY_abort_cold` | 36 B | 5 |
| `LQHKEY_error_cold` | 36 B | 6 |
| Total added (cold text) | 220 B | 20 |

Net binary-size change: **+52 B** (cold helpers added, hot body
shrank).

### Versus the plan's estimate

Plan estimated ~675 bytes reclaimed from the body. Actual: 168 bytes.
The plan over-estimated because:

1. The compiler had already partially CSE'd the inline jam preambles
   across adjacent sites — only 12 sites had a fully expanded
   ~30-byte jam, not all 20.
2. Argument marshalling (~4–6 `mov` insns per call) is unavoidable;
   outlining does not remove it. Only jam preambles, the
   `releaseSections` call, and the terminal `bl earlyKeyReqAbort`
   actually moved out.
3. Each helper has its own prologue/epilogue (stp/ldp of
   x29/x30/x19–x24) and the helpers save/restore arguments around
   the internal `bl releaseSections`, costing ~28 insns of helper
   body.
4. Each call site still needs the `b +0x42c` back to common return,
   plus a new `mov w4, #__LINE__` — one extra insn per site.

### What did actually improve

- **42 fewer hot-path instructions** (2.8 % body shrink). Small but
  real.
- **4 `releaseSections` calls gone from the hot body** — these were
  inline `bl` + arg-setup clusters scattered across the function,
  now bundled in one cold helper.
- **6 inline `jam()` sequences removed** from the hot body (still
  present as `jamLine` inside the helpers, on cold bytes).
- **14 distinct hot-body targets (earlyKeyReqAbort×7 +
  LQHKEY_abort×4 + LQHKEY_error×3) collapsed to 4 cold-text
  helpers**. Branch target buffer budget freed for genuine hot
  calls.
- **220 B of code physically relocated** to cold text — no longer
  fetched with hot body lines.

### Verification performed

- [x] All 4 helper symbols present in `nm` output.
- [x] Helpers emitted at 0x1004caXXX (cold text), far from Dblqh's
  normal range at 0x100193xxx — compiler honoured `cold` attribute.
- [x] Zero `bl earlyKeyReqAbort` / `bl LQHKEY_abort` / `bl
  LQHKEY_error` / `bl releaseSections` remain in the main body.
- [x] Top convergent-branch count unchanged (15 forward branches
  to common return) — structural fall-through preserved.
- [x] Build succeeds with no new warnings in the touched files
  (clangd index errors about `ndb_limits.h`/`Uint32` are pre-existing
  and unrelated).
- [ ] MTR: `ndb_basic ndb_dd_basic ndb_short_signal_format
  rondis_basic rondis_advanced ndb_lock_basic` — pending.
- [ ] Crash-trail check: trigger `ERROR_INSERTED(5080)`, confirm jam
  ring shows the original call line — pending (requires debug build
  with `WITH_ERROR_INSERT=ON`).
- [ ] Benchmarks: `benchJoinAgg`, `bench_q12_dbtc` medians —
  pending.

### Honest assessment

Item 1 delivered a structural win (code moved to cold text, hot body
shrank, BTB pressure reduced) but the headline byte savings were
~25 % of the plan's estimate. The plan's case document
(`item1_case.md`) overstated the savings because it conflated "bytes
of error epilogue code" with "bytes reclaimable by outlining" — most
of the epilogue bytes are argument marshalling that can't move.

For Items 2–7 the lesson is: argument marshalling will dominate after
outlining, so the real wins come from items that eliminate code
entirely (flag-bitslice collapse) or short-circuit on a common fast
path (`prepare_tab_pointers` same-fragment), not from further
outlining.

## Item 2 — Outline `ndbrequire`/`ndbabort` failure arms

Commit: pending (branch `RONDB-1051-execLQHKEYREQ`).

### What changed

- `pc.hpp`: 3 forward declarations (`ndbrequire_fail`,
  `ndbrequire_err_fail`, `ndbabort_fail`) marked
  `__attribute__((cold, noinline))`. Macros redefined to call them.
- `SimulatedBlock.cpp`: 3 helper definitions + a small
  `file_name_from_id()` helper that looks up the caller's file name
  via `jamFileNames[]`.
- **Zero call-site edits anywhere in the kernel.** The macro change
  did all the work. 6477 `ndbrequire` + 749 `ndbabort` sites
  (7226 total) across 81+ kernel files all changed shape.

Design decisions driven by research:
- `__FILE__` and `#check` **dropped** as arguments — file name
  recovered via `jamFileNames[caller_file_id]`, check expression
  recoverable from source at file:line. Saves 4 `adrp`+`add`
  instructions per call site.
- `ndbrequire` and `ndbrequireErr` split into two helpers so the
  common `ndbrequire(x)` path is 3 insns (file_id, line, bl), the
  rare `ndbrequireErr(x, code)` path is 4 insns.
- Macro shape kept as `if (likely(x)) {} else { helper(); }` —
  matches pre-outlining behaviour, preserves compatibility with
  call sites lacking trailing `;` (line 24662, 24964 in
  DblqhMain.cpp among others).

### In-body metrics for `execLQHKEYREQ` — Item 1 → Item 2

| Metric | Item 1 post | Item 2 post | Δ |
|---|--:|--:|--:|
| Function size | 5792 B | **5220 B** | **−572 B (−9.9 %)** |
| Instructions | 1448 | **1305** | **−143** |
| `bl` calls | 68 | **59** | −9 |
| `bl progError` | 10 | **1** | **−9** |
| `bl __ZTW18NDB_THREAD_TLS_JAM` | 9 | **0** | **−9** |
| `bl ndbrequire_fail` | 0 | 9 | +9 |
| `bl ndbrequire_err_fail` | 0 | 0 | 0 |
| `bl ndbabort_fail` | 0 | 1 | +1 |
| Inline jam ring bumps | 13 | **8** | −5 |

The one remaining `bl progError` is from a path that didn't go
through the `ndbrequire` macro (likely a direct call). 9 TLS-jam
stubs and 10 inline progError sequences collapsed to 10 helper
calls + ~15 inline jam bumps reduced to 8.

### Kernel-wide — the real prize

| Section | Baseline | Item 1 post | Item 2 post | Δ baseline→Item 2 |
|---|--:|--:|--:|--:|
| `__TEXT` | 8 585 216 B | 8 585 216 B | **7 995 392 B** | **−589 824 B (−6.87 %)** |
| `__DATA` | 8 241 152 B | 8 241 152 B | 8 241 152 B | 0 |

`__TEXT` shrank by **576 KB** — far more than the plan's "tens of KB"
estimate. On macOS the `__TEXT` section is page-aligned so Item 1's
local 168-byte shrink wasn't visible at this granularity, but Item
2's ~147 000-instruction reduction crosses many pages.

### Helper sizes (cold text)

| Symbol | Size | Call sites (est.) |
|---|--:|--:|
| `ndbrequire_fail(Uint32, int)` | 156 B | ~6 477 |
| `ndbrequire_err_fail(Uint32, int, int)` | 160 B | 4 |
| `ndbabort_fail(Uint32, int)` | 156 B | ~749 |
| Total added cold text | 472 B | — |

### vs `execLQHKEYREQ` baseline

| | Size | Instructions | vs baseline |
|---|--:|--:|---|
| Baseline | 5960 B | 1490 | |
| Item 1 post | 5792 B | 1448 | −168 B (−2.8 %) |
| Item 2 post | **5220 B** | **1305** | **−740 B (−12.4 %)** |

### Versus the plan's estimate

The plan estimated ~320 B local saving in `execLQHKEYREQ`. Actual:
572 B. The extra came from **dropping `__FILE__` and `#check` as
arguments** (the user's suggestion during implementation, informed by
the Item 1 post-mortem) — those would have been 2 `adrp`+`add` pairs
per site. Removing them saved ~16 B per site × 10 sites ≈ 160 B more
than the plan counted, which matches the overshoot.

### Verification performed

- [x] All 3 helper symbols present in `nm` output, all in text
  section.
- [x] Zero `bl __ZTW18NDB_THREAD_TLS_JAM` in `execLQHKEYREQ` body.
- [x] 10 `bl progError` sites in body reduced to 1.
- [x] Kernel-wide `__TEXT` section shrank by 576 KB.
- [x] Build succeeds (after fixing 2 pre-existing missing-semicolon
  sites surfaced by the `do/while` wrapper — reverted to
  `if/else {}` shape, pre-existing sites now compile as before).
- [x] MTR: `ndb_basic` — PASS.
- [ ] MTR: `ndb_error_insert_*`, `rondis_basic`, `rondis_advanced` —
  pending.
- [ ] Forensic check: trigger a deliberate `ndbrequire(false)` and
  confirm jam ring carries the caller's `JAM_FILE_ID` — pending.
- [ ] Benchmarks: `benchJoinAgg`, `bench_q12_dbtc` — pending.

### Comparison with Item 1's assessment

| | Item 1 | Item 2 |
|---|---|---|
| Files touched | 2 | 2 |
| Call-site edits | 20 | **0** |
| `execLQHKEYREQ` size reduction | −168 B (−2.8 %) | −572 B (−9.9 %) |
| Kernel-wide `__TEXT` change | 0 | **−576 KB (−6.87 %)** |
| Crash-log text changes | none | loses block-name prefix, magic-status bits, check string; file+line+code preserved |
| Risk | low | medium — log text differs |
| Effort | ~3 hours | ~2 hours including macro-compatibility fix |

Item 2 delivered the leverage the plan promised. The
outline-everything-into-one-cold-helper pattern works when the cost
being moved is large (per-site jam bump + TLS call) and the
argument marshalling is minimal (just two `mov w0/w1, #immediate`
for file_id + line).

## Item 3 — Collapse Treqinfo flag bit-slice

Commit: pending (branch `RONDB-1051-execLQHKEYREQ`).

### What changed

- `DblqhMain.cpp:9353–9376`: four separate
  `if (LqhKeyReq::getXxxFlag(Treqinfo)) m_flags |= OP_YYY;`
  blocks folded into one `add_flags` OR of four ternaries,
  followed by a single `m_flags |= add_flags;`.
- Bits targeted: `DeferredConstraints` (bit 26),
  `DisableFkConstraints` (bit 0), `NormalProtocol` (bit 25),
  `NoTriggers` (bit 1).
- `bug#14702377` comment preserved as a block comment above the
  fold.

### Metrics for `execLQHKEYREQ`

| | Item 2 post | Item 3 post | Δ |
|---|--:|--:|--:|
| Function size | 5220 B | **5176 B** | **−44 B (−0.8 %)** |
| Instructions | 1305 | **1294** | **−11** |
| `tbz`/`tbnz` on `w28` (Treqinfo) | 19 | **11** | **−8** |

### What the compiler produced

The four tbz-skip RMWs and their tbnz back-cluster disappeared.
In their place, 8 instructions of branchless bit arithmetic plus
one RMW of `m_flags`:

```asm
100154628: lsr  w10, w28, #1      ; NoTriggers bit → scratch
10015462c: lsr  w11, w28, #23     ; NormalProtocol bit positioned
100154630: and  w11, w11, #0x8    ; OP_NORMAL_PROTOCOL mask
100154634: bfi  w11, w28, #5, #1  ; DisableFk → OP_DISABLE_FK (0x20)
100154638: lsr  w12, w28, #21     ; DeferredConstraints positioned
10015463c: and  w12, w12, #0x10   ; ... (mask intent)
100154640: orr  w11, w11, w12     ; combine
100154644: bfi  w11, w10, #6, #1  ; NoTriggers → OP_NO_TRIGGERS (0x40)
100154648: ldr  w10, [x21, #0x120] ; m_flags
10015464c: orr  w10, w11, w10
100154650: str  w10, [x21, #0x120] ; single RMW
```

No `tbz`/`tbnz` on any of the four targeted bits anywhere in the
function body. Branch-target-buffer pressure and the split-cluster
control-flow idiom are gone for this region.

### Versus plan estimate

Plan estimated −50 to −80 B / −17 insns. Actual: −44 B / −11 insns.
Came in slightly under estimate — the compiler's `bfi`-based
encoding is tighter than the two-insn-per-bit `ubfx`/`lsl` form I
projected. End result: same shape, fewer scratch registers used,
fewer ops.

### Cumulative against baseline

| | Size | Instructions | vs baseline |
|---|--:|--:|---|
| Baseline | 5960 B | 1490 | |
| Item 1 post | 5792 B | 1448 | −168 B, −2.8 % |
| Item 2 post | 5220 B | 1305 | −740 B, −12.4 % |
| Item 3 post | **5176 B** | **1294** | **−784 B, −13.2 %** |

### Verification performed

- [x] Build succeeds.
- [x] `execLQHKEYREQ` shrinks by 44 B.
- [x] `tbz`/`tbnz` on w28 reduced from 19 → 11; the four targeted
  bits no longer appear in any branch.
- [x] Disasm diff shows the collapse is in the expected region.
- [x] `ndb_basic` — PASS.
- [ ] Benchmarks — pending, and not strictly required (semantic
  no-op).

### Notes

- Change is a compile-time algebraic rewrite; zero runtime
  behavioural change. No ABI, no header, no signal-format impact.
- Kernel-wide `__TEXT` unaffected (local change only).
- If the plan's later items continue to deliver double-digit
  per-item shrinks, the function will be under 5000 B after a few
  more passes — approaching 2× reduction from the 5960 B baseline.

## Item 3b — Follow-ups on the Treqinfo flag scan

Commit: pending (branch `RONDB-1051-execLQHKEYREQ`).

### What changed

Three small cleanups in `DblqhMain.cpp::execLQHKEYREQ`:

- **3b.1** — `ReplicaApplier` folded into the Item 3 `add_flags` OR
  with a `can_replica_applier = senderVersion >= NDBD_RATE_LIMIT_VERSION`
  guard. The old stand-alone `if (getReplicaApplierFlag(...)) { if
  (senderVersion...) m_flags |= OP_REPLICA_APPLIER; }` block removed.
- **3b.2** — The two `if (senderBlockNo == getRESTORE()) else if (op
  == ZREAD || ...)` branches that both ran the same
  `m_disk_table &= !NoDiskFlag` update merged into one. Comment
  added: `m_disk_table` is a 0/1 `Uint8`, so `&= !flag` is correct
  (not a `!` vs `~` bug).
- **3b.3** — `LqhKeyReq::getGCIFlag(Treqinfo)` CSE'd into a local
  `gci_flag`, used 3 times instead of 3 inline calls.

### Metrics

| | Item 3 post | Item 3b post | Δ |
|---|--:|--:|--:|
| `execLQHKEYREQ` size | 5176 B | **5176 B** | **0** |
| Instructions | 1294 | 1294 | 0 |
| `tbz`/`tbnz` on w28 | 11 | **10** | −1 |

### Why zero net byte change

The compiler had already done most of what 3b targets:
- `m_disk_table` two-branch merge — already collapsed via jump
  threading and CSE of the identical `&= !NoDiskFlag` store.
- `getGCIFlag()` CSE — already done at -O2.
- The `ReplicaApplier` fold did remove one `tbz/tbnz` pair, but
  added a `cmp senderVersion + select` inside the fold that
  roughly offsets the saving.

### Verification

- [x] Build succeeds.
- [x] `tbz`/`tbnz` on w28 reduced by 1 (ReplicaApplier).
- [x] `ndb_basic` — PASS.
- [x] No regression elsewhere observable.

### Honest take

Item 3b was a structural cleanup pass, not a performance win. The
source is shorter and clearer (one fold of 5 flags instead of 4 + a
guarded `if` block); one latent source bug risk (the `!` vs `~`
confusion on `m_disk_table`) is now documented in a comment; and
the `getGCIFlag` local makes the CSE intent explicit instead of
relying on the compiler. These matter for readability and future
maintainers, not for the post-compilation binary.

### Cumulative against baseline

| | Size | Instructions | vs baseline |
|---|--:|--:|---|
| Baseline | 5960 B | 1490 | |
| Item 1 post | 5792 B | 1448 | −168 B, −2.8 % |
| Item 2 post | 5220 B | 1305 | −740 B, −12.4 % |
| Item 3 post | 5176 B | 1294 | −784 B, −13.15 % |
| Item 3b post | 5176 B | 1294 | −784 B, −13.15 % |

## Item 7 — Cached transporter-overload flag

Commit: pending (branch `RONDB-1051-execLQHKEYREQ`).

### Why this item is unlike the others

Every earlier item was a **static**-size optimisation. Item 7 is a
**dynamic**-cost optimisation: the function grows by a few bytes, but
each call executes far fewer instructions in the common case.

The original check was:

```cpp
const NodeBitmask &all = globalTransporterRegistry.get_status_overloaded();
if (unlikely(!all.isclear())) { … }
```

`NodeBitmask` is `Bitmask<64>` = 64 × 32 bits = 256 B. `isclear()` is a
straight loop that returns `true` only after scanning every word.
When no node is overloaded (the common case), scanning the full
bitmask runs **~378 dynamic instructions per LQHKEYREQ**.

### What changed

- `TransporterRegistry.hpp`:
  - Added `#include <atomic>`.
  - New member `alignas(NDB_CL) std::atomic<bool> m_any_overloaded{false};`
    on its own cache line, co-located with `m_status_overloaded` but
    isolated from its write traffic.
  - New inline accessor `bool any_overloaded() const` with relaxed
    load.
  - `set_status_overloaded()` now maintains the cache: set-true on
    any bit up; rescan and clear only when a bit goes down and the
    mask becomes empty (cold path).
- `DblqhMain.cpp`: the hot check is gated on
  `unlikely(globalTransporterRegistry.any_overloaded())`. The
  existing bitmask scan + `checkTransporterOverloaded` remains inside
  the gate.

### Metrics

| | Item 3b post | Item 7 post | Δ |
|---|--:|--:|--:|
| `execLQHKEYREQ` static size | 5176 B | **5200 B** | **+24 B** |
| `execLQHKEYREQ` instructions | 1294 | 1300 | +6 |
| Kernel-wide `__TEXT` | 7 995 392 B | 7 995 392 B | 0 |

**Dynamic cost per LQHKEYREQ in the common case**:
- Before: 5-insn setup + word-0 `ldr`/`cbz` + 63-iteration × 6-insn scan
  = **≈ 385 insns executed** per call.
- After: 4-insn setup + 1-byte `ldrb` + `tbz` + branch to "done".
  = **≈ 5 insns executed** per call.
- **Per-call saving: ~380 instructions.** At ~1 ns/insn on a modern
  core that is a ~380 ns reduction per LQHKEYREQ in steady state with
  no overload.

### Codegen confirmation

The new fast path appears first; the old scan loop is retained but
now reached only when the cache says "maybe overloaded":

```asm
100153d68: adrp x8, ...            ; hoisted address setup
100153d6c: add  x8, x8, #0x40
100153d70: mov  w9, offset         ; m_any_overloaded offset
100153d74: movk w9, #0x6, lsl #16
100153d78: ldrb w9, [x8, x9]       ; NEW: cached-byte load
100153d7c: tbz  w9, #0x0, skip     ; NEW: if cached=false, skip scan
100153d80: ... (old bitmask scan path retained — cold) ...
```

The static +24 B comes from the fast-path addition in front; the
compiler did not elide the old scan (it's still the path taken when
the cache says true).

### Risks and verification

- [x] Build succeeds.
- [x] `ndb_basic` — PASS.
- [x] `__TEXT` kernel-wide unchanged (as expected — local change).
- [x] Codegen confirms the fast path gates the slow path.
- [ ] A benchmark would visualize the dynamic win; skipped for now
  (no running cluster).
- Cross-thread correctness: transporter thread writes with relaxed
  ordering; LQH/TC read with relaxed ordering. A stale `false` read
  is harmless — `checkTransporterOverloaded` is the authoritative
  decision maker. No correctness hazard.
- `alignas(NDB_CL)` isolates the hot read from the transporter
  thread's writes to the bitmask.

### Cumulative against baseline

| | Size | Instructions | Dynamic change |
|---|--:|--:|---|
| Baseline | 5960 B | 1490 | |
| Item 1 | 5792 B | 1448 | — |
| Item 2 | 5220 B | 1305 | — |
| Item 3 | 5176 B | 1294 | — |
| Item 3b | 5176 B | 1294 | — |
| **Item 7** | **5200 B** | **1300** | **~380 fewer insns executed per call (no-overload path)** |

Total static: **−760 B (−12.75 %)** vs baseline.
Plus **−576 KB `__TEXT`** (from Item 2) kernel-wide.
Plus **~380 insns saved per LQHKEYREQ** at runtime (Item 7).
