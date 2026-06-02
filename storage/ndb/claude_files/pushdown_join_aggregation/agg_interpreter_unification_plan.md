# AggInterpreter ↔ JoinAggInterpreter Unification — Analysis & Plan

**Status:** Steps 1 and 2 **complete**.  Step 1 (sub-steps 1.1 + 1.5 + 1.2 +
1.3 + 1.4) deduplicated the entire compute engine (~1,900 lines).  Step 2
(sub-steps 2a + 2b + 2c) swapped AggInterpreter's `std::map` + inline
`m_mem_buf` bump pool for the shared `JoinGBHashTable` (1024 buckets) plus
the chunk allocator that JoinAggInterpreter already used — same memory
model on both interpreters.  All shipped and verified (build + agg test
suite + RonSQL/CTE MTR green).  Step 3 (full collapse decision) not
started.
**Goal:** the two interpreters share a large amount of duplicated code.
`JoinAggInterpreter` has the better (more scalable) memory model. (1) Remove the
duplication, then (2) make `AggInterpreter` use `JoinAggInterpreter`'s memory model
as well. Whether to ultimately fold `AggInterpreter` away entirely is **deferred**
until it is clearer how much distinct code is left after (1) and (2).

### Approach agreed with the maintainer
1. **Step 1 — deduplicate first.** Extract the shared aggregation engine so both
   interpreters call one copy of it. Zero behavior change.
2. **Step 2 — adopt JoinAgg's memory model in AggInterpreter.** Replace
   `AggInterpreter`'s `std::map` + 8 KB inline bump allocator with
   `JoinAggInterpreter`'s `GBHashTable` + 32 KB-page chunk allocator (shared code).
   **`AggInterpreter` keeps its own streaming per-batch drain to the NDB API** — the
   drain is the normal-scan result-shipping path, it is simply re-pointed at the new
   container. CTE / merge / redistribute machinery is **not** brought into
   `AggInterpreter`.
3. **Step 3 — decide later.** After Steps 1–2, assess the remaining delta between
   the two classes and decide whether to collapse them into one.

### Two clarifications that shape the plan
- **The streaming drain is not new work.** `AggInterpreter::PrepareAggResIfNeeded` /
  `NumOfResRecords` already ship normal-scan aggregation results to the NDB API in
  per-batch `Signal`s. This is needed for the normal-scan-result-to-API case and is
  **not** used by CTEs (which redistribute/merge across nodes instead). Step 2
  preserves this drain and adapts it from `std::map` iterate/erase to `GBHashTable`
  iterate + `freeGroupData` — the same emit→erase→free that `evictOneGroup` already
  performs for one group.
- **CTE specials stay out of AggInterpreter.** Linked attributes, NULL-extended
  (outer-join) rows, multi-leaf, CTE-mode attrId rewrite, cross-LDM mutex, group
  eviction-to-DBLQH, and cross-node merge/redistribute are join/CTE concerns. They
  remain `JoinAggInterpreter`-only; the normal scan never engages them.

Files in scope:
- `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.{hpp,cpp}` (230 + 2504 lines)
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.{hpp,cpp}` (357 + 3015 lines)
- `storage/ndb/src/kernel/blocks/dbtup/PushdownInterpreter.{hpp,cpp}` (base + factory)
- `storage/ndb/src/kernel/blocks/dbtup/AggHashTable.hpp` (shared structs, both memory models)
- `storage/ndb/include/ndbapi/NdbAggregationCommon.hpp` (`Register`/`AggResItem`/`GBHashEntry`/constants)

---

## 1. Executive summary

`AggInterpreter` and `JoinAggInterpreter` are **the same aggregation engine forked
twice**. The opcode interpreter, the numeric accumulator kernels, the embedded-program
validator, the program optimizer, the entire string MIN/MAX sidecar, the wire
format, and the per-row processing loop are duplicated almost verbatim — roughly
**1,900–2,100 lines of near-identical code** across the two `.cpp` files. The only
genuine divergence is:

1. **The group container + allocator** — `std::map` + an 8 KB inline bump
   allocator (Agg) vs `GBHashTable<1024>` + a 32 KB-page chunk allocator (JoinAgg).
   *This is the part Step 2 unifies.*
2. **Result emission** — per-batch streaming drain into a `Signal`
   (`PrepareAggResIfNeeded`/`NumOfResRecords`, Agg) vs persist-then-merge-then-emit
   into a buffer with cross-node redistribute (JoinAgg, for CTEs/joins).
   *Both stay; the Agg drain is adapted to the new container in Step 2.*
3. **Join/CTE-only machinery** — linked attributes, NULL-extended rows, multi-leaf,
   mutex, eviction, CTE-mode, merge. *Stays JoinAgg-only; never enters Agg.*

The plan therefore is: **Step 1** factors out divergence-class (1)-and-(3)-free shared
code; **Step 2** moves the better memory model (divergence 1) into `AggInterpreter`
while keeping its streaming drain (divergence 2); **Step 3** revisits whether the two
thin remainders are worth merging.

The one real cost of Step 2 is **memory footprint for tiny queries** (§4) — the very
reason the split was originally made — which the codebase already anticipates with an
unused `AGG_HASH_BUCKET_COUNT = 256` / `AggGBHashTable` alias plus the
drain-every-batch behavior that keeps at most ≈ one chunk resident.

---

## 2. Background — why two classes exist today

Both classes derive from `PushdownInterpreter` (`PushdownInterpreter.hpp:70`) and
both are `PushdownType::AGGREGATION`. The split is documented in their own headers:

- `AggInterpreter.hpp:37-44`: *"lean aggregation interpreter for normal scan
  pushdown. All buffers are inline so that sizeof(AggInterpreter) fits in a single
  32KB page. Uses std::map with an inline bump allocator … no chunk allocator, no
  xfrm buffer, no dynamic allocation beyond std::map tree nodes."*
  Enforced by `static_assert(sizeof(AggInterpreter) <= MEM_CHUNK_SIZE)` (`:224`).

- `JoinAggInterpreter.hpp:36-45`: *"Holds all join-specific state: mutex, linked
  attribute buffer pointers, chunk-based group eviction, large GBHashTable (1024
  buckets), merge cache, and type-aware hashing… Separated from AggInterpreter so
  that normal scan aggregation (SELECT COUNT(*) FROM t) stays at 1 page (32KB) with
  all buffers inline."*

So the split was a **footprint optimization for the common small-aggregation case**,
not a difference in aggregation semantics. The two engines were forked rather than
factored, and have since drifted only in the memory model and the join/CTE features.

---

## 3. Duplication inventory

### 3a. Code that is identical or near-identical (Step 1 target)

| Component | Agg location | JoinAgg location | Notes |
|---|---|---|---|
| Numeric kernels: `TypeSupported`, `IsUnsigned`, `AlignedType`, `PrintValue`, `Sum/SumBigint/SumDouble`, `Max/MaxBigint/MaxDouble`, `Min/MinBigint/MinDouble`, `Count` | `AggInterpreter.cpp:372-1119` | `JoinAggInterpreter.cpp:88-550` | JoinAgg source literally comments *"identical to AggInterpreter.cpp"* (`:86`, `:183`). ~750 lines. |
| `validateEmbeddedProgram` | `:172-225` | `:555-654` | JoinAgg comments *"same as AggInterpreter version"* (`:553`). |
| `OptimizeProgram` (delegates to `OptimizeProgramBuffer`) | `:364-370` | `:942-948` | Identical 7-line wrapper. |
| Program-header parse in `Init` (magic `0x0721`, n_gb_cols / n_agg_results / version, gb-col ids, embedded-block scan) | `:227-348` | `:656-838` | Same parsing; differs only in buffer setup (inline vs `m_buf_block`). |
| `ProcessRec` **opcode-execution body** (all arithmetic + typed variants, `kOpLoadCol` numeric/decimal/string decode, `kOpLoadConst`, `kOpMov`, `kOpSetRegNull`, all aggregate opcodes, `kOpSkip`, `kOpEmbeddedInterp`) | `:1242-1962` | `:1130-1668` | ~700 lines each, near-identical **once `agg_res_ptr` is resolved**. |
| String MIN/MAX suite: `minMaxString`, `stringPayloadSize`, `encodeStringPayload`, `freeGroupStringSlots`, `release_string_results` | `:2306-2504` | `:2754-3015` | Byte-for-byte identical except `release_string_results` iterates the map type (std::map range-for vs `GBHashTable::Iterator`). |
| `Register` / `AggResItem` / `DataValue` / `DataType` / `StringResult` | shared | shared | Already in `NdbAggregationCommon.hpp` / `PushdownInterpreter.hpp`. `sizeof == 24`. Not forked. |
| Wire format: `AGG_RESULT` / `AGG_CHAR_RESULT` markers, 3-word header `marker<<16|0x0721`, `n_gb_cols<<16|n_agg_results`, group count, per-group `key_len<<16|v_len` + key + AggResItem[] + string payload | `PrepareAggResIfNeeded` `:2067-2148` | `getResultData` `:1786-1866`, `evictOneGroup` `:1713-1780` | **Produce the identical bytes.** Only the destination differs (Signal vs buffer). |

### 3b. Code that genuinely diverges

| Concern | AggInterpreter | JoinAggInterpreter | Plan disposition |
|---|---|---|---|
| Group container | `std::map<GBHashEntry,…,GBHashEntryCmp>` (`hpp:113,208`) | `GBHashTable<1024>` (`hpp:276`) | **Step 2: unify on the hash table.** |
| Group allocator | inline bump `MemAlloc` over `m_mem_buf[8192]` (`cpp:2291`) | 32 KB-page chunk allocator (`cpp:2655-2746`) | **Step 2: unify on the chunk allocator.** |
| Fixed buffers | inline arrays (32 KB single-page constraint) | one `m_buf_block` carve in `Init` (`cpp:666-698`) | **Step 2: Agg moves to `m_buf_block`-style carve.** |
| GB type metadata | `GBCmpContext`/`GBColMeta` (ordering only) `initGBCmpCtx` (`cpp:2045`) | `GBColTypeInfo` (hash **and** compare) `initGBTypes` (`cpp:2402`) | **Step 2: Agg uses `GBColTypeInfo`** (superset; drops `GBCmpContext`). |
| Result emission | per-batch streaming drain into `Signal`: `PrepareAggResIfNeeded` + `NumOfResRecords` | persist + merge + emit into buffer (`getResultData` dead; live path is `continueJoinAggSend`) | **Both kept.** Agg's drain re-pointed at the hash table; JoinAgg's merge path untouched. |
| Join/CTE features | none | linked attrs, NULL-extended rows, multi-leaf, mutex, eviction, CTE-mode, merge/redistribute, distribution hash | **Stay JoinAgg-only.** Never added to Agg. |

### 3c. Dead code spotted during analysis (cleanup opportunities)

- `AggInterpreter::Print()` — declared `hpp:79`, **zero call sites**.
- `JoinAggInterpreter::getResultData()` — declared `hpp:111`, **zero call sites**.
- `AGG_HASH_BUCKET_COUNT = 256` / `AggGBHashTable` alias (`AggHashTable.hpp:263,266`)
  — defined but **never instantiated**; the pre-positioned 256-bucket variant
  intended for exactly Step 2.

---

## 4. Memory-model comparison (what Step 2 adopts)

| Aspect | AggInterpreter (`std::map`) | JoinAggInterpreter (`GBHashTable<1024>` + chunks) |
|---|---|---|
| Lookup/insert | O(log n), scattered red-black nodes | O(1) avg, groups packed in chunk pages |
| Max group data | **8 KB hard cap**; `MemAlloc`→`nullptr`→`ZAGG_OTHER_ERROR` | budget-bounded, grows via `bookMoreMemory` |
| Over-capacity | errors the row; relies on per-batch drain | grows / evicts (`evictOneGroup`, O(1) page free) |
| Per-group overhead | RB node (~32 B malloc) + 2× `GBHashEntry` | 24 B inline `GROUP_LINK_OVERHEAD`, no node malloc |
| Charset grouping | compare-only (`GBHashEntryCmp::m_cmp`) | hash **and** compare consistent (`hashKeyFull`) |
| Output order | key-ordered (RB in-order) | bucket/hash order (unordered) |
| **Baseline footprint** | small: map object + inline `m_mem_buf` (within one 32 KB page) | **8 KB bucket array always** + **one 32 KB chunk on first group** |

**Why it's better for AggInterpreter too:** the chunk allocator **grows instead of
erroring** at the 8 KB boundary, so a high-cardinality GROUP BY within one batch no
longer risks `ZAGG_OTHER_ERROR`; lookups are O(1) and cache-friendly; reclamation is
O(1) page-granular; and `GBColTypeInfo` makes hashing and comparison charset-consistent.

**The one real cost — footprint for tiny queries — and its mitigations:**
- `GBHashTable<1024>` carries an **8 KB `m_buckets[]` array unconditionally**, and a
  one-group query forces a full **32 KB chunk** on first insert. The deliberate split
  was avoiding exactly this.
- **Mitigation 1 — 256 buckets.** Use the existing-but-unused `AggGBHashTable` /
  `AGG_HASH_BUCKET_COUNT = 256` (2 KB buckets) for the AggInterpreter config.
- **Mitigation 2 — drain-every-batch (already how Agg works).** `PrepareAggResIfNeeded`
  drains + frees groups every ~4 KB, so at most ≈ one chunk is resident at a time.
- **Mitigation 3 — lazy/minimal chunk budget.** `allocGroupData` already allocates
  the first chunk lazily on first group; give the Agg config a small
  `initChunkAllocator` budget.
- Net for a low-cardinality GROUP BY: ≈ 2 KB buckets + ≤ 1 chunk, in exchange for
  removing the duplication and gaining robust scaling. Verified by measurement in the
  Step 2 exit criteria.

---

## 5. Integration points (what Step 2 must keep working)

`AggInterpreter` is reached **only** through `Dblqh::ScanRecord::m_agg_interpreter`
(`Dblqh.hpp:865`), with **one** creation site and a small, fixed set of call sites:

**Creation (1 site):** `Dbtup::scanCopyAttrinfo` → `PushdownInterpreterFactory::Create`
(`DbtupExecQuery.cpp:1059-1066`; factory `PushdownInterpreter.cpp:259-285`). The
factory produces `AggInterpreter` or `VecSearchInterpreter` only (the Agg-vs-JoinAgg
choice is by code path, not program inspection). **Step 2 keeps this site; it just
constructs an AggInterpreter whose group store is the hash table + chunk allocator.**

**Per-row + per-batch drive loop (`Dbtup::interpreterStartLab`,
`DbtupExecQuery.cpp:5577-5613`):**
```cpp
ret = m_agg_interpreter->ProcessRec(this, req_struct, getThreadId());        // 5578
Uint32 res_len = m_agg_interpreter->PrepareAggResIfNeeded(signal, false);    // 5589
if (res_len != 0) { ... SendAggregationResult(signal, res_len, ...); }       // 5600
req_struct->agg_n_res_recs = m_agg_interpreter->NumOfResRecords();           // 5602
```

**End-of-scan drain (`Dbtup::SendAggResToAPI`, `DbtupBuffer.cpp:549-570`):**
```cpp
Uint32 res_len = interp->PrepareAggResIfNeeded(signal, true);                 // 551
bool all_sent = (interp->gb_map() == nullptr || interp->gb_map()->empty());   // 552-553
if (all_sent) lqhScanPtrP->m_agg_n_res_recs = interp->NumOfResRecords(true);  // 555
```

**DBLQH scan-complete gate (`DblqhMain.cpp:17612-17616`):** calls `SendAggResToAPI`
when `m_agg_interpreter->gb_map() != nullptr && !gb_map()->empty()`.

**Destruction (`gb_map().empty()` assert then `Destruct`):** `DblqhMain.cpp:24233-24237`
(scan release) and `:41989-41991` (`~ScanRecord`). Plus `frag_id()` checks at
`DblqhMain.cpp:23345`, `:24787`, `DbtupBuffer.cpp:572`.

**Contract Step 2 must preserve unchanged at the call sites:** `ProcessRec`,
`PrepareAggResIfNeeded(Signal*, force)` (streaming, ≤4 KB batches), `NumOfResRecords(last)`,
a `gb_map()`-style **"is it drained/empty?"** query, `frag_id()`, and
`PushdownInterpreter::Destruct`-compatible teardown. The only thing that changes
*inside* these is the container the drain walks/erases. **DBTC/DBSPJ are untouched**
(comments only).

---

## 6. Plan

### Step 1 — Extract the shared engine (zero behavior change) ⭐ do first
Both interpreters call one copy of the duplicated compute code. No memory-model or
call-site changes; both keep their existing containers, allocators, and emission.

1.1 ✅ **DONE — Shared numeric kernels.** Moved the file-static helpers
   (`TypeSupported`, `IsUnsigned`, `AlignedType`, `PrintValue`, `Sum*`, `Max*`,
   `Min*`, `Count`) into `AggInterpreterBase` as `protected static` methods
   (`AggInterpreterBase.{hpp,cpp}`).  All 14 pairs were verified logically identical
   before the move (differences were only cosmetic + JoinAgg having dropped the
   debug-only `#ifdef DEBUG_PA_INTERP` trace blocks — AggInterpreter's superset
   kept).  Both `ProcessRec`s call them unqualified via inherited name lookup (every
   call site is inside a member function ⇒ zero call-site churn).  ~748 duplicated
   lines removed; one canonical copy in `AggInterpreterBase.cpp`.  Added to
   `blocks/CMakeLists.txt`.  Build + agg test suite + RonSQL/CTE MTR green.

1.5 ✅ **DONE (landed with 1.1) — base-class foundation.**
   `AggInterpreterBase : public PushdownInterpreter` created; both `AggInterpreter`
   and `JoinAggInterpreter` reparented (ctors delegate `AggInterpreterBase(...)` →
   `PushdownInterpreter(...)`).  The base currently holds only the shared kernels;
   it adds **no data members**, so `sizeof` is unchanged and both
   `static_assert(... <= MEM_CHUNK_SIZE)` still hold.  Lifting the shared *fields*
   (`m_registers`, `m_string_results`, etc.) into the base is deferred to 1.3/1.4
   where they are actually needed.

1.2 ✅ **DONE — Shared embedded-program validator + optimizer.**
   `validateEmbeddedProgram` is now a `protected static` on `AggInterpreterBase`;
   `OptimizeProgram` is a public non-virtual method on the base.  Two surprises during
   landing:
   (a) The two validators were **not** byte-identical — JoinAgg's was strictly more
       rigorous (opcode allow-list + backward-branch reject + bounds check) while
       AggInterpreter's was bounds-check only.  Comment in JoinAgg ("same as
       AggInterpreter version") was stale.  Adopted JoinAgg's strict form for both
       paths (decision: this tightens AggInterpreter's normal-scan validation, but
       every opcode the normal-scan path emits is in the allow-list, so behavior on
       valid programs is unchanged).
   (b) `OptimizeProgram` needs `m_prog` and `m_agg_prog_start_pos`, which were
       per-subclass fields.  Lifted both into the base (one field-lifting step ahead
       of the 1.3/1.4 schedule, but minimal: two fields, no `sizeof` impact, no other
       refactoring).  Subclass usages bind to the inherited base fields with no call-site
       churn.
   ~110 lines removed from JoinAggInterpreter.cpp + ~80 lines from AggInterpreter.cpp;
   one canonical copy in AggInterpreterBase.cpp.  Build green.

1.3 ✅ **DONE — Shared string MIN/MAX suite.**
   `minMaxString`, `stringPayloadSize`, `encodeStringPayload`, `freeGroupStringSlots`
   live in `AggInterpreterBase.cpp` (one canonical copy each); accessible via inherited
   name lookup from both interpreters and as `public` members from DBLQH wire-format
   emit / group-eviction call sites that hold a `JoinAggInterpreter*`.
   `release_string_results` stays per-class — the only divergence is the group
   container iteration (`std::map` range-for vs `GBHashTable::Iterator`); each subclass
   walks its own container and reuses the shared `freeGroupStringSlots` per-group-array
   freer.

   Lifted into `AggInterpreterBase` together with the helpers: `m_n_agg_results`,
   `m_agg_results`, `m_registers[kRegTotal]`, `m_register_string_data[kRegTotal]`,
   `m_string_results`, `m_current_thread_id`, `m_decimal`, `m_decimal_buf[]`.
   Constructor initializations folded into the base ctor.

   Two snags during landing:
   (a) `DECIMAL_BUFF_LENGTH` was a `#define 9` in each subclass header; `my_decimal.h`
       (transitively pulled into some compilation units) declares a `static constexpr
       int DECIMAL_BUFF_LENGTH{9}` of the same name, so a macro of that name in the
       base header collides.  Switched to a `static constexpr Uint32
       AGG_DECIMAL_BUFF_LENGTH = 9` inside the class; subclass call sites updated to
       the qualified name.
   (b) DBLQH's wire-format emit + group-eviction call `hasStringSlots` /
       `stringPayloadSize` / `encodeStringPayload` / `string_results` through a
       `JoinAggInterpreter*`; the helpers had to be `public` on the base (not
       `protected`).  Symmetric: `minMaxString` / `freeGroupStringSlots` are also
       public for consistency since every call site is inside aggregation-aware code.

   ~280 duplicated lines removed across the two subclass `.cpp` files; one canonical
   copy in `AggInterpreterBase.cpp`.  Build + agg test suite + RonSQL/CTE MTR green.

1.4 ✅ **DONE — Shared opcode executor.**
   Approach landed differently than the original plan envisioned.  A full diff of
   the two opcode bodies showed the divergence was larger than the plan
   accounted for: `kOpLoadCol` has substantial linked-attr / CTE / NULL-injection
   branches in JoinAgg that don't exist in AggInterpreter, and `kOpEmbeddedInterp`
   sets / clears `req_struct->m_linked_attr_data` only in JoinAgg.  Per the
   maintainer's directive ("different jump tables, no linked-column code in
   AggInterpreter's path, keep AggInterpreter's debug verbosity"), the two
   divergent opcodes stay in each subclass's own switch; the other 28 opcodes
   factor into a single shared helper.

   **Implementation.**  Added `AggInterpreterBase::executeStandardOpcode(op, value,
   exec_pos&, agg_res_ptr, debug_print, *handled)` that handles:
   - generic arithmetic (kOpPlus / Minus / Mul / Div / DivInt / Mod)
   - typed arithmetic (kOpPlusBigint / PlusDouble / MinusBigint / MinusDouble /
     MulBigint / MulDouble / DivDouble / DivIntBigint)
   - aggregate-accumulate (kOpSum / SumBigint / SumDouble / Max / MaxBigint /
     MaxDouble / Min / MinBigint / MinDouble / Count)
   - misc (kOpLoadConst / kOpMov / kOpSetRegNull / kOpSkip)

   Each subclass's `ProcessRec` dispatch keeps just two arms (`kOpLoadCol` +
   `kOpEmbeddedInterp`) plus a `default:` that calls `executeStandardOpcode` and
   reports `ZAGG_WRONG_OPERATION` if `*handled == false`.  `executeStandardOpcode`
   is a non-virtual member; AggInterpreter's verbose `DEB_AGG(...)` /
   `PA_INTERP_TRACE(...)` form is preserved in the shared body (JoinAgg gains
   debug coverage in `VM_TRACE` builds; production builds compile both out via
   the standard `DEB_AGG` guard).

   **One subtle behavior bug caught by ronsql_cte_decimal during initial test
   run.**  `Max` / `Min` (the generic, non-typed kernels) return `1` for the
   "first row" / "null register" short-circuit; the original dispatch discarded
   that return via `ret = ...; break;`.  My first cut had `return Max(...)` /
   `return Min(...)`, which propagated the `1` to the caller and surfaced as
   ZAGG_OTHER_ERROR (1869) on every CTE-decimal aggregate row.  Fixed: call
   Max / Min with the return value discarded, then `return 0`.  The typed
   variants (MaxBigint / MaxDouble / MinBigint / MinDouble) and `Count` were
   correct from the start.

   ~720 lines moved into the shared helper; net diff ~ +408 / -549 across the
   four files.  AggInterpreter.cpp shrinks by 337 lines, JoinAggInterpreter.cpp
   by 240 lines.  Build + full agg test suite + RonSQL/CTE MTR (including
   ronsql_cte_decimal) green.

   **Step 1 exit criteria (met):** byte-identical behavior on valid programs;
   `testJoinAgg`, `testJoinAggSpj`, `testJoinAggNdbApi`, `testCaseAgg`, the
   bench targets, and the RonSQL/CTE MTR suites pass unchanged; cumulative
   1.1–1.4 dedup approaches the plan's 1,900–2,100-line target on the four
   sub-step net commits; both `sizeof(...) <= MEM_CHUNK_SIZE` asserts still
   hold.

### Step 2 — Give AggInterpreter the JoinAgg memory model ✅ DONE

Shipped in three sub-commits (2a + 2b + 2c) — AggInterpreter's `std::map` +
inline `m_mem_buf` bump pool is replaced by the shared `JoinGBHashTable` (1024
buckets) + the chunk allocator that JoinAggInterpreter already used.  The
streaming drain to the API is preserved; CTE / merge machinery stays
JoinAgg-only.

**2a — Lift to base.** Moved the chunk allocator (`initChunkAllocator`,
   `allocNewChunk`, `allocGroupData`, `freeGroupData`, `bookMoreMemory`,
   `freeAllChunks`) plus the GROUP BY per-column type-metadata fields
   (`m_gb_types`, `m_gb_types_inited`, `m_xfrm_buf`, `m_xfrm_buf_len`) and the
   `GROUP_LINK_OVERHEAD` constant out of `JoinAggInterpreter` into
   `AggInterpreterBase`.  Zero behavior change: JoinAgg reaches the moved code
   via inherited name lookup; AggInterpreter doesn't engage the chunk allocator
   yet.  Added the `AttributeHeader.hpp` include to `AggInterpreterBase.hpp`
   because `AggHashTable.hpp`'s templated `hashKeyFull` / `findInBucket`
   reference it.

**2b — Switch AggInterpreter to the shared memory model.**
   - **Bucket count decision** (revisited): per maintainer direction, both
     interpreters use the **same** 1024-bucket variant (`JoinGBHashTable` =
     `GBHashTable<1024>`) — not the original templated 256/1024 split.  One
     type alias serves both, no template instantiation duplication.
   - **Group container** changed from `std::map<GBHashEntry, ..., GBHashEntryCmp>`
     to `JoinGBHashTable`.  Inline storage stays inline: `m_gb_map_buf` is a
     `JoinGBHashTable` member (~8 KB of bucket pointers) on AggInterpreter,
     same as before but a different type.
   - **`m_n_gb_cols` / `m_gb_cols` / `m_gb_map` / `m_n_groups`** lifted to the
     base (both subclasses now share these fields, set up at Init time).
   - **`initGBTypes`** lifted to the base, parametrized on
     `(linked_attr_data, linked_attr_len)`; AggInterpreter passes `nullptr / 0`
     and the linked-attr branches inside it are dead-code on the normal-scan
     path (attr_id 0x8000 never appears in normal-scan GB columns).
   - **`ProcessRec` group prologue** in AggInterpreter rewritten to
     `m_gb_map->find` / `m_gb_map->insert` + `allocGroupData` (mirrors JoinAgg
     minus `m_acc_offset` / linked / CTE / multi-leaf branches).  First-row
     trigger calls the lifted `initGBTypes`.
   - **`PrepareAggResIfNeeded`** streaming drain re-pointed at
     `GBHashTable::Iterator` + `eraseAndNext` + `freeGroupData` per group —
     same emit→erase→free shape as `evictOneGroup`, generalized to drain every
     currently-resident group.  `m_n_groups` re-snapshots `m_gb_map->size()`
     after the drain.
   - **`release_string_results`** walks `m_gb_map` via `GBHashTable::Iterator`,
     delegating per-group val_ptr free to the shared `freeGroupStringSlots`
     helper.
   - **`Print`** iterates via `GBHashTable::Iterator`.
   - **Inline buffers** rebalanced: `m_mem_buf` (8 KB bump pool) + `m_alloc_len`
     + `MemAlloc` removed; `JoinGBHashTable m_gb_map_buf` (~8 KB) + inline
     `GBColTypeInfo m_gb_types_buf[MAX_AGG_N_GROUPBY_COLS]` (~3 KB) added.
     Net sizeof change is roughly a wash; `static_assert(sizeof(AggInterpreter)
     <= MEM_CHUNK_SIZE)` still holds.
   - **DBLQH call sites** (`scanPtr->m_agg_interpreter->gb_map()` checks +
     `interp->gb_map()->empty()` assertion) need no source change — they only
     use `nullptr` + `empty()` + `size()`, all supported by `JoinGBHashTable`.
     The `gb_map()` accessor return type changes from `const std::map<...>*` to
     `const JoinGBHashTable*` transparently.

**2c — Cleanup.** Removed the kernel-side dead body of
   `GBHashEntryCmp::operator()` from `AggInterpreter.cpp`, dropped the
   `#include <map>` from `AggInterpreter.hpp`, refreshed the inline-buffer
   header comment.  The struct itself (`GBHashEntry` / `GBHashEntryCmp` /
   `GBCmpContext` / `GBColMeta` in `NdbAggregationCommon.hpp`) stays because
   the API-side `NdbAggregator` still uses `std::map` and depends on them.

**Exit criteria — all met.**  Block-unit + RonSQL/CTE MTR suites green; the
high-cardinality GROUP BY corner case that previously risked AggInterpreter's
8 KB bump-pool cap now grows the chunk allocator instead; charset GROUP BY,
string MIN/MAX, and scalar queries all unchanged; both `sizeof` static_asserts
hold; `rowsExamined`, batch sizing, and flow control behavior preserved.

### Step 3 — Decide on full unification (deferred)
After Steps 1–2, `AggInterpreter` and `JoinAggInterpreter` share the compute engine
**and** the memory model; what remains distinct is only: AggInterpreter's streaming
`Signal` drain vs JoinAgg's mutex / eviction / linked-attr / multi-leaf / CTE-mode /
merge-redistribute cluster. Re-measure the remaining duplication and choose:

- **(a)** keep `AggInterpreter` as a thin normal-scan subclass of the base (its only
  unique content is the streaming drain + the small-config setup), or
- **(b)** fold it into `JoinAggInterpreter` as a "normal-scan mode" and delete the class,
  repointing the one creation site + ~6 call sites and routing the streaming drain through
  a mode flag.

This decision is intentionally left until the remainder is concrete. Also retire the
dead `Print()` / `getResultData()` and the obsolete `AGG_HASH_BUCKET_COUNT` alias if 2.2
chose runtime bucket sizing, and update the docs (`CLAUDE.md`,
`local_database_research.md`, `local_database_implementation.md`, block_unit
`TESTING_GUIDE.md`).

---

## 7. Risks & how the plan addresses them

| Risk | Mitigation |
|---|---|
| **Footprint regression for tiny aggregations** (8 KB buckets + 32 KB chunk vs inline bytes) | 256-bucket config (2 KB) + drain-every-batch (≤ 1 resident chunk) + lazy/minimal chunk budget; measured in Step 2 exit criteria. Accepted trade per the agreed goal. |
| **Streaming-drain correctness over the hash table** (erase+free mid-scan, flow control, `empty()` gates) | Built on the proven `evictOneGroup` emit/erase/free; byte-compare drain output to the old `std::map` path on identical inputs; keep `NumOfResRecords` / batch semantics exact. |
| **Partial-group emission semantics** (drain erases groups mid-scan; a later row recreates the group) | Unchanged from today — the API already re-aggregates partial per-group records across fragments/nodes; the hash-table drain reproduces the same partial-emit behavior. |
| **Charset GROUP BY divergence** (`GBHashEntryCmp` ordering vs `GBColTypeInfo` hash+compare) | Step 2.5 dedicated comparison tests on charset GROUP BY and string MIN/MAX; both already share `NdbSqlUtil` compare functions. |
| **Output ordering** (std::map key-ordered; hash unordered) | No stated ordering contract on the drain; JoinAgg already ships hash-order results through the same wire format; API/RonSQL carry GROUP BY keys and re-sort. Verify no consumer asserts order; if one does, sort per batch at emit (cheap). |
| **Touching JoinAgg's hot path while sharing the allocator/hash** | Step 2.1 moves code without changing JoinAgg's call sequence; prefer templated bucket count (2.2) so JoinAgg keeps compile-time 1024; benchmark `benchJoinAgg` / `bench_q9_dbtc` before/after. |
| **`ProcessRec` is hot path** | `executeOpcodes` stays non-virtual and inlinable; group-store ops are direct calls, not virtual dispatch. |
| **Teardown / lifecycle** (Agg uses `Destruct`; JoinAgg frees manually in `DblqhProxy`) | Step 2.6 routes the Agg instance's chunk+block free through `Destruct`; asan / pool accounting in exit criteria. |

---

## 8. Open sub-decisions (within the agreed approach)

1. **Bucket count: template vs runtime.** `GBHashTable<256>` / `<1024>` (templated, zero
   hot-path indirection, two instantiations) vs a runtime bucket count in a single
   non-templated store (one class in the base, pointer+mask). Default: **templated**,
   re-evaluate if a single store in the base is materially cleaner.
2. **Where the shared memory model lives.** In `AggInterpreterBase` directly, or in a
   standalone `AggGroupStore` member that both interpreters hold. Default: **base class**
   (fewer moving parts; eases a possible Step 3(b) collapse).
3. **Step 1 base vs free-function namespace.** Base class recommended (holds the shared
   fields too, and is the natural home for the Step 2 memory model).
4. **Chunk budget for the Agg config.** Start `initChunkAllocator` at 1–2 pages, grow on
   demand; tune via benches.

(The Step 3 full-removal decision is deliberately *not* listed here — it is taken later.)

---

## 9. Appendix — key file:line anchors

- Factory / dispatch: `PushdownInterpreter.cpp:242-285` (`DetectType`, `Create`).
- Normal-scan creation: `DbtupExecQuery.cpp:1059-1066` (`scanCopyAttrinfo`).
- Normal-scan drive loop: `DbtupExecQuery.cpp:5577-5613` (`interpreterStartLab`).
- End-of-scan drain: `DbtupBuffer.cpp:549-570` (`SendAggResToAPI`).
- DBLQH scan-complete gate: `DblqhMain.cpp:17612-17616`.
- AggInterpreter destruction: `DblqhMain.cpp:24233-24237`, `:41989-41991`.
- ScanRecord field: `Dblqh.hpp:865` (decl), `:579` (fwd-decl), `:715` (ctor init).
- AggInterpreter `ProcessRec` opcode body: `AggInterpreter.cpp:1242-1962`; group prologue
  `:1158-1216`; streaming drain `:2067-2289`.
- JoinAgg `ProcessRec` opcode body: `JoinAggInterpreter.cpp:1130-1668`; group prologue
  `:1058-1097`; emit/evict `:1713-1866`; chunk allocator `:2633-2746`.
- Shared structs: `AggHashTable.hpp` (`MemChunk` `:36-44`, `GBColTypeInfo` `:50-55`,
  `GBHashTable<>` `:67-260`, `GROUP_LINK_OVERHEAD`/`OVERHEAD=24`,
  `AGG_HASH_BUCKET_COUNT=256` `:263`, `JOIN_AGG_HASH_BUCKET_COUNT=1024` `:264`);
  `NdbAggregationCommon.hpp` (`Register`/`AggResItem` `:136-151`, `GBHashEntry` `:158-161`,
  `GBCmpContext`/`GBColMeta` `:163-172`, constants `:38-42`).
</content>
