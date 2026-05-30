# AggInterpreter ↔ JoinAggInterpreter Unification — Analysis & Plan

**Status:** analysis complete, plan proposed (not yet started)
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

1.1 **Shared numeric kernels.** Move the file-static helpers (`TypeSupported`,
   `IsUnsigned`, `AlignedType`, `PrintValue`, `Sum*`, `Max*`, `Min*`, `Count`,
   `AggInterpreter.cpp:372-1119` ≡ `JoinAggInterpreter.cpp:88-550`) into a shared unit
   (`AggKernels.{hpp,cpp}` or an `AggInterpreterBase` static method set). Delete the
   JoinAgg copies; include in both. ~750 lines removed.

1.2 **Shared embedded-program validator + optimizer.** Factor `validateEmbeddedProgram`
   and `OptimizeProgram` into the shared unit (`OptimizeProgram` already only calls the
   static `OptimizeProgramBuffer`).

1.3 **Shared string MIN/MAX suite.** Factor `minMaxString`, `stringPayloadSize`,
   `encodeStringPayload`, `freeGroupStringSlots` into shared helpers parameterized by an
   `AggResItem*` slot array + the `m_string_results` / `m_register_string_data` fields
   (both classes hold these identically). `release_string_results` keeps a thin per-class
   shim for the map-iteration difference, calling a shared per-group-array freer.

1.4 **Shared opcode executor.** Split each `ProcessRec` into:
   - a per-class **prologue** that resolves `agg_res_ptr` (read GB key → look up / insert
     group → accumulator base) — the only container-specific part; and
   - a **shared `executeOpcodes(agg_res_ptr, block_tup, req_struct, …)`** holding the
     entire opcode dispatch (`AggInterpreter.cpp:1242-1962` ≡
     `JoinAggInterpreter.cpp:1130-1668`).
   ~700 lines de-duplicated. The join/CTE-only branches (linked attrs, CTE rewrite,
   `m_acc_offset`, `m_null_local_columns`) stay in JoinAgg's prologue only.

1.5 **Mechanism: a shared base class** `AggInterpreterBase : public PushdownInterpreter`
   holding the shared fields (`m_registers`, `m_register_string_data`, `m_decimal*`,
   `m_string_results`, `m_n_gb_cols`, `m_n_agg_results`, `m_gb_cols`, `m_agg_results`,
   the count statics) and the shared methods. `AggInterpreter` and `JoinAggInterpreter`
   become subclasses supplying only the group store + emission. (Keep the base non-virtual
   in the hot path — `executeOpcodes` must stay inlinable; only `~PushdownInterpreter` is
   virtual, as today.)

   **Exit criteria:** byte-identical behavior; `testJoinAgg`, `testJoinAggSpj`,
   `testJoinAggNdbApi`, `testCaseAgg`, the bench targets, and the RonSQL/CTE MTR suites
   pass unchanged; ~1,900–2,100 net lines removed; `sizeof` asserts still hold.

### Step 2 — Give AggInterpreter the JoinAgg memory model
Replace AggInterpreter's `std::map` + inline bump allocator with the shared chunk
allocator + hash table. **Keep AggInterpreter's streaming drain to the API**, adapted
to the new container. Do **not** bring CTE/merge machinery into AggInterpreter.

2.1 **Share the memory model.** Move the chunk allocator (`initChunkAllocator`,
   `allocNewChunk`, `allocGroupData`, `freeGroupData`, `bookMoreMemory`, `freeAllChunks`,
   `MemChunk`) and the `GBHashTable` group-store management out of `JoinAggInterpreter`
   into the shared base (or a shared `AggGroupStore` member) so `AggInterpreter` can use
   the same code. JoinAgg's behavior is unchanged (it keeps calling the same operations).

2.2 **Bucket-count for the Agg config.** Use **256 buckets** (the existing
   `AggGBHashTable` / `AGG_HASH_BUCKET_COUNT` alias) for AggInterpreter, 1024 for JoinAgg.
   Sub-decision (§8): keep `GBHashTable<N>` templated (two instantiations, no hot-path
   indirection) **or** make the bucket count a runtime field (one non-templated store in
   the base, pointer+mask). Recommend deciding by a `benchJoinAgg`/`bench_q9_dbtc` check;
   default to **templated 256/1024** to avoid touching JoinAgg's hot path.

2.3 **Rewrite AggInterpreter's group prologue** to use `GBHashTable::find/insert` +
   `allocGroupData` instead of `m_gb_map->find/insert` + `MemAlloc`
   (`AggInterpreter.cpp:1158-1216` → hash-table form mirroring
   `JoinAggInterpreter.cpp:1058-1097`, minus `m_acc_offset` / linked / CTE branches).

2.4 **Adapt the streaming drain to the hash table.** Rewrite
   `PrepareAggResIfNeeded` (`AggInterpreter.cpp:2067-2148`) and `release_string_results`
   to iterate the `GBHashTable::Iterator`, emit each group in the existing wire format
   (reuse the shared encoder), and **erase + `freeGroupData`** each drained group (the
   same emit→erase→free as `evictOneGroup`, generalized to "all currently-resident
   groups under the batch threshold"). `NumOfResRecords` returns the hash table's
   remaining group count; the `gb_map()->empty()` gates become a hash-table
   `empty()`/`size()` query. Keep `DEF_AGG_RESULT_BATCH_BYTES` (4 KB) batching so resident
   footprint stays ≈ one chunk.

2.5 **Type metadata.** AggInterpreter uses `initGBTypes` + `GBColTypeInfo` (drop
   `initGBCmpCtx` / `GBCmpContext`); `GBColTypeInfo` is a superset for equality and adds
   the hash function. Verify charset GROUP BY produces identical groups to the old
   comparator.

2.6 **Config + teardown.** AggInterpreter sets `setUseMutex(false)`, `setMaxGroups(0)`
   (no eviction — the per-batch drain is the pressure release; the chunk allocator grows
   rather than erroring), no multi-leaf, no CTE-mode, small chunk budget. Teardown frees
   chunks + the `m_buf_block`-style carve under `PushdownInterpreter::Destruct`
   (`DblqhMain.cpp:24233`, `:41989`). The `static_assert(sizeof(AggInterpreter) <=
   MEM_CHUNK_SIZE)` is re-evaluated (buffers move out-of-line, so the object shrinks).

   **Exit criteria:** the normal-scan path passes the same inputs/outputs as before for
   SUM/COUNT/MIN/MAX, GROUP BY (incl. charset + string MIN/MAX), and scalar queries;
   high-cardinality GROUP BY that previously risked the 8 KB cap now succeeds; footprint
   measured (≈ 2 KB buckets + ≤ 1 chunk for low cardinality); no leaked chunks under
   asan / NDB pool accounting; `rowsExamined`, batch sizing, and flow control unchanged;
   full MTR + block_unit + RonSQL CTE suites green.

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
