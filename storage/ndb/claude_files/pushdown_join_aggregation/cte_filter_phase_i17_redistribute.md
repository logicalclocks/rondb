# Phase I.17 kernel — scalar aggregate cross-node redistribute

## Status

**Shipped** (this commit).  See "What shipped" at the end of the doc.  Companion to `cte_filter_phase_i17.md`.  The RonSQL-side
relaxation in I.17a (commit `a2e75657e47`) accepts CTE bodies
without `GROUP BY`, but the kernel materialization side has no
cross-node merge for scalar (n_gb_cols == 0) accumulators.  Each
node ends up with its own partial `m_agg_results`, leading to
duplicate / wrong rows from `cteScanEmitResults`.  This phase
extends `continueJoinAggRedistribute` to consolidate scalar
results onto a single designated owner before CTE_SCAN runs.

## Problem recap

CTE materialization splits across data nodes: each node aggregates
its local fragments into a per-node `JoinAggInterpreter`.  For
grouped CTEs (`gb_map != nullptr`), the existing redistribute
phase routes each group to its hash-derived owner node so every
group ends up on exactly one node before CTE_SCAN.  For scalar
CTEs (`gb_map == nullptr`), the redistribute phase short-circuits
at the existing `gb_map == nullptr || gb_map->size() == 0` guard
in `Dblqh::continueJoinAggRedistribute` — no scalar accumulator
is ever moved between nodes.

Symptoms surfaced by Tests 1-3 of `ronsql_cte_scalar.test`:
- Populated input on a non-first-node fragment plus a designated
  empty-emitter on `m_cte_node_list[0]`: two rows emitted ([100,
  NULL]) where MySQL gives one ([100]).
- Empty input across all nodes: no node has `processed_rows > 0`,
  so without a designated emitter no row surfaces (MySQL expects
  one).

## Goal

After redistribute completes, exactly one node ("the scalar
owner") holds the cluster-wide merged scalar result.  Every other
node's interpreter is irrelevant for emit purposes.
`cteScanEmitResults` then trivially gates scalar emit on
`getOwnNodeId() == scalar_owner_node`.

## Owner selection: DBTC's node

Use **the data node co-located with the DBTC instance that owns
the transaction** as the scalar owner.  Properties:

- Every participant already knows DBTC's node from the
  `senderRef` carried in `JOIN_AGG_SETUP_REQ` (and re-asserted by
  later redistribute / complete signals from DBTC), so no new
  state is needed beyond stashing `refToNode(setupSenderRef)` on
  the per-state record at SETUP time.
- The owner is naturally close to where the CTE result is
  consumed (DBSPJ runs on the same node as DBTC), so the final
  emit's TRANSID_AI is local rather than cross-node.
- Deterministic per-transaction: every node agrees because they
  all see the same SETUP signal with the same senderRef.
- Doesn't depend on the participant list ordering (which can
  shift across cluster reconfigurations).

The alternative considered — `m_cte_node_list[0]` — is also
deterministic but couples owner choice to the CTE-participant
list slot order rather than to the transaction.

Storage: no new field — every state already stores
`m_senderRef` (set in `DblqhProxy::execJOIN_AGG_SETUP_REQ` from
the SETUP signal's senderRef, which is DBTC's reference).
`refToNode(state->m_senderRef)` gives DBTC's node directly.

## Phase I.17e — implementation

### Kernel changes (Dblqh)

#### `continueJoinAggRedistribute` scalar branch

Today (`DblqhMain.cpp` ~20708):

```cpp
JoinGBHashTable *gb_map = interp->gb_map_mutable();
if (gb_map == nullptr || gb_map->size() == 0) {
  goto redistribution_done;
}
```

Refactor: split the bail into two cases.

```cpp
if (gb_map != nullptr) {
  if (gb_map->size() == 0) {
    goto redistribution_done;       // existing grouped-empty path
  }
  /* existing grouped redistribute body */
} else if (interp->n_gb_cols() == 0) {
  /* Phase I.17e: scalar redistribute — DBTC's node owns the
   * cluster-wide synthetic group. */
  Uint32 ownerNode = state->m_dbtc_node;
  if (ownerNode == getOwnNodeId()) {
    /* Owner: nothing to send.  Wait for inbound signals from
     * non-owner nodes; each merges via execJOIN_AGG_REDISTRIBUTE
     * (existing path's scalar-mode mergeFrom branch). */
    goto redistribution_done;
  }
  /* Non-owner: package m_agg_results and send to owner. */
  send_scalar_redistribute_req(signal, state, interp, ownerNode);
  goto redistribution_done;
}
```

#### `send_scalar_redistribute_req` (new helper)

Mirrors the existing per-group send loop but with a single
"synthetic group" payload:

- keyLen = 0 (no GROUP BY columns).
- valLen = `interp->val_len()` — same accumulator-array byte
  size used today for grouped redistribute.
- Body = `m_agg_results[0..n_agg_results-1]`.

Reuses `JoinAggRedistributeReq` signal format with keyLen == 0
as the scalar marker.

#### `execJOIN_AGG_REDISTRIBUTE` scalar branch

Today the receiver decodes (key, val) and inserts into the
owner's gb_map.  Add a scalar branch (keyLen == 0):

- Locate the receiver's interpreter via aggStateKey.
- Merge the inbound `m_agg_results` payload into the owner's
  accumulators.  Lift the existing scalar branch of
  `JoinAggInterpreter::mergeFrom` (line ~1815: scalar
  `m_processed_rows += other->m_processed_rows;` plus
  `mergeAccumulators(...)`) into a payload-driven entry point
  on the owner's interpreter.

Alternative implementation: keep `JoinAggRedistributeReq` as-is
and have the receiver instantiate a temporary
`JoinAggInterpreter` shell whose `m_agg_results` points at the
inbound payload.  The inline-merge variant avoids the temporary
allocation; pick whichever has cleaner symmetry with the
existing grouped path.

#### Phase L idempotency

The new scalar branch enters the existing redistribute state
machine and must respect:
- `m_cte_redistribution_done` guard.
- `s_node_fail_count` revalidation.
- `state->m_state == CTE_REDISTRIBUTING` precondition.

A node failure mid-redistribute aborts the same way as grouped
(`abortCteRedistribution`).

### `cteScanEmitResults` scalar gate

Replace the current `processed_rows() > 0 OR designated-emitter`
fallback with a single check:

```cpp
} else if (interp->n_gb_cols() == 0 &&
           scanState->groupsSent == 0 &&
           getOwnNodeId() == state->m_dbtc_node) {
  /* Owner emits the cluster-wide scalar result. */
  ...
}
```

`processed_rows()` is no longer relevant for the gate — after
redistribute the owner always has the canonical accumulators
(merged from all nodes if any rows were processed; pre-initialised
empty values otherwise).  The `kOpCount` pre-init in
`JoinAggInterpreter::Init` (commit `3aa426b3fb9`) already ensures
COUNT slots emit 0 rather than NULL on empty input.

### What does NOT change

- `JoinAggInterpreter::mergeFrom` already has the n_gb_cols == 0
  branch — the scalar-merge primitive is already implemented.
- `MUTEX_FREE` per-LDM merge inside a node still runs to
  completion before redistribute kicks off — owner's local
  contribution is the merged-across-LDMs view of its own
  fragments.
- RonSQL-side I.17a (CTE-without-GROUP-BY parser relaxation)
  needs no further changes.

## Test plan

After kernel ships, the existing `ronsql_cte_scalar.test` Tests
1-4 should all pass without skip / known-limitation markers:
- Test 1 (populated MAX): owner has merged max — emits one row.
- Test 2 (empty MAX): owner has pre-initialised NULL — emits one
  row of NULL.
- Test 3 (empty COUNT): owner has pre-initialised 0 — emits one
  row of 0.
- Test 4 (multi-aggregate populated): owner has merged accumulators.

Follow-up positive coverage:
- Multi-fragment populated scalar (force enough rows that primary
  fragments live on both nodes) — verify the owner accumulates
  across nodes via redistribute.
- Watermark shape from `cte_filter_phase_i17.md` once cross-CTE
  join parsing lands.

## Risks

1. **`JoinAggRedistributeReq` scalar variant** — the existing
   signal format expects (key, val) pairs.  Sending one
   key-less synthetic group is a new payload shape.  Care
   needed in send/receive symmetry, signal-length variants, and
   batch-size accounting.
2. **`m_dbtc_node` populate site** — the SETUP signal may not be
   the first signal that pins DBTC's node.  Audit that
   `senderRef` is reliably DBTC at SETUP time and that no later
   signal can shift the owner.
3. **Phase L Commit 5 cleanup** retired `m_redist_mutex` and
   `m_cteCompleteOutstanding`.  The scalar branch must use the
   surviving idempotency primitives.
4. **Owner-not-running-aggregator** edge case — owner had no
   local fragments to scan, so its `m_agg_results` is the
   pristine Init state.  Already the empty-input case and works
   correctly: pre-initialised accumulators, no inbound merges
   needed, emit produces NULL/0.
5. **Single-node cluster** — owner == this node, no signals
   sent, redistribute is a no-op.  Handled by the
   `ownerNode == getOwnNodeId()` early-return.
6. **Multi-node group / replication topology** — the data node
   chosen by `refToNode(senderRef)` must be a participant in the
   CTE materialization (i.e., present in `m_cte_node_list`).
   Should always be true since DBTC's node is by definition the
   transaction coordinator and participants in JOIN_AGG_SETUP,
   but worth an `ndbassert`.

## Sequencing within I.17

1. Plan landed (this doc).
2. Kernel scalar redistribute lands as a separate commit
   (signal, send, receive, owner storage).
3. `cteScanEmitResults` simplification follows in the same
   commit (the new gate only works once redistribute is live).
4. MTR: existing `ronsql_cte_scalar.test` Tests 1-4 should
   pass after this work without further changes.

## What shipped

| File / area | Change |
|---|---|
| `JoinAggInterpreter::mergeOneGroup` | Now dispatches to `mergeScalarAccumulators` when `keyLen == 0`.  Existing keyed-merge logic unchanged.  Same dispatch fires for both the `execJOIN_AGG_REDISTRIBUTE_REQ` direct path and the `processRedistQueue` drain path (queued during FINALIZING / SENDING_RESULTS) — no scattered keyLen == 0 checks needed. |
| `JoinAggInterpreter::mergeScalarAccumulators` (new) | Folds an inbound payload of `n_agg_results` `AggResItem`s into the local `m_agg_results` via the existing `mergeAccumulators` primitive (the same one used by grouped merges).  Caches `m_cached_agg_ops` on first use, mirroring `mergeOneGroup`. |
| `Dblqh::sendScalarRedistributeReq` (new) | Packages this node's `m_agg_results` into a `JoinAggRedistributeReq` with `keyLen = 0`, `valueLen = interp->val_len()`.  Section 0 carries a 1-word dummy (the receiver inspects `req->keyLen == 0` and ignores it; a real word avoids any 0-size-section quirks in the transporter).  Section 1 carries the `AggResItem` array.  Addresses the destination via `numberToRef(DBLQH, dstOwner, ownerNode)` using the existing per-node aggKey / ownerInstance arrays. |
| `Dblqh::continueJoinAggRedistribute` | When `gb_map == nullptr`, branch on `n_gb_cols == 0`: non-owner nodes call `sendScalarRedistributeReq(state, interp, refToNode(state->m_senderRef))`; the owner skips the send and waits for inbound peers.  After the scalar branch falls through to `redistribution_done`, the existing FINAL_REP broadcast and CTE_READY transition complete normally. |
| `Dblqh::cteScanShouldEmitScalar` | Gate simplifies to `refToNode(state->m_senderRef) == getOwnNodeId()`.  The earlier `processed_rows() > 0 OR m_cte_node_list[0]` heuristic retires.  The COUNT pre-init in `JoinAggInterpreter::Init` (commit `3aa426b3fb9`) keeps the empty-input scalar emitting `0` for COUNT and `NULL` for SUM / MIN / MAX. |

What did **not** change:
- The `JoinAggRedistributeReq` signal format / sections / field
  layout — `keyLen == 0` is a marker on the existing field, not a
  new variant.
- The grouped-CTE redistribute path — keyed sends and the drain
  queue replay are unchanged.
- RonSQL-side I.17a (CTE-without-GROUP-BY parser relaxation, commit
  `a2e75657e47`) — no further changes needed.

What remained intentionally unchanged:
- `JoinAggInterpreter::mergeScalarAccumulators` does NOT advance
  `m_processed_rows` on the owner.  The new emit gate doesn't
  consult `processed_rows()` so this is fine; if a future caller
  needs the post-merge processed-rows count it can be added then.
