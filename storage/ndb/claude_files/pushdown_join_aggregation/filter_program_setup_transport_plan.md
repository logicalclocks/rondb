# Filter programs via JOIN_AGG_SETUP_REQ: feasibility, pros and cons

**Status: PLAN / FEASIBILITY ANALYSIS ONLY (September 2026, maintainer
request).**  No implementation; this doc evaluates whether the per-op
FILTER interpreted programs of an aggregation query can ride the same
JOIN_AGG_SETUP_REQ that already transports the AGGREGATION program —
one filter program per joined table (QueryTree op) — and weighs the
approach.

## Today's transport, for contrast

- **Aggregation program**: travels ONCE per query per node.
  `JoinAggSetupReq` is a 12-word signal with long sections
  ("Section 0: Aggregation program", "Section 1: Receiver IDs",
  JoinAgg.hpp:65-66); DblqhProxy stores it on the per-node
  `JoinAggregationState`, and every fragment scan / probe on that node
  references it via `joinAggStateKey`.  The program is never re-sent
  with row traffic.

- **Filter programs** (the WHERE interpreted code, one per joined
  table): travel INSIDE each operation's parameter/attrinfo sections
  (`PI_ATTR_INTERPRET` bits in the serialized QueryTree params;
  DBSPJ decodes them per op).  The re-send frequency differs by op
  kind:
  - **Scan ops**: the interpreted section is shipped once per
    fragment scan inside the SCAN_FRAGREQ attrinfo, and DBLQH keeps
    it as the stored procedure for the scan's lifetime — SCAN_NEXTREQ
    batches do NOT re-send it.  Cost ≈ program words × fragments.
  - **Lookup ops with `T_ATTRINFO_CONSTRUCTED`** (any lookup child
    whose attrinfo embeds per-row material — linked values, and since
    i26 every CTE_LOOKUP with an attached jump-table filter):
    DBSPJ re-expands the attrinfo pattern PER PARENT ROW
    (`m_attrParamPattern` expansion in lookup_send /
    cte_lookup_send), so the CONSTANT program words are rebuilt and
    re-shipped with every probe.  Cost ≈ program words × probe rows.

The asymmetry is the whole story: for scans the current transport is
already near-optimal; for probe-heavy lookup children (the CTE
watermark / filtered-CTE_LOOKUP shapes) the same constant words are
copied, sectioned, sent, received, and parsed once per row.

## Feasibility sketch (what it would take)

Doable in principle; the machinery mirrors what the aggregation
program already does:

1. **Wire**: a new long section on JOIN_AGG_SETUP_REQ (a signal can
   carry 3 sections — the third slot is free; or frame all filter
   programs inside one section as `[opCount][opNo, len, words...]*`).
   Size is bounded: ≤ 32 ops × small programs (the root-lookup path
   caps at 64 words; jump-table CTE filters are typically tens of
   words) — a few KB worst case, within fragmented-signal limits.
2. **API → DBTC**: the API already serializes per-op programs into
   the QueryTree params; it would additionally (or instead) attach
   them to the setup material DBTC forwards.  DBTC itself forwards
   sections opaquely, as it does the aggregation program.
3. **Storage**: `JoinAggregationState` gains a per-op program table
   (op number → words), same lifetime as the state
   (SETUP → RELEASE), same node coverage (setup already reaches every
   node), shared read-only across LDM threads like the aggregation
   program.
4. **Reference + execution**: requests carry a flag + op number
   instead of the interpreted section; the DBLQH/DBTUP interpreter
   entry fetches the program from the state via the request's
   `joinAggStateKey` (DBTUP already dereferences the state in
   `handleJoinAggRow` — the cross-block access pattern exists).
5. **The program/parameter split is respected**: only the CONSTANT
   program words move to setup.  Per-row material — linked parent
   values, key constructions — keeps flowing per request exactly as
   today; the interpreter's sectioned layout already separates
   program words from parameter words, so the split is natural.

## Pros

- **Eliminates per-probe re-transport of constant words** on the hot
  shape: filtered CTE_LOOKUP / lookup children re-send their filter
  program per parent row today.  For a 100k-probe query with a
  40-word filter that is ~16 MB of section traffic plus the DBSPJ
  pattern-expansion CPU and the receive-side parse, all removed.
- **Cuts DBSPJ request-construction work**: `m_attrParamPattern`
  expansion shrinks to the genuinely per-row words; smaller sections
  also mean fewer long-signal segments allocated and freed per probe.
- **Proven pattern, proven lifetime**: this is exactly the transition
  the aggregation program already made (and defineCte made for CTE
  bodies).  Setup/teardown, node coverage, multi-LDM sharing and
  abort paths are all inherited from `JoinAggregationState` — no new
  lifetime machinery.
- **No new signal**: a section on an existing signal plus a
  request-info bit; the SETUP round-trip already happens before the
  first row flows, so no added latency phase.
- **Version-gating is localized**: an old node simply never gets the
  new section and requests keep carrying inline programs (the
  capability check picks the encoding per node, the DBSPJ side
  already does per-node capability dispatch for other features).

## Cons

- **The win is narrower than it first looks.**  Scan ops — the bulk
  of many aggregation queries — pay the inline cost once per
  fragment and then run from the stored procedure; moving their
  programs to setup saves fragments × words, i.e. almost nothing.
  The benefit concentrates entirely in probe-heavy lookup children
  with non-trivial filters.  Queries without such children gain zero
  while still paying the added setup decode.
- **Two permanent code paths for filter sourcing.**  Non-aggregation
  pushed queries have NO JoinAggregationState (setup is agg-only), so
  the inline path can never be deleted.  Every interpreter entry
  point grows a second program source, and the two must behave
  identically forever — the testing surface roughly doubles for
  filter execution (both paths × the filter matrix), and subtle
  divergence bugs (one path updated, the other not) become possible.
- **Three-hop wire change** (API serialization, DBTC forward,
  DblqhProxy decode + state storage) plus interpreter plumbing in
  DBLQH/DBTUP for a transport optimization — meaningfully more
  moving parts than its cousin optimizations (e.g. the DECIMAL fast
  path was one function).  Request-info bits are also a scarce
  resource on the affected requests (the extended-flags areas are
  filling up: bits 29/30/31 of the strategy word are taken, the
  storedProcId upper bits likewise).
- **Op-addressing must be watertight.**  The request's op number must
  resolve to the right program on every node, across the I.16-style
  root rewrites and DBSPJ's node renumbering; a mismatch silently
  filters with the wrong program — a wrong-results class, not an
  error class.  (The D22/D25 family showed how subtle
  per-node-state-addressing bugs are to catch.)
- **Setup grows a hard dependency for correctness, not just for
  aggregation**: today a lost/failed setup fails the aggregation
  cleanly; with filters in the state, a state lookup failure must
  fail the QUERY (filtering must never be silently skipped) — new
  error paths at every reference site.
- **Memory accounting**: per-query per-node program storage in the
  state pool (small, but the pool is TransientPool-backed and the
  fields need the explicit-init discipline that pool has burned us on
  before).
- **Measurement gap**: there is no current profile showing filter
  program transport as a material cost.  The per-probe words ride
  sections that are being assembled anyway (for keys + params), so
  the marginal cost may be far below the 16 MB naive arithmetic —
  transporter batching amortizes much of it.  The AGGT-style phase
  probes / `x-ronsql-phases` breakdown could quantify it cheaply
  before committing to the wire change.

## Alternatives worth weighing before committing

- **First-use caching in DBLQH** keyed by (state, op): the first
  probe carries the program inline (wire unchanged, old nodes
  unaffected), DBLQH stores it in the state, subsequent probes carry
  a short reference.  Saves the same per-probe bytes without the
  API/DBTC hops — at the cost of cache-consistency reasoning (all
  probes of one op must carry identical programs; assert, don't
  trust).
- **Scope to CTE_LOOKUP only**: the probe-heavy shape with the
  largest programs (typed compares + NULL guards) and an existing
  per-op setup vehicle — the CTE contexts already flow through setup
  (`defineCte` sections); filters could ride next to the CTE
  aggregation program specifically, leaving plain lookups alone.
  Smallest blast radius, captures most of the win.
- **Do nothing until measured**: instrument the probe-send path
  (words per LQHKEYREQ with/without filters) on fs_batch / watermark
  benchmarks; if program words are < 10% of per-probe section bytes,
  the whole idea is below the noise floor.

## Verdict

Doable, and architecturally clean — it extends the exact pattern the
aggregation program already uses, with inherited lifetime and node
coverage.  The honest cost is not the wire work but the SECOND
permanent filter-sourcing path and the op-addressing correctness
surface, bought for a win that only materializes on probe-heavy
filtered lookup children.  Recommendation: measure first (per-probe
section-byte breakdown on the watermark/CTE_LOOKUP benchmarks); if
the numbers justify it, prefer the CTE_LOOKUP-scoped variant riding
the existing CTE setup material over the fully general per-op
design — it captures the concentrated win at a fraction of the
compatibility and testing surface.
