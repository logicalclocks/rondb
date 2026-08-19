# nest_semantics family — findings

Part A of `join_nest_semantics_plan.md`: aggregate-path
INNER-below-LEFT verification probes.  The first `--record` run is the
verification — fill in the verdict column from it.

Background: SQL's left-associative `t LEFT JOIN cl ... JOIN n ON
n.ref(cl)` ≡ all-INNER (null-rejecting equality); RonSQL's tree
expresses `t LEFT (cl INNER n)`.  `testMultiOuterJoinAggNdbApi` Test 2
already proves the aggregate kernel NULL-injection INNER-below-aware
for lookup MISSES; sn-15 proved the pass-through row path wrong for
NULL keys (that shape is gate-rejected since Phase 2).

| Cell | Case | Expected (MySQL, left-assoc) | Verdict (record run 2026-08-19) |
|---|---|---|---|
| aggregate × NULL-key | ns-2 (`COUNT(*)`, NULL-ref slice) | 0 | **WRONG** (proven by ns-1's arithmetic) |
| aggregate × lookup-miss | ns-3 (dangling slice) | 0 (Test 2 had predicted green) | **WRONG** (proven by ns-1's arithmetic) |
| full mix | ns-1 (`COUNT(*)`) | 10 | **WRONG — recorded 30** (10 matched + 10 dangling + 10 NULL all counted) |
| grouped / COUNT(col) | ns-4 / ns-5 | matched-only groups / 10 | ns-4 NEXT-PHASE-disabled with ns-1..3; **ns-5 GREEN (recorded 10)** — the injected rows carry NULL for the leaf column, so COUNT(col) skips them; the defect is confined to COUNT(*)-style counting of the injected rows, not to bogus leaf values |
| controls | ns-6 (all-INNER), ns-7 (LEFT-LEFT) | 10 · (30, 10) | **GREEN (recorded 10 · 30/10)** |

**Headline (Part A): INNER-below-LEFT on the aggregate path returned
silently wrong results in shipped territory** — the kernel
NULL-injection fed NULL-extended rows through a MatchNonNull (INNER)
aggregate LEAF for both lookup misses and NULL join keys.
Reconciliation with `testMultiOuterJoinAggNdbApi` Test 2 (green, same
tree shape): there the aggregate leaf is a LEFT node BELOW the INNER;
here the INNER is the leaf — the injection logic differs by leaf
position (still worth a kernel-side look for future genuine nests).

**Part B (fixed)**: the `promote_left_joins()` pre-pass rewrites every
flat-chain INNER-below-LEFT to all-INNER (SQL's left-associative
semantics; equality join conditions are null-rejecting), so the
offending trees are never emitted.  ns-1..4 re-enabled as compares,
ns-8 added for the fixpoint cascade (LEFT LEFT + trailing INNER
promotes the whole chain), ns-7 pins non-promotion of pure LEFT
chains, EXPLAIN greps pin [INNER]/[LEFT JOIN] as appropriate.
