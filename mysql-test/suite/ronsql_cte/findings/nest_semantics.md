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

**Headline: INNER-below-LEFT on the aggregate path returns silently
wrong results in shipped territory** — the kernel NULL-injection feeds
NULL-extended rows through a MatchNonNull (INNER) aggregate LEAF for
both lookup misses and NULL join keys.  Reconciliation with
`testMultiOuterJoinAggNdbApi` Test 2 (green, same tree shape): there
the aggregate leaf is a LEFT node BELOW the INNER; here the INNER is
the leaf — the injection logic differs by leaf position.  ns-1..4 are
NEXT-PHASE-disabled pending `join_nest_semantics_plan.md` Part B (the
LEFT→INNER promotion pre-pass), which fixes every flat-chain shape and
upgrades from quality improvement to correctness fix.
