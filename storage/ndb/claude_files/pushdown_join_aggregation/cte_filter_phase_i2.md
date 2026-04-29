# Phase I.2 — Top-level OR / DNF in CTE_LOOKUP filter pushdown

## Status

Plan-only. Builds on Phase D / D2 / I.1 (`emit_cte_lookup_filter`).

## Scope

Lift the current AND-only restriction in
`emit_cte_lookup_filter` (`RonSQLPreparer.cpp:5713`) to accept a
**top-level disjunctive normal form** filter on CTE_LOOKUP output:

```
WHERE  D_1  OR  D_2  OR ... OR  D_n
```

where each disjunct `D_i` is either a single atom or a conjunction of
atoms `A_i1 AND A_i2 AND ...`.  Atoms remain the existing supported
set:

- column-vs-constant comparison (`= != < <= > >=`) on a CTE output —
  COLUMN projection (mem-opcode path) or numeric AGGREGATE output
  (inline-opcode path);
- `IS NULL` / `IS NOT NULL` on a CTE column or aggregate output (with
  the existing LEFT_OUTER guard for `IS NULL`).

The shape generalises the AND-only case (`n = 1`) and the single-atom
case (`n = 1`, one atom) without changing them — both must continue to
emit identical interpreted-code programs.

Out of scope:

- Mixed nesting that isn't already DNF.  An expression like
  `(a = 1 OR b = 2) AND (c = 3 OR d = 4)` is rejected with a clean
  message.  RonSQL doesn't normalise WHERE to DNF today, and a
  CNF-to-DNF conversion is exponential in the worst case.
- Column-vs-column comparisons (Phase I.3).
- `NOT` outside an `IS NOT NULL` atom (no SQL surface today).

## Background

The kernel's CTE filter dispatch (Phase A/B) accepts the full
`branch_linked_*` opcode family.  `testCteNdbApiFilter` Test 8
demonstrates `branch_label` + `def_label` for arbitrary forward jumps
inside a CTE filter program — exactly what OR needs to skip remaining
disjuncts after one has matched.  No kernel-side change required.

## Code-emit design

For an `n`-disjunct DNF, emit:

```
                         ; for each disjunct D_i (i = 0..n-1):
                         ;   FAIL_i = (i < n-1) ? new_label() : REJECT
   ; --- Disjunct 0 ---
   branch FAIL_0  if !A_01
   branch FAIL_0  if !A_02
   ...
   branch ACCEPT          ; all atoms in D_0 passed
def_label FAIL_0
   ; --- Disjunct 1 ---
   branch FAIL_1  if !A_11
   ...
   branch ACCEPT
def_label FAIL_1
   ...
   ; --- Disjunct n-1 (last) — fail goes straight to REJECT ---
   branch REJECT  if !A_(n-1)1
   ...
   ; fall through to ACCEPT
def_label ACCEPT
   interpret_exit_ok
def_label REJECT
   interpret_exit_nok
```

Properties:

- `n = 1`: no FAIL_i labels are allocated; the last-disjunct path is
  the only path; the program is byte-equivalent to today's AND
  emission (a small comment-only diff stays).
- Atom branches use the same inverted-branch primitives as today
  (`branch_linked_inline_ne` for `=`, etc.) — only the target label
  is parametrised.
- `branch ACCEPT` after each non-last disjunct uses
  `NdbInterpretedCode::branch_label`, identical to Test 8.

### Helper introduction

Add a sibling to `flatten_and_conjuncts` near `RonSQLPreparer.cpp:296`:

```cpp
static void
flatten_or_disjuncts(struct ConditionalExpression* ce,
                     ConditionalExpression** disjuncts,
                     Uint32* count);
```

Same recursive structure as `flatten_and_conjuncts`, walking `T_OR`.
Caller passes the simplified WHERE; if no top-level OR is present, the
output array has one element (the whole expression) — preserving the
current behaviour.

### Refactor of `emit_cte_lookup_filter`

Rename the inner conjunct-emit body into a local lambda
`emit_conjunct(atom, fail_label)` so the outer driver can call it
once per atom across all disjuncts.  The lambda contains the existing
T_IS / cmp dispatch (lines 5739-6020).  Result:

```cpp
ConditionalExpression* simplified = simplify_ce(where_ce, -1);
ConditionalExpression* disjuncts[MAX_WHERE_CONJUNCTS];
Uint32 num_disjuncts = 0;
flatten_or_disjuncts(simplified, disjuncts, &num_disjuncts);
require_prm(num_disjuncts > 0,
            "CTE_LOOKUP filter: no disjuncts after simplification.");

const Uint32 REJECT = 0;
const Uint32 ACCEPT = 1;
Uint32 next_label = 2;  // FAIL_i labels start here

for (Uint32 di = 0; di < num_disjuncts; di++) {
  ConditionalExpression* conjs[MAX_WHERE_CONJUNCTS];
  Uint32 num_conjs = 0;
  flatten_and_conjuncts(disjuncts[di], conjs, &num_conjs);
  require_prm(num_conjs > 0,
              "CTE_LOOKUP filter: empty disjunct.");

  const bool is_last = (di == num_disjuncts - 1);
  const Uint32 fail_label = is_last ? REJECT : next_label++;

  for (Uint32 ci = 0; ci < num_conjs; ci++) {
    emit_conjunct(conjs[ci], fail_label);
  }

  if (!is_last) {
    require_prm(code.branch_label(ACCEPT) == 0, "...");
    require_prm(code.def_label(fail_label) == 0, "...");
  }
  /* On the last disjunct, falling through reaches the ACCEPT label
   * defined below — no extra branch needed. */
}

require_prm(code.def_label(ACCEPT) == 0, "...");
require_prm(code.interpret_exit_ok() == 0, "...");
require_prm(code.def_label(REJECT) == 0, "...");
require_prm(code.interpret_exit_nok() == 0, "...");
require_prm(code.finalise() == 0, "...");
```

The atom-emit lambda preserves all current restrictions verbatim:
column-vs-const only, T_IS LEFT_OUTER guard, virt-table-column
type/charset lookup, swapped-operand canonicalisation.

### Capacity check

`MAX_WHERE_CONJUNCTS` (current cap, used for `flatten_and_conjuncts`)
applies separately to disjuncts and to atoms within each disjunct.
Total atoms across all disjuncts ≤ `MAX_WHERE_CONJUNCTS` × N is the
program-size bound for label space.  Verify
`NdbInterpretedCode`'s label slot count covers this; if not, raise the
limit or reject programs that would exceed it.

### Mixed-shape rejection

After `flatten_or_disjuncts`, walk each disjunct: if any disjunct
contains a `T_OR` deeper than the top level (i.e. a child of an AND
inside a disjunct), reject:

```
"CTE_LOOKUP filter: only top-level OR / DNF supported.  Convert
'(A OR B) AND C' to DNF or split into UNION."
```

Same rejection if the simplified expression contains `T_NOT` outside
an `IS NOT NULL` atom.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` —
  `flatten_or_disjuncts` helper, refactor `emit_cte_lookup_filter`.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` — no signature change
  (the helper is file-static).
- `mysql-test/suite/ronsql/t/ronsql_cte_or.test` — new file.
- `mysql-test/suite/ronsql/r/ronsql_cte_or.result` — recorded.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_i.md`
  — flip I.2 status to "shipped" once landed.

## Test plan

New file `mysql-test/suite/ronsql/t/ronsql_cte_or.test`, fixture
identical to `ronsql_cte_basic.test` (cte_orders / cte_lineitem) so
the same `sums` / `maxes` virt-table shapes apply.

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `WHERE k = 100 OR k = 300` | 2-disjunct OR on GB column (mem-opcode path) |
| 2 | `WHERE k < 100 OR k > 300` | 2-disjunct OR with inequality |
| 3 | `WHERE t = 50 OR t = 150` | 2-disjunct OR on aggregate (inline-opcode) |
| 4 | `WHERE k = 100 OR k = 200 OR k = 300` | 3-disjunct OR |
| 5 | `WHERE k = 100 OR t > 100` | OR mixing GB column and aggregate |
| 6 | `WHERE (k = 100 AND t > 50) OR (k = 200)` | DNF: AND-conjunction inside one disjunct |
| 7 | `WHERE k IS NULL OR k = 100` | OR with IS NULL on aggregate-output (works on inner-join CTE_LOOKUP per I.1) |
| 8 | `WHERE k = 100 AND (t = 50 OR t = 150)` | **Rejected cleanly** — non-DNF shape |
| 9 | `WHERE NOT (k = 100)` | **Rejected cleanly** — NOT outside IS NOT NULL |
| 10 | regression: existing `WHERE k != 100 AND t > 50` | unchanged 1-disjunct path |

Tests 1-7 use the `ronsql_compare.inc` harness (same cluster vs mysql
result-set comparison as `ronsql_cte_basic`).  Tests 8-9 use
`--error` to verify the rejection text.  Test 10 confirms no
regression in the AND-only path's emitted program (recorded result
matches today's).

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_or
./mtr --suite=ronsql                       # full suite — no regressions
./mtr --suite=ndb_push_agg                 # block tests — no regressions
```

## Commit cadence

Single commit:

| Commit | Contents | Approx LoC |
|---|---|---|
| 1 | `flatten_or_disjuncts` helper + `emit_cte_lookup_filter` DNF refactor + MTR tests + plan-doc status flip | ~250-350 |

The refactor is small and self-contained; splitting code+test would
churn `_cte_filter_phase_i.md` twice.

## Risks

1. **Label space.**  N disjuncts → N-1 internal labels plus ACCEPT
   and REJECT.  `NdbInterpretedCode` allocates labels up to
   `NDB_MAX_INSTRUCTIONS_PER_CODE_FRAGMENT` (large); a defensive cap
   in the emitter (e.g. 16 disjuncts) keeps program size sane.
2. **Single-disjunct equivalence.**  The 1-disjunct path must
   produce the same byte sequence as today's AND-only emit — record
   tests for `ronsql_cte_basic.test` should not need re-recording.
   Verified by running `./mtr --suite=ronsql ronsql_cte_basic`
   without `--record` after the refactor.
3. **DNF detection vs. parser tree shape.**  Rejection of non-DNF
   shapes must not be too aggressive.  Test 8's recorded message
   matches the exact text in `emit_cte_lookup_filter` so a future
   tweak is detected.
4. **Aggregate vs column path interleaving in one disjunct.**  Already
   handled by the existing per-atom dispatch — no new code.
5. **`IS NULL` on LEFT_OUTER inside an OR.**  Existing per-atom
   guard fires regardless of OR position; the rejection text already
   tells the user why.

## What we're not doing

- DNF normalisation of arbitrary boolean trees.  RonSQL would need a
  full `simplify_to_dnf` pass; out of scope for I.2.
- Column-vs-column comparisons (Phase I.3).
- Lifting the same OR support to non-CTE filter pushdown
  (`AggregationAPICompiler` / `ndbinterpreter` predicate emission for
  scan WHERE) — different code path, different consumers, different
  phase.
