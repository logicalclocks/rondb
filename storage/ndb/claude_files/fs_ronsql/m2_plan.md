# M2 — 64-bit numeric fidelity and display rules

Updated 2026-09-17 for the approved first-version scope. This supersedes
the original exact-DECIMAL design. See `ronsql_fs_support_plan.md`, WP-D.

## Scope and result contract

- Numeric accumulators remain signed Int64, unsigned Uint64, and double.
  No Int128 or Float128 accumulators, new wide opcodes, or wide wire records.
- Keep the existing aggregate record representation and result accessors.
- Integer SUM reports NDB error 1860 when an addition exceeds the
  accumulator's signed or unsigned 64-bit range. Never silently wrap.
  MySQL can return a wider DECIMAL for the same input; that difference is
  intentional and must be represented in the test expectations.
- Check additions both while accumulating rows and while merging partials.
  This is an intermediate-overflow contract: mixed positive/negative
  inputs may overflow in one evaluation order even when their final
  mathematical total fits. Do not promise topology-independent success.
- Double arithmetic retains its current representation and precision.
  Floating-point SUM can vary with accumulation order; retain appropriate
  comparison tolerances. Audit existing non-finite handling separately
  before claiming identical overflow behavior across every double path.
- Exact scaled DECIMAL accumulation is deferred. Keep current DECIMAL
  conversion/range restrictions and document remaining precision loss.
  Display formatting cannot recover digits already lost during loading.
- COUNT behavior is unchanged.

## Status

| Work | Status |
|---|---|
| Shared merge error contract and propagation through API/kernel | Complete |
| Preserve original errors, reject late work, cancel and drain CTE completion | Complete |
| Checked signed/unsigned 64-bit SUM in shared aggregate merges | Complete |
| Merge unit tests, including string-state cleanup and recovery | Complete |
| Distributed CTE SUM boundaries, overflow and recovery | Complete |
| CTE lookup/scan delivery while the direct API connection is unavailable | Complete |
| RonSQL integer SUM boundary/error tests and interpreter/JIT parity | Complete |
| AVG scale and FLOAT display rules | In progress: AVG column formatting |
| Framework expectations and requirements reports for the limited scope | Pending |

Completed commits include `8f064822249` (checked SUM) and `bd9c41158cd`
(distributed SUM tests and CTE result routing). Both are pushed.
The routing test passed 100 repetitions with diagnostics; the user
subsequently reported all requested tests passing after their removal.
Commit `2f618dae6bf` adds the passing RonSQL SUM and strict JIT regressions,
completing the M2.1/M2.2 gates for checked integer SUM.

## M2.1 — RonSQL coverage of the checked 64-bit contract

1. Add a focused RonSQL SUM regression shared by the base and JIT suites:
   - signed maximum/minimum, opposite signs, and exact integers above 2^53;
   - unsigned maximum and zero;
   - all-NULL groups and empty input;
   - positive signed, negative signed, and unsigned overflow;
   - scalar and grouped aggregation;
   - successful queries after each error.
2. For representable totals, compare CLI and HTTP results strictly with
   MySQL. For overflow, assert RonSQL error 1860 instead of comparing with
   MySQL's wider result; also check HTTP 400 and the semantic error class.
3. Keep the existing API/kernel tests as the deterministic coverage of
   fragment/merge paths, distributed placement, reply ordering, cancellation,
   and cleanup. SQL tests alone do not prove where the overflow occurred.
4. The user builds and runs the tests. Fix any discovered differences
   through individually reviewed patches.

## M2.2 — Interpreter/JIT parity

Run the same M2.1 cases with compiled interpretation disabled and enabled.
The JIT wrapper must arm strict compilation and pin the program-fallback
delta to zero; per-row fallback behavior follows the existing JIT policy.

Inspect row-accumulation and merge behavior if results differ. Keep the
existing i64/u64/f64 accumulator families and wire layout. No new
accumulator family or stencil widening is planned.

Gate: matching exact in-range integer results, error 1860 for each
overflow case, successful recovery, and no unexpected program fallback.

## M2.3 — AVG scale and FLOAT display

1. Carry source type and scale to the printer where needed.
2. Apply type-specific AVG formatting: integer scale 4, DECIMAL scale
   min(source scale + 4, 30), and the existing compact double formatter
   for floating inputs. Check scalar, grouped, ordered, and CTE results.
3. Use FLOAT-appropriate rendering for FLOAT MIN/MAX, pass-through, and
   GROUP BY values; SUM/AVG over FLOAT continue to use double.
4. Add focused base/JIT formatting tests. Preserve documented DECIMAL
   precision and rounding limitations; do not label double-based AVG or
   DECIMAL accumulation exact.

## M2.4 — Framework expectations and acceptance

- Update overflow expectations to the checked 64-bit contract. Require
  1860 for intentional overflow probes and exact values for in-range
  integer probes; never accept wrapped results.
- Retire F22's unchecked-merge finding when the relevant paths are verified.
  Record the intentional MySQL/RonSQL SUM range difference associated with
  F6 rather than claiming wider results now match.
- Retain F5/F21 and any remaining F2/F3/F4 expectations until their actual
  precision/formatting issues are fixed. Remove numeric canonicalization
  only where strict comparisons pass without it.
- Regenerate affected MTR expectations and run requirements/vector checks
  on base, JIT, ng2r2, and ng4r2.
- Report remaining DECIMAL limitations honestly. R-A5-types and overall
  acceptance cannot become PASS under the original exact-DECIMAL
  requirement solely because checked integer overflow now works.
- Run relevant performance checks if execution or formatting changes
  warrant them; there is no accumulator/wire expansion to benchmark.

## Execution order and review

M2.1 RonSQL regression → M2.2 parity validation/fixes → M2.3 formatting →
M2.4 framework and requirements closeout.

Present one normal diff at a time and obtain explicit approval before
editing. The user handles builds and tests unless explicitly delegated.
