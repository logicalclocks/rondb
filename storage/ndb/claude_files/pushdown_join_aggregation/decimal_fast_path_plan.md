# DECIMAL fast path: direct decimal-to-double conversion

**Status: W1 + W2 IMPLEMENTED and W3 VALIDATED (September 2026:
DecimalFastPathTest gunit sweep green, RonDB + RonSQL regression
suites green — the recorded DECIMAL baselines did not move, the
bit-identity contract held in practice; the one main-suite failure,
all_persisted_variables 443 vs 441, is a pre-existing fork pin
unrelated to this change.  W4 microbenchmark datum, DEBUG build over
the TPC-H-like decimal_testdata set: fast path 67 ns/iter vs string
path 4792 ns/iter (~71x; debug mode inflates the string side — the
assert-heavy decimal2string/my_strtod chain — so treat the release
ratio as smaller but still decisive).  W4 end-to-end benchmarks
(bench_q12_tpch / bench_q9_dbtc / .bench_ronsql vs .bench_sql)
pending.)**  Maintainer
decision:
the fast path stays in `decimal2double` itself (both engines), not a
kernel-local helper — the bit-identity gate carries the safety
argument.  W1: fold-and-divide fast path in mysys/decimal.cc with the
`v <= 2^53 && frac <= 22` gate, per-word bail-out, exact power-of-ten
tables, verbatim string-path fallback.  W2: `DecimalFastPathTest` in
unittest/gunit/decimal-t.cc (merge_small_tests-t) — bit-for-bit +
return-code oracle sweep against a copy of the old string
implementation (boundaries, every frac % 9 residue, 2^53 gate
straddles, signed zero, kernel-style bin2decimal leading-zero-word
layouts via decimal2bin/bin2decimal round-trips, 100k seeded random
values) plus A/B microbenchmarks BM_Decimal2Double_FastPath /
_StringPath over the TPC-H-like decimal_testdata set.

Backlog item recorded from the profiling finding below; sits above the
JIT idea in expected payoff on TPC-H shapes and benefits BOTH engines
from one change.

## The finding (profile evidence)

The real per-row cost of DECIMAL-heavy pushed aggregation is the
DECIMAL-to-double conversion, in both engines.  MySQL's
`decimal2double` is implemented as decimal-to-string followed by
`strtod` — a string round-trip per DECIMAL column per row — and it is
about two thirds of the aggregation core on both arms.  A direct
conversion (scale the decimal to an integer, one double divide, exact
for the precisions TPC-H uses) cuts that core by roughly 60% for every
DECIMAL-heavy pushed query.  Bigger win than the JIT on TPC-H shapes,
shared by both engines.

## The code, as it stands

- **The slow conversion** — `mysys/decimal.cc:1075`:

  ```c
  int decimal2double(const decimal_t *from, double *to) {
    char strbuf[FLOATING_POINT_BUFFER];
    ...
    rc = decimal2string(from, strbuf, &len);
    *to = my_strtod(strbuf, &end, &error);
    ...
  }
  ```

  Every call formats up to 81 digits into a buffer and re-parses it.

- **Kernel arm (pushed aggregation)** — the shared load kernel
  `AggInterpreterBase::loadColumnTypedFromBuf`
  (AggInterpreterBase.cpp:294-390): the `NDB_TYPE_DECIMAL` /
  `NDB_TYPE_DECIMALUNSIGNED` arms run `bin2decimal` (binary column
  format → `decimal_t`) and then, for scale > 0, `decimal2double` —
  once per row per DECIMAL column, for SUM / MIN / MAX / AVG loads,
  CASE arms, and every kOpLoadCol over DECIMAL.  Since the
  interpreter unification both interpreters (`AggInterpreter`
  normal-scan and `JoinAggInterpreter` join/CTE) share this ONE call
  site pair — a single fix covers both arms.

- **mysqld arm (baseline engine)** — `my_decimal2double`
  (sql-common/my_decimal.h:333) is a thin wrapper around the same
  `decimal2double`, called from the `val_real()` paths in
  sql/item*.cc — so the comparison baselines pay the identical string
  round-trip per row in double-context aggregation and expressions.

- **Already fine, leave alone**: the scale-0 arm uses
  `decimal2longlong` / `decimal2ulonglong` (mysys/decimal.cc:1175) —
  a pure base-10^9 digit-word loop, no strings.

## Design

### The direct conversion

`decimal_t` stores the number as base-10^9 words (`dec1`,
`DIG_PER_DEC1 = 9`), `intg` integer digits + `frac` fraction digits +
`sign`.  The mathematical value is exactly

    value = ± v / 10^frac

where `v` is the scaled integer formed by all digits.  The fast path:

1. Fold the integer-part words into an unsigned 64-bit `v`
   (`v = v * 10^9 + word`, the `decimal2longlong` pattern).
2. Fold the fraction words the same way; the LAST fraction word is
   partial when `frac % 9 != 0` — its stored value is left-justified,
   so divide it by `10^(9 - frac % 9)` (integer divide by a small
   constant-table power) before folding, or equivalently fold and
   track the effective scale.
3. Bail out to the existing string path the moment `v` would exceed
   2^53 (cheap comparison per word) or `frac > 22`.
4. Result: `*to = sign ? -(double)v / p10[frac] : (double)v / p10[frac]`
   with `p10[]` a 23-entry constant table of exact powers of ten.

### Why the gated result is BIT-IDENTICAL (the safety argument)

Within the gate:

- `v <= 2^53` ⇒ `(double)v` is exact (no rounding).
- `frac <= 22` ⇒ `10^frac` is exactly representable in a double
  (10^22 is the largest such power).
- IEEE division returns the correctly rounded quotient of its operand
  VALUES; both operands are exact, so the result is the correctly
  rounded double of the real number `v / 10^frac`.
- `my_strtod` is a correctly rounded decimal-to-double conversion of
  the SAME real number.

Two correctly rounded conversions of the same real value are the same
double, bit for bit.  Outside the gate nothing changes (string path).
Therefore the fast path can live INSIDE `decimal2double` itself with
zero behavior change anywhere in the server — no result drift, no
recorded-baseline movement, no display change.  Return-code parity:
within the gate no truncation or overflow is possible, so returning
`E_DEC_OK` matches the string path's behavior exactly.

The gate covers all practical analytics data: TPC-H's DECIMAL(12,2) /
DECIMAL(15,2) money values are far below 2^53 (~9.0e15, i.e. every
value up to 15 full digits and 16-digit values below 9007199254740992).
Values above it — 16+ significant digits — keep today's exact behavior
via the fallback.

### Where it lives — one change, both engines

**Phase 1 (the plan): put the fast path in `decimal2double`
(mysys/decimal.cc) directly**, fallback to the current
decimal2string+strtod body when the gate misses.  This is the single
change that serves both engines at once: the kernel aggregation arms
pick it up through `loadColumnTypedFromBuf`, the mysqld baseline
through `my_decimal2double`.  A kernel-local helper was considered and
rejected: it would leave the mysqld arm slow and duplicate the logic,
and the bit-exactness argument makes the shared-function change safe.

### Phase 2 (optional, measure first): fused bin2double in the kernel

`bin2decimal` is the other per-row cost in the kernel arms (binary
big-endian word format → `decimal_t`, with sign-bit unflipping).  A
fused `bin2double` reading the binary format straight into the scaled
integer would skip the `decimal_t` materialization entirely,
kernel-local (both interpreter arms, same two call sites), same gate
and fallback (`bin2decimal` + slow `decimal2double`).  Only worth doing
if the Phase 1 profile shows `bin2decimal` as the new top cost; it
does NOT apply to mysqld (which gets its decimals from the row format
via Field_new_decimal, not this path).

### Non-goals

- `decimal2longlong` / `decimal2ulonglong` (scale-0 arm) stay as they
  are — already string-free.
- No change to display formatting (D15 fixed-scale gates, printer) —
  the conversion is bit-identical, so nothing downstream can move.
- No arbitrary-precision improvement: the I.22 64-bit range guards and
  the >15-digit compact-display behavior stand unchanged.
- `double2decimal` (the reverse direction) untouched.

## Risks

- **Server-wide blast radius of touching decimal2double**: mitigated
  entirely by the bit-identity argument + fallback; the validation
  suite (W3) is the proof in practice.  Every caller — optimizer
  statistics, partitioning, replication, Items — sees identical
  outputs.
- **Partial-frac-word folding**: the one fiddly detail (step 2).  The
  unit sweep (W2) must cover every `frac % 9` residue and every
  intg/frac word-count combination at the gate boundaries.
- **decimal_t with intg = 0 or frac = 0, zero values, sign of zero**:
  covered by the sweep; the string path is the oracle.
- **Non-normalized decimal_t inputs** (leading zero words): folding
  handles them naturally (they contribute zero); the oracle comparison
  catches any deviation.

## Work items

- **W1 — implementation**: fast path in `decimal2double`
  (mysys/decimal.cc) per the design; `p10[]` table; per-word 2^53
  bail-out; fallback preserved verbatim.
- **W2 — unit oracle sweep**: a unit test (mysys/ or NDB block test
  style) comparing fast vs string conversion BIT-FOR-BIT over (a) an
  exhaustive boundary set — every precision 1..65 × scale 0..30
  at values 0, ±1, ±(10^k ± 1), ±(2^53 ± 1 scaled), max-digit
  patterns, every frac % 9 residue — and (b) a large randomized set;
  assert fallback engagement above the gate.
- **W3 — regression**: full MTR pass with emphasis on byte-identical
  recorded baselines: the ronsql_cte suites ×5 (DECIMAL-heavy
  families: agg, avg, dtwide, orderby_limit_cte, chain), ronsql_large,
  mysqld main.decimal / main.type_newdecimal / func_math, and the ndb
  pushdown suites.  ANY baseline diff is a fast-path bug by
  construction.
- **W4 — benchmarks**: before/after on the DECIMAL-heavy set —
  `bench_q12_tpch`, `bench_q9_dbtc`, `.bench_ronsql cte_tpch_q*` /
  `fs_*`, and the `.bench_sql` MySQL baselines (both engines should
  move); record the aggregation-core profile delta against the ~60%
  estimate.
- **W5 — decision point**: profile after W4; open Phase 2 (fused
  kernel bin2double) only if `bin2decimal` dominates the remaining
  core.
