# G: Cross-cutting duplication sweep of the RonSQL executor

Baseline: HEAD 50eb491eb3f (branch 26.10-main), storage/ndb/src/ronsql/.
Scope: RonSQLPreparer.cpp lines 9014-20106 (executor regions), ResultPrinter.cpp,
AggregationAPICompiler.cpp. Parser code (lines 1-9013) was used only as a
reference for helpers that already exist.
Read-only review. Nothing was built or run.

Region tags used below: ST = single-table/subqueries/lifecycle
(9014-11125, 15566-16102), JN = join (11125-13516, 15097-15566),
CTE (13516-15097, 17622-18300), FA = filter/aggregation program generation
(16102-17622, 18300-19170), EX = EXPLAIN (19170-20106), RP = ResultPrinter.cpp,
AC = AggregationAPICompiler.cpp, PARSER = lines before 9014.

## Method

1. Mechanical pass. The scripts are in this directory:
   - dupfind.py: strips comments and whitespace, drops blank and brace-only lines,
     then finds repeated windows across all regions and clusters overlapping
     windows. It has three modes. `exact` uses the normalised text. `notrace`
     also drops DEB_/STAT_/RONSQL_PERF/DBUG_/assert/require lines. `ident`
     replaces local identifiers and literals with placeholders, but keeps
     keywords, member names after `->`, `.` and `::`, and CamelCase type names.
   - Outputs: exact6.txt (48 clusters), notrace6.txt (47), ident6.txt (78),
     ident4.txt (127 clusters at window size 4, used for small cross-region
     idioms).
   - funclen.py: function lengths, used for the "longest functions" list.
   Result: verbatim copy-paste is concentrated in a few hotspots: ResultPrinter's
   two print loops, programAggregator against programAggregator_join, and
   resolve_chained_column_type against build_cte_virtual_tables. Most other
   duplication is at the idiom level: the same switch or sequence rewritten
   with renamed variables.
2. Idiom pass. I grepped for and counted the following sites: switches on
   column-type families, NdbScanFilter program builders, embedded-interpreter
   exit stanzas, mappings from operators to branches, operator flips,
   NdbError classification, nextResult drains, `execute()` with STAT pairs,
   output-list counts, CTE-output-to-source resolution, word-count estimates,
   and the aggregator emit error helpers.
3. Every site of the clusters below was read in full. For each cluster, I
   checked the differences between copies and how the kernel and NDB API
   behave where it mattered: NdbAggregator.cpp, AggInterpreter*.cpp,
   JoinAggInterpreter.cpp.

Divergence verdicts follow the brief's rule. A copy difference is called a
**BUG** only when I can name an SQL input that behaves differently between the
copies and where the difference is wrong. **PROBABLE** means the code
divergence is certain but reachability depends on earlier checks I could not
fully exclude. Every BUG and PROBABLE item needs an MTR test before it is fixed.

---

## Ranked cluster summary

| ID | Title | Sites | Est. lines saved | Divergence |
| --- | --- | --- | --- | --- |
| D-1 | Aggregation-program word sizes duplicated (raw_word_size, multi-leaf magic numbers, NdbAggregator emitters) | 4 | ~20 (small, but a correctness fix) | **BUG**: DECIMAL load inside a CASE arm, and in the multi-leaf subquery update |
| D-2 | CTE output type derivation (resolve_chained_column_type against build_cte_virtual_tables) | 2 x 3 aggregate kinds | ~170 | None today. A past divergence bug (obc-11) is documented in the comments |
| D-3 | SVM instruction to NdbAggregator translation (programAggregator, programAggregator_join, multi-leaf leaf aggregators) | 3 | ~110 | Minor: unsupported column types and SUM over strings give "report a bug" or generic errors on linked and multi-leaf loads |
| D-4 | Hidden sentinel slot computation (compile() against programAggregator_join) | 2 | ~20 | **PROBABLE**: CTE body with a cross-table WHERE (collides with an AVG slot, and empty groups are not suppressed) |
| D-5 | Scalar-CTE dummy lookup key (emit_root_op against emit_child_ops) | 2 | ~30 | **BUG**: VARCHAR scalar CTE as a filtered root |
| D-6 | ResultPrinter print loop (print_record against print_stored_record), plus the type gap against print_passthrough_value | 2 (+1) | ~200 | Gap: GROUP BY printing rejects YEAR, TIME2 and BINARY, which pass-through prints |
| D-7 | ResultPrinter aggregate-argument metadata lookups | 6 functions | ~60 | **PROBABLE**: GREATEST/LEAST watermark over temporal or FLOAT scalar-CTE outputs |
| D-8 | Pass-through drain skeleton (single-table scan arm against join drain) | 2 | ~90 | None |
| D-9 | NdbScanFilter program builder from a ConditionalExpression | 10 | ~45 | Hardening: JN sites ignore begin/end/finalise and setInterpretedCode results |
| D-10 | Embedded-interpreter emit primitives (exit stanza, branch-opcode maps) | 10 + 4 | ~70 | None |
| D-11 | Subquery result parsing and CE node construction | 3 + ~20 | ~70 | None (only the message text differs) |
| D-12 | CTE COLUMN output to body source column resolution | 2 full + 3 partial | ~40 | None |
| D-13 | Column-type family predicates (integer, float, string, temporal) | ~16 switches | ~80 | Policy differences only (DATE envelope) |
| D-14 | NdbError to RonSQL error classification | 2 + 2 tails | ~40 | Latent: SchemaError gives SEMANTIC in one copy and INTERNAL in the other (unreachable today) |
| D-15 | Comparison-operator mirror re-implemented next to flip_cmp_op | 1 + 5 | ~30 | None |
| D-16 | EXPLAIN CONDITIONS tree printer and IN-list summary | 3 | ~60 | Cosmetic wording only |
| D-17 | Small cross-file duplicates (quoted_identifier, macros, list counts) | ~14 | ~50 | None |

Ranking weighs lines saved, number of sites and bug risk. D-1, D-5, D-4 and D-7
rank high for their bug content. D-2, D-3 and D-6 rank high for size and for
how hard the copies are to keep in sync.

---

## D-1 Aggregation-program word sizes are duplicated, and one copy is wrong for DECIMAL (BUG)

**Sites**
- AC `AggregationAPICompiler::raw_word_size` (AggregationAPICompiler.cpp:1316-1345).
  It counts LoadConstantInteger as 3 words, Greatest2/Least2 as 18 or 11 words,
  and **every other instruction, including Load, as 1 word**.
- NdbAggregator emitters (storage/ndb/src/ndbapi/NdbAggregator.cpp:1155-1199
  `LoadColumn`, 1204-1236 `LoadLinkedColumn`) emit **2 words for
  Decimal/Decimalunsigned**: the opcode plus a precision/scale word.
- Kernel: `kOpSkip` advances `exec_pos` by a **word** count
  (AggInterpreterBase.cpp:1867-1870), and the DECIMAL kOpLoadCol consumes the
  extra word (AggInterpreterBase.cpp:181-182). The embedded-interpreter output
  is added to `exec_pos` as words (AggInterpreter.cpp:459-462).
- FA `programAggregator` / CTE `programAggregator_join`: the `EmbeddedInterp`
  arm computes `then_arm_total = raw_word_size(then_start, skip_pos) + 1`, and
  the `Skip` arm uses `raw_word_size(else_start, else_end)`
  (RonSQLPreparer.cpp:17257-17281 and 18146-18170).
- JN `execute_join` multi-leaf subquery path:
  `aggregate_update_raw_size = count_star_needs_row_guard ? 12 : 2;`
  (RonSQLPreparer.cpp:11449-11452). The comment says "Column load + aggregate:
  2", which assumes a 1-word load.
- FA `emit_pair_op_embedded` (18300-18372) is the emitter whose sizes
  (14-word body + 4 = 18, 9 + 2 = 11) are copied into raw_word_size.

**Divergence (BUG)**
`AggregationAPICompiler::compile(AggExpr*, idx)` (AC:567-615) places the Load
of a CASE arm's value inside the arm: THEN arm from `then_start` to `skip_pos`,
ELSE arm after `svm_init()`. A DECIMAL column in an arm therefore makes the arm
one word longer than raw_word_size counts.

- `SELECT g, SUM(CASE WHEN a = 1 THEN d ELSE 0 END) FROM t GROUP BY g`, with
  `d DECIMAL(10,2)`. The THEN arm is really [Load d (2 words), Sum (1)] = 3
  words, but it is computed as 2, so `then_arm_total` = 3 instead of 4. On a
  row where `a <> 1`, the embedded program outputs 3 (generate_embedded_condition
  sets `first_exit_output = then_arm_raw_size` for OR, 18910-18913). The jump
  lands on the `Skip` word, which skips the whole ELSE arm. A group with no
  `a = 1` row returns NULL where MySQL returns 0.00. With `ELSE other_col`, the
  ELSE contributions are silently lost.
- `SUM(CASE WHEN a = 1 THEN 0 ELSE d END)`. On a THEN row, `Skip(else_raw)`
  is one word short. It lands on the ELSE arm's last word (`AggRepeat`), which
  re-aggregates the ELSE register (not loaded on this row) into the slot.
- Multi-leaf: a SELECT-list subquery `SUM(t2.d)` (DECIMAL) with a cross-table
  filter. When the filter fails, the update is skipped by 2 words, but the
  update is 3 words: LoadColumn (2) + Sum (1). Execution lands on `Sum` with
  an unloaded register. The guarded COUNT(*) path (12) is unaffected: its
  LoadUint64 is a fixed 3 words.

I found no earlier rejection of DECIMAL columns in CASE arms. CASE arms that
load columns are exercised by tests (`THEN o.o_amt`, `THEN sums.t`), but not
with a DECIMAL column.

**Proposed helper**
- One source of truth for words per instruction:
  `Uint32 RonSQLPreparer::agg_instr_words(const QueryScope&, const AggregationAPICompiler::Instr&, Uint32 pc)`.
  It returns 2 for a Load of a DECIMAL column (through
  `scope.resolved_columns[src].dict_column`, or the CTE virt column, which
  never holds DECIMAL because it is widened), 3 for LoadConstantInteger, the
  pair-op constants for Greatest2/Least2, and 1 otherwise.
- Option A: cache it per instruction in the compiler, in the same way as
  `m_pair_op_needs_null_check`, filled by an extended
  `prepare_pair_op_null_check_cache(scope)` that becomes
  `prepare_program_word_sizes`. `raw_word_size` then just sums the cache.
- Option B: give `raw_word_size` a `load_words(col_idx)` callback.
- Declare `kPairOpNullableWords = 18` and `kPairOpFastWords = 11` once, next
  to `emit_pair_op_embedded`, and use them in raw_word_size.
- Multi-leaf: `aggregate_update_raw_size = load_words(attr) + 1`.

**Lines saved**: ~20. The value is the fix, not the size.
**Risk**: low to medium. Word counting changes only for DECIMAL loads.
Regression tests: SUM/COUNT over CASE with a DECIMAL column in the THEN arm,
then in the ELSE arm, single-table and join; groups whose rows all take the
ELSE path; a multi-leaf SELECT-list subquery with SUM(decimal) and a
cross-table filter.

---

## D-2 CTE output type derivation is written twice

**Sites**
- JN `resolve_chained_column_type` (13192-13502, 311 lines). Arms: SUM
  13251-13310, MIN/MAX 13311-13423, AVG 13424-13500.
- CTE `build_cte_virtual_tables` (13516-14080, 565 lines). Arms: COUNT/SUM
  13612-13705, MIN/MAX 13706-13833, AVG 13834-13926.
  Mechanical hits: ident6 "6 x 6" at 13251/13316/13444/13631/13710/13855, and
  JN 13396 against CTE 13790, JN 13469 against CTE 13879.

**How they differ**
- Failure mode: the chained version returns `false`, and build_cte_virtual_tables
  throws per-type messages: "SUM over this column type…", "AVG over string
  columns…", "AVG over temporal…", "Unsupported aggregate function…". This is
  intentional, because the chain walk is a probe.
- MIN/MAX on Char/Varchar and the default arm: the chained version copies the
  argument's scale/precision, build_cte_virtual_tables leaves 0. No observable
  difference: those types carry scale 0, and the default arm is documented as
  unsupported end to end.
- Everything else, including the D15/obc-11 display-metadata gating
  (`Double && CteResultColumn && 0<scale<=30 && precision>0`), the D1 DECIMAL
  widening and the AVG scale+4 / precision rule, is textually the same.

**Divergence**: none today. The comments document that the copies already
diverged once. resolve_chained_column_type's SUM arm says "found by obc-11:
zeroing here starved every CHAINED layer of the fixed-scale display", and
most arms say "mirror build_cte_virtual_tables". Each new aggregate rule has to
be added twice.

**Proposed helper** (next to resolve_chained_column_type, declared in
RonSQLPreparer.hpp):
```cpp
struct CteColumnType {
  NdbDictionary::Column::Type type; Uint32 length; const void* cs;
  Int32 scale; Int32 precision;
};
enum class CteTypeMode { Probe, Strict };   // Probe: return false; Strict: throw
bool derive_cte_output_type(QueryScope& cte_scope, const Outputs* o,
                            CteTypeMode mode, CteColumnType& out);
```
resolve_chained_column_type keeps the StoredColumn and COLUMN recursion and
calls `derive_cte_output_type(*cs, o, Probe, …)`. build_cte_virtual_tables
calls it in `Strict` mode and keeps only the PK/nullable and attr-size logic.
**Lines saved**: ~170. **Risk**: medium. The display metadata is subtle; the
obc-11 and D15 tests (ronsql_cte*) and the AVG display tests cover it.

---

## D-3 The SVM instruction translation exists three times, and error classification differs between them

**Sites**
- FA `programAggregator` (17148-17293). Its instruction switch is 17171-17292.
- CTE `programAggregator_join` (17866-18197). Its instruction switch is
  18005-18173.
  Mechanical hits: notrace "24 lines x 2", 17206-17240 against 18096-18130;
  ident "27 x 2", "14 x 2", "9 x 2", "7 x 2".
- JN `execute_join` multi-leaf subquery leaf aggregators (11325-11515): a
  third, reduced emitter covering GROUP BY / GroupByLinked, LoadColumn, Sum,
  Count, Min and Max.

**How they differ**
- Same in both programAggregator copies: LoadConstantInteger, Mov, the 7
  arithmetic ops, Sum (through `throw_sum_avg_emit_error`), Min, Max, Count,
  Greatest2/Least2, AggRepeat, EmbeddedInterp, Skip. That is about 75 lines.
- Intentional differences: the Load arm (linked parent, CTE leaf and non-leaf
  CTE loads in the join version), Avg (single-table `require_run(false)`,
  because only CTE scopes register Avg), the EmbeddedInterp
  `cteVirtualTables` argument (nullptr against the parameter), the GROUP BY
  loop (GroupByLinked) and the sentinel tail (see D-4).
- **Divergence in error classification (minor bug)**:
  - The F16 handling (`kErrUnSupportedColumn` gives "Aggregation over a column
    of this type is not supported.") exists only for `LoadColumn` (17188-17198,
    18076-18087). Both `LoadLinkedColumn` calls (18036-18050) go through
    `programAggregator_do_or_fail`, which says "Failed writing aggregation
    program. Please report a bug." NdbAggregator::LoadLinkedColumn sets
    `kErrUnSupportedColumn` for the same types (NdbAggregator.cpp:1211-1213).
    Example: `SELECT t2.k, MAX(t1.bin8) FROM t1 JOIN t2 ON t2.pk = t1.fk GROUP BY t2.k`,
    where t1 is the non-leaf (linked) op and `bin8` is BINARY(8). The query
    reports an internal bug, while `SELECT MAX(bin8) FROM t1` reports
    "not supported".
  - The multi-leaf path uses `require_prm(agg->LoadColumn(...), "Failed to load
    column for subquery aggregate.")` and `require_prm(agg->Sum(...), "Failed
    to program SUM.")` (11481-11497). It loses both F16 classifications
    (unsupported column type, and SUM over string or temporal).
  - These only affect the error message and class (UNSUPPORTED against
    INTERNAL or a generic PermanentError). Results are not affected.

**Proposed helpers** (FA, beside throw_sum_avg_emit_error):
```cpp
// One SVM instruction other than Load/Avg-policy; shared by both programAggregators.
void emit_svm_instr(NdbAggregator*, QueryScope&, const SelectStatement&,
                    Uint32 pc, NdbDictionary::Table* const* cteVirtualTables);
// Load result check with F16 classification; used for LoadColumn,
// LoadLinkedColumn and the multi-leaf path.
void check_agg_load(NdbAggregator*, bool ok, const LexCString& col_name);
void emit_agg_fun(NdbAggregator*, TokenKind fun, Uint32 slot, Uint32 reg,
                  const SelectStatement&);   // SUM/COUNT/MIN/MAX/AVG with F16
```
programAggregator becomes "GROUP BY loop + Load arm + emit_svm_instr". It
could also be expressed as programAggregator_join with a single-op scope, but
keeping two thin callers is lower risk.
**Lines saved**: ~110. **Risk**: low. The arms are mechanical; keep the Avg
policy as a flag.

---

## D-4 The hidden sentinel slot is computed twice, ignores AVG, and runs for CTE scopes with no consumer (PROBABLE)

**Sites**
- PARSER `compile()` (8679-8696): sets `ast_root.sentinel_agg_slot` for the
  main scope only, and only when `!m_has_select_subqueries`.
- CTE `programAggregator_join` (18176-18196): emits `LoadUint64(1)` and
  `Count(sentinel_slot)` for **any** scope with remaining cross-table filters,
  the main scope and CTE body scopes alike.
- Both compute `max(dest)+1` over `Sum|Count|Min|Max|AggRepeat`. **`Avg` is
  missing**, although `FORALL_AGGS` includes it (AggregationAPICompiler.hpp:63-68).

**Why this is a divergence**: the compiler already guarantees that aggregate
slots are exactly `0..m_aggs.size()-1`: `compile()` checks
`next_aggregate == m_aggs.size()` (AC:546-562). The two hand-written scans
duplicate that knowledge and missed `Avg` when it was added.

**PROBABLE bug.** CTE body scopes do get cross-table filters:
`build_cte_scopes` calls `classify_where_by_table(*scope, …)` (7450),
`classify_where_by_table` routes only the main scope (`&scope == &m_main_scope`,
3766), and `build_cte_linked_projections` registers CTE cross-table filter
columns (4704-4708). `check_cte_body_col_vs_col` rejects only
identifier-against-identifier comparisons (6137-6166). So
`WITH c AS (SELECT t1.g AS g, AVG(t2.v) AS a FROM t1 JOIN t2 ON t2.pk = t1.fk WHERE t1.y < t2.z + 1 GROUP BY t1.g) SELECT g, a FROM c`
appears to reach programAggregator_join with a CTE scope and a remaining
cross-table filter.
1. The program is `Avg(dest=0)`. The sentinel scan ignores Avg, so
   `sentinel_slot = 0`, and `Count(0, reg)` follows `Avg(0, …)`.
   NdbAggregator::Count sets `agg_ops_[0] = kOpCount`, overwriting kOpAvg, and
   increments `n_agg_results_` to 2 with slot 1 undefined
   (NdbAggregator.cpp:1494-1510).
2. Even without AVG, nothing suppresses zero-pass groups of a CTE body. The
   kernel allocates the group record in the prologue before the program runs
   (JoinAggInterpreter.cpp:1551-1587), and STOP_PROGRAM only ends the row's
   program (1861-1862). For the main scope, ResultPrinter drops
   `sentinel == 0` groups (ResultPrinter.cpp:1164-1168). CTE output has no such
   step, so `WITH c AS (SELECT t1.g, COUNT(*) AS n … WHERE t1.y < t2.z + 1 GROUP BY t1.g)`
   would contain `(g, 0)` rows that MySQL does not produce.
3. Separately, in the main scope with `m_has_select_subqueries`, compile()
   leaves `sentinel_agg_slot = -1`, but programAggregator_join still emits the
   sentinel if that path is reached. Whether the single-leaf path can run with
   select subqueries plus cross-table filters needs checking.

**Proposed helper**: `Uint32 AggregationAPICompiler::num_aggregates() const { return m_aggs.size(); }`.
Use it in both places, with one function
`bool scope_needs_sentinel(const QueryScope&)` used by both compile() and
programAggregator_join. Then either reject cross-table filters in CTE bodies
with a clean UNSUPPORTED error, or add sentinel suppression on the CTE path
(kernel-side or via a HAVING-like check).
**Lines saved**: ~20. **Risk**: low for the helper. The CTE behaviour change
needs MTR coverage first: confirm items 1 and 2 with the two queries above.

---

## D-5 The scalar-CTE dummy lookup key is built twice, and the root copy lacks the VARCHAR fix (BUG)

**Sites** (both in the JN region; may overlap with the JN reviewer)
- `emit_root_op` (12703-12735), filtered scalar-CTE root
  (`root_cte_is_scalar && join_where_ce[0] != NULL`, 12689).
- `emit_child_ops` (15385-15430), scalar CTE as a child.

**Difference**: both switch on the virt PK column type: Bigint, Bigunsigned
and Double get typed `constValue(0)`, and the default arm gets a zero buffer.
The child copy has the sc-20 fix. For non-fixed array types it passes
`dummy_len = 0`, because "VAR columns take the raw VALUE length and prepend
the 1-or-2-byte length prefix themselves — passing getSizeInBytes() there
overflows by the prefix size and fails bindOperand with
QRY_CHAR_OPERAND_TRUNCATED". The root copy still passes
`rpksz = getSizeInBytes()` for every non-numeric type.

**BUG input**: `WITH s AS (SELECT MAX(name) AS mx FROM t) SELECT mx FROM s WHERE mx > 'a'`
with `name VARCHAR(n)`. The scalar CTE's PK is its first output. A VARCHAR
MIN/MAX output keeps type Varchar (build_cte_virtual_tables 13775-13789), with
`m_arraySize = 1+n` (14031-14035). emit_cte_lookup_filter accepts VARCHAR
MIN/MAX outputs (14745-14756). The root then builds a `1+n`-byte operand for a
VAR column, which the child copy's comment documents as failing. The same
scalar CTE as a join child works. Severity: query error, not wrong results.

**Proposed helper**: `const NdbQueryOperand* scalar_cte_dummy_key(NdbQueryBuilder*, const NdbDictionary::Column* pk)`,
the child version, called from both places. **Lines saved**: ~30.
**Risk**: low.

---

## D-6 ResultPrinter's two print loops are ~200 identical lines

**Sites**
- RP `print_stored_record` (731-978) and `print_record` (1917-2197).
  Mechanical: notrace clusters of 71, 55 and 31 lines. A `diff` of the two
  functions shows only the STORE_*/END_OF_* commands in print_record, the
  register restore in print_stored_record, and comment differences.
- Related type switch: `print_passthrough_value` (1392-1589).

**Divergence**: none between the two loops. Against the pass-through printer,
the GROUP BY printer raises `feature_not_implemented` for Year, Time2, Binary,
Varbinary and Longvarbinary, and decodes Date by hand. The pass-through
printer supports all of them through `print_temporal_packed` and
`binary_value_bytes`. So `SELECT y, COUNT(*) FROM t GROUP BY y` with a YEAR
column fails with "RonSQL feature not implemented: Print GROUP BY column of
type Year", while `SELECT y FROM t` prints. This is a feature gap, not a copy
bug. Whether NdbAggregator GroupBy accepts these types was not verified.

**Proposed helper**: `void ResultPrinter::print_group_by_value(std::ostream&, const Cmd&, const NdbAggregator::Column&)`
and `void ResultPrinter::run_print_cmd(std::ostream&, const Cmd&)`, shared by
both loops. In a second step, route GROUP BY values through the pass-through
type printer by building a byte view, which closes the gap.
**Lines saved**: ~200. **Risk**: low.

---

## D-7 ResultPrinter aggregate-argument metadata lookups follow GREATEST/LEAST inconsistently (PROBABLE)

**Sites**: RP `avg_arg_scale` (2655), `aggregate_uses_float_display` (2670),
`aggregate_arg_charset` (2685), `aggregate_arg_scale` (2732),
`aggregate_arg_precision` (2764) and `aggregate_arg_temporal` (2794). Each
repeats "type check → arg → isLoad → bounds → has_metadata → field".
Mechanical hits: RP 2734-2754 against 2766-2784.

**Divergence**: scale and precision use `ronsql_first_load_in_expr`, which
walks Greatest2/Least2 (the D8 watermark fix). temporal, charset and
float_display require `arg->isLoad()` directly.

**PROBABLE bug**:
`WITH a AS (SELECT MAX(d) AS m FROM t1), b AS (SELECT MAX(d) AS m FROM t2) SELECT GREATEST(a.m, b.m) AS w`
with `d DATE`. The CTE MIN/MAX(date) output is a Bigunsigned packed value
(13790-13806). `validate_greatest_least_pair_loads` skips CTE operands
(18541-18551). The implicit-MAX wrapper's argument is a Greatest2, so
`aggregate_arg_temporal` returns NONE and the result prints as the packed
integer. `SELECT MAX(a.m)` prints a date. The same applies to FLOAT sources,
which would print with double formatting.

**Proposed helper**:
`const ColumnMetadata* ResultPrinter::aggregate_arg_metadata(const Outputs*, bool allow_sum) const`,
which walks through `ronsql_first_load_in_expr` for every caller. The six
functions become one-line field reads, apart from avg_arg_scale's default of 4.
**Lines saved**: ~60. **Risk**: low. Add an MTR test of GREATEST over
DATE/FLOAT scalar-CTE MAX outputs.

---

## D-8 The pass-through drain skeleton is written twice

**Sites**
- ST `execute_single_table_passthrough`, scan arm (10594-11087, 494 lines).
- JN `execute_passthrough_drain` (11920-12319, 400 lines).
  Mechanical hits: 10919 against 12077 and 10960 against 12135, 9 lines each;
  4-line hits at 10626/12030 and 10651/11947.

**Shared**: output and ORDER BY counting, ORDER BY sort-key resolution
(10892-10942 against 12079-12145, slot reuse, extra getValue and the m_cmp
check), `PassthroughSortBuffer`, `execute(NoCommit)` with STAT_TS/STAT_SET,
the drain loop with the first-row STAT, the identical
`#ifdef RONSQL_PHASE_STATS` drain_us block (11051-11059 against 12257-12265),
`sort_and_print_passthrough_rows`, and the JSON framing tail.

**Intentional differences**: single-table only: api_rows (J5), the PK-lookup
arm, index-order streaming with batch_rows and `closeNoWait`; it accepts only
StoredColumn sort keys. Join only: CTE attrs, `isRowNULL` substitution, dummy
getValue for empty projections, and explicit query error classification.
Single-table relies on `require_run(rc == 1)` and handle_ronsql_exception,
which also honours `m_output_started`. No divergence bug found.

**Proposed helpers** (ST, beside sort_and_print_passthrough_rows):
```cpp
class PassthroughFraming {            // is_json, header_emitted, m_output_started
 public: void row(attrs, n, is_first); void finish(attrs, n); };
Uint32 resolve_passthrough_sort_keys(const SelectStatement&, QueryScope&,
    Uint32 num_cols, const Uint32* out_cols,
    std::function<const NdbRecAttr*(const ResolvedColumnRef&, Uint32 slot)> fetch_extra,
    PassthroughSortKey* keys);
void record_drain_stats(Uint64 s_exec_sent, Uint32 rows);
```
**Lines saved**: ~90. **Risk**: medium, because of output-framing subtleties
(LIMIT 0, JSON on early close). Keep the early-close ordering at each caller.

---

## D-9 The NdbScanFilter program builder appears at 10 sites

**Sites**: ST 9741 (pk-lookup aggregate residual), 9937 and 10109
(open_single_table_scan_op, through apply_filter_top_level), 10732
(pass-through residual). JN 11646 (CTE body root), 12475
(emit_index_scan_root), 12561 (emit_pk_equality_index_scan_root), 13120 and
13175 (emit_root_op), 15157 (emit_child_ops). Mechanical: ident "6 x 6" at
11646/12475/12561/13120/13175/15157.

**Differences**: ST sites check `begin() >= 0`, `end() >= 0` and
`finalise() == 0`. JN sites ignore all three, and four of six ignore
`setInterpretedCode()`'s return (12561 checks it). An NdbInterpretedCode
failure (OOM) on a JN root would leave an unfinalised program, so
setInterpretedCode fails silently and the scan runs unfiltered. That cannot be
triggered from SQL, so it is hardening, not a bug.
`emit_index_scan_root` (12460-12477) also rebuilds AND-combination by hand;
`append_condition` (PARSER 857) already does this.

**Proposed helper** (FA, next to apply_filter):
```cpp
void build_filter_program(NdbInterpretedCode& code, QueryScope& scope,
                          ConditionalExpression* ce);   // begin/apply/end/finalise, all checked
```
and use `append_condition(m_amalloc, T_AND, …)` for residual chains.
**Lines saved**: ~45. **Risk**: low.

---

## D-10 Embedded-interpreter emit primitives

**Sites**
- Exit stanza `LoadConst16(r, out) / WriteInterpreterOutput(r, 0) / ExitOK()`:
  JN 11476-11478, and FA 17587-17599, 18324-18364 (5 times), 19147-19160.
- TokenKind to `BRANCH_*_REG_REG` with invert/direct: FA 17533-17553 and FA
  18985-19005 (identical 19-line lambdas). CTE 14530-14535 uses the
  NdbInterpretedCode `branch_*` API with the same inversion table.
  FA 18306-18307 picks GE/LE.
- TokenKind to `Interpreter::BinaryCondition` for the arg family:
  FA 18966-18984.

**Divergence**: none. I checked every mapping against the documented
inversion rules (comment at 18950-18961).

**Proposed helpers** (an anonymous namespace before programAggregator):
`emit_output_exit(NdbAggregator*, Uint32 reg, Uint32 output)`,
`Uint32 reg_reg_branch_opcode(TokenKind, bool invert)` and
`Uint32 arg_branch_cond(TokenKind, bool invert)`. emit_pair_op_embedded keeps
its fixed layout but uses `emit_output_exit`; its word count ties to D-1.
**Lines saved**: ~70. **Risk**: low.

---

## D-11 Subquery result parsing and CE node construction

**Sites**
- `parse_subquery_value` (PARSER 6563-6591) is used only for correlated pairs
  (ST 9087-9088). The IN arm (ST 9124-9152) and the scalar arm (ST 9172-9198)
  re-implement it inline. They differ only in the error text.
- SubqueryResult to constant CE: ST 9261-9271, 9290-9301 and 9403-9414, plus
  in-place at 9513-9526.
- Binary CE allocation: two identical local `node` lambdas (ST 9240-9247 and
  9446-9453), ~20 open-coded `alloc_exc<ConditionalExpression>` +
  op/left/right sites in ST (9275-9350, 9417-9434) and FA (simplify_ce
  16747/16858-16944, rewrite_minmax_comparison 17063-17105), and OR-chain
  accumulation (9318-9327, 9424-9434) that duplicates `append_condition`.

**Divergence**: none.

**Proposed helpers**: `parse_subquery_value(const char*, SubqueryResult&, const char* err)`
(extend the existing function), a member
`ConditionalExpression* new_ce(TokenKind, ConditionalExpression*, ConditionalExpression*)`,
`ConditionalExpression* subquery_result_ce(const SubqueryResult&)`, and
`append_condition` for the OR chains. The grammar's `init_cond` macro is the
parser-side counterpart.
**Lines saved**: ~70. **Risk**: low.

Note: `substitute_subquery_results_ce` (9225-9577) also hand-rolls child
traversal instead of using `condition_children` (PARSER 643). Its default arm
recurses into `args.left/right` for any operator, including layouts such as
T_EXISTS (`subquery.stmt` aliases `args.left`). This is harmless today,
because `check_no_nested_exists` and decorrelate_exists remove T_EXISTS. It is
still a latent hazard.

---

## D-12 Resolving a CTE COLUMN output to its body source column

**Sites**: CTE `emit_cte_lookup_filter` (14683-14730), FA
`generate_embedded_condition::resolve_col_side` (18712-18750), CTE
`require_cte_case_condition_column_output` (18200-18225) and
`resolve_case_condition_column` (18227-18252), and the CTE chain-walking
lambda `source_type` (14450-14480).
Mechanical: CTE 14688 against FA 18715, CTE 18216 against FA 18720.

**Divergence**: none. The checks are identical except for the message prefix.
Both reject chained CTE sources (StoredColumn required), so they agree.

**Proposed helper**:
```cpp
struct CteColumnSource { const NdbDictionary::Column* col;
                         const NdbDictionary::Table* table; Uint32 attr_no; };
CteColumnSource resolve_cte_column_source(QueryScope& scope,
    const QueryScope::ResolvedColumnRef& ref, const char* ctx);
```
**Lines saved**: ~40. **Risk**: low.

---

## D-13 Column-type family predicates

**Sites** (the switch statements that list Tinyint..Bigunsigned, plus extras):
PARSER 5782; ST 10236-10255 (string check); JN 11392, 12712, 12833, 12905
and 15394; CTE 13252, 13317, 13445, 13632, 13711, 13856, 13995 and 14189;
FA 16388, 17408, 18565 and 18813; RP 757, 1414 and 1974. Several of these are
removed by D-2 and D-6.

**Policy differences**, intentional:
- Typed register loads: the CTE filter accepts ints, Float, Double and Date
  (14187).
- The cross-table arithmetic accepts ints only (17407).
- CASE col-against-col accepts ints, Float and Double, with no Date (18811).
  Its message says so; the leaf `READ_ATTR_INTO_REG` Date support is unknown.
- Pair-op stored operands accept ints only (18565).

**Proposed helpers** (RonSQLCommon.hpp): `is_int_type`, `is_unsigned_int_type`,
`is_float_type`, `is_decimal_type`, `is_string_type` and `is_temporal2_type`.
Each policy then becomes a one-line expression next to its message.
**Lines saved**: ~80 (excluding D-2 and D-6). **Risk**: very low.

---

## D-14 NdbError classification

**Sites**: ST `throw_classified_ndb_error` (15575-15616) and the
`handle_ronsql_exception` runtime_error arm (15700-15780). Both contain the
rate-limit test, the temporary test
(`status == TemporaryError || mysql_code == HA_ERR_LOCK_WAIT_TIMEOUT`), the
",od" output-started rule and the classification switch. There are also the
two query-error tails: JN execute_join (11855-11883) and execute_passthrough_drain
(12274-12295).

**Divergence (latent)**: `NdbError::SchemaError` maps to SEMANTIC in
handle_ronsql_exception but falls to INTERNAL in throw_classified_ndb_error.
All current callers handle SchemaError first: throw_key_read_error and both
drains. So the difference is unreachable.

**Proposed helpers**: `static RonSQLErrorClass classify_ndb_error(const NdbError&)`,
`static bool is_temporary_ndb_error(const NdbError&)`, and
`[[noreturn]] void throw_query_error(const NdbError&, const char* what, const char* where)`
for the two drain tails. The tails differ only in message text and the
`details = NULL` copy, which can stay with the caller.
**Lines saved**: ~40. **Risk**: low.

---

## D-15 The comparison-operator mirror is re-implemented next to `flip_cmp_op`

**Sites**: the helper `flip_cmp_op` (PARSER 6507) and five inline copies:
PARSER 4166-4174 (cross_table_bound_op), CTE 14601-14609 and 14784-14791
(emit_cte_lookup_filter), FA 16729-16733 (simplify_ce) and FA 17010-17016
(rewrite_minmax_comparison).
**Divergence**: none.
**Fix**: move `flip_cmp_op` and `negate_cmp_op` above line 4122, or into
RonSQLCommon.hpp, and call them. **Lines saved**: ~30. **Risk**: trivial.

---

## D-16 EXPLAIN CONDITIONS tree printer

**Sites**: EX 19235-19295 (join/CTE-body root), 19728-19760 (PK plus residual)
and 19817-19850 (single-table index scan). All three compute label and prefix
widths (11, 13 or 11, +1 when handling > 9, +2 for the multi-byte "│").
IN-list summary lines: 19243, 19485 and 19788. Their wording differs:
"; SF_MultiRange", "; multi-range" and "range(s)". JoinOp type and match-type
name switches: 19181-19194 and 19475-19482.
**Divergence**: cosmetic wording only.
**Helpers**: `print_condition_tree(out, prefix, conds, n, handling_fn, label_kind)`,
`join_op_type_name(JoinOp::Type)` and `print_in_list_summary(out, sc)`.
**Lines saved**: ~60. **Risk**: low, but EXPLAIN text in result files must
stay byte-identical; check the ronsql_explain baselines.

---

## D-17 Small cross-file duplicates

- `quoted_identifier` formatter, 4 copies: RonSQLPreparer.cpp 126 (LexString)
  and 139 (char*), ResultPrinter.cpp 116 (LexCString), AggregationAPICompiler.cpp
  1144 (char*). Move one `std::string_view` core to RonSQLCommon.hpp or
  define_formatter.hpp.
- `feature_not_implemented` macro: RonSQLPreparer.cpp 120 and ResultPrinter.cpp 111.
  `require_sch`: RonSQLPreparer.cpp 612 and ResultPrinter.cpp 144.
- Output-list length loops: 10623, 11590, 11942, 12646, 15363 and 15479.
  ORDER BY count: 10654 and 11950. Add a `count_list(const T*)` template.
- PERF_TS and STAT_TS pairs for the same events in execute_join (for example
  11829-11836). A single macro would emit both.
**Lines saved**: ~50. **Risk**: trivial.

---

## Five longest executor functions and suggested decomposition

1. **`emit_cte_lookup_filter`** (CTE 14114-14957, 844 lines). Almost all of it
   is the `emit_atom` lambda (14218-14901, ~680 lines). Split it into members
   that take a small `CteFilterCtx {code, scope, op_idx, virtTab, linked_base}`:
   `emit_cte_isnull_atom`, `emit_cte_col_vs_col_atom` (typed reg loads; with
   D-13 and D-10), `emit_cte_ancestor_const_atom` (~14560-14660),
   `emit_cte_output_const_atom` (inline-type path against source-column path;
   with D-12) and `emit_cte_dnf_driver` (14903-14948).
2. **`execute_join`** (JN 11291-11907, 617 lines). Extract
   `build_multi_leaf_aggregators` (11325-11515, through the D-3 helpers),
   `emit_cte_subtrees(qb, …)` (CTE body aggregators and op defs, ~11530-11760),
   and `run_aggregating_join(query)`, which covers execute, drain, error (D-14)
   and print. `execute_passthrough_drain` stays.
3. **`emit_root_op`** (JN 12599-13185, 587 lines). Extract `emit_cte_root`
   (scalar, CTE lookup and scanCte arms, 12640-13010, using D-5's helper and a
   `make_cte_key_operand` that the kind_ok prevalidation (12827-12858) and the
   construction (12896-12955) can share), `emit_pk_lookup_root`
   (13034-13150), and `emit_table_scan_root` (13172-13184, with D-9).
4. **`generate_embedded_condition`** (FA 18585-19161, 577 lines). Make
   `resolve_col_side` a member `resolve_case_side` (D-12). Split the pre-pass
   into `plan_case_atom`, which computes word counts, and the emission into
   `emit_case_atom`, which covers col-against-col, float-const and
   col-against-const arms. Use the D-10 primitives for the branch maps and the
   exit stanzas. `generate_embedded_filter_condition` (17496-17600) can then
   share the exits and opcode maps.
5. **`build_cte_virtual_tables`** (CTE 13516-14080, 565 lines). Move type
   derivation into `derive_cte_output_type` (D-2). Move the attr-size
   population switch (13994-14075) into
   `populate_virtual_column_sizes(NdbDictionary::Table*)`. What remains is the
   PK and nullability decision plus `vt->aggregate()`: about 90 lines.

Next in length: `print()` (EX 19367-19882, 516; D-16), and
`execute_single_table_passthrough` (ST 10594-11087, 494; D-8, split the PK
arm into `execute_pk_lookup_passthrough`).

---

## Suggested order of work

1. Write MTR tests for D-1, D-5, D-4 and D-7 that reproduce the reported
   divergences. Then fix D-1 (word sizes) and D-5 (dummy key) as small
   behaviour patches.
2. Make the mechanical extractions with no behaviour change: D-15, D-17, D-13,
   D-10, D-11, D-9 and D-12.
3. Make the larger merges, each in its own patch with the existing suites:
   D-2 (CTE type derivation), D-3 (SVM translation and error classification),
   D-6 (ResultPrinter print loop), D-8 (pass-through drain), D-14, then D-16
   (EXPLAIN baselines).
4. Decide on D-4 (reject or support cross-table filters in CTE bodies) once
   the test shows the actual behaviour.
