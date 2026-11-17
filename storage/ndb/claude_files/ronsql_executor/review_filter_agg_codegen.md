# Executor review D: generation of pushed-down filter and aggregation programs

Repository: /Users/mikael/mysql_trees/rondb_2610_main, branch 26.10-main, HEAD 50eb491eb3f.
Paths below are relative to `storage/ndb/src/ronsql/` unless stated otherwise.
Method: source reading only. Nothing was built or run. Each "trigger" below is
derived from reading the code; none has been executed.

Scope: `apply_filter*`, `literal_in_column_charset`, `query_const_value`,
`encode_constant`, `simplify_ce`, `ronsql_eval_int_cmp`,
`rewrite_minmax_comparison`, `throw_sum_avg_emit_error`, `programAggregator`,
`programAggregator_join` (compared, and its sentinel), `embedded_filter_expr_word_count`,
`emit_embedded_filter_expr`, `generate_embedded_filter_condition`,
`emit_pair_op_embedded`, the pair-op null-check cache and validators,
`generate_embedded_condition`, and `AggregationAPICompiler.{hpp,cpp}`.

Excluded because earlier reviews cover them: Boolean-term collection, the 32-term limits and the
parser-side simplification (`ronsql_parser_review_plan.md`). Also excluded are the DBSPJ/kernel
findings E-1..E-30 (`storage/ndb/claude_files/dbspj/README.md`). The note at the end
links one kernel behaviour (E-4 family) to a pair-op path without re-reporting it.

---

## Summary table (most severe first)

| ID | Title | Category | Severity | Confidence |
|----|-------|----------|----------|------------|
| F-1 | `AVG(CASE ...)` in the main query aborts the process in `AggregationAPICompiler::compile` | bug | crash | verified-by-reading |
| F-2 | Unary minus over a substituted float scalar subquery dereferences a NULL source text in `simplify_ce` (join path) | bug | crash | likely |
| F-3 | `raw_word_size` counts a DECIMAL load as 1 word, but `LoadColumn` emits 2, so CASE skip offsets land inside the wrong arm | bug | wrong-result | verified-by-reading |
| F-4 | Right-nested arithmetic in a cross-table filter overwrites the register holding the left operand | bug | wrong-result | verified-by-reading |
| F-5 | The GREATEST/LEAST rewrite breaks three-valued logic under NOT and XOR | bug | wrong-result | verified-by-reading |
| F-6 | The cross-table filter sentinel drops the single row of a scalar aggregate when no row passes | bug | wrong-result | verified-by-reading |
| F-7 | CTE body with a cross-table filter: the sentinel is never used (phantom groups), and its slot can collide with an AVG slot | bug | wrong-result / error-handling | likely |
| F-8 | The cross-table filter loads leaf columns of any type into integer registers (DECIMAL, DATE and strings compared as raw bits) | bug | wrong-result | likely |
| F-9 | A FLOAT column compared with a literal behaves differently in WHERE (float constant) and in CASE (double), and the WHERE path differs from MySQL | bug | wrong-result | verified-by-reading (MySQL side likely) |
| F-10 | A substituted float scalar subquery is rounded to the DECIMAL column's scale before comparison | bug | wrong-result | verified-by-reading |
| F-11 | Temporal constants are truncated to the column's fractional precision before comparison | bug | wrong-result | likely |
| F-12 | Literals out of range or not representable raise errors instead of folding to TRUE/FALSE/UNKNOWN | design | error-handling | verified-by-reading |
| F-13 | WHERE (NdbScanFilter) and CASE (embedded interpreter) accept different predicate sets for the same text | design | error-handling | verified-by-reading |
| F-14 | `!` (T_EXCLAMATION) is not lowered in WHERE and fails as a "Non-boolean term" | bug | error-handling | verified-by-reading |
| F-15 | An aggregation program larger than the 8192-word buffer reports "Please report a bug" instead of a LIMIT error | bug | error-handling | verified-by-reading |
| F-16 | Word counts are hand-maintained in parallel with the emitters; a mismatch aborts (`ndbrequire`) or misjumps silently | design | maintainability (root cause of F-3) | verified-by-reading |
| F-17 | `programAggregator` and `programAggregator_join` duplicate the instruction switch; the sentinel computation is also duplicated | duplication | maintainability | verified-by-reading |
| F-18 | The integer-type predicate is copied about 15 times, and the copies differ | duplication | maintainability | verified-by-reading |
| F-19 | The two embedded predicate emitters duplicate atom layout, branch-opcode tables, NULL guards and exits, and have diverged | duplication | maintainability | verified-by-reading |
| F-20 | NdbScanFilter boilerplate is repeated at about 11 sites with inconsistent return-code checks | duplication | error-handling | verified-by-reading |
| F-21 | Test gaps in WHERE type coverage, CASE arm types, cross-table filters, 3VL rewrites and WHERE/CASE parity | test-gap | n/a | verified-by-reading |
| F-22 | No unit test ties `raw_word_size` and the word counts to the emitted programs; OverflowUnitTest covers only the folding helpers | test-gap | n/a | verified-by-reading |

---

## F-1 `AVG(CASE ...)` in the main query aborts the process

- **Category / severity / confidence:** bug, crash, verified-by-reading.
- **Location:** `AggregationAPICompiler::compile(AggExpr*, Uint32)` (AggregationAPICompiler.cpp:567-624, line 615); `svm_use` (475-484). Callers: main-scope AVG decomposition at RonSQLPreparer.cpp:1056-1057 and HAVING `AVG` at RonSQLParser.y:742.
- **Evidence:**
  The main query turns AVG into two aggregates over the same `Expr*`:
  ```cpp
  outputs->avg.agg_index_sum = m_main_scope.agg->Sum(outputs->avg.arg);
  outputs->avg.agg_index_count = m_main_scope.agg->Count(outputs->avg.arg);
  ```
  When that `Expr*` is a CASE, each aggregate compiles the CASE again. The THEN arm passes `is_first`, which is false on the second pass. The ELSE arm hard-codes `true`:
  ```cpp
  bool is_first = !case_expr->has_been_compiled;
  ...
  pushInstr(agg->agg_type, idx, then_reg, is_first);        // THEN
  ...
  pushInstr(SVMInstrType::AggRepeat, idx, else_reg, true);  // ELSE: always "first"
  ```
  `svm_execute(AggRepeat)` then calls `svm_use(src, true)`, which runs:
  ```cpp
  ndbrequire(value->usage - value->program_usage > 0);
  ```
  `CaseExpr()` increments `else_expr->usage` only once (line 366). The Sum pass already consumed that use, so the Count pass fails the check. `ndbrequire` is `ndbcluster::ndbrequire` (`storage/ndb/plugin/ndb_require.h`), which calls `std::abort()`. This takes down RDRS or `ronsql_cli`.
- **Trigger:** `CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT) ENGINE=NDB;` then `SELECT AVG(CASE WHEN k > 5 THEN v ELSE 0 END) FROM t;`. `... HAVING AVG(CASE ...) > 0` triggers it too. CTE bodies are not affected because they use the single `Avg` slot.
- **Suggested fix:** pass `is_first` to the ELSE `AggRepeat`, as the THEN arm does. Also count the Case node's own `usage` and check it in the final consistency loop instead of skipping Case nodes. A cheaper alternative is to give AVG over CASE two separate CaseExpr copies at registration.
- **Suggested test:** ronsql suite, a new case in `ronsql_cte_case` or a new `ronsql_case_types` test. Compare MySQL and RonSQL for `SELECT AVG(CASE WHEN k > 5 THEN v ELSE 0 END) FROM t` and the HAVING form, with and without GROUP BY.

## F-2 Unary minus over a substituted float scalar subquery: NULL dereference in `simplify_ce`

- **Category / severity / confidence:** bug, crash, likely (the path was read end to end; no earlier guard was found).
- **Location:** `simplify_ce`, `case T_MINUS` (RonSQLPreparer.cpp:16936-16962); substitution at 9518-9524.
- **Evidence:**
  ```cpp
  ce->op = T_FLOAT;                        // substitute_subquery_results_ce
  ce->constant_float.dbl = result.float_val;
  ce->constant_float.ls = LexString{NULL, 0};
  ...
  assert(n->constant_float.ls.len > 0);     // simplify_ce T_MINUS
  if (n->constant_float.ls.str[0] == '-') { // NULL deref in release builds
  ```
  `encode_constant` was fixed for `ls.str == NULL` (16519-16538), but `simplify_ce` was not. On the join and CTE paths, `emit_root_op` calls `simplify_ce(scope.join_where_ce[0], -1)` at execute time (12743, 12866, 13044), after `substitute_subquery_results()` (9621). The parser accepts `T_MINUS cond_expr` over `( subquery )` (RonSQLParser.y:659, 758-763). The scalar result becomes T_FLOAT whenever its text is not an integer (9158-9180), for example DECIMAL `MIN/MAX/SUM` or `AVG`.
- **Trigger:** `SELECT o.c, COUNT(*) FROM o JOIN l ON l.ok = o.id WHERE o.total > -(SELECT MIN(price) FROM p) GROUP BY o.c;` with `price DECIMAL(10,2)`. The debug build hits the assert and aborts. The release build gets SIGSEGV. On the single-table path the T_MINUS node is simplified before substitution, so it reaches `encode_constant` and fails with a misleading "incompatible value" error instead.
- **Suggested fix:** in `simplify_ce`, treat `ls.str == NULL` like the parser does: negate `.dbl` and keep `ls = {NULL,0}`. Better, fold unary minus into the substituted constant inside `substitute_subquery_results_ce`. Also simplify after substitution on the single-table path, so both paths behave the same.
- **Suggested test:** `ronsql_cte_subquery`: compare MySQL and RonSQL on a two-table join with `WHERE root.col > -(SELECT MIN(dec_col) FROM t3)`, and on the single-table form.

## F-3 `raw_word_size` undercounts DECIMAL loads, so CASE skip offsets point to the wrong instruction

- **Category / severity / confidence:** bug, wrong-result, verified-by-reading.
- **Location:** `AggregationAPICompiler::raw_word_size` (AggregationAPICompiler.cpp:1316-1345). Consumers: `programAggregator` (RonSQLPreparer.cpp:17258-17281) and `programAggregator_join` (18147-18170).
- **Evidence:** every Load counts as one word:
  ```cpp
  if (t == SVMInstrType::LoadConstantInteger) count += 3;
  else if (Greatest2/Least2) count += needs_null_check ? 18 : 11;
  else count += 1;                        // Load: always 1
  ```
  `NdbAggregator::LoadColumn` and `LoadLinkedColumn` emit an extra precision/scale word for DECIMAL (`src/ndbapi/NdbAggregator.cpp:1190-1199` and `1233-1240`). The kernel consumes that word (`AggInterpreterBase.cpp:173-185`: `m_prog[exec_pos++]`). Take `COUNT(CASE WHEN k > 5 THEN d ELSE 0 END)` with `d DECIMAL(10,2)`:
  - the THEN arm is `[LoadCol][dec_info][Count][Skip(4)]`, counted as 2+1=3 but actually 4 words;
  - when the condition is false, the embedded program returns skip 3 and lands on `Skip(4)`, which jumps over the whole ELSE arm, so the row is never counted;
  - when a DECIMAL load is in the ELSE arm, `Skip(else_raw)` is too short and lands on the ELSE `AggRepeat`. For `SUM(CASE WHEN k>5 THEN 1 ELSE d END)` that adds the THEN register again (double counting). On the other path it adds a stale register value.
- **Second site, same root cause:** `execute_join`, SELECT-list subquery leaf (RonSQLPreparer.cpp:11448-11450). There `aggregate_update_raw_size = count_star_needs_row_guard ? 12 : 2` assumes a 1-word `LoadColumn`. For `SUM(dec_col)` with a cross-table filter, a failed filter skips 2 words and lands on the `Sum`, which adds the previous row's register (likely).
- **Trigger:** `CREATE TABLE t (id INT PRIMARY KEY, k INT, d DECIMAL(10,2)) ENGINE=NDB; INSERT (1,1,2.00),(2,10,3.00);` then `SELECT COUNT(CASE WHEN k > 5 THEN d ELSE 0 END) FROM t;`. MySQL returns 2 and RonSQL returns 1. Also `SELECT SUM(CASE WHEN k > 50 THEN d ELSE 0 END) FROM t;`: MySQL returns 0.00 and RonSQL returns NULL.
- **Suggested fix:** do not estimate. Either have `raw_word_size` call a shared `NdbAggregator`-side size function (`load_words(type)`) with the column type, or measure the arm by emitting it and taking the difference of `curr_prog_pos_` (see F-16). At minimum, pass the scope's resolved column types into `raw_word_size`.
- **Suggested test:** ronsql `ronsql_cte_case` or `ronsql_case_types`: compare MySQL and RonSQL for SUM, COUNT and MIN over `CASE` with DECIMAL in the THEN arm, the ELSE arm and both, single-table and join, and with all rows failing the condition. Add a `ronsql_subquery_agg` case for a SELECT-list subquery `SUM(dec)` with a cross-table predicate.

## F-4 Right-nested arithmetic in a cross-table filter overwrites the left operand's register

- **Category / severity / confidence:** bug, wrong-result, verified-by-reading.
- **Location:** `emit_embedded_filter_expr` (RonSQLPreparer.cpp:17458-17488); accepted by `filter_expr_reg_depth` (17296-17319) through `classify_where_by_table` (3711-3712).
- **Evidence:**
  ```cpp
  emit_embedded_filter_expr(..., ce->args.left,  ..., reg,     tmp_reg, ...);
  emit_embedded_filter_expr(..., ce->args.right, ..., tmp_reg, reg,     ...);  // right uses reg as scratch
  ... Add/Sub/Mul(reg, reg, tmp_reg)
  ```
  The left operand is always evaluated first. If the right operand is itself binary, it writes its own right child into `reg`, which overwrites the left value. `filter_expr_reg_depth(c*(a+b)) == 2` passes the `> 2` check.
- **Trigger:** `SELECT o.c, COUNT(*) FROM o JOIN l ON l.ok = o.id WHERE o.x < l.y * (l.z + 1) GROUP BY o.c;` evaluates `o.x < 1 * (l.z + 1)`: `l.y` is in R2, the constant 1 is loaded into R2, and then `R2 = R2 * R3`. `WHERE o.x > 10 - (l.a - l.b)` evaluates `l.b - (l.a - l.b)`.
- **Suggested fix:** follow Sethi-Ullman. When `depth(right) > depth(left)`, emit the right subtree first into `tmp_reg` (scratch `reg`) and then the left leaf into `reg`. Word counts do not change. Alternatively use separate scratch registers (R4..R7 are free).
- **Suggested test:** `ronsql_join_where_classify` or `ronsql_join_agg`: compare MySQL and RonSQL for `a < b*(c+1)`, `a > 10-(b-c)` and `(b+1)*c < a`, with nullable and non-nullable operands.

## F-5 GREATEST/LEAST rewrite breaks three-valued logic under NOT and XOR

- **Category / severity / confidence:** bug, wrong-result, verified-by-reading.
- **Location:** `rewrite_minmax_comparison` (RonSQLPreparer.cpp:16999-17110), called from `simplify_ce` (16737-16742).
- **Evidence:** the rewrite says in its comment that it is valid only in a positive context:
  ```
  // GREATEST/LEAST is NULL when any argument is NULL, so the OR direction
  // additionally requires every column argument to be non-NULL; the AND
  // direction is already NULL-rejecting through its per-arg comparisons.
  ```
  The `IS NOT NULL` guards evaluate to FALSE for a NULL argument, where SQL gives UNKNOWN. In the AND direction, `(a<c) AND (b<c)` is FALSE when a is NULL and b >= c. `simplify_ce` handles `T_NOT` after rewriting the child (16889-16919), so the negation applies to the rewritten tree. NdbScanFilter (SQL semantics) then computes `NOT FALSE = TRUE`. XOR is expanded to CNF with `NOT x` (16869-16885) and is affected in the same way.
- **Trigger:** rows `(a=NULL, b=20)` and `(a=NULL, b=5)`.
  - `SELECT COUNT(*) FROM t WHERE NOT (GREATEST(a, b) < 10);`: MySQL returns 0 (GREATEST is NULL). RonSQL counts the b=20 row.
  - `SELECT COUNT(*) FROM t WHERE NOT (GREATEST(a, 5) > 10);`: MySQL returns 0. RonSQL returns 2, because the guard `a IS NOT NULL` is FALSE and its negation is TRUE.
- **Suggested fix:** put the predicate in negation normal form before the rewrite, pushing NOT through AND/OR/XOR. Then turn `NOT (x op c)` into `x op' c`, which is valid in 3VL for scalar comparisons, so the rewrite only runs in positive context. The alternative is an encoding that is UNKNOWN for NULL inputs, for example `col = col` instead of `col IS NOT NULL` for non-float columns.
- **Suggested test:** `ronsql_cte_greatest_least` (WHERE section): compare MySQL and RonSQL for `NOT (GREATEST(a,b) < c)`, `NOT (LEAST(a,b) > c)`, the forced-true form `NOT (GREATEST(a, 20) > 10)`, and `(GREATEST(a,b) > c) XOR (d = 1)`, on rows with NULL arguments.

## F-6 The cross-table filter sentinel drops a scalar aggregate's only row

- **Category / severity / confidence:** bug, wrong-result, verified-by-reading.
- **Location:** `programAggregator_join` sentinel (RonSQLPreparer.cpp:18176-18196); slot set at 8679-8696; `ResultPrinter::print_result_ordered` (ResultPrinter.cpp:1163-1168, 1180-1186).
- **Evidence:** for a query without GROUP BY, `NdbAggregator` always produces one record (`NdbAggregator.cpp:213-221` and the COUNT fix-up at 1875-1893). The sentinel COUNT is then 0, and the printer does this:
  ```cpp
  if (m_query->sentinel_agg_slot >= 0) {
    ... if (sentinel.data_int64() == 0) continue;     // also for no GROUP BY
  ...
  if (num_rows == 0) { if (m_json_output) out << "[]\n"; return; }
  ```
- **Trigger:** `SELECT COUNT(*), SUM(l.qty) FROM o JOIN l ON l.ok = o.id WHERE l.qty > o.c + 1000000;`. MySQL returns `0, NULL`; RonSQL returns no row.
- **Suggested fix:** apply the sentinel suppression only when `groupby_columns != NULL`.
- **Suggested test:** `ronsql_join_where_classify`: compare MySQL and RonSQL on a scalar aggregate whose cross-table filter rejects every row, and on one that passes some rows.

## F-7 CTE body with a cross-table filter: phantom groups and a possible slot collision with AVG

- **Category / severity / confidence:** bug, wrong-result / error-handling, likely.
- **Location:** `programAggregator_join` called for CTE bodies (RonSQLPreparer.cpp:11611); sentinel loop at 18180-18191; `check_cte_body_col_vs_col` (6125-6160).
- **Evidence:**
  - CTE scopes run `classify_where_by_table` (7450). A cross-table conjunct with arithmetic on one side passes `check_cte_body_col_vs_col`, which rejects only bare identifier-vs-identifier pairs (6138-6141). It then becomes a `cross_table_where_filter`, and `programAggregator_join` emits the STOP_PROGRAM filter and a sentinel COUNT.
  - Nothing reads that sentinel for CTE bodies. Only the main `ast_root.sentinel_agg_slot` is set (8695). The kernel creates the group before running the program (`JoinAggInterpreter.cpp:1551-1587`), so groups in which no row passes survive with COUNT 0 / SUM NULL.
  - The sentinel loop ignores `Avg`:
    ```cpp
    if (t == Sum || t == Count || t == Min || t == Max || t == AggRepeat)
    ```
    When the body's highest slot is an `Avg` (CTE scopes use `Avg`), `sentinel_slot` equals that slot. `Count()` then fails `CheckAggAndReg` (`kErrAggNoUsed`), and the query fails with "Failed writing aggregation program. Please report a bug."
  - The same unused sentinel arises in the main query when `m_has_select_subqueries` is set with a single leaf, because 8681 skips setting the slot (speculative).
- **Trigger:** `WITH c AS (SELECT a.g AS g, COUNT(*) AS n FROM a JOIN b ON b.k = a.k WHERE a.x + 0 < b.y GROUP BY a.g) SELECT COUNT(*) FROM c;` counts groups whose rows all fail the filter. Replacing `COUNT(*) AS n` with `AVG(b.v) AS av` should raise the "report a bug" error.
- **Suggested fix:** compute the sentinel slot with one helper that includes `Avg` (see F-17). For CTE bodies, either reject cross-table filters cleanly (matching the intent of `check_cte_body_col_vs_col`) or teach the CTE materialisation to drop groups whose sentinel is 0.
- **Suggested test:** `ronsql_cte_colvscol`: compare MySQL and RonSQL for arithmetic cross-table predicates in a CTE body with GROUP BY (COUNT and AVG), including a group with no passing row.

## F-8 Cross-table filter loads leaf columns of any type into integer registers

- **Category / severity / confidence:** bug, wrong-result, likely.
- **Location:** `emit_embedded_filter_expr`, T_IDENTIFIER leaf branch (RonSQLPreparer.cpp:17391-17396); kernel `handleReadAttrIntoReg` default branch (`src/kernel/blocks/dbtup/DbtupExecQuery.cpp:9007-9015, 9033-9044`).
- **Evidence:** linked columns are restricted to integer types (17407-17423), but leaf columns are not checked:
  ```cpp
  if (ref.join_op_idx == leaf_idx) {
    programAggregator_do_or_fail(agg->EmitEmbeddedWord(
        Interpreter::Read((Uint32)ref.attr_id, reg)));
  ```
  The kernel marks any non-integer or non-float column of 1-2 data words as `REG_TYPE_INT` holding the raw bytes. Examples are DECIMAL(10,2) (5 bytes), DATE, DATETIME2(0) and short VARCHAR. The CASE emitter does validate both sides (`can_load_typed_reg`, 18809-18838).
- **Trigger:** `SELECT o.c, COUNT(*) FROM o JOIN l ON l.ok = o.id WHERE l.price > o.budget OR o.vip = 1 GROUP BY o.c;` with `l.price DECIMAL(10,2)` (leaf) compares the binary DECIMAL bytes as an Int64. `WHERE l.d > o.n` with a DATE leaf compares the packed date with an integer.
- **Suggested fix:** apply the same type whitelist to leaf operands (integers plus FLOAT/DOUBLE, which the kernel types correctly), at `classify_where_by_table` time so the error appears at prepare time.
- **Suggested test:** `ronsql_join_where_classify`: expect a CLI rejection for DECIMAL/DATE/VARCHAR leaf operands, and compare MySQL and RonSQL for a DOUBLE leaf.

## F-9 FLOAT column vs literal: the WHERE and CASE paths disagree

- **Category / severity / confidence:** bug, wrong-result. Verified-by-reading for the divergence; likely for the MySQL side (MySQL compares FLOAT columns with numeric literals as DOUBLE).
- **Location:** `encode_constant` (RonSQLPreparer.cpp:16465-16487) compared with `generate_embedded_condition` `is_float_const` (18853-18876, 19053-19064).
- **Evidence:**
  ```cpp
  } else if (op == T_FLOAT && bytes == 4) {
    float* val = ...; *val = float(ce->constant_float.dbl);     // WHERE: compare as float
  ...
  info.float_const_val = rhs_simplified->constant_float.dbl;     // CASE: compare as double
  ```
  Through `READ_ATTR_INTO_REG`, the kernel promotes a FLOAT column to double (DbtupExecQuery.cpp `NDB_TYPE_FLOAT` case) for the CASE compare. NdbScanFilter compares two floats.
- **Trigger:** `f FLOAT` holding 0.1 and 16777216.
  - `SELECT COUNT(*) FROM t WHERE f = 0.1` returns 1 in RonSQL and 0 in MySQL.
  - `SELECT SUM(CASE WHEN f = 0.1 THEN 1 ELSE 0 END) FROM t` returns 0 in RonSQL.
  - `WHERE f >= 16777217`: `float(16777217) == 16777216.0f` matches in RonSQL but not in MySQL.
- **Suggested fix:** for a FLOAT column in WHERE, either (a) use the register path (READ_ATTR_INTO_REG + LOAD_DOUBLE_CONST) as CASE does, or (b) turn the double constant into an exact float-domain range. For example `f = c` becomes always-false unless `(double)(float)c == c`, and `f >= c` becomes `f >= nextafterf` of the rounded value when rounding went down. Ordered-index bounds need the same adjustment.
- **Suggested test:** `ronsql_filtering`: a FLOAT/DOUBLE table, comparing MySQL and RonSQL for `=, <, <=, >, >=` against 0.1, 16777217 and -0.0. Also a parity check `COUNT(*) WHERE p` = `SUM(CASE WHEN p THEN 1 ELSE 0 END)`.

## F-10 Substituted float scalar subquery is rounded to the DECIMAL column's scale

- **Category / severity / confidence:** bug, wrong-result, verified-by-reading. The arithmetic was checked by hand, not run.
- **Location:** `encode_constant` DECIMAL branch (RonSQLPreparer.cpp:16519-16539).
- **Evidence:**
  ```cpp
  int n = snprintf(dbl_buf, sizeof(dbl_buf), "%.*f", scale, ce->constant_float.dbl);
  ```
  The comment assumes the inner query was "formatted at that scale". That does not hold for AVG, division, or a subquery over another column. A source literal with too many digits fails with E_DEC_TRUNCATED, but a substituted value is rounded silently.
- **Trigger:** `price DECIMAL(10,2)` with rows 1.00 and 1.01; `SELECT COUNT(*) FROM t WHERE price >= (SELECT AVG(price) FROM t);`. The AVG of 1.005 becomes the double 1.00499..., is rendered as "1.00", and both rows match. MySQL returns 1. `=` behaves the same way.
- **Suggested fix:** keep the inner query's decimal text (`result_str`) as `ls` for substituted values. When the constant has more fractional digits than the column scale, fold exactly: `=` becomes FALSE, `!=` becomes `IS NOT NULL`, `>=`/`>` round up, `<=`/`<` round down. Use the same fold for source literals instead of the E_DEC_TRUNCATED error (see F-12).
- **Suggested test:** `ronsql_cte_subquery`: compare MySQL and RonSQL for `dec_col >= (SELECT AVG(dec_col) ...)`, `=` and `<`, with values that bracket the average.

## F-11 Temporal constants are truncated to the column's fractional precision

- **Category / severity / confidence:** bug, wrong-result, likely. This relies on MySQL comparing a temporal column with a string at full precision: `Item_func::convert_constant_arg` skips the conversion for a temporal field compared with a string.
- **Location:** `encode_constant` TIME branch (RonSQLPreparer.cpp:16651 and 16682: `my_datetime_adjust_frac(&mt, precision, &warnings, true)`).
- **Evidence:** the constant is truncated to the column's precision before encoding. For `DATETIME(0)`, `'2025-01-01 10:00:00.5'` becomes `10:00:00`.
- **Trigger:** `dt DATETIME` (fsp 0) holding `10:00:00`. `WHERE dt = '2025-01-01 10:00:00.5'` matches in RonSQL; MySQL returns no row. `WHERE dt >= '... 10:00:00.5'` gives the same discrepancy. `ronsql_timestamp` only uses literals whose precision is at most the column's.
- **Related design notes:** TIMESTAMP literals are interpreted as UTC (comment at 16662-16664) and not in the session time zone. A DATETIME column against a DATE-only literal (`dt >= '2025-01-01'`) is rejected (16639-16644), although this is a very common query shape.
- **Suggested fix:** when the literal has more fractional digits than the column, adjust the operator as in F-10 (`=` becomes FALSE, `>=` becomes `>`, `<` becomes `<=` on the truncated value). Promote DATE literals to DATETIME 00:00:00.
- **Suggested test:** `ronsql_timestamp`: compare MySQL and RonSQL for `DATETIME(0)` and `TIMESTAMP(3)` against literals with 1-6 fractional digits, all six operators, and `dt >= 'YYYY-MM-DD'`.

## F-12 Out-of-range or non-representable literals raise errors instead of folding

- **Category / severity / confidence:** design, error-handling, verified-by-reading.
- **Location:** `encode_constant` and `apply_filter_cmp`.
- **Evidence and triggers.** In each case below MySQL returns a well-defined result:
  - an integer outside the column domain: `tinyint_col < 1000`, or `unsigned_col > -1` (16444-16455, permanent error);
  - `DECIMAL(5,2) < 100000` or `dec2 = 1.234`: E_DEC_OVERFLOW or E_DEC_TRUNCATED, thrown as `RonSQLMaybeStaleSchema` (16552-16554), which costs a dictionary reload and then gives a permanent error with the message "Failed converting float literal" even for integer literals;
  - `udec_col > -1` (16502-16516);
  - CHAR literal longer than the column (16572-16575);
  - VARCHAR literal longer than the column but at most 255 bytes: `encode_constant` accepts it (it checks the 255-byte type limit, not the column length), but `NdbInterpretedCode::branch_col_val` → `get_var_length` (`NdbDictionaryImpl.hpp:1203-1222`) rejects it with BadLength, which surfaces as "Failed to apply filter." (cryptic);
  - TIMESTAMP literal outside 1970-2038 (16676-16681);
  - INT column vs float literal or float subquery result: `qty > 2.5`, `qty > (SELECT AVG(qty) ...)` (16460-16463);
  - a scalar subquery that returns NULL becomes `T_NULL`, which `encode_constant` rejects with "Integer type column compared to an incompatible value". MySQL returns an empty result. The HAVING side was fixed in the parser review, the WHERE side was not;
  - YEAR, TIME, BIT, BINARY and VARBINARY columns are not supported at all (16438-16442).
- **Suggested fix:** add a tri-state "constant comparison fold" step before `encode_constant`, with results ALWAYS_TRUE, ALWAYS_FALSE, ALWAYS_UNKNOWN, or ENCODE. `apply_filter` would emit an empty AND/OR group (or `isnotnull`) for the fold results. This removes most of these errors and the MaybeStaleSchema misclassification.
- **Suggested test:** `ronsql_constants`: compare MySQL and RonSQL for every bullet, and for each operator under NOT.

## F-13 WHERE and CASE accept different predicate sets for the same text

- **Category / severity / confidence:** design, error-handling, verified-by-reading.
- **Evidence:**
  - Column vs column of different types (`int_col < bigint_col`) works in CASE (typed register compare, `compareTypedRegs`). In WHERE it fails: `NdbInterpretedCode::branch_col_col` requires `isBindable` (`NdbInterpretedCode.cpp:1648-1653`, error 4557), surfaced as "Failed to apply filter."
  - A long VARCHAR literal fails in WHERE (F-12) but works in CASE (BRANCH_ATTR_OP_ARG is given `byte_len` without the `get_var_length` check).
  - FLOAT semantics differ (F-9).
  - NOT, NOT IN, LIKE and IS NULL work in WHERE but are rejected in CASE (18788-18792). `NOT IN` is collected as a single T_NOT atom before `simplify_ce` runs.
- **Suggested fix:** route both through one predicate IR with a capability check, or at least give the WHERE path the typed register compare for column-vs-column pairs that are not bindable.
- **Suggested test:** a parity test: for each predicate P in a corpus, `SELECT COUNT(*) FROM t WHERE P` must equal `SELECT SUM(CASE WHEN P THEN 1 ELSE 0 END) FROM t`.

## F-14 `!` is not lowered in WHERE

- **Category / severity / confidence:** bug, error-handling, verified-by-reading.
- **Evidence:** the grammar builds T_EXCLAMATION (RonSQLParser.y:715). `simplify_ce` has no case for it (it falls to `default: return ce;` at 16963-16965), and `apply_filter`'s default throws "Non-boolean term in WHERE condition" (16184-16186). Subquery substitution and EXISTS handling do treat `!` as NOT (6290, 9550).
- **Trigger:** `SELECT COUNT(*) FROM t WHERE !(a = 1);` fails, while `NOT (a = 1)` works.
- **Suggested fix:** in `simplify_ce`, rewrite T_EXCLAMATION to T_NOT before the T_NOT case.
- **Suggested test:** `ronsql_emptytable_and_nulls`: copy the NOT matrix and use `!`.

## F-15 Aggregation program over 8192 words reports "Please report a bug"

- **Category / severity / confidence:** bug, error-handling, verified-by-reading.
- **Evidence:** `NdbAggregator::HasRoom` stops at the buffer size, `MAX_VEC_SEARCH_PROGRAM_WORD_SIZE` = 8192 (NdbAggregator.cpp:1299-1308). `programAggregator_do_or_fail` maps every failure to "Failed writing aggregation program. Please report a bug." (17112-17113). Programs between 1024 and 8191 words reach `finalize_aggregator`, which gives the proper LIMIT message (RonSQLPreparer.cpp:201-205).
- **Trigger:** `SUM(CASE WHEN x IN (<3000 integers>) THEN 1 ELSE 0 END)`, which needs about 9000 embedded words.
- **Suggested fix:** check `GetError().errno_ == kErrTooBigProgram` inside `programAggregator_do_or_fail`, or in a helper, and throw the LIMIT error.
- **Suggested test:** `ronsql_size_limits`: expect a CLI LIMIT error for a CASE IN-list sized just over the limit.

## F-16 Word counts are maintained by hand in parallel with the emitters

- **Category / severity / confidence:** design, maintainability, verified-by-reading. This is the root cause of F-3.
- **Evidence:** there are four independent size models.
  - `raw_word_size` (AggregationAPICompiler.cpp).
  - `embedded_filter_expr_word_count` mirrors `emit_embedded_filter_expr`, guarded by `ndbrequire(cur == branch_instr_pos)` at 17575. A mismatch therefore calls `std::abort()` in RDRS.
  - `generate_embedded_condition`'s `info.word_count` (18845, 18875, 18886-18899) has no post-emission check, so a mismatch silently misjumps.
  - `execute_join`'s literal 12 and 2 (11448-11450).
- **Suggested fix:** give the emitter a word sink: `struct WordSink { virtual bool word(Uint32) = 0; }`, with `CountingSink` and `AggregatorSink` implementations. Run the emitter once against the counter to get sizes and once to emit. For aggregation-level arms, record `curr_prog_pos_` before and after emitting the arm and back-patch the `EmbeddedInterp` output constant and the `Skip` count (both are single words). Replace `ndbrequire` with `require_run`.

## F-17 Duplicated instruction switch and sentinel computation

- **Category / severity / confidence:** duplication, maintainability, verified-by-reading.
- **Sites:** `programAggregator` 17173-17286 and `programAggregator_join` 18007-18174. Only the Load arm differs (stored-only, compared with linked/CTE/virtual-table loads); the Avg arm also differs (one rejects it, the other emits it). The Mov, arithmetic, Sum/Min/Max/Count, pair-op, AggRepeat, EmbeddedInterp and Skip arms are identical apart from `m_main_scope` vs `scope`. Sentinel slot: 8682-8695 and 18180-18191, both missing `Avg` (F-7).
- **Proposed helper:** `void emit_agg_program(NdbAggregator*, QueryScope&, const SelectStatement&, NdbDictionary::Table* const* cteVT, LoadEmitter load_fn, bool allow_avg)`, plus `static Uint32 compute_sentinel_slot(const AggregationAPICompiler&)`. Simpler still, implement `programAggregator` as `programAggregator_join` on a single-op plan.
- **Lines saved / risk:** about 100 lines saved; low risk (the arms are verbatim copies today).

## F-18 The integer-type predicate is copied about 15 times, and the copies differ

- **Category / severity / confidence:** duplication, maintainability, verified-by-reading.
- **Sites:** `minmax_index_source_type_supported` (~5789), `execute_join` count-column selection (~11397), `emit_root_op` (~12840, ~12920), `resolve_chained_column_type` (x3), `build_cte_virtual_tables` (x4), `emit_cte_lookup_filter` (x2), `encode_constant` (with ranges), `emit_embedded_filter_expr` (17407-17423), `validate_greatest_least_pair_loads` (18568-18577), `generate_embedded_condition::can_load_typed_reg` (18809-18829).
- **Divergence:** the linked side of a cross-table filter allows integers only; the CASE register path allows integers plus Float and Double; the leaf side of a cross-table filter allows anything (F-8); the CTE lookup filter adds Date.
- **Proposed helpers:** `static bool ndb_is_integer_type(Column::Type)`, `ndb_is_register_numeric_type(Column::Type)` (integers, Float, Double), and `ndb_integer_range(Column::Type, Int64* min, Int64* max, Uint32* bytes)` for `encode_constant`.
- **Lines saved / risk:** about 120 lines saved; low risk.

## F-19 The two embedded predicate emitters overlap and have diverged

- **Category / severity / confidence:** duplication, maintainability, verified-by-reading.
- **Sites:**
  - `reg_branch_opcode` lambda: 17533-17555 and 18985-19007 (identical).
  - Two-exit epilogue (`LoadConst16` / `WriteInterpreterOutput` / `ExitOK`): 17587-17599, 19146-19160, `emit_pair_op_embedded` (18324-18341, 18353-18364) and `execute_join` (11467-11477).
  - NULL-guard word `Branch(BRANCH_REG_EQ_NULL,0,reg)|(off<<16)`: 17436-17439, 19045-19050, 18316-18320 (twice) and 11473-11474.
- **Divergence between `generate_embedded_filter_condition` and `generate_embedded_condition`:**
  - fixed 32-atom array vs DynamicArray;
  - arithmetic only in the first, FLOAT only in the second;
  - leaf type validation only in the second;
  - `simplify_ce` applied only in the second;
  - integer constants loaded with LOAD_CONST64 into a register vs typed `encode_constant` and BRANCH_ATTR_OP_ARG (out-of-range literals: silently compared vs error);
  - NULL guards only for nullable columns vs always.
- **Proposed helpers:** `static Uint32 reg_branch_opcode(TokenKind, bool invert)`, `static Uint32 arg_branch_cond(TokenKind, bool invert)`, `static Uint32 null_guard_word(Uint32 reg, Uint32 off)`, `bool emit_exit_with_output(NdbAggregator*, Uint32 reg, Uint32 value)`. Longer term, one `EmbeddedPredicateEmitter` with `size(atom)` and `emit(atom)` (see F-16).
- **Lines saved / risk:** about 80 lines saved for the small helpers (low risk); about 200 for the merged emitter (medium risk).

## F-20 NdbScanFilter boilerplate with inconsistent return-code checks

- **Category / severity / confidence:** duplication, error-handling, verified-by-reading.
- **Sites:** RonSQLPreparer.cpp 5173, 9741, 9937, 10109, 10732, 11646, 12475, 12561, 13120, 13175 and 15157. Each runs `NdbScanFilter f(&code); setSqlCmpSemantics(); begin(AND); apply_filter(...); end(); finalise();`.
- **Divergence:** `apply_filter_top_level` checks `begin`/`end` (16111, 16120). 12475-12480 and 12561-12566 ignore the results of `begin()`, `end()` and `finalise()`.
- **Proposed helper:** `void build_scan_filter(NdbInterpretedCode&, QueryScope&, ConditionalExpression* root)` that checks every step with `require_sch`.
- **Lines saved / risk:** about 50 lines saved; low risk.

## F-21 Test gaps (MTR)

- `ronsql_filtering.test` covers only one `SMALLINT UNSIGNED NOT NULL` column with six operators. It has no FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR, temporal or nullable columns, no LIKE, IN, XOR or `!`, and no out-of-range literals. These cover F-9 and F-12.
- No CASE test uses a DECIMAL or FLOAT arm, and none uses `AVG(CASE ...)`. These cover F-1 and F-3. `ronsql_feature_view` R59 (CASE in a 4-way join) is still disabled.
- Cross-table filters: there is only one arithmetic test (`ronsql_join_agg` Test 66, left-nested). There is no right-nested arithmetic (F-4), no non-integer leaf operand (F-8), no scalar aggregate with every row rejected (F-6), and no CTE-body cross-table filter (F-7).
- WHERE GREATEST/LEAST is never tested under NOT or XOR with NULL arguments (F-5).
- Temporal: no literal has more fractional digits than its column, no DATETIME vs DATE literal, and no TIMESTAMP range boundary (F-11, F-12).
- Scalar-subquery WHERE: no NULL result, no AVG compared with a DECIMAL column, and no unary minus over a subquery (F-2, F-10, F-12).
- There is no WHERE/CASE parity test (F-13).

## F-22 No unit test for program sizes

- `OverflowUnitTest.cpp` tests only `int64_{add,sub,mul}_overflow`. It is wired as a CTest (`CMakeLists.txt:72`), although the file still carries a stale "todo: make sure this test gets run".
- Suggestion: add a cluster-free CTest that builds `AggregationAPICompiler` programs, including CASE with each column type, pair-ops and constants, against a synthetic `NdbDictionary::Table`. It should emit them through `NdbAggregator` and assert that `raw_word_size` and the embedded `info.word_count` equal the words actually emitted. This would have caught F-3.

---

## Checked and found consistent (no finding)

- NdbScanFilter is always built with `setSqlCmpSemantics()`. NOT/NAND, NOT IN (an OR of `=` under NOT), IS NULL inside NOT, and XOR expanded to CNF are all 3VL-correct, apart from the GREATEST/LEAST rewrite in F-5.
- Register branch encoding: `Interpreter::Branch(op, Reg1, Reg2)` puts Reg1 in bits 9-11 (`getReg2`) and Reg2 in bits 6-8 (`getReg1` is the left register). `Branch(op, R2, R1)` therefore compares R1 `op` R2, as the comments state. The inverted tables for the ARG family and the REG family are correct.
- Except for DECIMAL loads (F-3), `generate_embedded_condition`'s per-atom word counts match emission: col-vs-col 5, float-const 6, leaf 2+data, linked 5+data, inline 4+data. Pair-op sizes 18 and 11 match `emit_pair_op_embedded`. Pair-op NULL propagation (outputs 0/1/2 → Mov / keep / SetRegNull) is correct.
- Constant folding in `new_expr` guards add/sub/mul overflow, `INT64_MIN DIV -1` and `% -1`, and does not fold `/`.
- CHAR padding uses the character set's `pad_char` through `cset->fill`. `literal_in_column_charset` sizes the buffer as `len * mbmaxlen`, which is enough. The lexer keeps `\%` and `\_` for LIKE, matching MySQL, and the kernel uses `\` as the escape character with the column collation.
- The `query_const_value` prefix stripping matches `encode_constant`'s VARCHAR layout.

## Note (not re-reported, E-4 family)

On a NULL-extended outer-join row, the kernel's `evalEmbeddedForNullRow` (`JoinAggInterpreter.cpp:994ff`) does not handle `READ_AGG_REG_TO_REG`. It skips the pair-op program with skip 0, so `Mov(dest, src)` runs, and `GREATEST(oj.col, 5)` yields 5 instead of NULL whether or not `needs_null_check` is set. This is the "programs that read registers keep the old behaviour" part of E-4 in the DBSPJ README. A RonSQL-side mitigation would need an aggregation-level null test.
