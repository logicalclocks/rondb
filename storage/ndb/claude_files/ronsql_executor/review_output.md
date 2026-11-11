# RonSQL executor review, slice E: result collection and output

Tree: /Users/mikael/mysql_trees/rondb_2610_main, branch 26.10-main, HEAD 50eb491eb3f.
Files read: storage/ndb/src/ronsql/ResultPrinter.cpp (3184 lines), ResultPrinter.hpp,
define_formatter.hpp, DynamicArray.hpp; in RonSQLPreparer.cpp: printer construction
(8700-8740), column_uses_float_display (8748), build_result_column_metadata (8772),
determine_explain, is_avro_output, prepare_avro_outputs (8940), the three pass-through
drains (10780-10895, 10990-11090, 12200-12320), the pass-through sort helpers
(10440-10590), resolve_cte_output_columns_for_scope (7499), resolve_chained_column_type
(13192), validate_having_* (7223-7358). MySQL reference behaviour checked in the same tree:
sql/protocol_classic.cc (floating_point_to_text), strings/dtoa.cc (my_fcvt, my_gcvt,
rounding), mysys/decimal.cc (decimal_round, decimal2string), sql/item_sum.cc
(Item_sum_avg), sql/item.cc (val_string_from_decimal).

Not re-reported: the parser/preparation review (ronsql_parser_review_plan.md) and the
DBSPJ review (E-1..E-30, Q-*, in particular E-5 collation group splitting and Q-6/E-16
exact DECIMAL/integer accumulation). Where a finding below is a printer-side consequence of
inexact DOUBLE accumulation, it says so; the printer-side fix is independent of Q-6.

Severity scale: crash, wrong-result, hang-or-leak, error-handling, performance,
maintainability. Confidence: verified-by-reading, likely, speculative.

---

## Part 1: bugs and design weaknesses

### O-1. Chained-CTE outputs lose their charset, temporal and DECIMAL metadata (crash with ORDER BY, garbage DECIMAL keys, packed temporal integers)

- Category: bug
- Severity: crash (ORDER BY variant), otherwise wrong-result / error-handling
- Confidence: likely (every step is verified by reading; the end-to-end shape was not run)
- Location: RonSQLPreparer::resolve_cte_output_columns_for_scope (RonSQLPreparer.cpp:7518,
  7535); RonSQLPreparer::build_result_column_metadata (8796-8826);
  ResultPrinter::compare_rows GROUPBY_COL arm (ResultPrinter.cpp:1090-1097)
- Evidence:
  - The dict column is copied from the CTE body only when the body column is a stored column:
    ```
    if (src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn)
      ref.dict_column = src_ref.dict_column;          // 7518 and 7535
    ```
    CTE scopes are resolved in declaration order, so a column of CTE `b` that refers to CTE `a` already
    has `dict_column` set. Its `kind` is still `CteResultColumn`, though, so the main scope never
    inherits the descriptor through two levels.
  - For such a column `build_result_column_metadata` takes the `col == NULL` branch. It calls
    `resolve_chained_column_type`, which does return the charset (`rcs`) and the precision, but the
    code copies only `precision`/`scale` and sets `has_metadata = true` only `if (rscale > 0)`
    (line 8819). `charset` stays NULL, `temporal` stays NONE, and a DECIMAL(p,0) key gets
    precision 0. The comment at 13404 already says "Temporal display is only recovered at the top
    level for a single-CTE MIN/MAX today."
  - `validate_orderby_columns` then gives a string GROUP BY key `spec.charset = NULL`. The
    GROUPBY_COL arm of `compare_rows` passes it straight to `NdbSqlUtil::cmpVarchar`/`cmpChar`, which
    dereference `cs->coll` with only an `assert(info != nullptr)` (NdbSqlUtil.cpp:327, 342). The sort
    runs before any print, so the `require_sch(charset != nullptr)` in the print arm never gets a
    chance to throw. The AGGREGATE arm (line 1014) has that guard; the GROUPBY_COL arm does not.
- Triggers (t: `pk INT PK, s VARCHAR(20), k DECIMAL(10,0), d DATE, v INT`):
  - Crash (2 or more groups):
    `WITH a AS (SELECT s, COUNT(*) AS n FROM t GROUP BY s), b AS (SELECT s, SUM(n) AS n2 FROM a GROUP BY s) SELECT s, SUM(n2) FROM b GROUP BY s ORDER BY s`
    gives a NULL CHARSET_INFO dereference in RDRS. Without ORDER BY it fails with
    RonSQLMaybeStaleSchema "Could not find charset for VARCHAR column", which is the wrong error class
    and triggers a schema reload and retry.
  - Garbage: the same chain on `k DECIMAL(10,0)`. Precision 0 makes `decimal_bin2str` return
    E_DEC_BAD_PREC; the return value is ignored (see O-14), so the uninitialized `char decStr[68]` is
    printed.
  - Temporal: `WITH a AS (SELECT v, MAX(d) AS m FROM t GROUP BY v), b AS (SELECT v, MAX(m) AS mm FROM a GROUP BY v) SELECT v, mm FROM b`
    prints `1036389` where mysqld prints `2024-03-05`.
- Suggested fix: in resolve_cte_output_columns_for_scope, plumb whenever
  `src_ref.dict_column != NULL`, not only for StoredColumn. In the `col == NULL` branch of
  build_result_column_metadata, always copy `rcs` into `charset` and set precision from
  `rprecision`. Carry the temporal kind through `resolve_chained_column_type` (add an out
  parameter) instead of collapsing it to Bigunsigned. In compare_rows, add
  `require_sch(spec.charset != nullptr)` for string types to the GROUPBY_COL arm, and add the same
  check in validate_orderby_columns so the query fails before the scan.
- Suggested test: ronsql_cte_chained (strict_diff): two- and three-level chains with VARCHAR,
  DECIMAL(10,0), DECIMAL(10,2), DATE and DATETIME(3) GROUP BY keys and MIN/MAX outputs, each with and
  without `ORDER BY key`, compared against mysqld.

### O-2. Pass-through JSON rows do not escape column names (invalid JSON; JSON_ASCII not ASCII)

- Category: bug (diverged copy)
- Severity: wrong-result (unparseable response body)
- Confidence: verified-by-reading
- Location: ResultPrinter::print_passthrough_row, ResultPrinter.cpp:1698
- Evidence:
  ```
  out << '"' << o->output_name << "\":";
  ```
  `LexString::operator<<` writes raw bytes (LexString.cpp:48). The aggregate path escapes keys
  (compile() emits PRINT_STR_JSON, which calls `print_string(..., json_escape=true, m_utf8_output)`),
  and so does the COUNT(*) shortcut (1793). cfd835ae18d fixed the same defect for operationId "like
  the column names", but this sibling was missed. A backtick-quoted alias may contain `"`, `\` and
  control characters (`QUOTED_IDENTIFIER_CHARACTER_EXCEPT_BACKTICK [^\0`\200-\377]|{U_EXTBMP}`,
  RonSQLLexer.l:231). An unaliased AVRO(...) output takes its name from the raw SQL text, newlines
  included.
- Triggers:
  - `SELECT a AS `x"y` FROM t WHERE pk = 1` gives `[{"x"y":1}]`.
  - JSON_ASCII with `SELECT a AS `é` FROM t` puts raw UTF-8 into a body served as
    `charset=US-ASCII`.
  - `SELECT AVRO(\n c) FROM ...` puts a raw newline inside a key.
- Suggested fix: add one helper, `print_json_key(std::ostream&, LexString)`, that wraps
  `print_string(..., true, m_utf8_output, false)`, and use it at all three key sites (see D-5).
- Suggested test: ronsql_operation_id or ronsql_rdrs_basic with JSON and JSON_ASCII pass-through
  requests that use aliases containing `"`, `\`, TAB and `é`, piped through a JSON validator
  (`python3 -c 'import json,sys; json.load(sys.stdin)'`). Also a Go wrapper_test case.

### O-3. A GROUP BY over a CTE temporal MIN/MAX output prints the packed integer

- Category: bug (diverged copy)
- Severity: wrong-result
- Confidence: verified-by-reading
- Location: print_stored_record and print_record, Bigunsigned arm, ResultPrinter.cpp:785 and
  2006; Cmd::print_group_by_column (ResultPrinter.hpp)
- Evidence: a CTE MIN/MAX over DATE/YEAR/DATETIME2/TIME2/TIMESTAMP2 comes back as a Bigunsigned
  virtual column (D17). build_result_column_metadata tags the column's metadata `temporal`, and two
  printers use that tag: print_passthrough_value (1404-1411) and print_aggregate_result (2839-2844).
  `Cmd::print_group_by_column` has no temporal field, and the GROUP BY arm prints
  `out << column.data_uint64();`. The FLOAT version of this exact shape was fixed (b170940bbd2,
  `float_display` in print_group_by_column, test "Widened FLOAT CTE group keys" in
  ronsql_float_formatting); the temporal version was not.
- Trigger: `WITH a AS (SELECT g, MIN(d) AS lo FROM t GROUP BY g) SELECT lo, COUNT(*) AS n FROM a GROUP BY lo`
  with d = '2024-03-05' prints `1036389` (2024*512+3*32+5), where mysqld prints `2024-03-05`. A YEAR
  column prints `124`.
- Suggested fix: add `temporal` and `temporal_fsp` to `print_group_by_column`, fill them from
  ColumnMetadata in compile(), and in the Bigunsigned arm call `print_temporal_packed` when they are
  set. D-2 removes this whole class of divergence.
- Suggested test: ronsql_temporal_json / ronsql_groupby_date: the shape above for DATE, YEAR,
  DATETIME(6), TIME(3) and TIMESTAMP(3), in TEXT and JSON, with and without ORDER BY.

### O-4. DOUBLE values of magnitude 1e15 and above print in fixed point; mysqld uses an exponent

- Category: bug / design
- Severity: wrong-result
- Confidence: verified-by-reading
- Location: double_to_fixed (ResultPrinter.cpp:2567) through print_float_or_double (2602)
- Evidence: `if (!(std::fabs(value) < 0.1) || value == 0.0) return my_fcvt_compact(value, to, error);`
  prints 1e20 as `100000000000000000000`. mysqld sends a DOUBLE with `decimals == NOT_SPECIFIED`
  through `my_gcvt(value, MY_GCVT_ARG_DOUBLE, FLOATING_POINT_BUFFER, ...)`
  (protocol_classic.cc:3456). With `MAX_DECPT_FOR_F_FORMAT = DBL_DIG = 15` (dtoa.h:41, dtoa.cc:392)
  that switches to `e` format once decpt exceeds 15 and the digits do not cover the integer part:
  `1e20`, `1.5e15`, `1.2345678901234568e17`. RONSQL-44 (1394d10e5ca) deliberately chose fixed point
  for |x| < 1e-15 and documented it in the test. The large-magnitude case is undocumented, and no
  test compares it with mysqld: only the FLOAT column holds 1e20 in ronsql_float_formatting, and
  ronsql_hopsworks.pl's DOUBLE tops out at 1e9.
- Trigger: a DOUBLE column holding 1e20, then `SELECT MAX(d) FROM t`, `SELECT d FROM t`,
  `SELECT d, COUNT(*) FROM t GROUP BY d`, or `SUM`/`AVG` over a DOUBLE or FLOAT column whose result
  reaches 1e15 or more.
- Suggested fix: for |x| ≥ 1e15, use `my_gcvt(MY_GCVT_ARG_DOUBLE, FLOATING_POINT_BUFFER)` like
  mysqld. Or, if fixed point is wanted for every RDRS output, document that and test the TEXT format
  against a canonicalized mysqld result.
- Suggested test: ronsql_float_formatting (strict, compared with mysqld): DOUBLE values 1e15, 1.5e15,
  1e20, -1.2345678901234567e17 in MIN/MAX, GROUP BY, pass-through and SUM.

### O-5. DECIMAL-derived values print as negative zero ("-0.00", "-0.000000")

- Category: bug
- Severity: wrong-result
- Confidence: verified-by-reading (it depends on SUM over DECIMAL being accumulated as DOUBLE in the
  kernel, which the D1 comments state)
- Location: print_aggregate_result `snprintf("%.*f")` at 2863; the GROUP BY Double arms at 800 and
  2018; print_passthrough_value at 1447; print_avg_result `my_fcvt` at 2647
- Evidence: snprintf and my_fcvt both keep the sign of a value that rounds to zero
  (`if (sign) *dst++ = '-';`, dtoa.cc my_fcvt_internal). MySQL's decimal_round clears the sign when
  the rounded value is zero (`to->sign = false;` at mysys/decimal.cc:1894; `decimal_make_zero` at
  1771 and 1830), so mysqld never prints a negative DECIMAL zero.
- Trigger: `d DECIMAL(10,2)`, one group holding 0.30, -0.10 and -0.20. In every accumulation order
  the double sum is -2.8e-17 or -5.6e-17 (checked: 0.3-0.1-0.2, -0.1-0.2+0.3, -0.2+0.3-0.1, and so
  on). `SUM(d)` prints `-0.00` and `AVG(d)` prints `-0.000000`; mysqld prints `0.00` and
  `0.000000`. The same happens through CTE SUM/AVG outputs (GROUP BY and pass-through).
- Suggested fix: one shared `print_fixed_scale(out, v, scale)` (D-3) that formats with my_fcvt and
  drops a leading '-' when every digit is zero. Better still, round through
  `double2my_decimal` + `decimal_round(HALF_UP)` + `decimal2string`, which also fixes O-6. This is
  independent of Q-6.
- Suggested test: ronsql_avg_formatting / ronsql_cte_decimal: the group above, plus a CTE
  `SUM(d)` projected and grouped.

### O-6. AVG rounds exact ties half-to-even; MySQL rounds DECIMAL half-up

- Category: bug
- Severity: wrong-result (rare)
- Confidence: verified-by-reading
- Location: print_avg_result `my_fcvt(result, scale, ...)` (2647); avg_sort_key (1140) is consistent
  with it. The CTE AVG paths format with `snprintf("%.*f")` (800, 2018, 1447).
- Evidence: dtoa breaks exact ties toward the even digit (`if (j > 0 || (j == 0 && dig & 1))` in
  dtoa.cc "Round off last digit"; fast path `(dval(&u) == ds && L & 1)`). mysqld's AVG over an exact
  type is a DECIMAL computed with 9 fraction digits and then rounded HALF_UP
  (`Item::val_string_from_decimal` → `my_decimal_round(..., decimals, false, ...)`, item.cc:316). Its
  pushed-AVG branch does `double2my_decimal(avg)` followed by the same HALF_UP rounding
  (item_sum.cc:2401-2410), so both mysqld paths agree with each other and differ from RonSQL.
- Trigger: one group of 32 rows of an INT column, 31 zeros and one 1, then `AVG(i)`. RonSQL prints
  `0.0312`, mysqld `0.0313`; with sum 5, `0.1562` against `0.1563`. Ties that are not exact in binary
  (for example count 20000) go either way depending on the binary error.
- Suggested fix: format the AVG through `double2my_decimal` + `decimal_round(HALF_UP, scale)` +
  `decimal2string`, which is what mysqld's pushed-AVG path does, and use the same function for
  avg_sort_key.
- Suggested test: ronsql_avg_formatting with 32-row groups whose sums are 1, 3, 5 and -1, compared
  with mysqld under strict_diff.

### O-7. SUM/MIN/MAX over DECIMAL with precision above 15 drop their scale ("10.5", "0")

- Category: design (an accepted mismatch already recorded in result files)
- Severity: wrong-result
- Confidence: verified-by-reading (the expected-result files record the mysqld/RonSQL diff)
- Location: compile() `cmd.print_aggregate.scale = (pr > 0 && pr <= 15) ? sc : 0;` (574); the
  inline gates `sc > 0 && pr > 0 && pr <= 15` at 798, 2016 and 1446
- Evidence: the gate uses the declared precision, not the value. DECIMAL(18,2), the usual money
  type and the one the fs probes use, therefore prints compact. ronsql_fs/r/ronsql_fs_smoke.result
  records `-3.25 10.50 7.25` from mysqld against `-3.25 10.5 7.25` from RonSQL (390-395, 460-465),
  and `0.00` against `0` (578-582). ronsql_hopsworks.result records `0.0000000000` against `0` for a
  DECIMAL(30,10) MIN. AVG is not gated at all (avg_scale = scale+4), so in the same row an AVG
  prints its scale while SUM/MIN/MAX do not: the gating has diverged between the copies.
- Suggested fix: decide by magnitude, not declared precision. Print fixed-scale whenever
  `scale > 0` and `|v| < 10^(15-scale)`, where the double is exact to the scale. Above that, a
  fixed-scale print would show mantissa noise, so keep the current output there and leave the fix to
  Q-6.
- Suggested test: turn the ronsql_fs_smoke EDGE-NULL-A2 / EDGE-BIG-B probes into strict comparisons
  once fixed. Add DECIMAL(18,2) MIN/MAX/SUM rows to ronsql_avg_formatting.

### O-8. HAVING is evaluated entirely in DOUBLE

- Category: bug / design
- Severity: wrong-result
- Confidence: verified-by-reading
- Location: evaluate_having_value / evaluate_having (2968-3107); convert_result_to_double (2896)
- Evidence: every operand goes through `convert_result_to_double`, and `T_INT` becomes
  `static_cast<double>(expr->constant_integer)` (2987). Consequences:
  - (a) BIGINT/BIGINT UNSIGNED values above 2^53 compare wrongly. With MAX(id) =
    1234567890123456789, `HAVING MAX(id) > 1234567890123456788` is FALSE (both sides are
    1.2345678901234568e18); mysqld returns TRUE. `HAVING SUM(b) = 9007199254740992` over rows
    2^53 and 1 is TRUE in RonSQL and FALSE in mysqld.
  - (b) DECIMAL SUM against a constant: rows 0.10 and 0.20 in one group make
    `HAVING SUM(d) = 0.3` FALSE (0.30000000000000004), while the row itself prints `0.30` and
    mysqld keeps the group. This is a consequence of Q-6, but it can be fixed in the printer by
    comparing at the result scale, as avg_sort_key already does for ORDER BY AVG.
  - (c) Temporal MIN/MAX are compared as packed integers. `HAVING MAX(d) > 20240101` with DATE d is
    always FALSE (the packed value is about 1.04e6); mysqld compares as a date.
    validate_having_numeric_expression accepts this because the aggregate argument is not
    type-checked.
  - (d) String MIN/MAX throw "Unexpected data type in results underlying AVG. Please report a bug."
    (2910): the wrong message and the wrong class (bug instead of unsupported).
- Suggested fix: typed evaluation. Compare Int64/Uint64 exactly when both sides are integer
  (aggregate results Bigint/Bigunsigned and T_INT). Round DECIMAL-derived results to the source scale
  before comparing. Reject temporal and string aggregate operands in validate_having_support with
  UNSUPPORTED, or compare them with decoded values.
- Suggested test: ronsql_having_refs, compared with mysqld: the four shapes above.

### O-9. ORDER BY a SUM over DECIMAL (and CTE DECIMAL-derived keys) compares raw doubles

- Category: bug (sibling of 7fdf63b51b3, which fixed only AVG)
- Severity: wrong-result
- Confidence: likely (depends on DOUBLE accumulation of SUM(DECIMAL), as in O-5)
- Location: compare_rows AGGREGATE arm, `case NdbDictionary::Column::Double` (1005-1008); the
  GROUPBY_COL arm for Double keys with display scale (1093); compare_passthrough_rows
  (RonSQLPreparer.cpp:10479) for CTE outputs
- Evidence: 7fdf63b51b3 made ORDER BY on an AVG sort by "the average as PRINT_AVG prints it ... so
  averages that print alike tie and the next ORDER BY key decides". SUM(DECIMAL), and every Double
  carrying a D15 display scale, still compares the raw double.
- Trigger: g=1 with rows 0.10 and 0.20 (sum 0.30000000000000004), g=2 with row 0.30 (sum
  0.29999999999999999), then `SELECT g, SUM(d) AS s FROM t GROUP BY g ORDER BY s, g`. RonSQL returns
  g=2 before g=1. mysqld sees two equal DECIMALs and returns g=1, g=2. With LIMIT 1 the result row
  itself differs.
- Suggested fix: give OrderbySpec a `display_scale`. For an AGGREGATE with scale > 0 (and for
  GROUPBY_COL and pass-through Double keys with a carried scale), compute the key the way
  avg_sort_key does: round at the scale (shared helper from D-3) and cache it per row.
- Suggested test: ronsql_orderby section D3, the shape above with and without LIMIT 1, plus a
  CTE-projected `ORDER BY s, g`.

### O-10. GREATEST/LEAST watermarks over temporal or FLOAT scalar-CTE outputs lose their display type

- Category: bug (diverged helpers)
- Severity: wrong-result
- Confidence: likely
- Location: aggregate_arg_temporal (2794), aggregate_uses_float_display (2670),
  aggregate_arg_charset (2685); compare aggregate_arg_scale and aggregate_arg_precision (2732, 2764)
- Evidence: D8 added `ronsql_first_load_in_expr` so that `aggregate_arg_scale`/`precision` look
  through `Greatest2`/`Least2` to their operand columns. The temporal, float and charset lookups
  still begin with `if (arg == NULL || !arg->isLoad()) return NONE/false/NULL;`.
  validate_greatest_least_pair_loads skips the integer-only type check for CTE operands
  (RonSQLPreparer.cpp:18541-18550, `continue` for non-StoredColumn). The pair-op is a plain
  register compare, so it works on packed temporal values.
- Trigger: `WITH a AS (SELECT MAX(d) AS m FROM t1), b AS (SELECT MAX(d) AS m FROM t2) SELECT GREATEST(a.m, b.m) AS w FROM a, b`
  with DATE d prints a packed integer. With FLOAT columns it prints DOUBLE digits
  (`0.10000000149011612` against `0.1`).
- Suggested fix: one metadata walker (D-7) used by all six lookups, following Greatest2/Least2 for
  every attribute. Alternatively, reject non-integer CTE operands of GREATEST/LEAST until display is
  handled.
- Suggested test: ronsql_cte_greatest_least with DATE, DATETIME(3) and FLOAT scalar CTEs, compared
  with mysqld.

### O-11. Duplicate output names give JSON rows with duplicate keys

- Category: design
- Severity: wrong-result (from the client's point of view)
- Confidence: verified-by-reading
- Location: print_passthrough_row (1698), compile() PRINT_STR_JSON (515), print_count_star_result
  (1793)
- Evidence: each row is a JSON object keyed by `output_name`, and nothing checks the names for
  uniqueness. `SELECT a.id, b.id FROM a JOIN b ...` gives both outputs the name `id` (the unqualified
  column name, as in mysqld), producing `{"id":1,"id":7}`. Go's encoding/json, JavaScript and Python
  keep the last value, so the client silently loses a column. mysqld result sets allow duplicate
  column names.
- Suggested fix: reject duplicate output names under JSON/JSON_ASCII at prepare time with a clear
  "add an alias" error. The alternatives are deterministic suffixing or a positional array format.
- Suggested test: Go wrapper/integration test or ronsql_rdrs_basic with the join above under JSON,
  expecting the error.

### O-12. The pass-through ORDER BY memory cap undercounts real memory by up to about 8x

- Category: design
- Severity: performance (memory, RDRS OOM risk)
- Confidence: verified-by-reading
- Location: buffer_passthrough_row, RonSQLPreparer.cpp:10531;
  `PASSTHROUGH_SORT_MAX_BYTES = 256 MB` (10474); PassthroughValue::copy (ResultPrinter.cpp:1728)
- Evidence: `buf.bytes += row[c]->get_size_in_bytes();` counts only value bytes. Each value
  actually costs `sizeof(PassthroughValue)` (16) + `max(8, size)` rounded up to 8, plus an 8-byte
  pointer in the row array. A NULL counts 0 bytes but costs 8. A 4-byte INT is counted as 4 and
  costs 32.
- Trigger: 1,000,000 rows (within the row cap) × 30 INT columns with ORDER BY are counted as 120 MB
  (under the cap) but take about 960 MB of arena per request, and concurrent requests multiply that.
- Suggested fix: count `sizeof(PassthroughValue) + data_len + sizeof(void*)` per value, plus the row
  array, against a configurable cap.
- Suggested test: a unit test on buffer_passthrough_row accounting, or an MTR test in ronsql_large
  that expects the cap error at a computed row count.

### O-13. Unsupported output types are detected only at print time, after the scan and partial output

- Category: design
- Severity: error-handling
- Confidence: verified-by-reading (the behaviour is recorded in result files)
- Location: the GROUP BY arms `feature_not_implemented("Print GROUP BY column of type
  Year/Time2/Binary/Varbinary/Bit")` (873-902, 2092-2121); print_passthrough_value `default:` (1581)
- Evidence: ronsql_orderby.result:593-617 records the header and a `NULL 4` row being printed before
  "feature not implemented: Print GROUP BY column of type Year". Time2, Varbinary and Bit behave the
  same way. ronsql_cte_dd_passthrough_types.result:1280-1284 records the BIT pass-through being
  rejected after its header. The scan has already run by then. In RDRS the output is discarded, but
  ronsql_cli emits a partial result. The pass-through printer already decodes YEAR and TIME2
  (print_temporal_packed) and BINARY/VARBINARY (base64 / batch escaping), so the GROUP BY gap exists
  only because the code is duplicated (D-2).
- Suggested fix: validate every printed column type in prepare. Add YEAR/TIME2/BINARY/VARBINARY to
  the GROUP BY printer by routing it through the pass-through value formatter.
- Suggested test: update ronsql_orderby "unsupported" cases so they expect either correct output for
  YEAR/TIME/VARBINARY or an error with no header line.

### O-14. decimal_bin2str's return code is ignored, so an uninitialized buffer is printed on error

- Category: bug
- Severity: wrong-result (possible stack over-read)
- Confidence: verified-by-reading (code); reachable through O-1
- Location: ResultPrinter.cpp:821, 2039 (GROUP BY), 1543 (pass-through)
- Evidence: `char decStr[68]; decimal_bin2str(...); out << decStr;`. decimal_bin2str returns early
  without touching `str` on E_DEC_BAD_PREC, E_DEC_BAD_SCALE or E_DEC_BAD_NUM
  (decimal_utils.cpp:67-81). The buffer is never initialized, so it may contain no NUL, and
  `out << decStr` then reads past it.
- Suggested fix: check the return value and `require_sch`/`bug()` on failure; initialize
  `decStr[0] = 0`.
- Suggested test: covered by the O-1 DECIMAL(10,0) chained-key test.

### O-15. Display attributes that NDB does not carry: TIMESTAMP time zone, FLOAT(M,D)/DOUBLE(M,D), ZEROFILL

- Category: design
- Severity: wrong-result (low)
- Confidence: verified-by-reading
- Location: ronsql_timestamp_tv_to_TIME (68-110); print_float_or_double; build_result_column_metadata
- Evidence:
  - TIMESTAMP is always broken down in UTC. mysqld uses the session time_zone (default SYSTEM).
    ronsql_compare.inc forces `SET time_zone = "+00:00"` (line 41), so the difference can never show
    up in tests.
  - ha_ndbcluster stores DOUBLE as `setType(Double); setLength(1)` with no scale
    (ha_ndbcluster.cc:8868), so `DOUBLE(10,2)` prints `1.5` where mysqld prints `1.50`.
  - ZEROFILL padding (`INT(5) ZEROFILL` → `00042`) is unknown to NDB.
- Suggested fix: document these as RonSQL semantics (UTC-only TIMESTAMP), or reject such columns
  explicitly.
- Suggested test: a documentation test in ronsql_timestamp that runs mysqld at a non-UTC time_zone
  and records the expected divergence.

### O-16. Buffering and sorting costs

- Category: design
- Severity: performance (low)
- Confidence: verified-by-reading
- Location: print_result_ordered (1149-1246); RDRS CappedOStream (ronsql_ctrl.cpp:217, 507)
- Evidence:
  - The aggregate ORDER BY ... LIMIT n does a full `std::sort` of every group (1197), while the
    pass-through path uses `std::partial_sort` (RonSQLPreparer.cpp:10559).
  - Rows are copied out of DynamicArray into a second array purely to sort them.
  - RDRS builds the whole response in memory before sending it, so there is no streaming, and the
    arena's buffered rows and the response body coexist.
  - HAVING-only queries also buffer every group (`m_has_orderby` is set by HAVING), even though
    HAVING could be applied while streaming when there is no ORDER BY and no sentinel.
- Suggested fix: share a `sort_top_n` helper between the two paths. Evaluate HAVING on the streaming
  path when ORDER BY is absent.
- Suggested test: none needed (performance).

### O-17. Printer invariants abort the RDRS process

- Category: design
- Severity: maintainability (crash risk)
- Confidence: verified-by-reading
- Location: compile() `ndbrequire(precision <= 65); ndbrequire(scale <= 30); ndbrequire(scale <= precision);`
  (546-548); `abort()` in compile, print_result, print_result_ordered, setup_output_format and
  explain; double_to_fixed `ndbrequire`s
- Evidence: in API code `ndbrequire` aborts the process (storage/ndb/plugin/ndb_require.h:32-39).
  RONSQL-39 (the BLOB precision of 256) already aborted RDRS through exactly these checks
  (RonSQLPreparer.cpp:8830-8833 comment). Any future metadata path that sets odd precision or scale
  (chained CTE, new types) has the same exposure.
- Suggested fix: replace them with `require_bug(...)` / `bug(...)`, which throw RonSQLPermanentError.
- Suggested test: none needed (defensive).

### Checked and dropped (no concrete trigger)

- double_to_fixed buffer check (`3 + zeros + (e - src) <= FLOATING_POINT_BUFFER + 1`, 343 bytes):
  denormals have short shortest representations (`5e-324`), so the worst case stays around 330. No
  abort.
- JSON_ASCII surrogate-pair encoding (2536-2550): correct for U+10000..U+10FFFF. The illegal
  surrogate and out-of-range cases map to U+FFFD.
- TSV escaping (\0 \t \n \\) matches the mysql client's batch mode for strings and binary.
- LIMIT order: HAVING, then the sentinel filter, then sort, then LIMIT. Correct. NULLs sort first
  for ASC and last for DESC, matching MySQL.
- String MIN/MAX buffers stay valid across FetchResultRecord in the ordered path: they are per-slot
  heap allocations owned by the aggregator (NdbAggregator.cpp:266).
- GROUP BY DATE decoding with `data_uint32()` (4 bytes for a 3-byte column, 785/2101) relies on zero
  padding in the word-aligned group record. It is speculative only; the pass-through path uses
  `uint3korr`. Unifying them (D-10) removes the question.

---

## Part 2: duplicated code

| ID | Sites | How they differ (divergence = potential bug) | Proposed helper | Lines saved | Risk |
|---|---|---|---|---|---|
| D-1 | print_stored_record 731-978 and print_record 1917-2198 (GROUP BY arm, PRINT_AGGREGATE/AVG/STR/STR_JSON); store phase run_program 644-711 and print_record's STORE arms | Currently identical (diffed); any future fix must be made twice | `print_record(r, out) { run_program(0, m_print_start_idx, &r, NULL); run_print_phase(out); }`, where run_print_phase is print_stored_record without the memcpy | ~260 | Low |
| D-2 | Per-type value formatting in three printers: GROUP BY (785-905), print_passthrough_value (1392-1588), print_aggregate_result (2817-2894) | Pass-through supports YEAR, TIME2, BINARY and VARBINARY; GROUP BY does not (O-13). Temporal decode of Bigunsigned exists in pass-through and aggregate but not GROUP BY (O-3). DATE is read as 4 bytes vs 3. Double scale comes from Cmd metadata vs `NdbDictionary::Column::getScale()`. DATETIME2/TIMESTAMP2 decode is inline in GROUP BY but goes through print_temporal_packed elsewhere | `struct ValueView { Type t; const char* p; Uint32 len; }` and `void print_value(std::ostream&, const ValueView&, const ValueFormat& {charset, precision, scale, temporal, fsp, float_display})`, with adapters from NdbAggregator::Column, NdbRecAttr and PassthroughValue | ~150 | Medium |
| D-3 | Fixed-scale double: snprintf at 800, 2018, 1447, 2863; my_fcvt at 1140, 2647. Precision gate inline at 798, 2016, 1446, at compile time at 574, and absent for AVG | Gates differ (AVG ungated, O-7). snprintf vs my_fcvt (locale and rounding). Negative zero in all of them (O-5) | `bool fixed_scale_ok(double v, int scale)` and `void print_fixed_scale(std::ostream&, double v, int scale)` (HALF_UP, zero sign cleared); avg_sort_key and the O-9 sort keys reuse the rounding | ~30 | Low |
| D-4 | CHAR/VARCHAR printing: GROUP BY ×2 (834-866, 2052-2088), pass-through (1452-1498), aggregate (2869-2885). VARCHAR length-prefix decode ×5 (the above plus binary_value_bytes 155 and compare_rows re-encoding 1020-1052) | CHAR uses explicit if/else on json/tsv, VARCHAR uses m_quote and `m_utf8_output || m_tsv_output`. Equivalent today | `LexString var_payload(Type, const char*)`, `void print_text(std::ostream&, LexString, CHARSET_INFO*, bool trim)` | ~60 | Low |
| D-5 | Header/key emission: TSV header ×4 (1225-1234, 1804-1812, 1891-1900, 1601-1611); JSON key ×3 (compile 508-521, count-star 1793, pass-through 1698) | Pass-through key not escaped (O-2) | `print_tsv_header(out)`, `print_json_key(out, name)` | ~30 | Low |
| D-6 | Pass-through framing (header_emitted, is_first_row, JSON '[' and ']') maintained by three drains in RonSQLPreparer.cpp (10851-10891, 11014-11086, 12240-12317) plus sort_and_print_passthrough_rows (10576) | Same rules repeated, each with its own LIMIT 0 and empty-result handling. `print_passthrough_header` ignores `attrs` (`/*attrs*/`, 1593), yet the comment at RonSQLPreparer.cpp:11036-11043 orders closeNoWait after the header because it "reads the column descriptors from attrs", a spurious constraint | A `PassthroughWriter` in ResultPrinter (`begin()`, `row(attrs)`, `end()`) that owns first-row and framing state | ~40 | Medium |
| D-7 | Six metadata walkers: aggregate_arg_{scale, precision, charset, temporal}, aggregate_uses_float_display, avg_arg_scale (2654-2815) | scale/precision follow Greatest2/Least2 and accept SUM; charset/temporal/float accept only a direct Load (O-10). avg_arg_scale returns 4 when there is no metadata, the others return 0/NONE/NULL | `const ColumnMetadata* aggregate_arg_metadata(const Outputs*, FunMask, bool follow_pair_ops)` | ~80 | Low |
| D-8 | Comparators: compare_rows' three arms and compare_passthrough_rows (RonSQLPreparer.cpp:10479) | NULL-ordering logic ×4. The charset guard exists in the AGGREGATE arm only (O-1). The AGGREGATE string arm rebuilds length-prefixed copies in scratch buffers just to call cmpVarchar, where `cs->coll->strnncollsp` on the payloads would do. Full sort vs partial_sort (O-16) | `int compare_nullable(bool an, bool bn, bool asc)`, `int compare_typed(Type, CHARSET_INFO*, ...)` with the charset guard, `sort_top_n(...)` | ~50 | Low-Medium |
| D-9 | TSV escaping in print_string (non-JSON branch, 2490-2497) and print_binary_tsv (2279-2294) | Identical escape sets | `print_tsv_char(out, unsigned char)` | ~10 | Low |
| D-10 | DATE text: d4/d2 formatters in GROUP BY (880-887, 2099-2107) vs `snprintf("%04u-%02u-%02u")` in print_temporal_packed (1313) | Different 3/4-byte reads | Route GROUP BY temporal types through print_temporal_packed, packing the bytes as the pass-through path does | ~40 | Low |
| D-11 | `quoted_identifier` formatter defined 4 times (ResultPrinter.cpp:116, RonSQLPreparer.cpp:126 and 139, AggregationAPICompiler.cpp:1144); `d2` twice | Value types differ | One header with LexString/LexCString/char* overloads | ~30 | Low |

Total roughly 750 lines. D-1, D-3 and D-5 are mechanical and also fix O-2 and O-5.

---

## Part 3: testing weak spots

- T-1. Only TEXT is compared with mysqld. ronsql_compare.inc always sends
  `"outputFormat":"TEXT"` (line 37). JSON and JSON_ASCII are checked only by hand-written cases
  (ronsql_encoding JSON, ronsql_operation_id, ronsql_binary_passthrough, ronsql_temporal_json, the Go
  Avro tests). Nothing tests: pass-through key escaping (O-2), JSON validity of every JSON-format
  test result, JSON_ASCII escaping of column values (BMP, non-BMP surrogate pairs, invalid bytes;
  ronsql_encoding has no JSON_ASCII run and no non-BMP characters), or duplicate keys (O-11).
  Suggestion: add an `outputFormat` loop to ronsql_compare.inc that also fetches JSON, validates it
  with a JSON parser, and compares it to the TEXT result after a mechanical conversion.
- T-2. Accepted mismatches. 14 compare-based tests run without strict_diff, and recorded mysqld vs
  RonSQL diffs sit in ronsql_hopsworks, ronsql_fs_smoke, ronsql_fs_templates, ronsql_dbt3_1_2 and
  ronsql_overflow (.result files, plus their jit/ng copies). The canonicalization scripts rewrite
  both outputs and so hide formatting differences: ronsql_formatting.test strips `.0000`
  (`s/.\0000\$//`), and ronsql_hopsworks.pl has about 20 value-specific sed rules (lines 253-273),
  including `s/\.0000\t/\t/`. Suggestion: remove the canonicalizations now that AVG formatting is
  fixed (01d22bdc566), and turn each recorded diff into either a fixed strict comparison or an
  explicit "known divergence" section.
- T-3. No DOUBLE value of 1e15 or more is compared with mysqld (O-4). No AVG tie (O-6). No negative
  zero (O-5). No DECIMAL(16..18, s) in a strict test (O-7).
- T-4. HAVING tests (ronsql_having_refs, ronsql_orderby D-sections) never cover BIGINT values above
  2^53, a DECIMAL SUM equality, a temporal MIN/MAX or a string MIN/MAX (O-8).
- T-5. CTE metadata propagation. ronsql_cte_chained uses only integer keys. Nothing covers chained
  string, DECIMAL(p,0) or temporal keys (O-1), GROUP BY or ORDER BY over a CTE temporal MIN/MAX output
  (O-3; pt-4 covers only pass-through), or GREATEST/LEAST over temporal or FLOAT scalar CTEs (O-10;
  the ronsql_cte_greatest_least* tests are integer-only).
- T-6. ORDER BY ties at display scale exist only for AVG (ronsql_orderby D2). There is none for SUM
  over DECIMAL or for CTE Double keys (O-9).
- T-7. TIMESTAMP is compared only at time_zone `+00:00` (ronsql_compare.inc:41). The UTC-only
  rendering is a semantic choice that no test documents (O-15).
- T-8. The pass-through sort caps (1M rows / 256 MB) have no test, and nothing checks actual
  arena usage against them (O-12).
- T-9. Go tests: avro_test.go checks that the JSON_ASCII body is ASCII-only, but only with ASCII
  aliases; adding an `AS \`é\`` alias would catch O-2. wrapper_test.go contains only TestMain, so the
  printer has no Go-level coverage beyond Avro and status.
- T-10. Error mid-output: the ronsql_cli partial-output behaviour (O-13) is frozen in result files
  as expected output. No test asserts that RDRS returns a clean error body with no partial JSON.

---

## Interface note: ApiRowAggregation → printer

For WP-J J5 (`m_api_side_aggregation`), the printer gets a copy of the statement with
`orderby_columns = NULL` and `limit = -1` (RonSQLPreparer.cpp:8716-8723), so ORDER BY and LIMIT pick
the rows that are aggregated and never reach the one-row result. HAVING is kept and evaluated by
print_result_ordered. Rows reach ApiRowAggregation as NdbRecAttr/PassthroughValue arrays through
`for_each_sorted_passthrough_row`, so its sort uses the same comparator as the pass-through printer
and inherits O-9 for CTE Double keys. None of the findings above depends on ApiRowAggregation
internals.
