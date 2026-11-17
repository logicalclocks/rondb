# RonSQL executor review, slice C: CTE execution

Tree: `/Users/mikael/mysql_trees/rondb_2610_main` at `50eb491eb3f` (26.10-main).
File: `storage/ndb/src/ronsql/RonSQLPreparer.cpp` unless stated otherwise.
Read-only review; nothing was built or run. Line numbers are at HEAD.

Not re-reported: the parser review (`ronsql_parser_review_plan.md`) and the
DBSPJ review (`claude_files/dbspj/README.md`: S-1, S-5, S-6, E-4, E-5, E-8,
E-23, X-1 and so on). Where a finding touches one of these, it says so.

## Summary table (most severe first)

| ID | Title | Category | Severity | Confidence |
|----|-------|----------|----------|------------|
| C-1 | CTE virtual-table columns are numbered by output position; the kernel numbers them as GROUP BY keys, then aggregate slots | bug | wrong-result | verified-by-reading |
| C-2 | A cross-table OR in WHERE promotes a LEFT-joined CTE (or table) to INNER | bug | wrong-result | verified-by-reading |
| C-3 | Virtual columns of type DATETIME/TIMESTAMP/TIME (current formats), DECIMAL, BINARY, VARBINARY and BIT have size 0 | bug | crash (release: receive-buffer overflow); error-handling | likely (overflow); verified (lookup-key failure) |
| C-4 | A cross-table residual filter in a CTE body leaves phantom groups; the sentinel slot ignores AVG | bug | wrong-result; error-handling | likely |
| C-5 | CTE root lookup keys are built with `(Int32)`/`(Uint32)` casts: an out-of-range literal finds another group | bug | wrong-result | verified-by-reading |
| C-6 | The CTE_LOOKUP constant filter on MIN/MAX(temporal) compares the packed value with an integer literal | bug | wrong-result | verified-by-reading |
| C-7 | Filtered scalar CTE root with a VARCHAR first output fails: the dummy-key copy diverged from the child's copy | bug (diverged duplicate) | error-handling (valid query fails) | verified-by-reading |
| C-8 | Return values of `setInterpretedCode`, `finalise` and `filter.end` are ignored on two CTE paths | bug | wrong-result on failure (silent filter loss) | likely (needs OOM or filter failure) |
| C-9 | No prepare-time check of the 32-node SPJ tree budget or the 64-CTE limit; `beginCteSubtree` result ignored | design | error-handling | verified-by-reading |
| C-10 | Unreferenced CTEs are materialized in join queries | design | performance | verified-by-reading |
| C-11 | Type derivation duplicated between `build_cte_virtual_tables` and `resolve_chained_column_type` | duplication | maintainability | verified-by-reading |
| C-12 | `emit_cte_lookup_filter`: five copies of the inverted-branch switch, dead operand-swap code | duplication | maintainability | verified-by-reading |
| C-13 | CTE root lookup-key builder duplicates the real-table root builder without its range checks (cause of C-5) | duplication | maintainability, wrong-result | verified-by-reading |
| C-14 | Smaller duplicates: linked-projection attach ×3, sentinel slot ×2, CASE CTE checks ×2, CTE operand resolution (filter vs CASE) | duplication | maintainability | verified-by-reading |
| C-15 | Test gaps in this slice | test-gap | — | verified-by-reading |

---

## C-1 CTE virtual-table columns are numbered by output position; the kernel numbers them as GROUP BY keys, then aggregate slots

- Category: bug. Severity: wrong-result (silent). Confidence: verified-by-reading.
- Location: `build_cte_virtual_tables` (13516). It is consumed by `lookup_relation_column` (7728), `programAggregator_join` (17866), `emit_cte_lookup_filter` (14114), `execute_passthrough_drain` (11920) and `emit_root_op` (12599, G4 key order).

### Evidence

The API numbers virtual columns in output order:

```
for (const Outputs* o = cte->stmt->outputs; o != NULL; o = o->next) { ... vt->addColumn(vcol); }   // 13534..13970
mut_impl.m_attrId = (int)cidx;                                                                     // 13980
```

A main-query reference resolves to the output index (7735-7744):
```
for (const Outputs* o = cte->stmt->outputs; o; o = o->next, cte_col_idx++) { ... result.cte_result_idx = cte_col_idx;
```

The kernel numbers the same row as GROUP BY keys (in GROUP BY order), then
the visible aggregate slots (`DblqhMain.cpp` `emitCteGroupOutput` 21892-21960;
`buildCteLinkedBuffer` 21530-21560 uses the same layout):
```
if (attrId < n_gb_cols) { /* GROUP BY key column — walk key data to the N-th entry */ ...
} else if (attrId < n_gb_cols + n_agg_results) { const Uint32 aggIdx = attrId - n_gb_cols; ...
} else { /* Unknown attribute ID — write NULL */
```

The GROUP BY keys are registered in `groupby_columns` order
(`programAggregator_join` 17904-17970). The slots are registered in output
order with deduplication (`AggregationAPICompiler::new_agg` "Deduplication",
`AggregationAPICompiler.cpp` ~300; `register_cte_outputs` 1093-1118).

Nothing enforces that the outputs are exactly
`[GROUP BY columns in GROUP BY order] + [distinct aggregates in slot order]`.
`analyze_ctes` (7131-7170) only checks "at least one aggregate" and "non-aggregate
columns need GROUP BY". No code reorders outputs or the GROUP BY list.
`claude_files/pushdown_join_aggregation/cte_filter_phase_i20.md` states
"`build_cte_virtual_tables()` honours [GROUP BY] ordering", but the code uses
output order.

The same positional assumption appears at:
- `emit_cte_lookup_filter`: `position = linked_base + cte_col_idx` (14233, 14682);
- `programAggregator_join`: `GroupByLinked(cte_base_pos + cte_col_idx, …)`, `LoadLinkedColumn(cte_base_pos + cte_col_idx, …)` (17924, 18063);
- `lookupCte` binds key i to the i-th PK column in virtual-column order (`NdbQueryBuilder.cpp` 1270-1281). The keys are sorted in GROUP BY order (`cte_key_coverage` 2200-2215).

`emit_cte_orderby_limit` (17622) is the one consumer that maps correctly:
GROUP BY position for columns and `agg_index` for aggregates.

### Concrete triggers (cte_schema tables)

1. Aggregate listed first:
   `WITH c AS (SELECT COUNT(*) AS n, o_custkey AS k FROM orders GROUP BY o_custkey) SELECT k, n FROM c;`
   Virtual column 0 `n` (BIGINT UNSIGNED) receives kernel attr 0, which is the
   INT key. Virtual column 1 `k` receives the 8-byte COUNT. The two values come
   out swapped.
2. Output order differs from GROUP BY order (same types):
   `WITH c AS (SELECT o_clerk AS b, o_custkey AS a, COUNT(*) AS n FROM orders GROUP BY o_custkey, o_clerk) SELECT ... JOIN c ON c.a = x.a AND c.b = x.b`.
   The lookup matches the right group, but `c.a` and `c.b` are swapped. With
   different types, `lookupCte` fails to bind the keys ("Failed to create
   child operation").
3. Duplicate aggregate:
   `WITH c AS (SELECT o_custkey AS k, SUM(o_shippriority) AS s1, SUM(o_shippriority) AS s2 FROM orders GROUP BY o_custkey) SELECT k, s1, s2 FROM c;`
   There is one kernel slot. Attr 2 is "unknown", so `s2` is always NULL.
4. GROUP BY column not in the output (valid SQL):
   `WITH c AS (SELECT COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT n FROM c;` returns `o_custkey` values as `n`.
5. A non-grouped column in the output (MySQL with ONLY_FULL_GROUP_BY refuses it):
   `SELECT o_custkey, o_clerk, COUNT(*) … GROUP BY o_custkey`. `o_clerk` shows the
   count and the count column shows NULL, where an error is expected.
6. Bare GROUP BY column with a qualified output: `SELECT o.o_custkey AS k, COUNT(*) FROM orders o GROUP BY o_custkey`.
   `is_groupby` compares raw `col_idx` (13573), so `k` is not marked PK.
   `cte_key_coverage` (2203) also compares raw `col_idx`, so a CTE_LOOKUP on
   `k` fails with "Could not derive CTE virtual primary key columns."
   `emit_cte_orderby_limit` and `is_single_group_cte_body` use
   `same_resolved_column` here, so the paths disagree.

The production generator (`fs_ronsql/shape_catalog.md`) always lists keys
first, in GROUP BY order. That is why this has not shown up. The test scan
found no CTE body that violates the layout, apart from one HAVING rejection
test.

### Suggested fix

Preferred: make the mapping explicit. Give each output a `kernel_pos`:
- a COLUMN output gets the position of its GROUP BY column, matched with `same_resolved_column`;
- an AGGREGATE or AVG output gets `n_gb + agg_index`.

Use `kernel_pos` wherever `cte_result_idx` or `cidx` is used as a buffer or
attribute position. Build the virtual table in kernel order: GROUP BY columns
in GROUP BY order (hidden columns for GROUP BY columns not in the output),
then one column per slot.

Minimum: reject at `analyze_ctes` with a clear UNSUPPORTED message when
1. the COLUMN outputs do not equal the GROUP BY list in order, or
2. an aggregate precedes a column, or
3. two outputs share an `agg_index`.

Rewriting `groupby_columns` into output order is semantically free in SQL and
fixes case 2 cheaply.

### Suggested test

`ronsql_cte`, new `ronsql_cte_dd_output_layout` (also add it to the ng and jit
suites). Run triggers 1-4 through `ronsql_compare.inc` against MySQL, for:
- a CTE_SCAN root with passthrough;
- a CTE_LOOKUP child with passthrough;
- a main aggregate over a CTE leaf (`SUM(c.n)`);
- a CTE_LOOKUP filter (`WHERE c.n > 3`).

Expect an error for case 5.

---

## C-2 A cross-table OR in WHERE promotes a LEFT-joined CTE (or table) to INNER

- Category: bug. Severity: wrong-result. Confidence: verified-by-reading.
- Location: `is_null_rejecting` (3895), `promote_left_to_inner_for_where` (4039-4113), `classify_where_by_table` routing (3763-3812), `emit_cte_lookup_filter` IS-NULL guard (14244-14253).

### Evidence

`is_null_rejecting` ignores which table a predicate references:
```
case T_EQUALS: case T_NOT_EQUALS: case T_LT: case T_LE: case T_GT: case T_GE:
    return true;
...
case T_OR:  return is_null_rejecting(ce->args.left) && is_null_rejecting(ce->args.right);
```

`classify_where_by_table` routes a two-table OR conjunct
(`c.n > 4 OR cu.c_nationkey = 3`) to the CTE_LOOKUP op's `join_where_ce`
(W6 routing; there is no `match_type` check). Non-routed conjuncts become
cross-table filters with `child_table_idx = c`. Both promotion loops then
promote:
```
if (is_null_rejecting(scope.join_where_ce[t])) { op.match_type = JoinOp::INNER; }   // 4046
...
if (op.match_type == JoinOp::LEFT_OUTER && is_null_rejecting(ctf.ce)) op.match_type = JoinOp::INNER;   // 4066
```

`cu.c_nationkey = 3` does not reject a NULL-extended `c`, so the promotion is
wrong. MySQL keeps unmatched `cu` rows that have `c_nationkey = 3`.

The final safety loop exempts CTE_LOOKUP (`op.type == JoinOp::CTE_LOOKUP ||`,
4105) because `emit_cte_lookup_filter` is said to reject this. It only rejects
`IS NULL` atoms (14244-14253). If the promotion were fixed alone, the routed
OR would be pushed as a filter on a LEFT CTE_LOOKUP. Rows that fail it would
be NULL-extended and kept, where WHERE removes them.

### Trigger

```
WITH c AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, c.n FROM customer AS cu LEFT JOIN c ON c.k = cu.c_custkey
WHERE c.n > 4 OR cu.c_nationkey = 3;
```
This needs customers without orders that have `c_nationkey = 3`: MySQL returns
them with NULL, RonSQL drops them. The same happens for real tables in an
aggregate query: `a LEFT JOIN b … WHERE b.x > 5 OR a.y = 1 GROUP BY …`.
The same promotion runs on CTE bodies (7462).

### Suggested fix

Make the check per table, `is_null_rejecting_for(scope, t, ce)`:
- a comparison rejects NULL for `t` only if one of its operands references a column of `t`;
- `IS NOT NULL` only if its argument is a column of `t`;
- OR needs every branch to reject;
- AND needs at least one branch to reject.

Then remove the CTE_LOOKUP exemption in the last loop of
`promote_left_to_inner_for_where`. A filter that does not reject NULL on a
LEFT CTE_LOOKUP then fails cleanly, as it does for other ops; the anti-join
path still handles `IS NULL`.

### Suggested test

`ronsql_cte`, `ronsql_cte_dd_outer_or` with dedicated tables: parents with and
without CTE matches, the OR on both sides, compared against MySQL. Add a
real-table twin in `ronsql`.

---

## C-3 Virtual columns of type DATETIME/TIMESTAMP/TIME (current formats), DECIMAL, BINARY, VARBINARY and BIT have size 0

- Category: bug. Severity: crash (release builds: RDRS receive-buffer overflow) and error-handling. Confidence: likely for the overflow (traced through the NDB API, not run); verified-by-reading for the key failures.
- Location: `build_cte_virtual_tables` attribute-size switch, 13994-14075.

### Evidence

`NdbColumnImpl::init()` sets `m_attrSize = m_arraySize = 0`
(`NdbDictionaryImpl.cpp` 383). The switch fills in sizes only for:
- the integer types, FLOAT, DOUBLE;
- CHAR, VARCHAR, LONGVARCHAR;
- YEAR, DATE;
- the legacy Time, Datetime and Timestamp types.

Datetime2, Timestamp2 and Time2 (the types every MySQL 8 table uses) are
missing, as are Decimal, Binary, Varbinary and Bit:
```
default:
  // DECIMAL, fractional-second temporals (Datetime2 / Time2 / Timestamp2) etc.: existing scanCte path
  // tolerated 0; the lookupCte path with such virt column types is not exercised yet by RonSQL.
```

A COLUMN output keeps its source type (13612-13618), so a CTE that groups by
such a column has a zero-size virtual column. This has three effects.

1. **Overflow in the passthrough path.** `execute_passthrough_drain` calls
   `getValue(vcol)` on every virtual column (12004-12012). The receive buffer
   is sized as `1 + (getSizeInBytes()+3)/4` words per value
   (`NdbReceiver.cpp` 647). For a lookup child, `result_bufsize` sizes the
   buffer at exactly `batch_rows * rowSizeWords`, and `allocRow` only
   `assert`s the free space (`NdbReceiver.cpp` 75). Each delivered DATETIME
   key is 2 words larger than reserved: an assertion in debug builds, a heap
   overflow in release builds.
   Trigger: `WITH g AS (SELECT e_modified AS t, COUNT(*) AS n FROM evlog GROUP BY e_modified) SELECT e.e_id, g.t, g.n FROM evlog AS e JOIN g ON g.t = e.e_modified;`
   (`check_cte_join_key_types` passes: type, precision, length and charset are equal).
   A BINARY/VARBINARY key over 16 bytes also overflows `NdbRecAttr::theStorage`
   (16 bytes, `NdbRecAttr.cpp` 96-101).
2. **Key failures.** The lookupCte root (main `WHERE t = '…'`, 12945-12951) and
   the G4 single-group root (12775-12866) build the key with
   `query_const_value` → `constValue(ptr,len)`. That fails because
   `len != getSizeInBytes()` (0) (`NdbQueryBuilder.cpp` 2087,
   QRY_OPERAND_HAS_WRONG_TYPE), and the query stops with
   "Failed to create lookupCte root." This contradicts the G4 contract in the
   comment at 12825: "a mismatch must fall back to scanCte, never turn a
   previously-working query into an error".
   Trigger: `WITH g AS (SELECT e_modified AS t, COUNT(*) AS n FROM evlog WHERE e_modified = '2024-03-05 10:20:30' GROUP BY e_modified) SELECT t, n FROM g;`
3. **Undercount.** `check_cte_group_key_size` counts these columns as 0 bytes
   for chained CTEs (228-245), so it undercounts the E-23 bound. This is
   minor, because the kernel now refuses cleanly with 20023.

### Suggested fix

Cover every type that `resolve_chained_column_type` can return:
- Datetime2: 5 + (prec+1)/2 bytes;
- Timestamp2: 4 + (prec+1)/2 bytes;
- Time2: 3 + (prec+1)/2 bytes;
- Decimal: `decimal_bin_size(p,s)`;
- Binary/Varbinary: as Char/Varchar;
- Bit: the word-array size.

End the switch with a `require_unsupported` default instead of silently
leaving 0. A cleaner option is to build the descriptor through the dictionary
helper that real tables use.

### Suggested test

`ronsql_cte`: GROUP BY on DATETIME, DATETIME(6), TIMESTAMP(6), TIME(3) and
DECIMAL(12,2) keys, as (a) a passthrough lookup child over all `evlog` rows,
(b) a root `WHERE key = const`, and (c) a single-group root. Run them in the
ng suites too: a larger batch makes the overflow more likely.

---

## C-4 A cross-table residual filter in a CTE body leaves phantom groups; the sentinel slot ignores AVG

- Category: bug. Severity: wrong-result (phantom groups); error-handling ("Please report a bug" for AVG). Confidence: likely (API and kernel read; not run).
- Location: `programAggregator_join` 17977-18000 (filter emit) and 18175-18195 (sentinel); `compile()` 8680-8695.

### Evidence

`classify_where_by_table` builds `cross_table_where_filters` for CTE bodies too
(7450; routing is main-scope only, 3766). `programAggregator_join` emits the
filter as an embedded program with `AGG_EMBEDDED_INTERP_STOP_PROGRAM`, plus a
hidden sentinel COUNT ("Groups where sentinel is 0 are suppressed at result
time"). Only `ResultPrinter` for the main query reads the sentinel
(`ResultPrinter.cpp` 1165).

The kernel creates the group record before the program runs
(`JoinAggInterpreter.cpp` 1556-1602: "the group prologue above has already
resolved agg_res_ptr"). A STOP_PROGRAM ends the row but keeps the group (1861).
So in a CTE body, every group whose rows all fail the filter is materialized
with COUNT 0 and NULL SUM/MIN/MAX. Consumers (CTE_SCAN, CTE_LOOKUP, chained
CTEs) see groups that MySQL does not produce.

The sentinel slot is computed from Sum/Count/Min/Max/AggRepeat only (18183-18190;
the same logic in compile() at 8685-8692). A CTE-scope `SVMInstrType::Avg`
slot is not counted. When AVG holds the highest slot, `Count(sentinel_slot)`
reuses it. `CheckAggAndReg` then fails with `kErrAggNoUsed`, and the user gets
"Failed writing aggregation program. Please report a bug."

### Trigger

```
WITH c AS (SELECT o.o_custkey AS k, COUNT(*) AS n
           FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
           WHERE o.o_shippriority > cu.c_nationkey GROUP BY o.o_custkey)
SELECT k, n FROM c;
```
This returns `(k, 0)` rows that MySQL omits. Adding `, AVG(o.o_shippriority) AS av`
gives the internal error.

### Suggested fix

Until the kernel can drop groups with sentinel 0 at CTE completion, reject
cross-table residual filters in CTE bodies with a clear UNSUPPORTED message.
Two alternatives:
- push the filter as the leaf op's interpreted code (it runs before grouping), using linked parent values as W6 does;
- add a kernel-side prune.

In either case, compute the sentinel slot in one shared helper that includes
`Avg`.

### Suggested test

`ronsql_cte`, `ronsql_cte_dd_body_xtable`: the query above compared with MySQL;
an AVG variant; a chained consumer (`SELECT COUNT(*) FROM c`).

---

## C-5 CTE root lookup keys are built with `(Int32)`/`(Uint32)` casts: an out-of-range literal finds another group

- Category: bug. Severity: wrong-result. Confidence: verified-by-reading.
- Location: `emit_root_op`, CTE root lookup, 12905-12930.

### Evidence

```
case NdbDictionary::Column::Int:
  lookup_keys[k] = qb->constValue((Int32) ce_const->constant_integer);      // 12911
case NdbDictionary::Column::Unsigned:
  lookup_keys[k] = qb->constValue((Uint32) ce_const->constant_integer);     // 12924
case NdbDictionary::Column::Bigunsigned:
  lookup_keys[k] = qb->constValue((Uint64) ce_const->constant_integer);
```

The cast wraps before `NdbInt64ConstOperandImpl` can range-check
(`NdbQueryBuilder.cpp` 2000-2012). The equality is consumed as the key and
left out of the residual (`build_root_residual`, 11211).

For comparison:
- the real-table root uses `encode_constant` + `query_const_value`, which refuses out-of-range literals;
- the same predicate on a CTE_LOOKUP child or scanCte goes through `encode_constant`.

### Trigger

`WITH c AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT k, n FROM c WHERE k = 4294967297;`
returns `(1, 5)`. MySQL returns no rows.

### Suggested fix

Use `encode_constant` + `query_const_value` for every type, as the real-table
root does; this needs C-3 for complete sizes. Or range-check before the cast
and treat an out-of-range literal as "no row" or as an error, consistently
with the other paths.

### Suggested test

`ronsql_cte_root_lookup`: `k = 4294967297`, `k = -4294967295`, and an INT
UNSIGNED key with `= -1`, compared with MySQL (empty result).

---

## C-6 The CTE_LOOKUP constant filter on MIN/MAX(temporal) compares the packed value with an integer literal

- Category: bug (diverged from the col-vs-col arm). Severity: wrong-result. Confidence: verified-by-reading.
- Location: `emit_cte_lookup_filter`, constant path, 14732-14773.

### Evidence

MIN/MAX over DATE/DATETIME/TIMESTAMP/TIME/YEAR produces a BIGINT UNSIGNED
virtual column that holds the packed value (13792-13806). The constant path
accepts it like any number:
```
require_prm(vt == NdbDictionary::Column::Bigint || vt == NdbDictionary::Column::Bigunsigned || ...   // 14743
...
raw_value rv = encode_constant(const_side, vtcol);                                                    // 14773
```

The col-vs-col arm of the same function has a source-type guard: a temporal
operand is allowed only as DATE vs DATE (14443-14506). The constant arm has
no such guard. `encode_constant(T_INT, Bigunsigned)` encodes the literal as a
plain number. A packed DATE is d + 32·m + 512·y; 2024-01-01 packs to 1036321.

A string literal fails with "Integer type column compared to an incompatible
value". It is raised as `RonSQLMaybeStaleSchema`, which unloads and reloads
the schema for an error that cannot be schema-related.

### Trigger

`WITH c AS (SELECT o_custkey AS k, MAX(o_orderdate) AS d FROM orders GROUP BY o_custkey) SELECT cu.c_custkey FROM customer AS cu JOIN c ON c.k = cu.c_custkey WHERE c.d >= 19950101;`
returns nothing. MySQL compares the column as a DATE and returns rows.
`… WHERE c.d >= '1995-01-01'` fails after a schema reload.

### Suggested fix

Apply the same `source_type` walk as the col-vs-col arm. For a temporal
source, encode the literal with the source column's type through
`encode_constant`, then widen it to the packed 64-bit value the kernel stores.
Otherwise reject it as unsupported (permanent, not MaybeStaleSchema).

### Suggested test

`ronsql_cte_dd_filter` (body_filter.inc): `MAX(date)` and `MAX(datetime)`
outputs compared with string and integer literals, against MySQL (or the
expected error).

---

## C-7 Filtered scalar CTE root with a VARCHAR first output fails: the dummy-key copy diverged from the child's copy

- Category: bug (diverged duplicate). Severity: error-handling (a valid query fails as INTERNAL). Confidence: verified-by-reading.
- Location: `emit_root_op` 12704-12745 and `emit_child_ops` 15384-15425.

### Evidence

The child copy learned (sc-20) that VAR columns need a value length of 0:
```
const Uint32 dummy_len = (pkc->getArrayType() == NdbDictionary::Column::ArrayTypeFixed) ? pksz : 0;   // 15409
```

The root copy still passes `getSizeInBytes()`:
```
scalar_keys[0] = qb->constValue(static_cast<const void*>(rzero), rpksz);   // 12726
```

For a SHORT_VAR column, `convert2ColumnType` returns
QRY_CHAR_OPERAND_TRUNCATED when `len + 1 > maxSize`
(`NdbQueryBuilder.cpp` 2092). `lookupCte` then returns NULL and the query
fails with "Failed to create scalar lookupCte root."

### Trigger

`WITH s AS (SELECT MAX(c_mktsegment) AS m FROM customer) SELECT m FROM s WHERE m >= 'A';`
(Tests 10-12 in `ronsql_cte_scalar` only use numeric MAX.)

### Suggested fix

Extract one helper, `const NdbQueryOperand* scalar_cte_dummy_key(NdbQueryBuilder*, const NdbDictionary::Table* vt)`,
and use it from both places. This saves about 35 lines and carries no risk.

### Suggested test

`ronsql_cte_scalar`: a string MIN/MAX scalar root with `=`, `>=`, and with no
matching rows.

---

## C-8 Return values of `setInterpretedCode`, `finalise` and `filter.end` are ignored on two CTE paths

- Category: bug (error handling). Severity: wrong-result on failure (the filter is silently dropped). Confidence: likely (needs OOM or a filter-builder failure to trigger).
- Location: `execute_join`, single-table CTE body root, 11643-11652; `emit_child_ops`, CTE_LOOKUP filter, 15177. The same pattern is at 12480/12481, 13180/13181 and 15162/15163 (other reviewers' code).

### Evidence

```
filter.begin(NdbScanFilter::AND); apply_filter(&filter, cs, root_ce); filter.end();
rootCode.finalise();
rootOpts.setInterpretedCode(rootCode);                 // 11648-11652: none checked
...
opts.setInterpretedCode(cte_filter_code_storage);      // 15177: unchecked
```

`NdbQueryOptionsImpl::copyInterpretedCode` returns `Err_MemoryAlloc` or
`Err_FinaliseNotCalled` and keeps no code (`NdbQueryBuilder.cpp` 769-794).
With an unchecked return, the CTE body aggregates unfiltered rows, or the
CTE_LOOKUP keeps every group. The README of the DBSPJ review records the same
mechanism for mysqld (E-11, "the same applies when setInterpretedCode fails").
Elsewhere in RonSQL these calls are checked (12567, 12695, 12970, 12995, 13146).

### Suggested fix

Use one helper,
`void attach_scan_filter(NdbQueryOptions&, NdbInterpretedCode&, QueryScope&, ConditionalExpression*, const char* what)`,
which checks begin, end, finalise and `setInterpretedCode`. Use it at all six
sites; this also removes about 40 lines of duplication.

### Suggested test

A unit test or error-injection hook (as in `ronsql_parser_oom`) that makes
`copyInterpretedCode` fail, and checks that the query fails instead of
returning unfiltered rows.

---

## C-9 No prepare-time check of the 32-node SPJ tree budget or the 64-CTE limit; `beginCteSubtree` result ignored

- Category: design. Severity: error-handling. Confidence: verified-by-reading.
- Location: `execute_join` 11621 (`qb->beginCteSubtree(c);` unchecked) and 11732 (`endCteSubtree` unchecked); `QueryPlanner.cpp` 521-541.

### Evidence

`QueryPlanner` checks the 32-node budget per plan and leaves CTE subtrees to
the API ("CTE subtree nodes are accounted by the API's own prepare-time cap").
Each CTE adds one subtree node, plus its body ops, plus one more for the
MIN/MAX self-join. `NdbQueryOperationDefImpl` refuses
`internalOpNo >= NDB_SPJ_MAX_TREE_NODES` (`NdbQueryBuilder.cpp` 2642).
`beginCteSubtree` refuses `cteId >= 64`.

Both failures surface later as `require_run(... "Failed to create CTE body
scan root.")` or "Failed to create child operation.". `require_run` throws a
plain `std::runtime_error`. `handle_ronsql_exception` classifies it from
`m_trans->getNdbError()` (no error), so it becomes INTERNAL, and the builder's
QRY_DEFINITION_TOO_LARGE is lost.

Trigger: about 12 single-table CTEs joined into a main query of 9 tables,
or 65 CTEs.

### Suggested fix

After `build_cte_scopes`, sum the node counts:
- for each CTE: 1 + its body's internal nodes + 1 if the MIN/MAX self-join is used;
- plus the main plan's internal nodes.

Throw a LIMIT error before emitting if the sum exceeds 32 or there are more
than 64 CTEs. Check the `beginCteSubtree` and `endCteSubtree` results. Report
`qb->getNdbError()` in builder-failure messages.

### Suggested test

`ronsql_cte`: a 17-CTE query (or 12 CTEs plus 9 joins) that expects the LIMIT
error text.

---

## C-10 Unreferenced CTEs are materialized in join queries

- Category: design. Severity: performance (and an unneeded failure point). Confidence: verified-by-reading.
- Location: `build_cte_scopes` 7418-7488 (pushes every CTE); `execute_join` 11549-11765 (defines and materializes every scope).

### Evidence

There is no reference check. `WITH a AS (…), unused AS (SELECT … FROM lineitem GROUP BY …) SELECT … FROM t JOIN a …`
scans and redistributes `lineitem`. If `unused` has a runtime problem (for
example a single-row violation), the query fails.

The behaviour is inconsistent: when the main query is a single table
(`is_join_query()` false, 1908), the CTEs are never planned.

### Suggested fix

Mark referenced CTEs, transitively from the main plan's CTE ops, and skip the
rest. CTE ids must stay dense, or the depMask and cteId must be remapped.

### Suggested test

`ronsql_cte`: a query with an unused expensive CTE. Check EXPLAIN (no
subtree) or `ndbinfo` scan counts.

---

## C-11 Type derivation duplicated between `build_cte_virtual_tables` and `resolve_chained_column_type`

- Category: duplication. Severity: maintainability. Confidence: verified-by-reading.
- Sites:
  - `build_cte_virtual_tables` SUM arm 13640-13697, MIN/MAX arm 13699-13822, AVG arm 13832-13933;
  - `resolve_chained_column_type` SUM 13244-13310, MIN/MAX 13311-13420, AVG 13421-13500.

The "chained display metadata" block appears six times:
```
st == Double && resolved_columns[...].kind == CteResultColumn && scale in (0,30] && precision > 0
```
The AVG "scale+4, precision gate" rule appears four times. The ten-way
integer `case` lists repeat about eight times.

- Differences today: only the error reporting. The builder throws messages per
  type; the resolver returns `false`, which callers turn into the vaguer
  "CTE aggregate references unresolved source column.". I found no semantic
  divergence (I compared every arm).
- Proposed helper: `bool derive_cte_output_type(const QueryScope& body, const Outputs& o, ColType& out /*type,len,cs,scale,prec*/, const char** reject)`.
  `build_cte_virtual_tables` becomes a loop over the outputs plus the
  storage-size helper from C-3. `resolve_chained_column_type`'s CTE branch
  calls the helper.
- Saving: about 250 lines. Risk: medium, because the display metadata (D15,
  obc-11) is fragile. Keep the existing passthrough and decimal tests as the
  check.

---

## C-12 `emit_cte_lookup_filter`: five copies of the inverted-branch switch, dead operand-swap code

- Category: duplication. Severity: maintainability. Confidence: verified-by-reading.
- Sites, each a six-case switch mapping the SQL operator to the inverted
  `branch_*` method:
  - string linked-vs-linked 14398-14425;
  - register-vs-register 14522-14533;
  - ancestor `branch_linked_mem_*` 14589-14625;
  - inline type 14817-14853;
  - source-column `branch_linked_mem_*` 14858-14896.
- Dead code: `simplify_ce` already rewrites `const OP col` into `col OP' const`
  (16729-16734). After that, `swapped` is always false, so the two mirror
  switches (14577-14585, 14776-14785) never run.
- Re-implemented helpers: `is_cmp` (14278) duplicates `is_comparison_op`
  (6519), and the mirror switch duplicates `flip_cmp_op` (6507).
- Proposed helper: `static TokenKind reject_cmp(TokenKind accept_op)`, which
  maps EQ→NE and LT→GE. Add one thin wrapper per opcode family that maps the
  reject comparison to the NdbInterpretedCode method, keeping the inverted
  naming in one place. Alternatively, use a table of member-function pointers
  per family.
- Saving: about 120 lines. Risk: low, and the inverted naming has been a
  source of bugs, so one table helps.

---

## C-13 CTE root lookup-key builder duplicates the real-table root builder without its range checks

- Category: duplication (diverged; this is the cause of C-5). Severity: maintainability, wrong-result. Confidence: verified-by-reading.
- Sites: CTE root 12895-12955 (typed `constValue` with casts, comment "Synthetic virt-table columns have m_attrSize == 0"); real-table root 13128-13140 (`encode_constant` + `query_const_value`); G4 kind pre-validation 12826-12857.
- How they differ:
  - the CTE copy exists only to work around C-3 (zero sizes);
  - it skips the range checks and the MaybeStaleSchema/Permanent classification that `encode_constant` provides;
  - G4's `kind_ok` table repeats the same type families a third time.
- Proposed helper: after C-3 is fixed, `const NdbQueryOperand* const_key_operand(NdbQueryBuilder*, ConditionalExpression*, const NdbDictionary::Column*, bool* kind_ok)`, shared by both roots and G4.
- Saving: about 60 lines. Risk: low once virtual column sizes are complete.

---

## C-14 Smaller duplicates

Each item: verified-by-reading, maintainability.

1. **Linked-projection attach loop ×3** in `emit_child_ops`: 15190-15203, 15209-15222, 15233-15246. The loops are identical apart from the error text. Helper: `void attach_linked_projections(NdbQueryBuilder*, NdbQueryOptions&, const JoinPlan&, Uint32 op_idx, const NdbQueryOperationDef**, const char* err)`. Saves about 30 lines; low risk.
2. **Sentinel slot computation ×2**: `compile()` 8682-8695 and `programAggregator_join` 18180-18190. Both omit `Avg` (see C-4). Helper: `static Uint32 sentinel_slot(const DynamicArray<Instr>&)`.
3. **CASE over a CTE: checks done twice.** `require_cte_case_condition_column_output` (18200) checks `cte_def`, `cte_def_idx` and `o->type == COLUMN`. `resolve_col_side` in `generate_embedded_condition` checks them again for the non-leaf path (18712-18727), with near-identical messages. Also, its `ref.join_op_idx == op_idx` check is always true at the only call site (18705 passes `ref.join_op_idx`).
4. **CTE operand resolution, filter vs CASE.** `emit_cte_lookup_filter` (14670-14770) and `generate_embedded_condition` (18700-18775) both resolve a CTE output to one of two forms:
   - a COLUMN output becomes `branch_linked_mem` on the body's source table and attribute;
   - an aggregate output becomes an inline-typed branch.

   CASE rejects aggregate outputs even on the CTE leaf, although the inline
   opcode would serve them. A shared
   `resolve_cte_output_operand(scope, op_idx, col_idx, virtTab) -> {position, kind, src_table, attrId, vtcol}`
   would remove about 80 lines and lift that restriction. Risk: medium, because
   of the linked-base offsets.

---

## C-15 Test gaps in this slice

The scan used `mysql-test/suite/ronsql_cte/{t,include}`, `ronsql/t/ronsql_cte_*.test`
and the ng and jit mirrors.

| Gap | Covers | Proposed test |
|-----|--------|---------------|
| No CTE body whose outputs are not `[GROUP BY cols in order] + [distinct aggregates]` (my scan found none) | C-1 | `ronsql_cte_dd_output_layout` (triggers in C-1) |
| No LEFT JOIN to a CTE with a cross-table OR in WHERE | C-2 | `ronsql_cte_dd_outer_or` |
| CTE GROUP BY only on INT/TINYINT/CHAR/DATE keys (`body_dtwide`: "GROUP BY keyed on TINYINT / CHAR / DATE"); no DATETIME/TIMESTAMP/TIME/DECIMAL/BINARY key through passthrough, root lookup or single-group root | C-3 | `ronsql_cte_dd_temporal_keys` |
| No CTE body with a residual cross-table WHERE (my scan found none) | C-4 | `ronsql_cte_dd_body_xtable` |
| `ronsql_cte_root_lookup` has no out-of-range or negative key literal | C-5 | extend `ronsql_cte_root_lookup` |
| `body_filter.inc`: no MIN/MAX(temporal) output against a constant | C-6 | extend `body_filter.inc` |
| `ronsql_cte_scalar` Tests 10-12 only use numeric scalar roots | C-7 | extend `ronsql_cte_scalar` |
| No test exceeds the SPJ node budget or the 64-CTE limit | C-9 | limit-error test |
| No test with an unreferenced CTE in a join query | C-10 | EXPLAIN/scan-count test |
| `ronsql_cte_jit` lacks `ronsql_cte_dd_orderby_limit_cte` (CTE-body ORDER BY/LIMIT trailer under the JIT), `_single_group_cte`, `_count_star`, `_cte_dag` and `_outer_chain_miss`; the ng suites have them | JIT | add the mirrors |
| CTE-body ORDER BY/LIMIT: no tie at the LIMIT boundary across nodes (the result is not deterministic; it should be documented and tested with a unique order key), no ORDER BY over AVG with an all-NULL group, no LIMIT on a chained CTE whose consumer is a CTE_LOOKUP | ORDER BY/LIMIT | extend `body_orderby_limit_cte.inc` |
| Single-group CTE: only INT keys; no case-insensitive string key with mixed case, and no trailing-space CHAR key (the kernel hash is collation-aware, so this should pass, but it is not pinned) | single-group | extend `body_single_group_cte.inc` |
| Several CTE_LOOKUP ops with routed ancestor filters (W6) in one query: shared `linked_projs` list, `linked_base` per op | W6 | new case in `body_filter.inc` |

## Checked and found sound (not reported)

- `emit_cte_lookup_filter` three-valued logic. Every atom guards NULL to the
  current disjunct's fail label, so UNKNOWN acts as FALSE inside a DNF without
  NOT. That is correct for WHERE. IS NULL on a LEFT op is rejected; the
  anti-join idiom is handled separately.
- `check_cte_join_key_types`. It compares type, precision, scale, length and
  charset pointer, the same fields as `NdbDictionary::Column::isBindable`.
  Signedness is part of the type. NULL keys are dropped by DBSPJ (E-15
  context), which matches SQL.
- `is_single_group_cte_body`. It walks only the AND spine with constant kinds.
  A case-insensitive string key is safe because `GBHashTable` hashes with
  strnxfrm (`AggHashTable.hpp` 223-244) and compares with the collation (E-5
  fix), so collation-equal spellings give one group.
- `emit_cte_orderby_limit`. It maps outputs correctly (GROUP BY position or
  agg slot). The kernel orders NULLs first (`compareGroupsByOrderSpec`).
  LIMIT goes through the CTE_LIMIT single-owner redistribution. HAVING is
  rejected in CTE bodies (7100), so HAVING-before-LIMIT cannot arise.
- `linked_base` (`emit_cte_lookup_filter`) and `cte_base_pos`
  (`programAggregator_join`) are consistent with what `emit_child_ops`
  attaches. Linked projections are pre-registered before emission
  (`build_agg_linked_projections` and `build_cte_linked_projections`), so
  emission does not shift positions.
- `JoinQueryObjects` releases the CTE aggregators and virtual tables on every
  exit path. The aggregation programs are copied into the query options
  (`copyAggregation`), so the stack `singleAgg` going out of scope first is
  safe.
- The CTE_LOOKUP attrinfo size is bounded by the kernel (`ZATTR_BUFFER_SIZE`
  → ZATTRINFO_TOO_LARGE REF). The DNF filter is capped at 16 disjuncts and
  `MAX_WHERE_CONJUNCTS`. The body LIMIT is capped at 2^26-1 and ORDER BY at
  8 columns.
