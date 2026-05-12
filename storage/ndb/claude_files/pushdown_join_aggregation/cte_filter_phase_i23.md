# Phase I.23 - Scoped CTE and table name resolution

## Status

**Shipped** in `ffae6a9627a`.  Added scoped per-SELECT column
collection, resolved CTE bodies in declaration order with only stored
tables plus already-published CTE result interfaces visible, published
only each CTE name and exposed result columns to later scopes, rejected
ambiguous unqualified names across stored tables and visible CTEs,
preserved the legacy emit maps as compatibility output, fixed
HAVING-only aggregate resolution, and added `ronsql_cte_name_resolution`
MTR coverage.  I.24 (`cf188857098` … `7e0f33b7890`) followed and made
the resolved column descriptor the authoritative emit contract,
removing the legacy compatibility arrays.  I.11's kernel-topology
coverage in `3ca4f897af3` then resumed on top of the new resolver +
descriptor model.

The original plan body follows.

Phase context (preserved): the aborted Phase I.11 implementation
attempt exposed that RonSQL's CTE name handling was too global —
internal table aliases and columns referenced inside one CTE could
leak into later scopes, while CTE result columns sometimes lost the
metadata needed to build virtual tables or resolve join operands.
I.23 was a prerequisite for returning to I.11 and for any later phase
that composes nested CTEs with normal stored tables.

## Problem

RonSQL currently tracks parsed column names in process-wide vectors
such as `m_columns`, `m_column_qualifiers`, `m_col_is_inner`, and
per-scope maps populated after planning.  That coarse separation is no
longer enough once a query has multiple CTE bodies plus a main SELECT:

- a stored-table alias used inside `cte0` must not be visible while
  resolving `cte1` or the main SELECT;
- a stored-table column used to produce a CTE output must not be
  addressable outside that CTE body;
- only a CTE's exposed result names are visible to later CTEs and the
  main SELECT;
- a CTE operation must carry enough metadata to identify both the CTE
  index and the referenced result-column index;
- unqualified names are valid only when they are unique in the current
  scope across both stored tables and visible CTE outputs.

The Phase I.11 test attempts triggered all of these issues in different
forms: ambiguous internal `grp` names, missing `cte_def` metadata on a
CTE op, unresolved source type for a chained CTE output, and RDRS aborts
from `ndbrequire()` paths that assumed name resolution had already been
made consistent.

## Core rule

Build name resolution one query scope at a time, in SQL visibility
order:

1. Resolve `cte0` with access to stored tables only.
2. Publish only `cte0`'s exposed result-column interface.
3. Resolve `cte1` with access to stored tables plus `cte0`'s exposed
   interface.
4. Continue left-to-right through the CTE list.
5. Resolve the main SELECT with access to stored tables plus all CTE
   exposed interfaces.

At no point may a CTE body's internal stored-table aliases or columns be
visible outside that CTE body.  Later scopes see only the CTE name and
its result names.

## Name model

Introduce an explicit resolver model instead of relying on global
column-index side effects.

### Scope catalog

Each SELECT / CTE body gets a `NameScope`:

- stored table entries for tables and aliases in that scope's FROM /
  JOIN chain;
- visible CTE entries from previously processed CTEs;
- no entries for future CTEs;
- no entries for internal tables from earlier CTE bodies.

Stored table entry:

- visible name: alias if present, otherwise table name;
- physical table index / join op index once planned;
- dictionary table pointer;
- column name to dictionary column / attr id.

CTE entry:

- visible name: CTE name;
- CTE definition index;
- ordered result interface;
- result name to result-column index;
- result type/nullability/key metadata after the CTE body is analysed.

### Resolved column reference

Every parsed column reference should resolve to a stable descriptor:

- source kind: stored table column or CTE result column;
- source scope id;
- visible table/CTE entry index;
- join op index after planning;
- column index within the stored table or CTE result interface;
- dictionary source column when the result ultimately maps to a stored
  column;
- derived result type metadata for aggregate or expression outputs.

Unqualified lookup searches the current `NameScope` only.  It succeeds
only if exactly one visible source has that column/result name.  If more
than one stored table or CTE result exposes the same name, reject with a
clear "ambiguous column" error and require `table.column` or
`cte.column`.

Qualified lookup searches only the named stored table alias or visible
CTE name in the current scope.  Referencing a CTE body's internal alias
from outside that CTE must fail as an unknown table/qualifier.

## Implementation plan

### Step 1 - Inventory and isolate current resolver paths

Map every path that consumes `m_columns`, `m_column_qualifiers`,
`m_col_is_inner`, `column_attrId_map`, `column_table_idx`, and
`column_map`.

Expected areas:

- main `load_join()` column resolution;
- `resolve_columns_for_cte_scope()`;
- `resolve_cte_output_columns_for_scope()`;
- `build_cte_virtual_tables()`;
- CTE lookup filter emission;
- aggregation and embedded-CASE codegen;
- pass-through result draining.

The output of this step should be a short inventory in the commit
message or phase notes, so later refactoring can be reviewed against a
complete list.

### Step 2 - Build CTE result interfaces in order

For each CTE in declaration order:

1. Create a `NameScope` containing stored tables plus already-published
   CTE interfaces.
2. Resolve that CTE body's FROM / JOIN / WHERE / GROUP BY / SELECT
   expressions only against this scope.
3. Build the CTE's result interface from its SELECT output list.
4. Publish only that result interface under the CTE name.

The result interface must include enough information for later scopes to
derive virtual table metadata without reopening internal aliases:

- output name;
- output ordinal;
- whether it is part of the virtual key (`GROUP BY` output);
- source kind;
- source CTE index and result index for chained CTE COLUMN outputs;
- dictionary column metadata for direct stored-column outputs;
- derived type/nullability for aggregate and expression outputs.

### Step 3 - Resolve the main SELECT against final scope

After all CTE interfaces are published, build the main `NameScope` from
stored tables plus all CTE interfaces.

Resolve all main SELECT, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, and
aggregation expression references against that final scope.  Internal
aliases from CTE bodies must not be present.

### Step 4 - Make planner metadata authoritative

Ensure every planned CTE op gets:

- `cte_def_idx`;
- a pointer or stable handle to the CTE interface;
- result-column lookup metadata for each referenced CTE column;
- correct join-op parent/child mapping for both CTE roots and CTE
  children.

Code that builds virtual tables or emits filters should not rediscover a
CTE by alias string as a fallback.  Alias matching can be useful as an
assertion while migrating, but the final path must consume resolved
metadata produced by the resolver.

### Step 5 - Replace aborts with resolver assertions or clean errors

Remove `ndbrequire()` assumptions that user SQL can trigger through
missing CTE metadata.  They should become either:

- internal assertions after the resolver guarantees the condition; or
- `RonSQLPermanentError` / `RonSQLRuntimeError` with a clear message if
  the shape is deliberately unsupported.

The important distinction is that malformed or unsupported SQL must not
abort RDRS.

### Step 6 - Restore I.11 coverage after resolver lands

Once scoped resolution is in place, return to the I.11 shapes from
`testCteNdbApi.cpp` Tests 12-16.  The test should validate positive
coverage without compensating SQL rewrites that only avoid resolver
bugs.

## Test plan

Add a dedicated MTR file for name-resolution behaviour before reviving
the full I.11 shape tests.

### Positive tests

1. CTE body can use a stored-table alias, and the next CTE can reference
   only the prior CTE's result names.
2. Main SELECT can join a stored table and a CTE when names are
   qualified.
3. Chained CTE result column resolves through multiple CTE layers and
   keeps the correct virtual-table type.
4. Unqualified column works when unique across current stored tables and
   visible CTE outputs.
5. Qualified `stored_alias.column` and `cte_name.result_column` both
   work when the short name would be ambiguous.

### Negative tests

1. Main SELECT references a CTE body's internal stored-table alias:
   reject as unknown qualifier.
2. Later CTE references a previous CTE body's internal alias:
   reject as unknown qualifier.
3. Unqualified name exists in two stored tables: reject as ambiguous.
4. Unqualified name exists in one stored table and one visible CTE:
   reject as ambiguous.
5. Qualified name references a future CTE: reject as unknown table/CTE.
6. Qualified name references a CTE result that was not exposed:
   reject as unknown column on CTE.

### Regression tests

After the name-resolution MTR passes, reintroduce the I.11 MTR for
`testCteNdbApi.cpp` Tests 12-16.  These tests should be checked for:

- no leakage of internal `grp` / alias names;
- no missing `cte_def` / CTE index on planned CTE ops;
- no unresolved source type while building CTE virtual tables;
- no RDRS aborts.

## Out of scope

I.23 is not a feature-expansion phase.  It should not add new SQL
shapes except where needed to prove name resolution.  In particular:

- no comma/CROSS JOIN parser work;
- no partial-key CTE scan fallback;
- no new aggregate expression support;
- no ORDER BY / LIMIT support.

Those should remain in their existing phases once scoped resolution is
sound.
