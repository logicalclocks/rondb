# RonSQL parser review and cleanup plan

Review started: 2026-10-27
Updated: 2026-11-03
Original baseline: 7128c75dca405f5e0fff6be19ae2b584beb1566d
Cleanup review baseline: 45a7568faed6fe58aca99170be3a50c03a4495c9
Follow-up review baseline: 6b0b7da91c5dacf5d6401b2ec65135fa7d254ecb

## Scope and working agreement

Review the lexer, grammar, AST, semantic analysis and preparation passes.
Read executor interfaces where needed to understand frontend contracts.
Data-node implementation changes are outside this review.

Three parallel source investigations covered grammar and AST construction,
condition traversal and collection, and preparation phases and registration.
Open investigations below remain source-review findings. Completed items include
the approved implementations and user-reported validation recorded below.

Present one normal diff at a time and obtain explicit approval before editing.
The user owns building and testing. Keep behavior corrections separate from
mechanical cleanup, and preserve existing uncommitted work.

Source references below are relative to storage/ndb/src/ronsql/. Numbered
references describe the original baseline, not the current line positions.
Function names identify the durable locations.

## Progress

| Items | Status | Commit |
| --- | --- | --- |
| I1, I2, I3 | Completed, tested by the user, committed and pushed | 09176e0a3ed |
| I4 | Fixes and fault-injection tests committed and pushed; user reported tests passed | cac1d1bf650 (fixes), 6b0b7da91c5 (tests) |
| C1 | Completed for statement construction and synthetic table defaults | 45a7568faed |
| C3 | Original work and list-construction follow-ups committed and pushed; user reported tests passed | 45a7568faed, 6b0b7da91c5 |
| C4 | Projection/HAVING follow-ups and regression coverage applied; user validation pending | 45a7568faed, 6b0b7da91c5; current follow-ups uncommitted |
| C2 | C2a-C2e approved and applied; validation is user-owned | cfcc91bc44e |
| C5 | C5a-C5d committed; remaining collector/overflow changes and regressions applied; user validation pending | cfcc91bc44e; current follow-ups uncommitted |
| C6 | C6a-C6b approved and applied; validation is user-owned | cfcc91bc44e |
| C7 | C7a-C7d approved and applied; validation is user-owned | cfcc91bc44e |
| C8 | C8a-C8c approved and applied; validation is user-owned | cfcc91bc44e |
| C9 | C9a approved and applied; validation is user-owned | cfcc91bc44e |
| Unary minus | Precedence and cross-table diagnostic changes plus eight regression cases applied; user validation pending | Uncommitted |
| Feature candidates | Remaining candidates require individual evaluation and approval | - |
| Training module | Completed, reviewed by the user, committed and pushed in hops_slideware | 387c866 |

The first two commits were pushed to origin/RONDB-1124-fixes:

- 09176e0a3ed: RONDB-1124: Fix RonSQL subquery parsing and COUNT semantics.

- 45a7568faed: RONDB-1124: Simplify RonSQL AST construction and traversal.

The user reported all tests passed before each commit. The assistant did not
build or run tests; git diff --check passed for both changesets. Those initial
C1/C3/C4 changes added no test cases. Subsequent work is recorded below.

### Cleanup batch: C2 and C5-C9

The user selected these six items on 2026-10-29. Independent investigations
ran in parallel, with each source patch reviewed and approved before application.
C2a-C2e, C5a-C5d, C6a-C6b, C7a-C7d, C8a-C8c and C9a are approved and applied
and were committed and pushed in cfcc91bc44e. The user requested
commit and push on 2026-10-30. No test result was reported for this batch in the
conversation. The assistant did not build, test or regenerate lexer/parser
files; git diff --check passed.

| Track | First patch and sequence | Dependencies |
| --- | --- | --- |
| C2 | Shared inner SELECT body, then small grammar/construction helpers | C1 complete; preserve the public column-registration methods for C8 |
| C5 | Shared Boolean collector, then checked 32-term and fallback callers, then reconstruction | Independent of grammar and compiler factories |
| C6 then C7 | Compiler factory and aggregate registration, then preparation-phase extraction | Keep aggregate and output-phase changes in the same sequential track |
| C8 | Shared column interning, then ORDER BY name matching and relation-local lookup | Independent of C2 while its existing public interfaces remain stable |
| C9 | Shared normalized MySQL-keyword lookup | Independent lexer change |

C7's parse-error extraction is independent of C6, but remains in the same
track for review. Output-registration extraction follows the C6 helpers.
Worker capacity determined when independent drafts started; it did not create
a source-code dependency between them. All patches in this selected cleanup
sequence are included in cfcc91bc44e; validation remains user-owned.
I4 OOM corrections and subsequent follow-ups are recorded below.

### Follow-up review of C4/C5 and unary minus (2026-11-03)

Baseline: 6b0b7da91c5. All source and test changes below were reviewed and
approved individually and are applied locally, uncommitted. git diff --check
passed. The assistant has not built, regenerated the parser or run tests;
no user test result has yet been reported for this follow-up batch.
The findings describe source paths, not reproduced runtime failures.

Applied changes:

- classify_where_by_table() now rejects more than 32 OR atoms before consuming
  a partial validation result. The previous code retained the original tree;
  predicate loss was not established. Later projection discovery could inspect
  operands beyond the validated prefix.

- register_condition_linked_projections() shares safe, same-scope child access
  across main and CTE cross-table filters and CASE conditions. Complete CASE
  discovery includes literal-left and right-hand column references before CTE
  projection offsets are calculated. GROUP BY projections remain first.
  Aggregate programs and inner SELECT bodies retain separate traversal.

- Cross-table Boolean emission uses the checked 32-term collector. CASE
  emission uses a dynamic collector. Both flatten all terms of the same
  connective, including right-associated and balanced trees, and preserve the
  former emission order for previously supported left-associated trees.
  Mixed-connective expression support and downstream limits remain separate.

- Main and CTE/root scan planning share dynamic AND collection. The CTE
  planner's 64-entry pending-work stack and its early return were removed.
  Simplification can expand an expression after raw-conjunct checks. The old
  fallback retained the complete filter but could skip FORCE INDEX validation;
  predicate loss was not established. Existing raw-conjunct limits remain.

- Both expression grammars use explicit T_UNARY_MINUS precedence. Unary minus
  in cross-table WHERE arithmetic now raises UNSUPPORTED before register-depth
  calculation can follow its absent left operand. Binary subtraction retains
  its existing handling.

- having_reference_children() shares the operand policy between reference
  validation and alias resolution. Validation still reads column indices
  before compile() replaces supported aliases with aggregate-register indices.
  Recursive callables use descriptive names, as requested by the user.

- HAVING IN now exposes its outer operand to both walks and to
  ResultPrinter::scan_having_max_agg(). Register allocation happens before
  execution substitutes subquery results. Inner SELECT bodies contribute no
  registers to the outer result printer.

Fourteen regression cases are applied, awaiting user execution:

- ronsql_subquery: seven strict MySQL/CLI/RDRS comparisons covering SUM and AVG
  absent from SELECT, arithmetic around an aggregate, nullable operands,
  NULL-containing inner results, and empty positive/negated IN results.
- ronsql_having_refs: four disconnected preparation-time rejection cases for
  grouped columns, qualified columns, ordinary aggregate aliases and columns
  under arithmetic/NOT inside IN.
- ronsql_subquery_agg: three strict MySQL/RDRS comparisons covering nullable SUM
  aliases, COUNT aliases under NOT IN and aliases inside arithmetic.
  Expected-result files were drafted from the fixtures, not recorded by a run.

Eight unary-minus regression cases are also applied, awaiting user execution:
ronsql_constants adds five strict MySQL/CLI/RDRS comparisons for folded and
column arithmetic, repeated minus, and HAVING with explicit grouping controls.
Its existing negative-overflow diagnostic now reflects negation before
multiplication. ronsql_join_agg adds two CLI rejection cases and a strict
MySQL/RDRS binary-subtraction control. Expected results have not been recorded
by a test run.

Eight cross-table OR regressions are applied in ronsql_join_where_classify,
awaiting user execution. Strict MySQL/RDRS comparisons cover 31/32 terms and
left-associated, right-associated and balanced trees; only the final term
matches rows. CLI cases require the limit diagnostic at 33 terms, including
late IS NULL/EXTRACT operands, and retain rejection of an AND branch under OR.
The query files are removed after each successful comparison or expected error.

Five CASE regression queries are applied in ronsql_cte_case, awaiting user
execution. Strict MySQL/RDRS comparisons cover left-associated, right-associated
and balanced AND/OR trees and 33-term AND/OR conditions whose final atom changes
the result. CLI cases retain rejection of mixed connectives in both directions
and check clean rejection of a literal condition. Expected results were drafted
from the fixtures and source diagnostics, not recorded by a run.

Two CASE projection-ordering comparisons are applied in ronsql_cte_case
(Tests 17 and 18). They cover a literal-left comparison and a
column-versus-column comparison with a parent column used only on the RHS,
with parent GROUP BY columns and a CTE aggregate in the main query's CASE arm.
The fixtures distinguish CTE keys, aggregate values and parent-column values,
exposing shifted output offsets.

The user's 2026-11-03 run failed at Test 19: its SUM(CASE ...) output inside
a chained CTE hits the pre-existing "CTE aggregate over complex expression
not yet supported" restriction in build_cte_virtual_tables(). Test 20 uses
the same unsupported aggregate shape. Both tests now require that rejection
through the CLI instead of incorrectly expecting successful comparisons.
The revised expectations await user execution.

Positive CTE-body CASE projection-ordering coverage remains an open follow-up.
It needs a supported, result-sensitive aggregate shape, or separately reviewed
expression-result type inference for SUM(CASE ...) CTE outputs. Tests 19 and
20 do not validate chained projection offsets.

Eleven CTE Boolean-collection regressions are applied in ronsql_cte_index_body,
awaiting user execution. Two raw conjuncts expand to 63/64/65 AND terms;
left-associated trees exercise the former 64-entry pending-stack boundary,
with right-associated and balanced controls. Seven strict MySQL/RDRS
comparisons check the bound and final residual predicate, and EXPLAIN checks
require idx_grp or the intended USE INDEX table-scan fallback. Four CLI cases
require FORCE INDEX errors for unusable/missing indexes, including the
65-term overflow shape. All generated SQL and EXPLAIN files are removed.
Expected results were drafted from fixtures and source, not recorded by a run.

Scalar-subquery NULL handling in HAVING numeric expressions is applied,
awaiting user validation. An empty scalar result or SQL NULL is substituted as
T_NULL; evaluate_having_value() now returns the same NaN marker used for NULL
aggregate results. Six strict MySQL/CLI/RDRS comparisons in ronsql_subquery cover
empty aggregate input, zero returned rows, an all-NULL inner value, NOT,
arithmetic with OR/AND, and a non-NULL control. Expected results were drafted
from fixtures, not recorded by a run.

Subqueries inside an IN subquery's left operand are now explicitly rejected
during preparation with UNSUPPORTED classification. The shared registration
and substitution walks stop at the enclosing IN, so nested nodes in that
operand would remain unprocessed. The guard uses ce_has_subquery() and covers
WHERE and HAVING. Six disconnected CLI regressions in ronsql_subquery cover
scalar subqueries, arithmetic, negation, EXISTS and nested IN; user execution
is pending. Supporting these shapes needs coordinated registration and
substitution traversal.

Preparation-time HAVING support validation is applied. Separate helpers check
predicate and numeric-value positions, sharing the aggregate-reference test
used for IS NULL and negated IN. Validation runs after reference/subquery
checks and before dictionary access or alias-register mutation. Both Boolean
branches are checked, scalar numeric placeholders remain supported, and
NOT/! polarity matches later IN substitution. Unsupported shapes now fail
with UNSUPPORTED classification independently of empty results or
short-circuiting. Aggregate-program arguments and inner SELECT bodies keep
their separate validation.

Twelve disconnected rejection cases in ronsql_having_refs cover unsupported
operators, invalid expression positions, subquery positions, negated IN and
an empty-input WHERE condition. Two strict MySQL/CLI/RDRS controls in
ronsql_subquery cover double-NOT arithmetic IN and supported numeric/IS NULL
expressions, including DIV inside an aggregate argument. Existing scalar,
nullable IN and SELECT-subquery-alias tests remain relevant. All await user
execution; expected results were drafted from fixtures and diagnostics.

Remaining work:

1. The user builds and runs the relevant suites, including Bison generation
   with the existing warning-as-error settings.
2. Address any reported failures; commit/push follows a separate user request.
   The training module remains pinned to the baseline below.

## Correctness investigations

### I1 Preserve residual predicates during subquery decorrelation

Status: completed in 09176e0a3ed.

The original append_non_correlation_where() removed SQL text by matching
correlation column names and an equals sign. With identical column names it
could also remove a residual predicate. It recognized only a literal space
after WHERE.

Implemented:

- Retain source spans for original WHERE conjuncts and inner SELECT lists.

- Build residual SQL from the AST-selected non-correlation conjuncts.

- Share source-span validation and identifier quoting for EXISTS and scalar
  decorrelation, preserving quoted names and original expression text.

Regression coverage in mysql-test/suite/ronsql/t/ronsql_subquery.test includes
same-name residual predicates, EXISTS/NOT EXISTS and scalar subqueries,
parenthesized conditions, SQL whitespace, strings containing SQL keywords,
and escaped identifiers. Corresponding normal and JIT results were updated.

The spans are for original parser nodes at the documented rewrite phase.
conjunct_sql is not a general source location valid on every synthetic node;
future rewrites must preserve that distinction.

### I2 Check silent truncation of SELECT-list subquery predicates

Status: completed in 09176e0a3ed.

analyze_select_subqueries() previously stopped collecting after 32 WHERE
conjuncts and could execute a query with later predicates omitted. Both
SELECT-list subquery collection paths now reject overflow explicitly.
The limit remains 32 conjuncts, including the correlation predicate.

Regression coverage in ronsql_subquery_agg_ext.test verifies:

- 32 conjuncts retain the final restrictive filter.

- 33 conjuncts reject instead of dropping the final filter.

- A correlation at position 33 in a nested AND tree also rejects.

- Rejections return HTTP 400 with the expected limit diagnostic.

Supporting more than 32 conjuncts is a separate change. Sharing collection
mechanics with other Boolean collectors remains C5 work.

### I3 Check COUNT expression classification in SELECT-list subqueries

Status: completed in 09176e0a3ed, including an executor correction exposed by
the new regression cases.

The parser now recognizes COUNT-star semantics only for COUNT arguments
represented as integer constants by AggregationAPICompiler. Direct column
loads retain COUNT(column) semantics. Other expression shapes are rejected;
a non-column expression is no longer assumed to mean COUNT(*).

Regression coverage in ronsql_subquery_agg.test compares COUNT(*), COUNT(0),
COUNT(-1), folded COUNT(1 + 2), and COUNT(nullable_column), including unmatched
inner rows. COUNT(column + 1), COUNT(CASE ...), and COUNT(1 / 2) are rejected.

The first test run exposed COUNT(*) returning 1 for an unmatched parent.
execute_join() now distinguishes real inner rows from synthetic NULL rows
before updating COUNT. The associated instruction-skip accounting was adjusted,
and COUNT-star processing avoids looking up its placeholder aggregate column.
A merged filtered/unfiltered COUNT query covers these paths together.

The user reported all tests passed after this correction. General aggregate
expression support remains a separate feature, not part of this fix.

### I4 Make frontend out-of-memory handling consistent

Status: added on 2026-10-29 following review of the proposed C2d factory;
selected for implementation on 2026-10-30 after cleanup commit cfcc91bc44e.
I4a-I4c were approved and applied on 2026-10-31; the source matches the approved
patches, including the requested ronsql_parser_alloc name, and git diff --check
passed. These fixes and the allocation audit were committed and pushed in
cac1d1bf650. On 2026-10-31 the user reported that all tests in the ronsql suite
passed and requested dedicated allocation-failure tests. The passing suite
does not validate allocation failure itself. Fault-injection coverage and
the subsequent user-run results are recorded under I4d. Building, testing and
parser/lexer generation remain user-owned.

The C2d placement construction itself uses the existing protected path:
ArenaMalloc::alloc_exc() returns valid storage or throws std::bad_alloc before
the constructor runs. The grammar's initptr macro uses the same allocator.
Both RonSQLPreparer constructors catch exceptions from parsing/preparation and
route them through handle_ronsql_exception(), which catches std::bad_alloc,
cleans up the transaction and throws a RESOURCE-class RonSQLPermanentError
with "Out of memory while processing the query." The REST boundary maps that
class to HTTP 503. This does not protect unchecked nonthrowing allocations:
a null-pointer fault is not a C++ exception caught by catch (...).

#### I4a Check the DECIMAL constant buffer allocation

Finding: RonSQLPreparer::encode_constant() allocated its DECIMAL output buffer
with alloc_bytes(bin_len, 4) without checking for NULL. The integer-literal
branch passes that pointer to decimal2bin(), which writes through it. This is
a potential process crash when allocation fails, in frontend preparation.
The floating-literal branch passes the same pointer to decimal_str2bin(),
which asserts bin != nullptr and then calls decimal2bin(). Both branches
therefore need the check before conversion. encode_constant() is also used
during execution, whose outer catch routes failures through the same handler.

Applied correction: check the allocation and throw std::bad_alloc before
either integer or floating-literal conversion. The buffer size and four-byte
alignment are preserved, and failure uses the existing RESOURCE error handler.

I4a checks bin == NULL immediately after alloc_bytes() and throws
std::bad_alloc(). The existing <new> include is sufficient. The
successful conversion paths and existing conversion-error classification are
unchanged. The helper's E_DEC_OOM code denotes an undersized output buffer;
it is separate from failure to allocate this buffer.

#### I4b Distinguish Bison allocation failure from parser depth exhaustion

Finding: RonSQLParser.y defined YYMALLOC using nonthrowing alloc_bytes(). Bison
returned status 2 both when stack allocation failed and when YYMAXDEPTH was
exceeded. RonSQLPreparer::throw_parse_error() treated every status 2 as a depth
limit and incorrectly commented that its allocator could not return NULL.
C7a extracted that handling without changing its behavior.

Applied correction: the Bison allocation wrapper throws std::bad_alloc on
allocation failure while preserving alignment and arena ownership. The
depth-limit diagnostic is retained and its explanatory comment is corrected.
This behavior change follows the committed C7 extraction.

I4b routes YYMALLOC through a parser-private ronsql_parser_alloc() helper in
the grammar's %code top section, using the name requested at approval. It preserves
the requested byte count and alignof(union yyalloc), checks for NULL and throws
std::bad_alloc(). YYFREE remains a no-op because the query arena owns storage.
The existing normal and ParseOnly constructor handlers classify the exception
as RESOURCE; the depth-limit message remains unchanged.

Read-only audit of the existing generated Bison 3.8.2 yacc.c parser found three
YYNOMEM sites: unavailable stack relocation, reaching YYMAXDEPTH, and a NULL
stack allocation. The configured trivial-type flags enable relocation. There
is no generated exception catch or semantic-symbol cleanup beyond tracing, so
the exception propagates to the constructor handler and arena ownership is
preserved. The generated grammar predates the latest cleanup; this establishes
the existing skeleton paths, not validation of regenerated output. No parser
generation, build or tests were run; those remain user-owned.

#### I4c Align lexer allocation failures and audit remaining raw allocations

Finding: RonSQLLexer.l's rsqlp_alloc() checked alloc_bytes() but threw
RonSQLRetryableError on failure, unlike the AST's RESOURCE-class failure.

Applied correction: throw std::bad_alloc here as well so the common handler
reports a nonretryable RESOURCE failure consistently. Source review covered
normal preparation and ParseOnly callers, including cleanup and retry behavior.

I4c adds an explicit <new> include to the lexer's C++ section and replaces
the retryable exception with std::bad_alloc(). Allocation
size, alignment, successful returns and arena-owned cleanup remain unchanged.
Both constructor modes call configure() inside their try blocks. The generated
lex_init_extra() passes a temporary scanner with the Context installed before
allocating scanner state; later buffer and stack allocations use that scanner.
The allocation hook throws before Flex's NULL-result fatal-error paths run.

The REST execution wrapper otherwise retries RonSQLRetryableError up to ten
times, but immediately returns HTTP 503 for a RESOURCE-class permanent error.
The authorization ParseOnly path previously mapped the retryable lexer exception
through its generic std::exception catch to HTTP 500; I4c instead reaches its
classified-error catch and returns HTTP 503. Request-owned arena destruction
reclaims scanner allocations after constructor failure, when the preparer's
destructor does not run.

Audit remaining alloc_bytes(), alloc(), calloc() and realloc() uses in the
lexer, parser, AST helpers and preparation code for checks before use. Inspect
the lexer's unconditional abort() in rsqlp_realloc() separately: its comment
says reallocation is unnecessary, but reachability must be established before
classifying it as an OOM crash. Keep data-node implementation outside scope.

Source audit on 2026-10-31 found that the remaining explicit nonthrowing arena
allocations in RonSQL source are guarded: the two CTE execution arrays check
for NULL and throw std::bad_alloc(), as does PassthroughValue::copy(). The
DECIMAL and Bison sites are now covered by I4a-I4b. DynamicArray and LexString
use alloc_exc()/realloc_exc(); AST actions and compiler helpers use alloc_exc().

The existing generated scanner's realloc paths are not reached by RonSQL's
current one-buffer setup: configure() initializes a fresh scanner and calls
scan_buffer() once; the buffer has yy_fill_buffer = 0, initial lexing sees an
existing buffer, and noyywrap terminates at EOF. No RonSQL caller switches or
pushes another buffer. Input-buffer growth is bypassed and buffer-stack growth
is never requested after initial allocation. The aborting realloc hook remains
unchanged; additional buffer operations would require revisiting this invariant.
This is a source audit of existing generated code, not a regenerated-build or
fault-injection result. CLI input/option allocation before construction and
other request-boundary allocations remain outside these parser corrections.

Proposed validation, to be reviewed separately and run by the user:

- Use deterministic allocation-failure injection, not machine-wide exhaustion.
- Fail AST allocation in normal and ParseOnly parsing; expect the RESOURCE
  diagnostic and safe cleanup.
- Fail the DECIMAL buffer allocation for integer and floating literals; expect
  the same error without entering conversion with a NULL destination.
- Fail Bison stack growth allocation; expect an OOM error. Separately exceed
  the parser depth limit with successful allocations; retain the depth error.
- Fail lexer allocation; verify RESOURCE classification and no automatic
  retry at the request boundary, including the HTTP 503 mapping.
- Retain successful-query coverage for each affected path and verify arena
  and transaction cleanup after an injected failure.

#### I4d Deterministic allocation-failure regression tests

Selected by the user on 2026-10-31. All three patches are approved and applied.
The driver covers both constructors and ronsql_op, enumerates allocation
failures, checks RESOURCE/503 and no retries, and exercises recovery and
request isolation. It also preserves the depth-limit SYNTAX/400 diagnostic.
DECIMAL cases cover signed/unsigned integer and floating literals, preparation
during IN-list deduplication, and outer comparisons with substituted integer
and floating scalar-subquery results. Transaction counters are checked while
the preparer is still alive, so destructor cleanup cannot hide a missing
cleanup_trans() in the exception handler.

CTest's ronsql_parser_oom_test runs the cluster-free parser and DECIMAL
ParseOnly cases. MTR's ronsql.ronsql_parser_oom creates its own small NDB fixture
and runs the entire driver. It rejects missing/skipped test cases and signal
deaths; detailed gtest output and XML are kept under MYSQLTEST_VARDIR/log
as ronsql_oom.output.log and ronsql_oom.xml. Successful follow-up queries verify
reuse of the same Ndb object after injected failures.

On 2026-10-31 the user's run completed all 13 C++ tests with zero failures or
skips, confirmed by the retained gtest output and XML. MTR's subsequent
state-preservation check failed because those two diagnostic files were left
in MYSQL_TMP_DIR. The test now writes them under MYSQLTEST_VARDIR/log, which
MTR excludes from its directory-content comparison. The user subsequently
reported that the MTR test passed. On 2026-11-02, after the C3 list-stack
regressions were added, the user reported that all tests passed.
The driver, MTR integration and list-stack regressions were committed and
pushed in 6b0b7da91c5. The assistant inspected the existing logs but has not
built, regenerated the parser/lexer, run tests or recorded MTR results.

Implementation sequence:

1. Add a request-local C++ test hook for the three corrected allocation sites.
   Return NULL at the chosen allocation so the real caller checks and exception
   translation execute. Count matching allocations after the one-shot failure
   to detect retries. Do not add a SQL or HTTP input for enabling injection.
2. Add a C++ regression driver using RonSQLPreparer and the real ronsql_op
   wrapper. Cover each lexer initialization allocation and successive Bison
   stack growth allocations in normal and ParseOnly modes. Check the RESOURCE
   class, OOM diagnostic, HTTP 503 mapping, no retries, and successful reuse.
3. Add DECIMAL cases and wire the driver into the ronsql MTR suite with a
   minimal NDB table. Cover integer and floating literals, substituted scalar
   values, and failures after a transaction opens; check transaction start/close
   counts and a successful follow-up query.

ParseOnly performs no DECIMAL encoding: its DECIMAL queries must succeed
without reaching that failpoint. Keep successful deep parsing and depth-limit
rejection separate from injected Bison OOM. HTTP assertions in the driver use
the actual operation wrapper and the mapper used by ParseOnly authorization;
they do not constitute a network-level controller test.

Release request arenas after partial construction and exercise subsequent
requests. Transaction counters verify transaction cleanup directly; proving
absence of arena leaks additionally requires a user-run sanitizer/leak checker.
Fault injection into other AST allocations remains a separate extension of
the broader validation list above.

## Cleanup investigations and proposed boundaries

### C1 Initialize AST nodes through typed factories

Status: completed for the agreed scope in 45a7568faed.

Context::make_select_statement() placement-constructs SelectStatement() in
the existing arena. All four fresh statement sites use it: CTE and subquery
grammar actions, plus EXISTS and scalar decorrelation. Existing explicit
grammar assignments and intentional statement copy construction are retained.

This initializes fields previously omitted by raw arena allocation, including
cte_list and sentinel_agg_slot, and preserves nonzero defaults such as
limit = -1 and sentinel_agg_slot = -1.

synthesize_from_for_scalar_ctes() now value-initializes its synthetic TableRef
and JoinClause, including the embedded table's hint_kind and hint_indexes.
The allocation helper does not record authorization references or validate ON
conditions.

General table/join construction helpers are now applied locally in C2d/C2e.
Existing ronsql_cte_scalar Test 8 exercises the synthesized-FROM shape.

### C2 Share inner SELECT construction and grammar components

Status: implementation complete in cfcc91bc44e.
C2a-C2e are approved and applied. CTE and subquery
wrappers now share inner_select_body while retaining their scope transitions
and compiler ownership. Source spans and required inner FROM are preserved.
ORDER BY now shares sort_direction_opt, preserving default ascending order and
explicit ASC/DESC. A syntax-only column_ref now serves plain and AVRO outputs,
arithmetic/condition expressions, GROUP BY and ORDER BY. Enclosing actions still
perform column registration at their original points; ON remains separate.
The four table_ref_base actions now share Context::make_table_ref(), which
value-initializes TableRef in the arena and sets its database, name and alias.
Index hints and authorization recording remain in the enclosing table_ref
action. Allocation retains the existing alloc_exc() exception handling.
The four join_clause actions now share Context::make_join_clause(), which
value-initializes JoinClause and copies the complete TableRef, including hints.
Explicit joins retain ON validation before construction; comma joins retain
NULL conditions. Each new node starts with next = NULL.
git diff --check passed and the source matches the approved C2a-C2e patches.
Bison generation, building and tests remain user-owned and have not been run
by the assistant. C1 is complete.

Keep enter_subquery()/leave_subquery() in their wrappers in further refactoring,
preserving CTE visibility, compiler ownership and SQL source spans.

Preserved boundaries for validation and future changes:

- Preserve plain-column and AVRO output naming.

- Keep ON references separate: they record access without entering the same
  column registry as ordinary expression references.

- Keep FROM mandatory inside CTEs/subqueries and optional only in the existing
  top-level scalar-CTE form. Do not widen accepted syntax during cleanup.

### C3 Make list construction consistent

Status: original three list families completed in 45a7568faed. GROUP/ORDER
and output/CTE/join follow-ups were committed and pushed in 6b0b7da91c5 after
the user reported all tests passed on 2026-11-02.

RonSQLListBuilder stores head/tail pointers and provides constant-time append.
Separate approved patches converted index-name lists, ON-condition lists and
arithmetic GREATEST/LEAST argument lists. All consumers unwrap the list head.

Arena ownership, source order, validation and authorization recording remain
unchanged. The builder has no constructor or default member initializers;
std::is_trivial checks protect Bison's existing semantic-value copying.

Follow-up review on 2026-10-31:

- GROUP BY and ORDER BY now use left-recursive RonSQLListBuilder lists. The
  previous rules were linear-time but retained parser-stack entries for every
  column. Node construction and column registration still run in source order;
  clause consumers receive the same linked-list heads. Empty clauses, sort
  defaults and semantic column limits are unchanged.

- User validation: regenerate the parser with the existing warning-as-error
  settings; run ronsql_orderby, ronsql_parser_cte and ronsql_agg_drain.

- On 2026-11-02, added two ParseOnly regressions to ronsql_oom_test: GROUP BY
  and ORDER BY lists of 12,000 columns must parse with zero parser-stack
  allocation attempts. The first such allocation is armed to fail. CTest's
  ronsql_parser_list_stack_test runs these cases without a cluster; MTR's
  ronsql.ronsql_parser_oom now requires all 15 tests and the new suite.
  Existing nested-NOT tests retain coverage of actual stack growth and
  depth-limit rejection.

- outputlist, cte_list and join_list now use RonSQLListBuilder instead of manual
  head/tail append actions. Their existing left recursion, node-construction
  order and empty-list behavior are preserved. Every node constructor sets
  next to NULL; triviality assertions cover all three builder types.
  User validation: ronsql_parser_cte, ronsql_join and ronsql_chained_left_join.

- Keep the reversed T_COMMA representation for IN unchanged. Conditional
  GREATEST/LEAST also consumes it; changing its order changes the lowered
  expression tree. Arithmetic GREATEST/LEAST uses a separate ArithExprList.

### C4 Centralize conditional-expression child access

Status: original helper and three walkers completed in 45a7568faed.
Validator and subquery-discovery follow-ups were committed and pushed in
6b0b7da91c5 after the user reported all tests passed on 2026-11-02. Current
projection/HAVING follow-ups are recorded above and await user validation.

A local condition_children() returns at most two children in the current
SELECT scope. Read-only and mutable views share one dispatch switch and
preserve input constness. Explicit token cases select the active union members.
Unknown operators raise an INTERNAL error rather than guessing a layout.

The following walkers now share the helper:

- mark_scope_column_refs_ce().

- ce_has_subquery(), retaining immediate detection of all four subquery forms.

- classify_ce_table_resolved(), retaining identifier resolution and table-result
  combination rules and accepting a const expression pointer.

- check_no_nested_exists(), retaining EXISTS rejection and subquery stops.

- check_cte_body_col_vs_col(), retaining comparison checks and subquery stops.

- analyze_subqueries_ce(), retaining registration order and subquery stops.

Layout and scope contracts:

- Binary operators expose args.left/right; unary minus exposes its right child.

- NOT and ! expose only args.left. Synthetic NOT nodes can leave the right
  pointer uninitialized, so the helper never reads it.

- IS, INTERVAL and EXTRACT expose their dedicated arg field.

- GREATEST/LEAST expose their T_COMMA list; list nodes expose item and remainder.

- IN subqueries expose only the outer expression; correlated scalar nodes
  expose the outer comparison and key expressions.

- EXISTS/scalar bodies are separate SelectStatements. Literals, identifiers
  and aggregate-register nodes have no condition children.

- A childless node is not necessarily a constant.

Follow-up review on 2026-10-31:

- check_no_nested_exists() and check_cte_body_col_vs_col() now accept const
  expression pointers and use shared child access. Their explicit subquery
  stops, the CTE checker's stop at comparisons and existing call sites remain.
  Unary and aggregate-register nodes no longer fall through to binary-field
  traversal; unknown operators use the helper's INTERNAL error. No reachable
  crash was reproduced before this change. User validation: ronsql_subquery,
  ronsql_cte_colvscol and ronsql_cte.ronsql_cte_dd_d4_colvscol.

- analyze_subqueries_ce() now uses a mutable child view to retain node pointers
  for later rewriting. The helper deduces const or mutable pointers from its
  input; existing read-only callers keep const pointers without const_cast.
  All four subquery cases still return before child traversal, preserving
  registration order and the stop at IN's outer operand. User validation:
  ronsql_subquery, ronsql_subquery_agg, ronsql_subquery_agg_ext and
  ronsql_cte_subquery.

- Keep result substitution specialized: NOT changes polarity, while several
  operators reset it. simplify_ce() replaces nodes and applies depth limits;
  same_inner_filter() has operator-specific equality and conservative fallback
  rules. Aggregate-expression traversal uses a separate expression type.

- The current follow-up batch adds shared main/CTE projection discovery and
  HAVING reference traversal, including IN's outer operand. HAVING identifiers
  still change from column indices to aggregate registers in compile(); the
  validation and resolution phases retain their order.

### C5 Share checked Boolean collection and reconstruction

Status: agreed C5a-C5d implementation complete in cfcc91bc44e.
All eight throwing AND callers and the single OR caller now
use the shared collector through their existing wrappers. Their 64-term limits,
messages, source order and original leaf pointers are preserved. git diff
--check passed and the source matches the approved patches; building and tests
remain user-owned and have not been run by the assistant. The current
collector follow-ups and their pending regression coverage are recorded above.

The original review found these different contracts:

- AND/OR collectors at RonSQLPreparer.cpp:709 and :737: 64 terms, throw.

- Residual collector at :11348: 64 terms, report overflow for optimization
  fallback to the original whole-tree filter.

- SELECT-subquery collectors at :8205 and :8342: 32 terms; I2 has now made
  overflow rejection consistent.

- Main scan planning at :4671: dynamic collection.

- CTE index planning at :5826: limits pending stack entries, not term count.

Implemented collect_boolean_terms(root, connective, output, capacity), returning
complete/overflow and count. It flattens only the requested AND or OR connective,
preserves left-to-right order and original leaf pointers, and returns count zero
on overflow; any partial output must be discarded. Caller-specific rejection
wrappers retain their limits and error text.

C5b replaced the two checked 32-term SELECT-subquery collectors with the shared
helper. Each capacity comes from its existing 32-element array. Both callers
reject overflow before consuming output, retaining the exact I2 diagnostic and
including correlation in the first 32-term limit.

C5c removed the duplicate nonthrowing collector. build_root_residual() now uses
the shared collector with its existing 64-element array and returns false on
overflow before consuming any partial output; residual_out remains NULL.
Both callers retain the complete WHERE; the CTE caller's existing single-group
lookup alternative also keeps the complete WHERE as its residual. Downstream
filter limits still apply.

C5d added append_condition() and migrated four AND reconstruction loops for
child residuals, EXISTS decorrelation, scalar-subquery decorrelation and root
residuals. A NULL pointer means no tree or term, not a SQL NULL node. Left
association, original leaf pointers and allocation order are preserved;
synthetic connective nodes retain the existing source-span contract.

The follow-up audit led to checked OR validation, safe projection discovery,
shared cross-table/CASE collectors and dynamic CTE scan-planning collection.
These changes are applied locally; details and remaining tests are recorded
in the follow-up review section above.

### C6 Share aggregate construction and registration

Status: agreed C6a-C6b implementation complete in cfcc91bc44e.
The source matches the approved patches and git diff --check
passed. Building and tests remain user-owned and have not been run by the
assistant. C7 output-phase extraction can now use the aggregate helpers.

C6a extracted make_aggregation_compiler() from the four constructions originally
at RonSQLPreparer.cpp:2778, :2935, :20338 and :20356. It returns a fresh compiler
in the same arena, retaining the column callback and streams. Callers retain
scope assignment, replacement and lazy caching.
The compiler stores its callback by value, and the factory captures the same
preparer pointer as the existing constructors. Each factory call creates a fresh
compiler; it does not choose or update a scope's cached compiler.

C6b added register_simple_aggregate() for the COUNT/MIN/MAX/SUM dispatch
originally at :889, :1229 and :3224. The main and CTE callers retain their
output indices, while unselected flattened-CTE expressions still register usage
even though their returned index is discarded. Registration order, compiler
ownership, existing guards and AVG cases remain unchanged in the callers.

Additional follow-up candidate: EXPLAIN parse-tree rendering also repeats the
COUNT/MIN/MAX/SUM dispatch. Review a separate migration after the helper exists;
it is outside the three preparation paths in C6b.

AVG needs an explicit representation:

- Main query: Sum() followed by Count().

- Materialized CTE: one Avg() index stored in both output fields.

- Flattened CTE expressions: the main-query representation.

Separate main and CTE output-registration wrappers remain useful: main
SUBQUERY_AGG is deferred, whereas the CTE path rejects it. Preserve the existing
NULL-compiler guard and aggregate-limit accounting.

### C7 Extract preparation phases without reordering them

Status: C7a-C7d committed and pushed in cfcc91bc44e. Both source files matched
the approved patches and git diff --check passed. Building and tests remain
user-owned and have not been run by the assistant.

C7a extracted throw_parse_error(parse_result), moving the error rendering
originally at RonSQLPreparer.cpp:1267 unchanged. All four diagnostic blocks in
the successful parse path keep their local err bindings; the function-wide
binding moved into the new error helper. Classifications, messages, parser
depth handling, empty input, UTF-8 caret alignment and source locations are
preserved. The private declaration is marked [[noreturn]].
The Bison OOM diagnostic correction is tracked separately in I4b; do not fold
that behavior change into this mechanical extraction.

C7b extracted register_main_outputs(), moving the main-output loop and compiler
checks intact and returning the combined aggregate/HAVING/subquery flag.
Registration remains before promote_left_joins(), and m_is_aggregate_query is
assigned afterward. Promotion changes only join types, so the captured flags
remain valid. AVG registration order and deferred subquery handling are retained.

C7c extracted register_cte_outputs(), moving the existing loop intact apart
from indentation. Its call remains after projection-only preparation.
The NULL-compiler skip, CTE/output ordering, returned indices, single-slot CTE
AVG representation and subquery rejection remain unchanged.

C7d extracted prepare_projection_only_query(). It includes a root rewrite,
so naming the entire block validate_* would hide its mutation. Its call remains
under the existing nonaggregate guard, after rewrite_lastn_cte_bodies() and
before register_cte_outputs().
The complete 247-line body moved with only indentation changes. It uses
members, class constants and its own locals; all eleven
break statements remain in their original loops. The first executable statement
remains maybe_rewrite_partial_key_cte_root(), and the four local error-stream
bindings and all diagnostics move intact. ParseOnly protections are preserved.

Preserve successful-parse order:

1. Record original references while Bison runs, then disable recording.
2. Capture WITH names and check qualified/subquery CTE references.
3. Flatten single-group CTEs and route last-N aggregation.
4. Register main aggregates and check compiler state.
5. Promote LEFT joins and determine aggregate-query status.
6. Detect single-row CTEs, collapse collect CTEs, rewrite other last-N bodies.
7. Rewrite/validate projection-only shapes.
8. Register CTE aggregates.

Keep later alias resolution, CTE/subquery analysis, schema loading and compilation
in their existing constructor phases.

ParseOnly uses the same parse() before authorization and must remain schema
independent. rewrite_lastn_cte_bodies() explicitly skips ParseOnly/disconnected
operation. Moving its schema-dependent work is a separate design change.

### C8 Share name lookup while preserving scope policies

Status: C8a-C8c committed and pushed in cfcc91bc44e. Both source files matched
the approved patches and git diff --check passed. Builds and tests remain
user-owned and have not been run by the assistant. Public Context interfaces
remain stable, preserving the C2 grammar changes.

Unify column interning behind the existing Context methods
(RonSQLPreparer.cpp:20161). Record every syntactic occurrence for authorization,
including references that reuse a registry entry. Preserve insertion order and
the current qualifier/name key and inner-reference flags.

C8a introduced a private intern_column() helper behind both existing public
registration methods. Each wrapper still records every reference before lookup.
Bare lookup retains its NULL-qualifier-pointer check; qualified lookup retains
LexCString equality, including its empty-string semantics. First-match behavior,
first-seen inner flags and insertion order (column, qualifier, inner flag) remain
unchanged. This patch does not change scope policy or allocation failure handling.

Share ORDER BY output-name matching (:1467 and :7159), but preserve:

- First matching output for ORDER BY.

- Main-query alias bookkeeping and its shared-column exception.

- No global alias marking for CTE-body ORDER BY.

- HAVING's different, last-matching-output policy.

C8b introduced find_orderby_output(), returning the first matching output and
its zero-based position. Both callers keep their TABLE_COLUMN and unqualified
name guards. The helper retains the existing predicate and evaluation order;
main-query alias marking and its shared-column exception remain in that caller,
and the CTE caller still does not mark global aliases. HAVING's separate
find_output_alias() remains unchanged.

Extract relation-local stored/CTE column lookup from :7866 and :7920.
Leave visibility, ambiguity and error choice in callers. Do not initially merge
single-table resolution with the scoped resolver: their alias precedence differs.

C8c introduced lookup_relation_column(), returning a default-initialized resolved
descriptor for one stored table or CTE. CTE matching retains the exact predicate
and first matching output, including empty-name behavior. Qualified resolution
still uses the first matching relation alias; unqualified resolution still scans
all visible operations and counts one match per relation before diagnosing
ambiguity. Missing-name diagnostics, scope filtering and alias precedence remain
in the caller; the single-table resolver remains unchanged.

Longer term: investigate per-reference scope identities and a parse-scope stack.
The current single m_inner_agg pointer is a barrier to general nesting.
Retain QueryScope and the pre-rewrite authorization inventory as foundations.

### C9 Deduplicate lexer lookup and improve current contracts

Status: C9a committed and pushed in cfcc91bc44e. The lexer source matched the
approved patch and git diff --check passed. Generated files, builds and tests
remain user-owned and have not been run by the assistant.

Extract the normalized MySQL-keyword binary search shared by RonSQLLexer.l:127
and :404. Keep normalization at callers and leave the implemented-keyword table
lookup unchanged. Preserve longest match, case handling, quoted identifiers,
digit-containing keywords, length limits and error locations.

C9a introduced is_normalized_mysql_keyword(), requiring an uppercase,
NUL-terminated string. It reuses the existing signed binary search from
is_mysql_keyword(); the short-word rule calls it after the implemented-keyword
search. The short-word search previously used an unsigned midpoint with signed
bounds; the shared helper uses signed indices throughout, allowing high = -1
directly. Both normalization paths and their char(0) terminators stay unchanged,
as do Flex patterns, rule order, identifier checks and error reporting. No
generated files, builds or tests are part of the assistant's changes.

Replace historical phase commentary with concise current invariants when
touching those sections. Consider retaining source locations in semantic nodes
after factory construction is centralized.

## Proposed implementation order and user validation

Completed sequence:

1. Investigate and fix I1-I3 in separate approved patches.
2. Add regression cases in parallel, then review and apply them separately.
3. Fix the unmatched-row COUNT(*) issue exposed by those tests.
4. Commit and push 09176e0a3ed after the user reported all tests passed.
5. Investigate C1/C3/C4 in parallel; review and apply one patch at a time.
6. Commit and push 45a7568faed after the user reported all tests passed.
7. Investigate C2 and C5-C9 in parallel and apply the selected cleanup patches
   one at a time after approval; commit and push them and this plan in
   cfcc91bc44e at the user's request. Validation remains user-owned.
8. Review and apply I4a-I4c individually, record the allocation audit, and
   commit/push cac1d1bf650 at the user's request.
9. Add fault-injection tests and deferred C3/C4 cleanup; commit/push
   6b0b7da91c5 after the user reported all tests passed.
10. Finish the 30-slide training module; commit/push 387c866 in hops_slideware
    after the user reviewed it.
11. Investigate the selected C4/C5 and unary-minus candidates. Apply the
    individually approved follow-up fixes and fourteen HAVING IN tests locally.
    Remaining work and validation are listed in the follow-up review above.

The earlier C2 and C5-C9 work used parallel investigations and individual patch
approval within these boundaries:

- C2: factor inner SELECT construction, then column/sort and table/join actions.

- C5: share bounded collectors while retaining each limit and overflow policy.

- C6: extract the compiler factory, then simple registration and explicit AVG.

- C7: extract parse-error reporting before broader preparation orchestration.

- C8: share name lookup only after scope and alias invariants are explicit.

- C9: share keyword lookup and improve current contracts.

- I4: review the DECIMAL allocation check first, then Bison OOM classification,
  then lexer consistency and the remaining raw-allocation audit. Keep these
  corrections separate from cleanup patches and coordinate I4b with C7.

Deferred C3/C4 list/traversal cleanup is included in 6b0b7da91c5; the current
projection/HAVING/collector work is a separate, uncommitted follow-up batch.

Validation for future patches should cover the relevant existing suites and
contracts below; this is proposed coverage, not a claim that new cases exist:

- Parsing, keywords, ordering, CTEs, CASE, subqueries and NULL handling.

- 31/32/33 and 63/64/65 terms; balanced, left/right-associated trees.

- Correlation and restrictive predicates at different list positions.

- Original-filter fallback, leaf pointer identity and scope isolation.

- Main/CTE/flattened AVG register layouts, NULLs and empty inputs.

- Output names, EXPLAIN, diagnostics and authorization inventories.

- Bison generation with the repository's warning-as-error configuration.

Dedicated ParseOnly inventory coverage was not established during the review.
Check that coverage before relying on execution tests for authorization safety.
The user continues to own all building and testing.

## Feature candidates after cleanup

Status: pending evaluation and separate approval. No feature candidate below
was implemented by the I1-I3 or C1/C3/C4 patches.

Small frontend candidates: explicit INNER JOIN, optional AS, unary plus,
ordinary SQL comments and broader floating-literal spellings.
ORDER BY ordinals can reuse OUTPUT_REF with range validation.
Ordinary aggregate aliases in HAVING need explicit register and AVG handling.

NOT LIKE and BETWEEN need lowering, precedence and NULL-semantics review.
Do not classify general NOT IN subqueries as a syntax-only addition.

Explicit unary-minus precedence is now applied locally to both expression
grammars using T_UNARY_MINUS. The source-level precedence discrepancy led to
this correction. Eight regression cases are applied; executable validation
remains pending. Unary plus is still a separate feature candidate.

General SELECT expressions, richer CASE, nested subqueries, DISTINCT, UNION,
window functions and OFFSET require broader semantic/execution design.
Long-term direction: syntax AST, scoped resolution, validation, explicit
lowering, compilation. Avoid undertaking that redesign as one cleanup patch.

## Training module deliverable

Target: /Users/mikael/github/hops_slideware/2026-10/ronsql_parser_training/index.html

Status: completed. The 30-slide HTML module uses the existing slideshow engine
and RonDB theme. The user reviewed it and requested commit/push. It is saved
and committed as 387c86655e32c81b19f6dc599dbadf8ff696e8d6 on rondb26-edits in
https://github.com/mronstro/hops-slideware; that commit was pushed.

Examples and source references are pinned to parser commit
6b0b7da91c5dacf5d6401b2ec65135fa7d254ecb. Grammar acceptance is distinguished
from preparation and execution support. The current parser follow-up changes
postdate that baseline. The assistant did not render or deploy the module.
