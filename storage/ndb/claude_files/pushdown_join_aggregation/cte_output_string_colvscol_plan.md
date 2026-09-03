# String Column-vs-Column Comparisons on CTE Outputs (main WHERE)

**Status: SHIPPED (2026-09-03) — S1–S4 recorded green ×5 topology
suites + full regression pass; S5 rondb-docs updated (commit `41d3640`
on PR #104).  Landed as `791bd451f02` on RONDB-1107-step3, cherry-picked
to RONDB-1107-step4 (based on 26.10-main) as `06f1a9155f0`.**

Landing notes beyond the S-item logs below: the first record surfaced a
PRE-EXISTING latent bug in the I.21 scalar dummy-key arm, fixed here —
sc-20 is the first scalar CTE whose virt PK (= first output) is a
VARCHAR, and the dummy zero-key passed `getSizeInBytes()` (value +
length prefix) to the generic const operand, whose VAR arms add the
prefix themselves → `QRY_CHAR_OPERAND_TRUNCATED` at bindOperand,
surfaced as "Failed to create child operation" with the builder error
hidden (diagnosability follow-up: that wrapper prints the Ndb object's
error, not the builder's).  Fix: arrayType-aware dummy length (FIXED ⇒
`getSizeInBytes()`, VAR ⇒ empty value).  A CHAR scalar would have
passed by luck (FIXED wants exactly `getSizeInBytes()`).  The feature
also flipped one old rejection pin: `ronsql.ronsql_cte_colvscol`
Test 10 (the I.3-era string MIN-vs-MAX rejection) is now Tests 10a/10b
value compares (`<=` keeps all groups, strict `<` pins the empty
result).  A tree-wide sweep found no other flipping `--error` asserts;
the changed typed-reg message text was pinned only in the
scalar-CTE family baselines (re-recorded).

S3/S4 outcome notes:
- `resolve_side` now surfaces the resolved descriptor (virt or dict
  column); the string arm sits before the typed-register gate — accepts
  same-type same-charset Char/Varchar/Longvarchar pairs, derives sizes
  via `getLength()` (the const-vs-col inline convention; kernel clamps
  to actual payload), keeps the explicit `branch_linked_isnull` guards
  (raw NdbInterpretedCode defaults to NULL_CMP_EQUAL, not SQL
  semantics), and picks the reject-branch method per the inverted
  naming.  Three new rejection messages: string-vs-non-string,
  CHAR-vs-VARCHAR mix, charset mismatch.  The main typed-reg message
  now reads "integer, FLOAT, DOUBLE, DATE and matching string" —
  **sc-P4's pinned text moves** (family re-record).
- MTR: `body_filter.inc` Group 9 (filter-40..46: CHAR(1) and
  VARCHAR(12) MIN/MAX pairs on shared tables, cvc cross-column CHAR(8)
  pair with per-group <,= variety, GB-string-passthrough vs aggregate,
  OR/DNF, pass-through CTE_SCAN-root filter, utf8mb4 pair with ASCII
  values) + probes filter-P8 (string vs integer), P9 (CHAR vs VARCHAR
  via part p_brand/p_name), P10 (latin1 vs utf8mb4 via new cvc w1/w2
  explicit-charset columns); `body_passthrough_scalar_cte.inc` sc-20
  (VARCHAR watermark), sc-21 (empty scalar ⇒ NULL rejects all), sc-P5
  (string vs integer output probe).
- Deliberate scope trims vs the original S4 sketch: the rpr string
  twin is dropped (filter-42 pins the same CTE-own-output shape class;
  keeps the re-record surface at two families), and deep multibyte
  value probing (non-ASCII utf8mb4 payloads vs the getLength() size
  convention) is deferred — filter-46 pins the charset id path with
  ASCII values, and the open question also applies to the shipped
  const-vs-col path (audit item).

S1/S2 outcome notes:
- Opcode **`BRANCH_LINKED_OP_LINKED` = 46** landed exactly per design
  (4 words; per-side sizes clamped kernel-side to the entries'
  AttributeHeader byte sizes so malformed programs cannot overread the
  linked buffer).  Registered in the CTE jump-table dispatch (slot 46)
  and the main interpreter switch; the embedded-aggregation dispatch
  keeps nullptr (CTE-filter-only op, comment marks it).  There is no
  separate load-time opcode whitelist for CTE filter programs — the
  jump-table nullptr slots are the enforcement.
- `lookupLinkedEntry` factored as a shared position-walk helper (the
  handleReadLinkedColumnToReg loop); missing entry ⇒ NULL semantics.
- API `branch_linked_linked_{eq,ne,lt,le,gt,ge}` +
  `branch_linked_linked_val` mirror `branch_linked_inline_*` (same
  type rejections, same inverted-inequality naming).
- Block test: **testCteNdbApiFilter Test 24** — CTE `GROUP BY grp,
  MIN(tag), MAX(tag)` over a reseeded `filter_src` with per-group tag
  variety; two sub-cases compare positions 1 vs 2 (accept `mn < mx` ⇒
  COUNT=5; accept `mn = mx` ⇒ COUNT=2) through the aggregator-delivery
  path.  NULL-entry coverage deferred to the S4 MTR family (the
  handler's NULL path mirrors the proven INLINE handler).

Closes the last string gap in the CTE-output comparison matrix: the
main-query WHERE shapes `WHERE cte.mn < cte.mx` (two outputs of the same
CTE) and `WHERE t.name > s.min_name` (tree-ancestor real string column
vs a string CTE output — the I.26 watermark shape) for CHAR / VARCHAR
operands.  Documented today as "string outputs in column-vs-column
comparisons are rejected" (rondb-docs) and rejected by the sc-P4-pinned
message ("only integer, FLOAT, DOUBLE and DATE operands are
supported...").

Distinct from the just-shipped `cte_body_colvscol_plan.md` (real-table
columns inside a CTE *body*, main interpreter, `BRANCH_ATTR_OP_ATTR`) —
this feature lives in the **CTE jump-table filter** on CTE_LOOKUP /
CTE_SCAN ops, where values are linked-buffer entries, not table rows.

## Current state (audit, September 2026)

- **The col-vs-col arm exists and is typed-register-based**
  (RonSQLPreparer.cpp ~:10466): `resolve_side` maps each identifier to a
  buffer position (CTE output → `linked_base + cte_result_idx`; tree
  ancestor → I.26 linked-projection index) + NDB type; two
  `branch_linked_isnull` NULL guards; two `READ_LINKED_COLUMN_TO_REG`
  typed loads; reg-vs-reg branch.  Registers are 64-bit — strings cannot
  ride this path, and `is_typed_reg_loadable` rejects them.
- **Const-vs-string-CTE-output already works** (`WHERE min_v < 'beta'`,
  I.25): `branch_linked_inline_*` → `BRANCH_MEM_OP_ARG_INLINE_TYPE`
  (opcode 40) compares a linked-buffer entry against an inline constant
  with inline metadata — Word 2 packs `(columnSizeBytes << 16) |
  csNumber`, the kernel resolves `all_charsets[csNumber]` and runs the
  charset-aware `NdbSqlUtil` compare.  RonSQL derives the metadata from
  the virt-table column: `getLength()` + `getCharsetNumber()` for
  Char/Varchar/Longvarchar (:10817-10829).
- **String values are already in the buffer**: F.3/F.4 string MIN/MAX
  outputs carry the source column's declared length + charset on the
  wire; GROUP BY string passthrough columns and I.26 linked projections
  of real string columns are raw column values (type-agnostic
  machinery).
- **What is missing is exactly one kernel primitive**: a compare of TWO
  runtime buffer entries (all existing branches compare entry-vs-inline
  or reg-vs-reg).  Opcode slot **46** is free (between
  LOAD_DOUBLE_CONST=45 and READ_PARTIAL_ATTR_TO_MEM=47).

## Design

New jump-table opcode **`BRANCH_LINKED_OP_LINKED` (46)** — compare two
linked-buffer entries with inline type metadata:

```
Word 0: opcode | cond | null_semantics | branch_offset   (BranchMem shape)
Word 1: (posL << 16) | posR
Word 2: (typeId << 16) | csNumber
Word 3: (columnSizeL << 16) | columnSizeR
```

Handler (`handleBranchLinkedOpLinked`, DbtupExecQuery.cpp): fetch both
entries from the filter buffer by position (the READ_LINKED_TO_MEM /
`handleReadLinkedColumnToReg` lookup), honor AttrHeader NULL flags per
the null-semantics field (belt-and-braces — RonSQL emits explicit
`branch_linked_isnull` guards first, uniform with the numeric arm), then
run the same charset-aware compare core as
`handleBranchMemOpArgInlineType` (factor it into a shared helper taking
typeId/cs + two (ptr, len) operands).  Both sides share one typeId +
csNumber; per-side sizes allow e.g. MIN over CHAR(8) vs MAX over
CHAR(12).  Register in: the CTE jump-table dispatch, the plain-switch
arm, `getInstructionPreProcessingInfo` (fixed 4 words,
LABEL_ADDRESS_REPLACEMENT), and any CTE-filter program validator.

API: `NdbInterpretedCode::branch_linked_linked_{eq,ne,lt,le,gt,ge}(posL,
posR, typeId, csNumber, sizeL, sizeR, label)` mirroring the
`branch_linked_inline_*` family (same inverted-inequality naming
convention).

RonSQL (the col-vs-col arm): extend `resolve_side` to also surface the
resolved column descriptor (virt column or dictionary column).  When
both types ∈ {Char, Varchar, Longvarchar}: require identical typeId AND
identical csNumber (v1), derive per-side sizes exactly as the
const-vs-col inline path does, emit the two NULL guards + the new
branch.  Otherwise fall through to the typed-register arm / updated
rejection.  New rejection message drops "string" from the unsupported
list and adds the two string-specific rejections (mixed Char/Varchar;
charset mismatch), sc-P4 style.

**v1 scope**: same-CTE output pairs (string aggregates AND GROUP BY
passthrough strings), ancestor-real vs CTE string output on both the
aggregate and pass-through paths; all six operators; inside DNF; NULL ⇒
UNKNOWN rejects the row/disjunct.  **Deferred**: Char-vs-Varchar mixes,
cross-charset compares (MySQL converts; we reject with a message),
DECIMAL and non-DATE temporals (unchanged), cross-CTE / sibling-branch
sides (existing rejection unchanged).

**No version gate** (26.04 alpha precedent — single-row CTE, DATE arm);
the opcode is emitted only by same-tree RonSQL.

## Work items

- **S1 — kernel**: shared string-compare helper factored from
  `handleBranchMemOpArgInlineType`; opcode 46 encoders in
  Interpreter.hpp (`BranchLinkedLinked(cond, nulls)` +
  `BranchLinkedLinked_2/3/4` word builders) + preprocessing-info arm;
  `handleBranchLinkedOpLinked` + dispatch entries.  Block test: extend
  `testCteNdbApiFilter.cpp` with a raw-program string col-vs-col case
  (two string MIN/MAX outputs; NULL entry ⇒ reject; per-side lengths).
- **S2 — NDB API**: the six `branch_linked_linked_*` methods (one-word
  wrappers over add_branch + 3 operand words, following
  `branch_linked_inline_*`).
- **S3 — RonSQL**: `resolve_side` descriptor plumb-through; string arm +
  same-type/same-charset gate; message rework (the "only integer, FLOAT,
  DOUBLE and DATE" text gains strings and loses them from the rejected
  list); EXPLAIN CONDITIONS print unchanged (conjunct already prints).
- **S4 — MTR (×5 topologies)**: new cases in the families that own these
  shapes —
  - `body_filter.inc` (main-WHERE group): `cte.mn < cte.mx` over CHAR
    MIN/MAX outputs; over VARCHAR; GROUP-BY-passthrough string vs string
    aggregate output; inside OR/DNF; a NULL-producing group (MIN over
    empty ⇒ UNKNOWN rejects); `=`/`<>` sweep.
  - `body_passthrough_scalar_cte.inc`: string watermark
    `t.varchar_col > s.min_v` on the pass-through path (sc-2x) + the
    aggregate twin; **sc-P4's message text changes** → family re-record.
  - `body_root_pk_residual.inc`: rpr string twin of rpr-16 (CTE-own
    string outputs on the aggregate path).
  - Rejection probes: Char-vs-Varchar mix, cross-charset pair (needs a
    local table with an explicit non-default charset column), string vs
    integer output.
- **S5 — docs**: rondb-docs `ronsql_cte.md` (the "for integer-typed
  outputs including mixed signedness; string outputs ... are rejected"
  passage and the "Not supported" bullet) + `ronsql_limitations.md`;
  close-out in this doc + CLAUDE.md index.

## Risks / open questions

- **Varchar size convention**: the INLINE_TYPE kernel doc says
  columnSizeBytes includes the length prefix, while RonSQL's const-path
  passes `vtcol->getLength()` — mirror the const path exactly (the
  kernel decodes both sides identically), and pin with a VARCHAR case
  whose values differ only beyond a shared prefix.
- **Ancestor string projections**: I.26 linked projections have only
  been exercised with numeric columns; a real CHAR/VARCHAR projection
  rides the same NI_LINKED_ATTR machinery (F.4 K.3b precedent for
  string slots) but gets its first coverage here — the block test pins
  it below RonSQL first.
- **Buffer entry layout for long VARCHAR**: MIN/MAX string payloads are
  capped by the AGG_CHAR_RESULT format; confirm the filter-side entry
  for a GROUP BY passthrough VARCHAR uses the same AttrHeader+data
  layout the const-vs-col path already decodes (it does for const
  compares — the same read serves both).
- sc-P4 and any other baselines quoting the old message move again —
  same families as the DATE re-pin, re-record is routine.
