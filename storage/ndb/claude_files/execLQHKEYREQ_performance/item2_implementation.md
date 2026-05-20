# Item 2 implementation plan: outline `ndbrequire` failure arm

Companion to the description in the chat. This plan is written after
Item 1's post-mortem — we now know that argument marshalling stays at
the call site no matter what, so the only bytes that actually move are
the ones belonging to the failure-arm body itself.

## Scope of changes

Three files modified. **Zero call-site edits anywhere in the kernel.**
The macro change is what does all the work.

| File | Edits |
|---|---|
| `storage/ndb/src/kernel/vm/pc.hpp` | Redefine `ndbrequireErr`, `ndbrequire`, `ndbabort`. Forward-declare `ndbrequire_fail` and `ndbabort_fail`. |
| `storage/ndb/src/kernel/vm/SimulatedBlock.cpp` | Add 2 helper definitions (`ndbrequire_fail`, `ndbabort_fail`), each `[[noreturn, cold, noinline]]`. |
| `storage/ndb/src/kernel/vm/SimulatedBlock.hpp` | Optional: declare the extern helpers in a header included by `pc.hpp`, if we want them not to be free functions. We will keep them as plain extern `"C++"` free functions to avoid include cycles. |

Not changed: any block source, any signal format, any test file, any
public header outside `pc.hpp`.

## Ground truth from the measurements

From `results.md`:

- `ndbrequire` expands to **13–18 instructions / 52–72 bytes** per
  site in the current binary, all in the cold tail region of each
  function.
- In `execLQHKEYREQ` alone: **10 sites, 564 bytes, 141 instructions**
  of failure-arm code.
- Kernel-wide: 6 480 `ndbrequire(Err)?` call sites across 81 files.
- `ndbabort()` is a similar macro and adds more sites (not counted,
  but the outlining path is the same).

## Constraint discovered during research

`jamNoBlock()` hard-codes the caller's `JAM_FILE_ID` at macro-expansion
time via a `static_assert` in `_internal_thrjamLinenumber` (pc.hpp:108-109):

```cpp
static_assert(JamEvent::verifyId((JAM_FILE_ID), __FILE__));
```

If the outlined helper calls `jamNoBlock()` from inside
`SimulatedBlock.cpp`, the jam-ring entry will show
`SimulatedBlock.cpp`'s file-id, not the original caller's. That
destroys crash forensics for every `ndbrequire` failure kernel-wide.

**Fix:** the helper takes the caller's `JAM_FILE_ID` as an argument
and builds a `JamEvent` directly:

```cpp
EmulatedJamBuffer* buf = NDB_THREAD_TLS_JAM;
buf->insertJamEvent(JamEvent(caller_file_id, line, /*isLine=*/true));
```

`JamEvent` (Emulator.hpp:833) and `insertJamEvent`
(Emulator.hpp:919) are already public and accept any runtime
`fileId`. The compile-time static-assert only runs at the call-site
where `JAM_FILE_ID` is a known constant.

## Step 1 — Add forward declarations to `pc.hpp`

Insert near the top of `pc.hpp` (before the `ndbrequire` macro
definitions, around line 375):

```cpp
// Outlined cold failure helpers for ndbrequire / ndbabort. Defined
// in SimulatedBlock.cpp. See claude_files/execLQHKEYREQ_performance/
// item2_case.md.
//
// Why a runtime fileId argument: jamNoBlock() hard-codes JAM_FILE_ID
// at the call site via a static_assert; moving it into the helper
// would lose the caller's file identity. Passing the caller's
// JAM_FILE_ID preserves the jam-ring content bit-for-bit.
[[noreturn]] void ndbrequire_fail(Uint32 caller_file_id,
                                  int line, int code,
                                  const char* file,
                                  const char* check)
    __attribute__((cold, noinline));
[[noreturn]] void ndbabort_fail(Uint32 caller_file_id,
                                int line, const char* file)
    __attribute__((cold, noinline));
```

## Step 2 — Redefine the macros in `pc.hpp`

Replace the current `ndbrequireErr` / `ndbrequire` / `ndbabort`
definitions (currently at lines 378–391):

```cpp
#define ndbrequireErr(check, error)                                  \
  do {                                                               \
    if (unlikely(!(check))) {                                        \
      ndbrequire_fail(JAM_FILE_ID, __LINE__, (error),                \
                      __FILE__, #check);                             \
    }                                                                \
  } while (false)

#define ndbrequire(check) ndbrequireErr(check, NDBD_EXIT_NDBREQUIRE)

#define ndbabort()                                                   \
  do {                                                               \
    ndbabort_fail(JAM_FILE_ID, __LINE__, __FILE__);                  \
  } while (false)
```

**Why `do { … } while (false)` wrapper.** The existing macros don't
have it, but the new forms contain only a single call statement;
the `do/while(false)` is standard defensive hygiene so the macro
behaves as a statement inside `if/else` chains without braces. Costs
nothing at runtime.

**Why `unlikely(!(check))` instead of `likely(check)` + `else`.** The
compiler treats both identically for branch prediction, but the
negated form lets the entire macro be one statement (the `if` with
no else), which matches how Item 1's outlined epilogues look.

## Step 3 — Add helper definitions to `SimulatedBlock.cpp`

At the top of `SimulatedBlock.cpp`, near the existing progError
infrastructure, define the two helpers:

```cpp
// See pc.hpp for why the caller's JAM_FILE_ID is passed explicitly.
[[noreturn]] void ndbrequire_fail(Uint32 caller_file_id,
                                  int line, int code,
                                  const char* file,
                                  const char* check) {
  EmulatedJamBuffer* buf = NDB_THREAD_TLS_JAM;
  buf->insertJamEvent(JamEvent(caller_file_id,
                               static_cast<Uint16>(line),
                               /*isLineNumber=*/true));
  progError(line, code, file, check);
  __builtin_unreachable();
}

[[noreturn]] void ndbabort_fail(Uint32 caller_file_id,
                                int line, const char* file) {
  EmulatedJamBuffer* buf = NDB_THREAD_TLS_JAM;
  buf->insertJamEvent(JamEvent(caller_file_id,
                               static_cast<Uint16>(line),
                               /*isLineNumber=*/true));
  progError(line, NDBD_EXIT_PRGERR, file, "");
  __builtin_unreachable();
}
```

`progError` is already a member of `SimulatedBlock` but is also
exposed as a standalone function in the kernel's vm layer. Confirm
during implementation that either form links — if it's only a member,
we route through a free wrapper or forward-declare the global
`progError`. `ndbabort()`'s current macro already calls `progError`
as a free function (pc.hpp:390), so the free form exists.

`__builtin_unreachable()` guards against any codegen that assumes
the function can return — keeps the compiler from extending live
ranges past the call.

## Step 4 — Confirm `NDB_THREAD_TLS_JAM` is visible from `SimulatedBlock.cpp`

It should be — `SimulatedBlock.cpp` already includes the emulator
headers that declare it. If not, add:

```cpp
#include "Emulator.hpp"  // NDB_THREAD_TLS_JAM, EmulatedJamBuffer, JamEvent
```

## Step 5 — Preserve `ERROR_INSERT` compatibility

The existing `ndbrequire` is active unconditionally. `ndbassert` is
gated on `VM_TRACE || ERROR_INSERT` (pc.hpp:365) and currently has its
own inline expansion. Leave `ndbassert` alone — this plan only touches
`ndbrequire`, `ndbrequireErr`, and `ndbabort`.

The `CRASH_INSERTION*` macros (pc.hpp:393+) have the same inline-arm
shape but are guarded by `ERROR_INSERT`, so they compile to nothing
in prod builds. **Skip them.** Only worth outlining if we ever revisit
debug-build performance.

## Step 6 — Build and verify

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/prod_build
make -j$(sysctl -n hw.ncpu) ndbmtd

# Confirm the helpers exist and are in cold text
nm -n bin/ndbmtd | grep -E "ndbrequire_fail|ndbabort_fail"

# Confirm execLQHKEYREQ shrank further past Item 1's 5792 B
python3 <<'EOF'
import subprocess
out = subprocess.check_output(['nm','-n','bin/ndbmtd']).decode()
addrs = {}
for l in out.splitlines():
    p = l.split()
    if len(p) >= 3 and p[1] in ('T','t'):
        addrs[p[2]] = int(p[0], 16)
sym = '__ZN5Dblqh13execLQHKEYREQEP6Signal'
a = addrs[sym]; nxt = min(x for x in addrs.values() if x > a)
print(f'execLQHKEYREQ: {nxt-a} B  (Item 1 post = 5792 B, baseline = 5960 B)')
EOF

# Measure total text shrinkage (the kernel-wide win)
size bin/ndbmtd
```

Expected outcome:

- Two new symbols in cold text: `ndbrequire_fail` (~20 bytes) and
  `ndbabort_fail` (~20 bytes).
- `execLQHKEYREQ` around **5470 B** (≈320 B smaller than Item 1's
  result).
- Total `__text` section shrinks noticeably — conservatively ~40 KB
  across 6 480 ndbrequire sites (at ~6 bytes saved per site).

If `nm` doesn't show the helpers as separate symbols, `noinline`
wasn't honoured. Confirm `__attribute__((cold, noinline))` is on
*both* the declaration (`pc.hpp`) and the definition
(`SimulatedBlock.cpp`).

## Step 7 — Preserve crash-forensic parity

This is the critical step. The change is behaviour-neutral only if
the jam ring and progError output match the old code byte-for-byte.

### Unit check on jam-ring content

Write a tiny test (or reuse an existing one from the `block_unit_test`
directory) that:

1. Seeds the jam ring with a known pattern.
2. Triggers an `ndbrequire(false)` at a known line.
3. Catches the crash and inspects the jam ring.
4. Asserts that the last jam entry has `file_id == JAM_FILE_ID` and
   `line == <known line>`, **not** the `SimulatedBlock.cpp`
   JAM_FILE_ID.

If there isn't a natural hook for this, the alternative is:

- Build a debug cluster.
- Cause a known `ndbrequire` failure (e.g., via a test that
  deliberately corrupts a data structure under `ERROR_INSERT`).
- Inspect the trace file and confirm the final jam-ring entries
  identify the original source file.

### Bit-identical `progError` output

The error log line produced by `progError` looks like:
```
Error handler shutting down system
Error: <code>, ...
Signaling: <line number> <filename>
(msg: <check string>)
```

None of these arguments change — the helper forwards `line`,
`code`, `file`, `check` verbatim. So the log text should be
bit-identical. Diff the log output of a known `ndbrequire` failure
against a baseline.

## Step 8 — MTR runs

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/mysql-test

# Tests that deliberately trigger ndbrequire / ndbabort failures
./mtr --force ndb_error_insert_01 ndb_error_insert_02 \
      ndb_restore_no_error
./mtr --suite=rondis
./mtr ndb_basic ndb_dd_basic
```

All must pass with no diff. Any failure in `ndb_error_insert_*` is
a signal that the jam ring or progError output changed detectably —
investigate before proceeding.

## Step 9 — Measure and record

Update `results.md` with the "Item 2" section following the Item 1
template:

| Metric | Item 1 post | Item 2 post | Δ from Item 1 |
|---|--:|--:|--:|
| `execLQHKEYREQ` size | 5792 B | _fill in_ | _fill in_ |
| `bl progError` in body | 10 | _expect 0_ | _expect −10_ |
| `bl NDB_THREAD_TLS_JAM` in body | 9 | _expect 0_ | _expect −9_ |
| Total `__text` section | _baseline_ | _new_ | _expect tens of KB shrink_ |

Record both the `execLQHKEYREQ` local delta and the kernel-wide
`__text` delta. The point of Item 2 is the kernel-wide effect;
`execLQHKEYREQ` is just the bellwether.

## Step 10 — Commit

```
RONDB-1051: Outline ndbrequire/ndbabort failure arms

Replace the inline failure arm of ndbrequire, ndbrequireErr and
ndbabort with a single [[noreturn, cold, noinline]] helper in
SimulatedBlock.cpp. Each kernel-wide call site now emits only the
compare + conditional branch + helper-call, reducing cold code in
the hot body of every function that uses these macros.

jam-ring content is preserved: the helper takes the caller's
JAM_FILE_ID and __LINE__ as arguments and builds the JamEvent
manually, bypassing jamNoBlock (which would hard-code the helper
file's JAM_FILE_ID and lose caller identity).

execLQHKEYREQ: <before> B → <after> B (<delta> B, <pct>%)
Total __text: <before> KB → <after> KB (<delta> KB shrink)
ndbrequire call sites affected: ~6480 across 81 kernel files.
```

## Open questions to resolve before starting

1. **Is `progError` callable as a free function from
   `SimulatedBlock.cpp`?** The current `ndbabort` macro already calls
   it as a free function (pc.hpp:390), so yes, but confirm the
   declaration is visible wherever `SimulatedBlock.cpp` includes it.
2. **Does `NDB_THREAD_TLS_JAM` work outside a `SimulatedBlock`
   member?** Yes — the macro `_internal_jamNoBlockLinenumber` already
   uses it in the macro body (pc.hpp:174), meaning it has to be
   available at any call site including free functions.
3. **Are there any `ndbrequire` sites in header files?** If so, the
   header needs to include `pc.hpp` transitively. A quick
   `grep -l "ndbrequire" storage/ndb/**/*.hpp` will show this.
4. **Are there `ndbrequire` sites that rely on side effects in the
   expression?** The current macro evaluates `check` once; the new
   macro evaluates it once inside `unlikely(!( … ))`. Identical
   semantics. Not a problem.

## Effort estimate

- Step 1 (decls): 5 minutes.
- Step 2 (macro rewrite): 10 minutes.
- Step 3 (helper defs): 15 minutes.
- Step 4-5 (header/visibility checks): 10 minutes.
- Step 6 (build + measurements): 20 minutes.
- Step 7 (jam-ring forensic check): **up to 2 hours** — this is the
  novel risk vs Item 1.
- Step 8 (MTR): 30 minutes running.
- Step 9 (record): 15 minutes.
- Step 10 (commit): 5 minutes.
- Total: ~4 hours, dominated by forensic verification.

## Rollback plan

If the forensic check reveals the jam ring is wrong, or MTR
`ndb_error_insert_*` regresses, the change is contained in three
files and is easily reverted — revert the commit, done. No call-site
churn elsewhere means no complicated cleanup.

## What NOT to do in this change

- Do **not** touch `ndbassert` (gated on debug builds, compiles out
  in prod anyway).
- Do **not** touch `CRASH_INSERTION*` (gated on `ERROR_INSERT`).
- Do **not** inline the helper's jam-ring work as a separate
  `jamNoBlockManually(fileId, line)` macro — keep it contained in
  the helper.
- Do **not** try to save the extra `JAM_FILE_ID` argument via some
  `thread_local` trick — the compiler already optimises
  `JAM_FILE_ID` (a `constexpr`) to a single `mov w0, #N` insn at
  the call site, which is cheap.
