# Compiler-Bug Reduction Session Setup

Goal: produce a standalone minimal C++ file (~50-100 lines) that reproduces
the codegen bug observed in `Dbtup::prepare_read`, so we can file it upstream
at LLVM and/or GCC.

## The Bug (what you are isolating)

- An `ALWAYS_INLINE` class-member helper (`prepare_read_fill_var_data`) was
  called from two sites inside `Dbtup::prepare_read`.
- With `-g -O0`-style debug builds, the compiler inlined the helper body at
  both sites, then **tail-merged** the two inlined copies into shared
  instructions.
- The merged instructions read an `EmulatedJamBuffer *jamBuffer` parameter
  from one stack slot (`[x29, #-0x70]` on aarch64-darwin), but the MM call
  site wrote `jamBuffer` to a *different* stack slot (`[x29, #-0x38]`).
- Along the hot call path (MM only, DD not taken), the read from `-0x70` is
  uninitialised → `jamBuffer == 0` → SIGSEGV inside `thrjam`.
- Reproduces on **Apple Clang 17** (arm64-darwin) and **GCC 12.2.1-7** on
  Linux. Both toolchains must reproduce for the minimal reducer to land.

Evidence captured in this tree:
- `/tmp/ndbmtd_working` — good binary (body duplicated at each site)
- `/tmp/ndbmtd_failing` — bad binary (helper, triggers the miscompile)
- `/tmp/prepare_read_working.s`, `/tmp/prepare_read_failing.s` — extracted
  disassembly for comparison
- Relevant commits on branch `RONDB-1051-performance`:
  - `ea2a2f16b25` — introduced the unroll + helper (buggy codegen)
  - `08430f38b6d` — ndbassert→assert rename in helper
  - HEAD — contains the workaround (duplicated body, no helper)

## Tools

Use **cvise** (actively maintained fork of creduce, better on modern C++).

```
# macOS:
brew install cvise llvm
# Linux:
sudo apt install cvise clang  # or build from https://github.com/marxin/cvise
```

## Strategy

1. **Start from a hand-seeded reducer**, not the full TU — the full TU pulls
   in ~50 headers and 50k lines preprocessed.
2. The seed reducer must already **compile and exhibit the bug** before you
   hand it to cvise. This is the hard part.
3. Once cvise has something to bite on, it reduces in minutes.

## Step 1 — Start a fresh Claude Code session

```
cd /Users/mikael/mysql_trees/rondb_1051_performance
# Work in a temp scratch dir to keep the repo clean:
mkdir -p /tmp/rondb_compiler_repro
cd /tmp/rondb_compiler_repro
claude
```

Kick the session off with:

> "I'm reducing a compiler bug observed in RonDB. Read
> `/Users/mikael/mysql_trees/rondb_1051_performance/storage/ndb/claude_files/pushdown_join_aggregation/compiler_bug_reducer_session.md`
> and start from there. Do not touch the main RonDB tree."

## Step 2 — Build a seed reproducer

The seed must mimic the RonDB pattern closely enough to trigger the same
codegen decision. Key ingredients, in decreasing order of importance:

- A large outer function with **20+ `thrjam`-like inline-asm/function calls**
  before and between the two helper call sites. The sheer density of jam
  calls is what seems to push the compiler into tail-merging.
- A struct with a pointer field (jamBuffer) at offset **0x28** — match
  RonDB's `KeyReqStruct` layout (2 reference fields + 3 `*` fields +
  `EmulatedJamBuffer*`).
- An `ALWAYS_INLINE` **class-member** helper (not free function) with:
  - 5 parameters (pointer, pointer, int, short, pointer)
  - Internal body that itself contains 2-4 calls that take the pointer
    parameter (analogue of the internal thrjams)
  - A for-loop whose bound is one of the parameters (prevents trivial unroll)
- Helper called from exactly two sites in the same outer function.
- One of the two call sites is behind `if (disk == false) return;` so at
  runtime only one path is live.
- **Compile with**: `clang++ -std=gnu++20 -g -fno-omit-frame-pointer
  -arch arm64` (for macOS) or the exact GCC 12.2.1-7 flags on Linux.

Earlier session's partial seeds at `/tmp/repro.cpp` and `/tmp/repro3.cpp` did
**not** trigger. You will likely need closer to 300-500 lines before the
trigger appears. Then cvise will take it down.

## Step 3 — Write the interestingness test

Save as `/tmp/rondb_compiler_repro/interesting.sh`:

```bash
#!/bin/bash
# Returns 0 iff the reducer (`candidate.cpp`) exhibits the miscompile:
# a stack slot is read inside `prepare`/`run` that is never written.
set -e
CXX=${CXX:-clang++}
FLAGS="-std=gnu++20 -g -fno-omit-frame-pointer -arch arm64 -c"

# Compile must succeed
$CXX $FLAGS -o candidate.o candidate.cpp 2>/dev/null || exit 1

# Disassemble the target function (adjust grep pattern per reducer)
objdump --disassemble --demangle --no-show-raw-insn candidate.o \
  | awk '/<.*prepare.*>:/,/^$/' > fn.s
[ -s fn.s ] || exit 1

# Collect stack slots read from [x29, #-0xNN] and slots written to same
python3 - fn.s <<'PY'
import re, sys
text = open(sys.argv[1]).read()
reads  = set(re.findall(r'ldur?\s+\S+,\s+\[x29,\s*#-0x([0-9a-f]+)\]', text))
writes = set(re.findall(r'stur?\s+\S+,\s+\[x29,\s*#-0x([0-9a-f]+)\]', text))
reads  |= set(re.findall(r'ldr\s+\S+,\s+\[x29,\s*#-0x([0-9a-f]+)\]', text))
writes |= set(re.findall(r'str\s+\S+,\s+\[x29,\s*#-0x([0-9a-f]+)\]', text))
uninit = reads - writes
sys.exit(0 if uninit else 1)  # 0 = bug present, 1 = clean
PY
```

Make it executable: `chmod +x interesting.sh`.

Test by hand:
```
cp your_seed.cpp candidate.cpp
./interesting.sh && echo BUG || echo CLEAN
```

Only hand it to cvise once `./interesting.sh` returns 0 on your seed.

## Step 4 — Run cvise

```
cvise --n $(sysctl -n hw.ncpu) ./interesting.sh candidate.cpp
```

cvise will iteratively transform `candidate.cpp`, running `interesting.sh`
each iteration, only keeping transformations where the script still returns
0. Expect 10-60 minutes depending on seed size.

When done, `candidate.cpp` will be the minimal reducer.

## Step 5 — Cross-validate on GCC 12

On the Linux box with GCC 12.2.1-7:

```
g++-12 -std=gnu++20 -g -fno-omit-frame-pointer -c -o /tmp/candidate.o candidate.cpp
objdump -d /tmp/candidate.o | grep 'prepare.*:' -A 200 > /tmp/candidate.s
# Manually verify the same "read-but-never-written stack slot" pattern
```

## Step 6 — File bug

Once you have a clean reducer that both compilers miscompile:

- **LLVM**: https://github.com/llvm/llvm-project/issues/new/choose
  - Use the "Bug Report" template
  - Title: "Clang mis-merges tail of ALWAYS_INLINE helper inlined at two
    call sites, producing uninitialised stack-slot read of parameter"
  - Labels: `backend:AArch64`, `miscompilation`
- **GCC**: https://gcc.gnu.org/bugzilla/enter_bug.cgi?product=gcc
  - Component: `tree-optimization` (if pre-codegen pass) or `target` (if
    backend tail-merge)

Include in both reports:
- Minimal reducer (cvise output, `candidate.cpp`)
- Both compilers' exact `-v` versions
- Command lines
- Disassembly snippet showing the read-never-written slot
- Workaround: duplicate body at each call site

## Context to give the fresh Claude session

- Bug location in real code: `storage/ndb/src/kernel/blocks/dbtup/`,
  the `Dbtup::prepare_read` function, commit `ea2a2f16b25`.
- The helper signature was:
  ```cpp
  ALWAYS_INLINE void prepare_read_fill_var_data(
      KeyReqStruct::Var_data *dst, const Uint32 *flex_data,
      Uint32 flex_len, Uint16 num_vars, EmulatedJamBuffer *jamBuffer);
  ```
- `thrjam` macro expands to (roughly):
  ```cpp
  do { jb->insertJamEvent(JamEvent(fileId, lineNo, true));
       assert((jb->theEmulatedJamIndex & 3) != 0 || jb == NDB_THREAD_TLS_JAM);
     } while (0)
  ```
- `TUP_DATA_VALIDATION` is defined in VM_TRACE/ERROR_INSERT builds, which
  turns on the `thrjam(jamBuffer)` calls inside the helper body — these
  are what crash.
