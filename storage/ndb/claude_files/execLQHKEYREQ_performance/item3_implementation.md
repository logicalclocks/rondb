# Item 3 implementation plan: collapse Treqinfo flag bit-slice

Companion to the Item 3 description. Narrow-scope, single-file,
single-function edit. Expected result per the description:
`execLQHKEYREQ` drops from 5220 B → ~5150 B (~17 insns).

## Scope of changes

**One file, one function, one ~20-line block.**

| File | Edits |
|---|---|
| `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` | Replace lines 9353–9376 (the four `if (getXxxFlag(Treqinfo)) m_flags \|= OP_YYY;` blocks) with a single folded OR. |

Not changed: headers, signal definitions, macros, anything outside
`execLQHKEYREQ`.

## The edit

### Before (DblqhMain.cpp:9353–9376)

```cpp
  regTcPtr->m_fire_trig_pass = 0;
  Uint32 Tdeferred = LqhKeyReq::getDeferredConstraints(Treqinfo);
  if (Tdeferred) {
    regTcPtr->m_flags |= TcConnectionrec::OP_DEFERRED_CONSTRAINTS;
  }

  Uint32 TdisableFk = LqhKeyReq::getDisableFkConstraints(Treqinfo);
  if (TdisableFk) {
    regTcPtr->m_flags |= TcConnectionrec::OP_DISABLE_FK;
  }

  Uint32 TnormalProtocolFlag = LqhKeyReq::getNormalProtocolFlag(Treqinfo);
  if (TnormalProtocolFlag) {
    /**
     * Only set normal protocol flag if long request.
     * As above, short lqhKeyReq ai-length in-signal overlaps the bit.
     * bug#14702377
     */
    regTcPtr->m_flags |= TcConnectionrec::OP_NORMAL_PROTOCOL;
  }

  if (LqhKeyReq::getNoTriggersFlag(Treqinfo)) {
    regTcPtr->m_flags |= TcConnectionrec::OP_NO_TRIGGERS;
  }
```

### After

```cpp
  regTcPtr->m_fire_trig_pass = 0;
  // Fold four independent boolean flag tests into one m_flags OR.
  // Each ternary lowers to a bit-test + cmov (or select) into a
  // scratch register; clang ORs them and issues a single ldr/orr/str
  // triple at the end instead of four separate RMWs of m_flags.
  //
  // Normal-protocol bit: comment retained verbatim.
  //   Only set normal protocol flag if long request.
  //   As above, short lqhKeyReq ai-length in-signal overlaps the bit.
  //   bug#14702377
  const Uint32 add_flags =
      (LqhKeyReq::getDeferredConstraints(Treqinfo)
           ? TcConnectionrec::OP_DEFERRED_CONSTRAINTS : 0) |
      (LqhKeyReq::getDisableFkConstraints(Treqinfo)
           ? TcConnectionrec::OP_DISABLE_FK : 0) |
      (LqhKeyReq::getNormalProtocolFlag(Treqinfo)
           ? TcConnectionrec::OP_NORMAL_PROTOCOL : 0) |
      (LqhKeyReq::getNoTriggersFlag(Treqinfo)
           ? TcConnectionrec::OP_NO_TRIGGERS : 0);
  regTcPtr->m_flags |= add_flags;
```

Notes on the diff:

- The four intermediate `Tdeferred` / `TdisableFk` / `TnormalProtocolFlag`
  locals were only used in their own `if`, so they disappear cleanly.
- The `bug#14702377` comment is preserved verbatim (as a block
  comment above the OR expression) — that comment is load-bearing
  tribal knowledge and must survive.
- `const Uint32 add_flags` so the compiler can choose to keep it in
  a register; no aliasing concerns since `add_flags` never has its
  address taken.
- Final `regTcPtr->m_flags |= add_flags;` keeps the original
  semantics of "or the new bits onto whatever was already there" —
  important because earlier lines (9289, 9297, 9256) also OR into
  `m_flags` before this point.

## Why not the shift-mask form

An even tighter form:
```cpp
regTcPtr->m_flags |=
    ((Treqinfo >> SHIFT_A) & OP_DEFERRED_CONSTRAINTS) |
    ...
```
would be 1-2 insns shorter if the shift amounts line up with the
OP_* bit positions. But it bakes in an assumption that both the
Treqinfo bit layout and the OP_* values are stable. Either
changing — which has happened historically — would silently produce
wrong flags with no static check. The ternary form is robust against
renumbering on either side, and the compiler still produces near-
optimal codegen. **Use the ternary form.** Skip the shift-mask
optimisation.

## Step 1 — Preserve baseline before rebuild

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/storage/ndb/claude_files/execLQHKEYREQ_performance/measurements

# After Item 2 already lives here as item2_post.*; copy as item2_post baseline
# if not already done. The current prod_build/bin/ndbmtd IS the post-Item-2 binary.
# Confirm nothing has been overwritten:
ls ndbmtd.item1_post exec_lqhkey.item2_post.asm nm.item2_post.txt size.item2_post.txt

# Snapshot the exact flag-region disassembly for before/after diffing
awk 'NR>=560 && NR<=660' exec_lqhkey.item2_post.asm > flag_region.before.asm
wc -l flag_region.before.asm  # expect ~100 lines
```

## Step 2 — Apply the edit

A single `Edit` replacing lines 9353–9376 with the folded form
above. No other file changes.

## Step 3 — Build `prod_build`

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/prod_build
make -j$(sysctl -n hw.ncpu) ndbmtd
```

Should be fast (one .cpp recompile, then relink).

## Step 4 — Verify the collapse happened as intended

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/storage/ndb/claude_files/execLQHKEYREQ_performance/measurements
BIN=/Users/mikael/mysql_trees/rondb_1051_performance/prod_build/bin/ndbmtd

objdump --disassemble-symbols=__ZN5Dblqh13execLQHKEYREQEP6Signal "$BIN" \
  > exec_lqhkey.item3_post.asm 2>/dev/null
size "$BIN" > size.item3_post.txt

# Metrics diff
python3 <<'EOF'
import subprocess
out = subprocess.check_output(['nm','-n',
    '/Users/mikael/mysql_trees/rondb_1051_performance/prod_build/bin/ndbmtd']).decode()
addrs = {}
for l in out.splitlines():
    p = l.split()
    if len(p) >= 3 and p[1] in ('T','t'):
        addrs[p[2]] = int(p[0], 16)
sym = '__ZN5Dblqh13execLQHKEYREQEP6Signal'
a = addrs[sym]
nxt = min(x for x in addrs.values() if x > a)
s = nxt - a
print(f'execLQHKEYREQ: {s} B ({s//4} insns)')
print(f'  baseline:     5960 B')
print(f'  Item 1 post:  5792 B')
print(f'  Item 2 post:  5220 B')
print(f'  Item 3 post:  {s} B  (delta vs Item 2: {s-5220:+d} B)')
EOF

# In-body tbz/tbnz on w28 count (should drop from 13 in the flag region to ~0)
echo -n "tbz/tbnz on w28 in body: "
grep -cE 'tb[nz]?z\s+w28,' exec_lqhkey.item3_post.asm

# Diff the flag region itself
awk 'NR>=540 && NR<=640' exec_lqhkey.item3_post.asm > flag_region.after.asm
diff flag_region.before.asm flag_region.after.asm | head -40
```

Expected outcome:

- `execLQHKEYREQ` ≈ 5140–5170 B (−50 to −80 B from 5220).
- Total `tbz w28` / `tbnz w28` in body drops from 19 to ~13 (the 6
  in the flag cluster disappear).
- The tail `tbnz` back-cluster at `+0x9b8..+0x9d4` should be gone
  entirely.

If the compiler failed to fold and the count is unchanged,
investigate: perhaps the `getXxxFlag()` inline definitions aren't
visible, or there's a surprising side effect. Unlikely but flag it.

## Step 5 — MTR smoke

```bash
cd /Users/mikael/mysql_trees/rondb_1051_performance/mysql-test
./mtr ndb_basic
```

Any NDB test will exercise the flag-OR path on every LQHKEYREQ.
`ndb_basic` is the fastest smoke and is sufficient — the change is
behaviour-neutral, so no specific test is needed beyond "kernel
still boots and signals flow".

## Step 6 — Record results

Append a section to `results.md` following the Items 1 and 2
template, with:

- Before/after byte count for `execLQHKEYREQ`.
- Before/after `tbz w28` count.
- Short note: "this item eliminates code, not just relocates it —
  net binary-size drop, no cold-text gain".

## Step 7 — Commit

Single commit on the `RONDB-1051-execLQHKEYREQ` branch:

```
RONDB-1051: Fold Treqinfo flag bit-tests into one m_flags OR

Four independent `if (getXxxFlag(Treqinfo)) m_flags |= OP_YYY`
tests at DblqhMain.cpp:9354–9376 each compiled to a separate
tbz + ldr + orr + str of m_flags, plus a secondary cluster of
tbnz back-jumps emitted by the compiler's if-conversion pass.

Replace them with a single folded OR of four constant-or-zero
ternaries into a scratch register, then one ldr/orr/str of
m_flags. Semantics identical; same four bits checked, same four
OP_* masks potentially set, same RMW order.

execLQHKEYREQ: <before> → <after> (<delta> B, <pct>%)
Kernel-wide __TEXT unchanged (local change only).

The bug#14702377 comment on NormalProtocol is preserved.
```

## Effort estimate

- Step 1 (baseline snapshot): 2 minutes.
- Step 2 (the edit): 5 minutes.
- Step 3 (build): 2–5 minutes.
- Step 4 (verify): 5 minutes.
- Step 5 (MTR): 3 minutes.
- Step 6 + 7 (record + commit): 5 minutes.
- Total: **~25 minutes**.

## Things to flag during review

- The `bug#14702377` comment is moved from *inside* the
  per-flag `if` to a block comment above the whole folded
  expression. Intent preserved; reviewer should confirm the move
  is acceptable.
- If clang's codegen doesn't collapse as expected (e.g. emits 4
  independent cmov sequences without CSE on `&regTcPtr->m_flags`),
  the local gain will be smaller — ~8 insns instead of 17. Still
  positive, but worth noting in `results.md`.
- The four `Tdeferred` / `TdisableFk` / `TnormalProtocolFlag` local
  variable names are removed. If any future debug code or log
  statement referenced them by name (grep should show none), add it
  back as a local re-extraction. Currently unreferenced.
