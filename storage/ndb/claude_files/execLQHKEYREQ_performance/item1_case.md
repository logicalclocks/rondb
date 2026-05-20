# The case for Item 1: outline the error-return epilogues

## What the disassembly actually shows

Counted directly in `prod_build/bin/ndbmtd`'s `__ZN5Dblqh13execLQHKEYREQEP6Signal`:

| Site | PC range | Bytes | Calls | Branches back to common return |
|---|---|--:|---|---|
| earlyKeyReqAbort #1 | 0x100193700 – 0x10019375c | 96 | jam + releaseSections + earlyKeyReqAbort | `b 0x100193b0c` |
| earlyKeyReqAbort #2 | 0x100193a30 – 0x100193a90 | 100 | jam + releaseSections + earlyKeyReqAbort | `b 0x100193b0c` |
| earlyKeyReqAbort #3 | 0x100193ac4 – 0x100193b08 | 84 | jam + releaseSections + earlyKeyReqAbort | falls through to return |
| earlyKeyReqAbort #4 | 0x100193e0c – 0x100193e5c | 84 | jam + releaseSections + earlyKeyReqAbort | `b 0x100193b0c` |
| earlyKeyReqAbort #5 | 0x1001940bc – 0x1001940f8 | 64 | jam + earlyKeyReqAbort | `b 0x100193b0c` |
| earlyKeyReqAbort #6 | 0x100194164 – 0x100194180 | 32 | earlyKeyReqAbort only | `b 0x100193b0c` |
| earlyKeyReqAbort #7 | 0x100194400 – 0x10019443c | 64 | jam + earlyKeyReqAbort | `b 0x100193b0c` |
| LQHKEY_abort #1 | 0x10019435c – 0x100194394 | 60 | jam + LQHKEY_abort | `b 0x100193b0c` |
| LQHKEY_error #1 | 0x1001943ac – 0x1001943d4 | 44 | LQHKEY_error | `b 0x100193b0c` |
| LQHKEY_error #2 | 0x100194548 – 0x100194560 | 28 | LQHKEY_error | `b 0x100193b0c` |
| LQHKEY_abort #2 | 0x100194564 – 0x100194584 | 36 | LQHKEY_abort | `b 0x100193b0c` |
| LQHKEY_abort #3 | 0x1001948c8 – 0x1001948dc | 24 | LQHKEY_abort | `b 0x100193b0c` |
| LQHKEY_error #3 | 0x1001949e4 – 0x1001949f4 | 20 | LQHKEY_error | `b 0x100193b0c` |
| LQHKEY_abort #4 | 0x1001949f8 – 0x100194a08 | 20 | LQHKEY_abort | `b 0x100193b0c` |

**Totals:**

- 14 distinct error-epilogue sites + 1 site that falls through to the
  common return = **15 branches converge on the common return** at
  +0x494.
- **744 bytes / 186 instructions of error-epilogue code** —
  **12.5 % of the entire function body**.
- A further **22 unconditional `b 0x100194dac`** at the function tail
  go to a C++ exception landing pad that is unreachable in prod
  (NDB never throws). That's another 88 bytes.

So **~13 % of `execLQHKEYREQ`'s bytes are error-return plumbing**, and
they are interleaved with the hot path, not packed at the end.

## Why interleaved cold code costs you

Every error-epilogue cluster does the same thing:

```asm
ldr   x8, [x19, #0x5c58]       ; m_jamBuffer    (jam preamble — 8 insns)
ldr   w9, [x8]
add   x10, x8, x9, lsl #2
add   w11, w25, #lineconst
str   w11, [x10, #0x4]
add   w9, w9, #1
and   w9, w9, #0x3ff
str   w9, [x8]
sub   x1, x29, #0x98           ; &handle             (releaseSections)
mov   x0, x19
bl    SimulatedBlock::releaseSections
ldur  x5, [x29, #-0xa0]        ; arg5
add   x2, x20, #0x30           ; arg2
mov   x0, x19                  ; arg0
mov   x1, x20                  ; arg1
mov   w3, #errcode             ; arg3
bl    Dblqh::earlyKeyReqAbort
b     +0x494                   ; back to common return
```

Same shape, 14 different copies, ranging from 5 to 25 instructions
each, scattered across the body. The CPU pays for this in three ways:

1. **L1-I footprint.** 744 bytes of cold instructions sit on the same
   I-cache lines as hot fall-through code. On a 64-byte line that's
   roughly 12 contaminated lines; on Apple's 128-byte lines it's
   ~6 lines. Every L1-I miss on a hot line is now amortised against
   bytes the CPU will never execute.
2. **Decoded-uop cache pressure.** On x86 the DSB caches decoded uops
   in 32–64-byte windows; cold bytes sitting between hot ones evict
   hot uops or fragment the windows so the CPU falls back to the
   legacy decoder. ARM front-ends have an analogous (smaller)
   structure.
3. **Branch-target buffer entries.** 15 forward branches all targeting
   the same common return + 22 landing-pad branches = 37 BTB entries
   spent on edges that are never taken in prod. BTB capacity is
   finite; entries spent here are entries unavailable to the hot
   indirect calls (`bl prepare_tab_pointers`, `bl getJoinAggState`,
   `bl prepareContinueAfterBlockedLab`).

## What outlining buys

Replace each in-line cluster with a single call:

```cpp
// In DblqhMain.cpp, file-static helpers:

[[gnu::cold, gnu::noinline]]
void Dblqh::earlyKeyReqAbort_releasing(Signal* signal,
                                       const LqhKeyReq* req,
                                       Uint32 errCode,
                                       Uint32 callerLine,
                                       SectionHandle& handle,
                                       TcConnectionrecPtr tcConnectptr) {
  jamLine(Uint16(callerLine));
  releaseSections(handle);
  earlyKeyReqAbort(signal, req, errCode, tcConnectptr);
}

[[gnu::cold, gnu::noinline]]
void Dblqh::LQHKEY_abort_cold(Signal* signal, int errCode,
                              Uint32 callerLine,
                              TcConnectionrecPtr tcConnectptr) {
  jamLine(Uint16(callerLine));
  LQHKEY_abort(signal, errCode, tcConnectptr);
}

[[gnu::cold, gnu::noinline]]
void Dblqh::LQHKEY_error_cold(Signal* signal, int errCode,
                              Uint32 callerLine,
                              TcConnectionrecPtr tcConnectptr) {
  jamLine(Uint16(callerLine));
  LQHKEY_error(signal, errCode, tcConnectptr);
}
```

Each call site collapses from 16-25 instructions to roughly 5
instructions:

```asm
mov   w2, #errcode             ; arg2 (or w3, depending on signature)
mov   w3, #__LINE__            ; callerLine
mov   x0, x19                  ; this
mov   x1, x20                  ; signal
bl    Dblqh::earlyKeyReqAbort_releasing
b     +0x494
```

`[[gnu::cold]]` tells the linker to emit the helper in the
`.text.cold.*` section, physically separated from `execLQHKEYREQ`'s
hot bytes.

### Concrete savings estimate

- **earlyKeyReqAbort sites (7):** 6 × ~80 bytes saved + 1 × ~20 bytes
  saved = ~500 bytes reclaimed.
- **LQHKEY_abort/error sites (7):** 7 × ~25 bytes saved on average =
  ~175 bytes reclaimed.
- **Net body shrink: ~675 bytes (~17 % of the original 5960 bytes
  that's currently in the function body)**, or about **170 fewer
  instructions** in the hot path.
- The 14 target labels (one per outlined helper call site) collapse
  to 3 BTB entries (one per cold helper) instead of 14, freeing
  budget for genuine hot calls.

## Why this is the right *first* item

Three reasons:

1. **It changes nothing about behaviour.** The outlined helper does
   exactly what the inline cluster did: jam, release sections, call
   the existing error function, return. No semantic change, no
   timing-sensitive code path moves. Reviewable by inspection.

2. **It is a prerequisite for clean measurements of later items.**
   Every subsequent item (Treqinfo flag collapse, jam reduction,
   `prepare_tab_pointers` short-circuit) measures its impact via
   function-size delta and `benchJoinAgg`. If the body still
   contains 13 % of cold epilogue interleaving, the noise floor on
   any of those measurements is contaminated. Outlining first
   establishes a clean baseline.

3. **It removes the strongest argument *against* further work.**
   Right now the function is 5960 bytes. After outlining, it should
   drop to ~5300 bytes. That makes register-allocation pressure
   (Item 8) measurably better — and may eliminate the need to do
   anything about it. Cheaper later items become easier to justify
   when the baseline is leaner.

## What can go wrong, and the mitigations

| Risk | Mitigation |
|---|---|
| Crash forensics: `jam()` line number now comes from inside the helper, not the original call site. | Pass `__LINE__` as `callerLine` and have the helper emit `jamLine(Uint16(callerLine))`. The jam ring entry shows the original site exactly as today. |
| Compiler ignores `[[gnu::cold]]` and inlines anyway. | `[[gnu::noinline]]` is mandatory in the same attribute list. Verify with `objdump` that the helper symbols exist as separate functions and that `execLQHKEYREQ`'s text section shrinks. |
| The `releaseSections(handle)` call needs a `SectionHandle&` reference, but `SectionHandle` is on the caller's stack. | Pass it by reference. Lifetime is fine — caller stack outlives the synchronous helper call. The current inline code already takes the address of the same stack object (`sub x1, x29, #0x98`). |
| Helper returns to a caller that immediately returns — extra `bl`/`ret` round-trip. | Two extra instructions per error invocation. Errors are off the hot path by definition; this is not a per-request cost. |
| Argument marshalling slightly differs from the inline form (e.g., site 6 doesn't currently call `releaseSections`). | Two helpers: `earlyKeyReqAbort_releasing` (calls releaseSections) and `earlyKeyReqAbort_simple` (does not). Pick at the call site based on whether `handle.clear()` was already done. The disassembly already differentiates these — sites 5, 6, 7 don't have releaseSections in their epilogue today. |
| MTR error-injection tests rely on a specific code path through the abort logic. | All MTR tests with `WITH_ERROR_INSERT=ON` exercise these paths. Run `./mtr ndb_*` and `./mtr --suite=rondis` before/after. |

## Verification protocol

1. **Build both variants** (prod, debug) before and after.
2. **Disassembly diff:**
   ```
   objdump --disassemble-symbols=__ZN5Dblqh13execLQHKEYREQEP6Signal \
     ndbmtd > exec_lqhkey.before
   # apply patch, rebuild
   objdump --disassemble-symbols=__ZN5Dblqh13execLQHKEYREQEP6Signal \
     ndbmtd > exec_lqhkey.after
   wc -l exec_lqhkey.before exec_lqhkey.after
   ```
   Expect ~170 fewer lines after.
3. **Symbol-size check:**
   ```
   nm -n ndbmtd | grep -E "earlyKeyReqAbort_|LQHKEY_(abort|error)_cold"
   ```
   Expect 3 new symbols, each in `__TEXT,__text_cold` (Mach-O) or
   `.text.cold` (ELF).
4. **Crash-trail check:** trigger a known abort (e.g.
   `ZNO_TC_CONNECT_ERROR` via existing MTR test) and confirm the
   produced jam ring entry's line number equals the original call
   site's line number, not the helper's.
5. **MTR runs:** `./mtr ndb_dd_basic ndb_basic ndb_short_signal_format
   rondis_basic rondis_advanced` — must pass without diff.
6. **Microbenchmarks:** `benchJoinAgg`, `bench_q12_dbtc`, median of
   5 runs each. Record the deltas in `results.md`. Even with no
   measured throughput improvement, the size reduction is justified
   on its own — it's a prerequisite for the rest of the plan.

## Estimated effort

- Helper definitions: ~30 lines of code in `DblqhMain.cpp` +
  declarations in `Dblqh.hpp`.
- Call-site rewrites: 14 sites, mechanical replacement.
- Measurement: 1–2 hours including MTR runs.
- Total: half a day, including code review.
