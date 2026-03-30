# NDB Trace File Analysis Guide

## File Locations (MTR)

After a data node crash in an MTR run:

```
mysql-test/var/mysql_cluster.1/ndbd.N/
  ndb_N_error.log          # Crash summary (start here)
  ndbd.log                 # Program output + stack traces (key for segfaults!)
  ndb_N_trace.log.M        # Combined trace (can be very large)
  ndb_N_trace.log.M_tT     # Per-thread trace file
```

Note: The program output log may have different names depending on context:
- MTR: `ndbd.log`
- Autotest: `log.out`
- Manual runs: `ndb_N_out.log`

## Step 1: Read the Error Log

`ndb_N_error.log` contains the crash summary:

```
Time: Thursday 12 February 2026 - 15:29:19
Status: Temporary error, restart node
Message: Error OS signal received (...)
Error: 6000
Error data: Signal 11 received; Segmentation fault: 11
Pid: 57565 thr: 8
Trace file name: ndb_1_trace.log.1_t8
```

Key fields:
- **Error data**: Crash type. `Signal 11` = segfault, `Check ... failed` = ndbrequire
- **thr: N**: Thread number that crashed
- **Trace file name**: The specific per-thread trace file to examine

## Step 2: Read the Program Output Log (ndbd.log)

`ndbd.log` contains program stdout/stderr including debug printouts and,
critically, **native stack traces** from crashes. For segfaults this is often
more useful than the jam trace since it shows the actual C++ call stack.

Example stack trace:
```
For help with below stacktrace consult:
https://dev.mysql.com/doc/refman/en/using-stack-trace.html
2026-02-12T12:29:19 [ndbd] INFO -- Received signal 11. Running error handler.
Base address/slide: 0xc74000
stack_bottom = 0 thread_stack 0x0
0   ndbmtd    0x0000000100ca5c20 my_print_stacktrace + 72
1   ndbmtd    0x00000001016e8e2c ndb_print_stacktrace + 116
2   ndbmtd    0x0000000100c7b598 handler_error + 260
3   libsystem_platform.dylib  _sigtramp + 56
4   ndbmtd    0x00000001015d1fd8 Trpman::distribute_signal() + 816
```

The frames above `_sigtramp` (frames 0-2) are the crash handler itself.
The frame immediately below `_sigtramp` (frame 4 here) is where the crash
occurred. Use `atos` or `llvm-symbolizer` with the base address to get
source file and line:
```bash
atos -o ndbmtd -s 0xc74000 0x00000001015d1fd8
```

## Step 3: Read the Per-Thread Trace File

Each `_tN` file is the trace for one block thread. It contains **signal prints**
mixed with **jam content**, listed in reverse chronological order (NEWEST first).

### Signal Entry Format

```
--------------- Signal ----------------
r.bn: 247/3 "DBLQH", r.nodeId: 1, ..., r.sigId: 12345, gsn: 353 "SCAN_FRAGREQ", prio: JBB
s.threadId: 5, ..., s.bn: 248/1 "DBSPJ", s.proc: 1, ..., length: 14, trace: 0, #sec: 1, fragInf: 0
 H'00000001 H'000004cc ...
    ---- Signal H'...: Jam content, OLDEST first ----
    SOURCE FILE                       LINE NUMBERS ##### OR DATA d#####
    DblqhMain.cpp                      01033  01284  01300
```

Fields:
- `r.bn: 247/3 "DBLQH"` — Receiver block number / instance "BlockName"
- `r.sigId: 12345` — Receiver's signal ID (unique per thread, monotonically increasing)
- `gsn: 353 "SCAN_FRAGREQ"` — Signal type (GlobalSignalNumber)
- `s.bn: 248/1 "DBSPJ"` — Sender block / instance
- `s.sigId: 6789` — Sender's signal ID (the signal that caused this one to be sent)
- `s.threadId` — Sender thread (only for local signals)
- `prio` — JBA (high priority) or JBB (normal priority)
- `length` — Number of Uint32 words in signal payload
- `#sec` — Number of long signal sections
- `fragInf` — Signal fragmentation (0=not fragmented, 1=first, 2=middle)
- `H'xxxxxxxx` lines — Signal data words in hex

### Signal Connections (r.sigId / s.sigId)

Signals on the same thread form chains via `r.sigId` and `s.sigId`:
- `r.sigId` is the signal's own ID on the receiving thread
- `s.sigId` is the ID of the signal that **caused** this signal to be sent

For continuation signals (e.g., ACC_CHECK_SCAN sent by DBLQH to itself during
scan processing), `s.sigId` points back to the **previous continuation** or the
**original triggering signal** in the chain. This lets you trace a scan's full
processing history across multiple continuations.

**Example**: A scan started by SCAN_FRAGREQ (sigId 43172) produces a chain of
ACC_CHECK_SCAN continuations. The latest ACC_CHECK_SCAN has `r.sigId: 43179`
and `s.sigId: 43172`, linking it back to the originating SCAN_FRAGREQ. Other
unrelated signals (e.g., a different SCAN_FRAGREQ with sigId 43177) may appear
between them in the trace — those are interleaved signals on the same thread,
not part of this scan's chain.

**Important**: Since the trace is printed NEWEST first, the crashing signal
appears at the top. Its jam content (OLDEST first within the signal) spans the
entire execution from initial setup through all continuations and real-time
breaks up to the crash. Interleaved signals from other operations appear
between continuation signals in the trace listing but are NOT part of the
crashing signal's jam content.

### Jam Content Format

The jam content following each signal shows the **code path** taken during
processing. It is a breadcrumb trail of source file line numbers recorded by
jam macros.

```
    SOURCE FILE                       LINE NUMBERS ##### OR DATA d#####
    DblqhMain.cpp                      01033  01284 d00003  01300
```

- **Plain numbers** (e.g. `01033`, `01284`): Line numbers from `jam()` or
  `jamEntry()` or `jamDebug()` macro calls. `jamDebug()` only records in
  debug builds. `jamEntry()` is equivalent to `jam()`.
- **`d` prefixed numbers** (e.g. `d00003`): Data values from `jamLine(N)` or
  `jamData(N)` macro calls. These record runtime values (loop counters,
  enum values, etc.) interleaved with the line-number trail.

The jam content is listed **OLDEST first** within each signal's section, so
you read left-to-right, top-to-bottom to follow the execution path.

## Step 3: Interpreting Common Crash Types

### Segfault (Signal 11)

The jam trace shows the code path up to the point of the crash. The last
jam entry is typically the last `jam()` call before the segfault occurred.
The actual crashing instruction is between the last jam entry and the next
`jam()` call in the source code.

Note: The crashing signal might not appear in the trace if the segfault
occurred before the signal header was fully recorded. Look at the most
recent signal entry's jam content for clues.

### ndbrequire Failure

```
Error data: DBLQH (Line: 1005) Check signal->header.m_noOfSections == 0 failed
```

The error message directly tells you the file (block name), line number,
and the failed condition. The jam trace provides additional context about
how execution reached that point.

## Step 4: Cross-Reference with Source

Use jam line numbers to trace the execution path in the source:

```bash
# Find the jam() calls around a specific line
grep -n 'jam\|jamDebug\|jamLine\|jamData\|jamEntry' DblqhMain.cpp | grep -A2 -B2 '1033'
```

## Tips

- In ndbmtd, each thread runs one or more block instances. The `r.bn`
  field shows which block instance processed each signal.
- Thread numbering: main thread is typically 0, receive threads, LDM threads,
  TC threads, etc. follow based on ThreadConfig.
- Block numbers: DBLQH=247, DBSPJ=248, DBTUP=249, DBACC=250, DBTC=245,
  TRPMAN=266, THRMAN=265, DBQLQH=267, V_QUERY=0x111
- `"Unknown Signal"` entries at the end of the trace file mean the signal
  metadata was overwritten in the circular buffer — only jam content remains.
