# Validation report: replica rowid forwarding + verification (TTL error 899 hardening)

Validated 2026-08-01 on macOS (Darwin 24.6.0, 12 cores), Debug build
(`debug_build/`, Unix Makefiles, clang, SAFE_MUTEX/ENABLED_DEBUG_SYNC,
WITH_ERROR_INSERT + VM_TRACE). mtr: `debug_build/mysql-test`, build-thread
300. Patches under test: `/tmp/ttl_rowid_fix/0001-*.patch` + `0002-*.patch`
(= branch `worktree-agent-a3f75365e3707f66b`, originally commits 0a3afbc1f5d
+ 1563db414bd on base 579ff6dd3c1). The fix was authored WITHOUT a build
environment; this validation discharges the "nothing compiled" risk (#1 in
fix_design.md) plus the runtime assumptions, on a tree that simultaneously
carried the sibling EI-5113 uncommitted deliverables.

## Headline: compile fixes required

**NONE.** Both patches compiled exactly as written on the first full build
(`make -j10`, exit 0, including the cmake re-run triggered by the
`ndb_version.h.in` change). No new compiler warnings in any fix-touched
file (the only build-log warnings are pre-existing `mgmsrv/Services.cpp`
ones, untouched). fix_design.md assumptions discharged by inspection +
build:

- `ndbd_replica_rowid_forwarding()` visible in DblqhMain.cpp
  (`#include <ndb_version.h>` at line 34).
- `TupKeyRef` field writes from Dblqh are legal (friend class; three
  pre-existing synthesized-TUPKEYREF precedents in DblqhMain.cpp).
- `AccKeyReq::setNoTTLDupConvert(Uint32, bool)` signature as assumed.
- In `continueACCKEYCONF`, `tcConnectptr` is the function PARAMETER
  (shadows the class member), so the REF block's `tcConnectptr.i` is the
  correct op record.
- `original_operation` (Uint8) and `nextReplica` (Uint16) members exist as
  used; receiver rowid intake asserts only `senderRef != DBTC` for
  rowid-carrying shapes, so the new rowid-carrying ZWRITE/ZUPDATE/ZDELETE
  wires pass the existing whitelists unchanged.

## Applying onto the sibling working tree: conflicts and the EI merge rule

The main tree carried the sibling's UNCOMMITTED EI-5113 work (timing-only
NR-copy throttle: Dblqh.hpp, DblqhMain.cpp, ERROR_codes.txt; plus new
ndb_ttl tests). Plain `git apply --3way` refuses on unstaged files
("does not match index"); the working method was `git add` the three dirty
files first, then `git apply --3way` (index == worktree makes the real
3-way merge possible), then `git reset` to unstage.

| File | Merge result |
|---|---|
| DblqhMain.cpp | clean auto-merge — sibling touches execCONTINUEB / accScanConfCopyLab / nextRecordCopy / closeCopyLab; fix touches execLQHKEYREQ / exec_acckeyreq / continueACCKEYCONF / acckeyconf_tupkeyreq / packLqhkeyreqLab (disjoint) |
| Dblqh.hpp | clean auto-merge — sibling: ZDELAY_NEXT_COPY_ROW + 2 EI members; fix: ZREPLICA_ROWID_MISMATCH 1245 |
| ERROR_codes.txt | **one conflict**: both sides append an EI entry after 5109 |
| ndb_version.h.in, ndberror.cpp, fix_design.md | clean (no sibling overlap) |
| patch 2 (on top of patch 1) | clean |

**EI merge rule (the resolution):** the merged catalogue must register BOTH
5113 (sibling: timing-only TTL copy-scan throttle) and 5118 (fix:
mismatch-escalation ndbabort), and `Next DBLQH` becomes **5119**. The
canonical fix branch documents only 5118 but now reserves
`Next DBLQH 5119` so the two series merge without renumbering (5114-5117
simply remain unused).

## Version-gate finding (fix author attention)

`MYSQL_VERSION_PATCH=15` on this branch while the gate cutoff is
`NDBD_REPLICA_ROWID_FORWARDING_2510 = 25.10.16`: binaries built from the
CURRENT tree self-report 25.10.15 and keep the entire attach-side feature
DORMANT. (The receiver-side verification of shapes that already carry
rowids — e.g. a plain ZINSERT dup-converted on a TTL backup — is live
regardless, but that arm is unreachable on healthy clusters.) Consequences:

1. Stock mtr/autotest on this tree exercises only the legacy (gate-off)
   wire shapes — bit-identical behavior to before the fix. Nothing in the
   default suite proves the active path.
2. If the fix misses the 25.10.16 train, bump the cutoff (fix_design.md
   assumption 4) — otherwise 25.10.16 binaries would activate against
   peers that don't have the code.
3. Validation therefore ran the whole matrix TWICE (below): Phase A
   as-built (gate off), Phase B with the floor temporarily lowered to
   25,10,15 in the GENERATED `debug_build/.../ndb_version.h` (validation
   harness tweak only — never committed) + `make ndbmtd`
   (DblqhMain.cpp.o rebuilt, so the active predicate is compiled in and
   deterministically true for the 25.10.15 test cluster on every hop).

## Test matrix (all mtr, build-thread 300)

Phase A = as-built, gate dormant: proves the fix does not disturb existing
behavior (including EI-5113 copy-window timing and RDRS purge).
Phase B = gate force-enabled: every forwarded ZINSERT_TTL / ZUPDATE /
ZDELETE carries the verification rowid and every non-primary hop runs the
found-case comparison — proves no false positives on healthy clusters
across node restarts (graceful + initial), copy races, blob/disk/unique
paths, 3-replica chains and the live purge.

| Test | Phase A | Phase B (gate ON) |
|---|---|---|
| ndb_ttl.ttl_nr_copy_window run 1 | pass 128.7s | pass 122.5s |
| ndb_ttl.ttl_nr_copy_window run 2 | pass 122.6s | pass 128.3s |
| ndb_ttl.ttl_rowid_899_control (EI 4019) | pass 0.4s | pass 0.4s |
| ndb_ttl.ttl_nr_copy_churn (3 restart cycles) | pass 144.2s | pass 150.3s |
| ndb_ttl.ttl_replica3_write (3 replicas / middle hop) | pass 2.3s | pass 2.4s |
| ndb_ttl.ttl_disk_expired_write (disk path) | pass 2.3s | pass 2.6s |
| ndb_ttl.ttl_blob_expired_reinsert | pass 5.0s | pass 4.9s |
| ndb_ttl.ttl_blob_part_leak | pass 4.2s | pass 4.1s |
| ndb_ttl.ttl_blob_scan_replica | pass 6.5s | pass 6.5s |
| ndb_ttl.ttl_insert | pass 37.8s | pass 38.0s |
| ndb_ttl.ttl_update | pass 37.2s | pass 37.2s |
| ndb_ttl.ttl_delete | pass 67.1s | pass 67.2s |
| ndb_ttl.ttl_replace | pass 50.0s | pass 50.1s |
| ndb_ttl.ttl_upsert | pass 53.0s | pass 53.2s |
| ndb_ttl_purge.ttl_purge_edge (live RDRS purge, takeover-scan deletes) | pass 63.0s | pass 63.1s |
| ttl_nr_copy_window on RESTORED baseline (fix reverted, sanity) | pass — see below | n/a |

32 fix-tree test executions, 32 pass, 0 fail, 0 skip. Phase B evidence
hunt: `grep -r "replica rowid mismatch"` and error-1245 patterns over every
Phase B vardir (kept per-run: var-fixvalB1..B6 before cleanup) — **zero
occurrences**, i.e. the always-equal verification never mis-fired, and
runtimes match Phase A (no measurable cost in these tests).

## Explicitly NOT validated (out of scope, stated per instruction)

Actively firing error 1245 (and EI 5118's ndbabort escalation) requires a
FABRICATED replica fork — a seeded presence/placement divergence between
replicas. No seeding lever exists in-tree yet (EI 5113 is deliberately
timing-only), so the mismatch arm was validated for compilation, placement
(precedent-identical synthesized-TUPKEYREF unwind), gating reachability and
absence of false positives — not for its positive firing. The planned
seeded single-row-drop error insert (findings.md section 9.1) is the
vehicle for that, and fix_design.md section 8 spells out the expected
outcome flip of the seeded repro under this fix.

## Canonical branch update

Branch `worktree-agent-a3f75365e3707f66b`
(worktree `.claude/worktrees/agent-a3f75365e3707f66b`), base 579ff6dd3c1:

- Compile corrections ported: none required.
- Commit 1 amended: ERROR_codes.txt now also bumps `Next DBLQH` 5113 ->
  5119 (the 5113 ENTRY itself deliberately stays out — it belongs to the
  sibling's uncommitted work; merge rule above), and a "Validated:"
  paragraph (build + matrix + gate note) appended before the trailer.
- Commit 2 re-applied unchanged, "Validated:" paragraph appended.
- New commit ids: **517d6e8fa94** (commit 1), **d429fda457f** (commit 2);
  authorship and Co-Authored-By trailers preserved. Content delta vs the
  original tip (1563db414bd) is exactly the one `Next DBLQH` line.
- Regenerated patches: `/tmp/ttl_rowid_fix_v2/0001-*.patch`,
  `/tmp/ttl_rowid_fix_v2/0002-*.patch`.

## Main tree restore

After validation the fix was removed from the main working tree
(checkout of the five touched tracked files to HEAD + re-apply of the
snapshot diff of the three sibling-modified files; fix_design.md and all
validation vardirs deleted). `git status --porcelain` and `git diff`
verified BYTE-IDENTICAL to the pre-validation snapshots
(/tmp/pre_fixval.status, /tmp/pre_fixval.diff): sibling EI-5113
deliverables intact, fix fully absent. ndbmtd rebuilt from the restored
tree (cmake re-ran; regenerated ndb_version.h no longer contains the fix
predicate) and ttl_nr_copy_window re-run once on the restored baseline:
PASS (120.1s, MTR exit 0). This report file is the only intentional new main-tree
artifact of the validation.
