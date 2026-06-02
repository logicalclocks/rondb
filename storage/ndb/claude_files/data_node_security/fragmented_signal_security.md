# Secure Fragmented Signal Protocol

## Phase 1: Defensive Hardening (DONE)

Committed in `fa1713b`. Fixes crashes and memory corruption in `assembleFragments`
with no protocol change:

- Runtime `sectionNo < 3` bounds check (was ndbassert only)
- Replace `ndbabort()` on fragment hash full with log + drop
- Replace `ndbabort()` on fragment not found with log + drop
- Reject new sections appearing in subsequent fragments
- Same fixes in `assembleDroppedFragments`

## Phase 1b: Security-system integration

The `sectionNo >= 3` drop paths in `assembleFragmentsSlow` now also report a
`MALICIOUS_SIGNAL_REPORT` to QMGR via `REPORT_MALICIOUS_SIGNAL`, using the new
`VT_FRAGMENT_INVALID_SECTION_NO` violation type. This is **Tier A**: a section
number outside `[0,2]` is a transporter-framing detail that no SQL/HTTP/REST/Redis
user input can influence (categorization rule, `tiered_response_policy.md` §6), so
it is user-untriggerable and listed as a Tier A example there. Sections are
released before the report (the report reuses the signal buffer and the caller
returns immediately).

The **hash-full** and **fragment-not-found** drops are deliberately NOT reported:
a full hash can be caused by aggregate load, and a "not found" fragment can be the
innocent tail of a train whose head we ourselves dropped — reporting either would
risk punishing the wrong node.

### KNOWN GAP — fragment-hash exhaustion wedge (for later audit)

Incomplete fragment trains from a *still-connected* node are only reclaimed on node
failure (`doCleanupFragInfo`, keyed by `failedNodeId`; `FragmentInfo` has no age /
timestamp field). A node that opens many fragmented signals and never sends the
final fragment can therefore fill `c_fragmentInfoHash` permanently — after which all
fragmented signals to that block are silently dropped until the offender disconnects
or the node restarts. The Phase 1 hardening turned this from a crash into a *silent*
partial outage, and because the hash-full path is (correctly) not reported, the wedge
is invisible to the security counters. A future fix would add age-based reclamation of
stale incomplete trains (new timestamp field on `FragmentInfo` + a periodic sweep),
which is a broader change to shared transporter infrastructure and is deferred.

## Phase 2: Protocol Extension (PLANNED)

Extend the first fragment's wire format to include declared total section sizes
so the receiver can validate accumulated data against declared totals.

### Wire Format Change

**Current (all fragments):**
```
theData: [signalData] [secNos[0..secs-1]] [fragmentId]
signal->length() = origSigLen + secs + 1
```

**New first fragment only (from new-version senders):**
```
theData: [signalData] [secNos[0..secs-1]] [fragmentId] [totalSz[0..N-1]] [totalNoOfSections]
signal->length() = origSigLen + secs + 1 + N + 1
```

`totalNoOfSections` (1-3) is the last word so the receiver reads it first.
Subsequent fragments (fragInfo=2/3) are unchanged.

### Implementation Steps

1. Version check function `ndbd_secure_frag_signal()` in `ndb_version.h.in`
2. Add `m_totalNoOfSections` + `m_expectedSectionSz[3]` to `FragmentInfo`
3. Add `m_totalNoOfSections` + `m_totalSectionSz[3]` to `FragmentSendInfo`
4. `sendFirstFragment` (segmented): store total section sizes
5. `sendFirstFragment` (linear): same
6. `sendNextSegmentedFragment`: append extra trailer on first fragment
7. `sendNextLinearFragment`: same
8. `assembleFragments` first fragment: parse total sizes from trailer
9. `assembleFragments` subsequent fragments: validate accumulated size after linkSegments
10. `assembleFragments` final fragment: verify final size == declared total
11. Adjust `sigLen` parsing for new format (read totalNoOfSections from last word)

### Backward Compatibility

- New senders check target node versions; send old format if any target is old
- New receivers check sender version; old-format signals get Phase 1 protection only
- Rolling upgrades: mixed-version window uses old format automatically

### Files

| File | Changes |
|------|---------|
| `storage/ndb/include/ndb_version.h.in` | Version check function |
| `storage/ndb/src/kernel/vm/SimulatedBlock.hpp` | FragmentInfo + FragmentSendInfo structs |
| `storage/ndb/src/kernel/vm/SimulatedBlock.cpp` | Sender + receiver changes |
