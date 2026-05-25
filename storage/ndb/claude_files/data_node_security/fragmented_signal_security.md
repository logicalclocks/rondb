# Secure Fragmented Signal Protocol

## Phase 1: Defensive Hardening (DONE)

Committed in `fa1713b`. Fixes crashes and memory corruption in `assembleFragments`
with no protocol change:

- Runtime `sectionNo < 3` bounds check (was ndbassert only)
- Replace `ndbabort()` on fragment hash full with log + drop
- Replace `ndbabort()` on fragment not found with log + drop
- Reject new sections appearing in subsequent fragments
- Same fixes in `assembleDroppedFragments`

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
