# CLAUDE.md — Data Node Security

## Overview

Hardening RonDB data nodes against malicious NDB protocol messages from both API nodes and data nodes. Instead of crashing on malformed signals, we disconnect the offending node (Tier A) or log to the cluster log (Tier B) depending on whether the violation could be produced by valid user input.

## Documents

| Document | Description |
|----------|-------------|
| `tckeyreq_security.md` | Original plan for securing TCKEYREQ/KEYINFO/ATTRINFO in DBTC — now fully implemented as part of the v2 framework |
| `fragmented_signal_security.md` | Securing assembleFragments: Phase 1 hardening (done) + Phase 2 protocol extension (planned) |
| `tiered_response_policy.md` | Two-tier malicious input response policy — full design doc (v2, FINAL) |
| `team_briefing.md` | Team-facing summary: what we built, key decisions, and why |
| `audit_implementation_guide.md` | Implementation reference for security audits: categorization rules, step-by-step for adding new violation sites, API reference, file locations, checklist |
| `monitoring.md` | Operator monitoring: alert recommendations, Loki/Splunk filters, Prometheus scrape config, ndbinfo tables |
