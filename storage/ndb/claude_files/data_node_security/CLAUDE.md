# CLAUDE.md — Data Node Security

## Overview

Hardening RonDB data nodes against malicious NDB protocol messages from both API nodes and data nodes. Instead of crashing on malformed signals, we disconnect the offending node and log to the cluster log.

## Documents

| Document | Description |
|----------|-------------|
| `tckeyreq_security.md` | Plan for securing TCKEYREQ signal handling in Dbtc |
| `fragmented_signal_security.md` | Securing assembleFragments: Phase 1 (done) + Phase 2 (planned) |
| `tiered_response_policy.md` | Three-tier malicious input response policy — full design doc (FINAL) |
| `team_briefing.md` | Team-facing summary: what we're building, key decisions, and why |
| `audit_implementation_guide.md` | Implementation reference for security audits: categorization rules, step-by-step for adding new violation sites, API reference, file locations, checklist |
| `tier_c_baseline_methodology.md` | How to measure your cluster's overload-count baseline before enabling the Tier C cluster-side safety net |
| `monitoring.md` | Operator monitoring: alert recommendations, Loki/Splunk filters, Prometheus scrape config, observation-mode caveats |
