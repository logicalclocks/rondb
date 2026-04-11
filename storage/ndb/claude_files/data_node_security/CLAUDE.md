# CLAUDE.md — Data Node Security

## Overview

Hardening RonDB data nodes against malicious NDB protocol messages from both API nodes and data nodes. Instead of crashing on malformed signals, we disconnect the offending node and log to the cluster log.

## Documents

| Document | Description |
|----------|-------------|
| `tckeyreq_security.md` | Plan for securing TCKEYREQ signal handling in Dbtc |
