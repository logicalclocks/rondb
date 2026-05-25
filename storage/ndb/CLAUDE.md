# CLAUDE.md — NDB Storage Engine

## Claude Context Files

Detailed documentation is organized by topic in `claude_files/`. Read the relevant files when working on a specific area.

| Topic | Directory | Description |
|-------|-----------|-------------|
| SET Config Param | `claude_files/set_config_param/` | Adding runtime-settable config parameters via the MGM client SET command |
| Data Node Security | `claude_files/data_node_security/` | Hardening data nodes against malicious NDB protocol messages |

### set_config_param

- `architecture.md` — Signal flow from MGM client through to data node, design notes
- `howto_add_new_param.md` — Step-by-step guide (only 2 files need changes)
- `reference.md` — File locations, what doesn't need changes, verification checklist

### data_node_security

- `tckeyreq_security.md` — Plan for securing TCKEYREQ handling: disconnect malicious senders instead of crashing
