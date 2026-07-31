# CLAUDE.md — NDB Storage Engine

## Claude Context Files

Detailed documentation is organized by topic in `claude_files/`. Read the relevant files when working on a specific area.

| Topic | Directory | Description |
|-------|-----------|-------------|
| SET Config Param | `claude_files/set_config_param/` | Adding runtime-settable config parameters via the MGM client SET command |
| TTL / Error 899 rowid | `claude_files/ttl_899_rowid/` | TTL "leftover rowid" investigation: purge is replica-safe, but rowid-less ZINSERT_TTL/ZWRITE forwarding silently amplifies replica divergence into permanent 899; test plan + hardening proposal |

### set_config_param

- `architecture.md` — Signal flow from MGM client through to data node, design notes
- `howto_add_new_param.md` — Step-by-step guide (only 2 files need changes)
- `reference.md` — File locations, what doesn't need changes, verification checklist

### ttl_899_rowid

- `findings.md` — Full audit findings (mechanism verdicts M1-M6, restart scenarios S1-S6, end-to-end 899 interleaving, seed analysis with fixed-bug history, observability, deterministic + probe test designs, hardening fix, continuation checklist)
