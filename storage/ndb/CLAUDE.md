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

- `findings.md` — Full audit findings (mechanism verdicts M1-M6, restart scenarios S1-S6, end-to-end 899 interleaving, seed analysis with fixed-bug history, observability, test designs, hardening fix, continuation checklist)
- `repro_test.md` — Natural-window verdict (NR-copy window closed, 3 traces agree) + ttl_nr_copy_window regression test + timing-only ERROR_INSERT 5113
- `probe_tests.md` — Replica-consistency invariant include, 899 detector control test (SQL 1205 + Warning 1297 signature), copy×churn probe results
- `impact_analysis.md` — Always-carry-rowid principle blast radius, per-subsystem impact, failure policy, rollout plan; fully-replicated findings (deferred)
- `normal_insert_analysis.md` — Proof a normal INSERT cannot create the fork; abort-ordering closure; benign transient-899 windows on healthy clusters
- `fix_design.md` — Design of the replica-rowid forwarding + verification fix (error 1245, EI 5118, version gate): decision log, REDO safety proof, NR-copy interplay, mixed-version behavior
- `validation_report.md` — Fix build+test validation (33/33 green, both gate states, zero 1245 false positives); version-gate dormancy caveat; ERROR_codes merge rule
