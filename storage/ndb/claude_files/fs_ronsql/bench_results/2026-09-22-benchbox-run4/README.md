# Run 4 — 2026-09-22, benchmark computer (Linux x86_64, host `localhost.localdomain`), `prod_build` of `/home/mikael/mysql_trees/rondb_2604`

The M3.0 performance census (`../../m3_plan.md` §3): `ronsql_bench_matrix.py
--queries all --load both --sf 1 --engines ronsql,mysqld_nopush --threads 1,8
--seconds 5 --cpubind census.cnf`, started 2026-09-22 20:17, 285 cases.
Full registry: `core` (10), `fs` (11), `offline_fs` (7), `tpch_cte` (5 + 5
official), `fs_hw` (23 + 3 twins).

| file | content |
|---|---|
| `results.json`, `report.md` | the driver's outputs (sections A–G) |
| `triage.md` | the triage as pasted by the user (first script version; its baseline column is the cross-machine laptop comparison) |
| `triage_v2.md` | the current `ronsql_bench_triage.py` on the same data: coverage line, cross-host handling, `ndbinfo.jit` columns |
| `cluster_extra_off.cnf` | the cluster configuration the run used (cpubind sets; `NumCPUs` left at the suite default of 4) |
| `cases/off_ronsql_offline_fs_batch_T8.txt` | F27: the case that took the cluster down (20008 → 1869 → node failure) |
| `cases/off_ronsql_core_pk_lookup_T1.txt`, `off_mysqld_nopush_core_pk_lookup_T1.txt` | F25 / F26: bimodal PK-lookup latency on both engines, the EXPLAIN |
| `cases/off_ronsql_fs_hw_floor_T1.txt`, `_fs_hw_composite_point_T1`, `_fs_hw_sessions_window2h_T1`, `_fs_floor_T1`, `_fs_latest_T1`, `_fs_hw_agg_point_T1`, `_fs_floor_T8` | F25: per-phase p95 / p99 of the affected and unaffected shapes, and the T=8 case without the stall |
| `cases/off_ronsql_core_in_pk100_T1.txt`, `_core_in_idx100_T1` | F23: `Execute as table scan.` for the complete-PK and secondary-index IN lists |
| `cases/off_ronsql_core_group_many_T1.txt`, `_core_group_few_T1`, `_tpch_q2_T1` | F24: the many-group cost and the CTE form |
| `cases/off_ronsql_core_scan_agg_T1.txt`, `on_ronsql_core_scan_agg_T1.txt` | compiled interpreter OFF / ON on 6 M rows |

The other 268 case logs stay in `/tmp/census_run4/cases/` on the box (and
`/Users/mikael/census4/census_run4/` on the laptop).

Coverage: T=1 complete; T=8 reached the 12 `fs` entries, then
`offline_fs_batch` on RonSQL failed and the data nodes went down.
`fs_hw_hash_point` failed because the hash twin was not loaded.

Reading and findings: `m3_plan.md` §6, `findings/bench.md` F23–F27.
