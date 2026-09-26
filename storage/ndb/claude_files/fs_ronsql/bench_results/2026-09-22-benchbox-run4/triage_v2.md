# RonSQL performance triage

run: host=localhost.localdomain/x86_64 build=/home/mikael/mysql_trees/rondb_2604/prod_build sf=1.0 threads=[1, 8] engines=ronsql,mysqld_nopush arms=OFF,ON started=2026-09-22T20:17:28 baseline=fs_ronsql/bench_results/2026-09-11-prod_build-run2 (host mikaels-MacBook-Pro.local/arm64)

**Coverage: T=8 has no case for 52 of 64 queries** — the last case at T=8 was off_ronsql_offline_fs_batch_T8 (FAILED: cluster unreachable after the case (   33. request failed with status 500: ): ERROR 1296 (HY000) at line 1: Got error 157 'Connection to NDB failed' from NDBINF); the run stopped or the selection changed. Missing: offline_fs_multi, offline_fs_join_body, offline_fs_anti, offline_fs_wide, offline_fs_chain, offline_fs_scalar, tpch_q2, tpch_q11, tpch_q13, tpch_q15, tpch_q22, core_pk_lookup, ….

**Baseline ran on another machine** (mikaels-MacBook-Pro.local/arm64 vs localhost.localdomain/x86_64): its verdicts compare hardware as much as code and are listed in section 5 as information only; they do not put a query on the needs-work list.

MySQL reference engine: `mysqld_nopush`; compiler arm for the engine comparison: `OFF`. Latency ratio = RonSQL avg / MySQL avg at T=1; throughput ratio = MySQL q/s / RonSQL q/s at T=8 (both > 1 = RonSQL behind). Classes: PARITY <= 1.25x, SLOW <= 3.00x, CRITICAL above; FAIL = the RonSQL case did not run.

## 1. Needs work (13 of 56 RonSQL-capable queries)

| # | query | latency T=1 | throughput T=8 | RonSQL / MySQL avg | RonSQL q/s / MySQL q/s | top phase | rows/req | plan pins | baseline |
|---:|---|---|---|---:|---:|---|---:|---|---|
| 1 | offline_fs_batch | PARITY (0.02x) | FAIL: cluster unreachable after the case (   33. request failed with status 500: ): ER | 1.54s / 78.73s | - / - | firstbatch 1.54s | 0.0 | - | - |
| 2 | fs_hw_hash_point | FAIL: [semantic] Caught exception: Failed to get table. Note that RonSQL only supports | N/A | - / - | - / - | - | - | - | - |
| 3 | core_in_pk100 | CRITICAL (793.03x) | N/A | 398.46ms / 502us | - / - | firstbatch 398.23ms | 0.0 | - | - |
| 4 | fs_hw_agg_batch10 | CRITICAL (276.76x) | N/A | 215.86ms / 780us | - / - | firstbatch 215.63ms | 0.0 | - | cross-host, see §5 |
| 5 | fs_hw_agg_batch100_window | CRITICAL (150.48x) | N/A | 981.11ms / 6.52ms | - / - | firstbatch 980.79ms | 0.0 | - | cross-host, see §5 |
| 6 | fs_hw_agg_batch100 | CRITICAL (128.99x) | N/A | 989.32ms / 7.67ms | - / - | firstbatch 988.99ms | 0.0 | - | cross-host, see §5 |
| 7 | fs_hw_agg_batch1000 | CRITICAL (92.02x) | N/A | 8.73s / 94.88ms | - / - | firstbatch 8.73s | 0.0 | - | cross-host, see §5 |
| 8 | core_in_idx100 | CRITICAL (81.80x) | N/A | 403.25ms / 4.93ms | - / - | firstbatch 402.95ms | 0.0 | - | - |
| 9 | fs_hw_strkey_batch100 | CRITICAL (65.86x) | N/A | 742.91ms / 11.28ms | - / - | firstbatch 742.59ms | 0.0 | - | cross-host, see §5 |
| 10 | tpch_q22 | CRITICAL (5.05x) | N/A | 1.10s / 216.91ms | - / - | firstbatch 1.10s | 0.0 | - | - |
| 11 | tpch_q2 | CRITICAL (4.10x) | N/A | 1.12s / 273.78ms | - / - | firstbatch 1.12s | 0.0 | - | - |
| 12 | fs_point | PARITY (0.97x) | SLOW (1.46x) | 225us / 230us | 14189 / 20752 | firstbatch 156us | 0.0 | - | - |
| 13 | fs_floor | SLOW (1.38x) | SLOW (1.31x) | 180us / 130us | 42778 / 56146 | firstbatch 148us | 0.0 | - | - |

## 2. Every RonSQL-capable query at T=1, arm OFF

| query | class | RonSQL avg | p99 | MySQL avg | ratio | execute share | http+client | top phase | rows/req | OFF/ON |
|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| core_avg_range | PARITY | 257us | 391us | 616us | 0.42x | 86% | 31us | firstbatch 219us | 0.0 | 1.00x |
| core_group_few | PARITY | 160.34ms | 196.09ms | 617.89ms | 0.26x | 100% | 248us | firstbatch 160.05ms | 0.0 | 0.94x |
| core_group_many | PARITY | 1.14s | 1.37s | 2.22s | 0.51x | 99% | 8.94ms | firstbatch 1.04s | 0.0 | 0.95x |
| core_idx_range | PARITY | 2.49ms | 5.04ms | 6.13ms | 0.41x | 98% | 35us | firstbatch 2.45ms | 0.0 | 0.94x |
| core_in_idx100 | CRITICAL | 403.25ms | 508.23ms | 4.93ms | 81.80x | 100% | 173us | firstbatch 402.95ms | 0.0 | 1.02x |
| core_in_pk100 | CRITICAL | 398.46ms | 510.35ms | 502us | 793.03x | 100% | 174us | firstbatch 398.23ms | 0.0 | 0.99x |
| core_pass_range | PARITY | 492us | 556us | 978us | 0.50x | 93% | 33us | drain 274us | 999.8 | 1.00x |
| core_pk_lookup | PARITY | 404us | 1.13ms | 439us | 0.92x | 92% | 28us | firstbatch 372us | 1.0 | 1.06x |
| core_scan_agg | PARITY | 642.03ms | 780.99ms | 1.99s | 0.32x | 100% | 252us | firstbatch 641.70ms | 0.0 | 0.99x |
| core_scan_filter | PARITY | 318.87ms | 384.98ms | 680.50ms | 0.47x | 100% | 259us | firstbatch 318.53ms | 0.0 | 0.99x |
| fs_batch | PARITY | 640us | 788us | 7.36ms | 0.09x | 91% | 41us | firstbatch 504us | 0.0 | 0.97x |
| fs_dnf | PARITY | 971us | 1.32ms | 112.32ms | 0.01x | 94% | 40us | firstbatch 874us | 0.0 | 1.04x |
| fs_floor | SLOW | 180us | 1.13ms | 130us | 1.38x | 83% | 28us | firstbatch 148us | 0.0 | 0.94x |
| fs_freshness | PARITY | 2.67ms | 3.50ms | 122.17ms | 0.02x | 98% | 42us | firstbatch 2.56ms | 0.0 | 0.95x |
| fs_history | PARITY | 1.35ms | 2.27ms | 1.98ms | 0.68x | 96% | 44us | firstbatch 346us | 1999.9 | 1.01x |
| fs_hw_agg_batch10 | CRITICAL | 215.86ms | 275.46ms | 780us | 276.76x | 100% | 178us | firstbatch 215.63ms | 0.0 | 1.01x |
| fs_hw_agg_batch100 | CRITICAL | 989.32ms | 1.21s | 7.67ms | 128.99x | 100% | 219us | firstbatch 988.99ms | 0.0 | 1.07x |
| fs_hw_agg_batch1000 | CRITICAL | 8.73s | 10.56s | 94.88ms | 92.02x | 100% | 294us | firstbatch 8.73s | 0.0 | 1.12x |
| fs_hw_agg_batch100_window | CRITICAL | 981.11ms | 1.22s | 6.52ms | 150.48x | 100% | 221us | firstbatch 980.79ms | 0.0 | 1.00x |
| fs_hw_agg_filter | PARITY | 76us | 125us | 218us | 0.35x | 56% | 29us | firstbatch 41us | 0.0 | 1.00x |
| fs_hw_agg_greatest | PARITY | 77us | 136us | 210us | 0.36x | 56% | 29us | firstbatch 42us | 0.0 | 1.02x |
| fs_hw_agg_point | PARITY | 80us | 148us | 210us | 0.38x | 58% | 29us | firstbatch 44us | 0.0 | 1.01x |
| fs_hw_agg_window7d | PARITY | 76us | 123us | 221us | 0.35x | 56% | 29us | firstbatch 41us | 0.0 | 0.98x |
| fs_hw_collect5 | PARITY | 77us | 138us | 210us | 0.37x | 58% | 29us | firstbatch 38us | 4.2 | 0.94x |
| fs_hw_collect50 | PARITY | 78us | 144us | 213us | 0.36x | 59% | 28us | firstbatch 36us | 17.6 | 1.00x |
| fs_hw_collect50_cte | PARITY | 84us | 150us | 236us | 0.35x | 61% | 28us | firstbatch 41us | 17.6 | 1.03x |
| fs_hw_collect5_cte | PARITY | 76us | 136us | 232us | 0.33x | 58% | 28us | firstbatch 38us | 4.2 | 0.95x |
| fs_hw_composite_point | PARITY | 93us | 1.13ms | 321us | 0.29x | 66% | 28us | firstbatch 60us | 0.0 | 1.14x |
| fs_hw_floor | PARITY | 140us | 1.12ms | 167us | 0.84x | 79% | 27us | firstbatch 110us | 0.0 | 1.33x |
| fs_hw_hash_point | FAIL | - | - | - | - | - | - | - | - | - |
| fs_hw_sessions_window2h | PARITY | 83us | 1.12ms | 280us | 0.30x | 62% | 28us | firstbatch 50us | 0.0 | 0.89x |
| fs_hw_snow1_batch100 | PARITY | 37.74ms | 69.54ms | 44.99ms | 0.84x | 100% | 98us | firstbatch 37.45ms | 88.9 | 0.84x |
| fs_hw_snow1_point | PARITY | 227us | 288us | 1.11ms | 0.20x | 76% | 42us | firstbatch 128us | 0.9 | 1.13x |
| fs_hw_snow2_left_chain | PARITY | 204us | 263us | 968us | 0.21x | 82% | 29us | firstbatch 138us | 0.8 | 1.00x |
| fs_hw_snow2_left_single | PARITY | 192us | 242us | 971us | 0.20x | 81% | 29us | firstbatch 126us | 1.0 | 0.95x |
| fs_hw_snow2_point | PARITY | 196us | 256us | 997us | 0.20x | 80% | 30us | firstbatch 130us | 0.8 | 0.94x |
| fs_hw_strkey_batch100 | CRITICAL | 742.91ms | 961.97ms | 11.28ms | 65.86x | 100% | 186us | firstbatch 742.59ms | 0.0 | 1.00x |
| fs_hw_strkey_point | PARITY | 79us | 144us | 220us | 0.36x | 58% | 28us | firstbatch 45us | 0.0 | 0.97x |
| fs_latest | PARITY | 579us | 1.17ms | 626us | 0.92x | 94% | 29us | firstbatch 484us | 100.0 | 1.01x |
| fs_minmax | PARITY | 3.60ms | 4.51ms | 9.58ms | 0.38x | 98% | 59us | firstbatch 3.48ms | 0.0 | 1.00x |
| fs_nation | PARITY | 24.61ms | 29.43ms | 43.56ms | 0.56x | 100% | 100us | firstbatch 24.44ms | 0.0 | 0.96x |
| fs_point | PARITY | 225us | 360us | 230us | 0.97x | 80% | 33us | firstbatch 156us | 0.0 | 1.01x |
| fs_supplier | PARITY | 2.84ms | 3.71ms | 6.68ms | 0.43x | 98% | 39us | firstbatch 2.76ms | 0.0 | 0.94x |
| fs_topk | PARITY | 25.90ms | 32.31ms | 989.60ms | 0.03x | 99% | 205us | firstbatch 21.94ms | 1382.1 | 0.98x |
| offline_fs_anti | PARITY | 205.84ms | 230.34ms | 218.48ms | 0.94x | 100% | 144us | firstbatch 205.56ms | 0.0 | 0.96x |
| offline_fs_batch | PARITY | 1.54s | 1.74s | 78.73s | 0.02x | 100% | 246us | firstbatch 1.54s | 0.0 | 1.00x |
| offline_fs_chain | PARITY | 1.12s | 1.14s | 2.17s | 0.51x | 100% | 263us | firstbatch 1.12s | 0.0 | 0.97x |
| offline_fs_join_body | PARITY | 2.00s | 2.00s | 11.56s | 0.17x | 100% | 229us | firstbatch 2.00s | 0.0 | 0.96x |
| offline_fs_multi | PARITY | 1.46s | 1.44s | 41.91s | 0.03x | 100% | 292us | firstbatch 1.46s | 0.0 | 0.96x |
| offline_fs_scalar | PARITY | 1.12s | 1.15s | 2.10s | 0.54x | 100% | 258us | firstbatch 1.12s | 0.0 | 1.02x |
| offline_fs_wide | PARITY | 2.03s | 2.02s | 82.29s | 0.02x | 100% | 275us | firstbatch 2.03s | 0.0 | 0.99x |
| tpch_q11 | PARITY | 102.48ms | 162.00ms | 2.04s | 0.05x | 100% | 169us | firstbatch 102.20ms | 0.0 | 0.85x |
| tpch_q13 | PARITY | 1.11s | 1.41s | 1.49s | 0.75x | 100% | 260us | firstbatch 1.11s | 0.0 | 0.97x |
| tpch_q15 | PARITY | 54.92ms | 88.61ms | 156.93ms | 0.35x | 100% | 200us | firstbatch 54.61ms | 0.0 | 0.95x |
| tpch_q2 | CRITICAL | 1.12s | 1.21s | 273.78ms | 4.10x | 100% | 184us | firstbatch 1.12s | 0.0 | 1.03x |
| tpch_q22 | CRITICAL | 1.10s | 1.35s | 216.91ms | 5.05x | 100% | 267us | firstbatch 1.10s | 0.0 | 0.99x |

## 3. Throughput at T=8 (q/s) and scaling T8/T1

| query | class | RonSQL q/s | MySQL q/s | MySQL/RonSQL | RonSQL scale | MySQL scale | RonSQL avg @T8 | p99 @T8 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| fs_batch | PARITY | 4148 | 2385 | 0.57x | 2.72x | 17.56x | 1.90ms | 2.77ms |
| fs_dnf | PARITY | 2221 | 588 | 0.26x | 2.18x | 66.07x | 3.58ms | 5.39ms |
| fs_floor | SLOW | 42778 | 56146 | 1.31x | 8.08x | 7.31x | 173us | 419us |
| fs_freshness | PARITY | 635 | 289 | 0.46x | 1.70x | 35.26x | 10.70ms | 23.43ms |
| fs_history | PARITY | 3958 | 1635 | 0.41x | 5.51x | 3.24x | 1.91ms | 3.01ms |
| fs_latest | PARITY | 2664 | 3089 | 1.16x | 1.57x | 1.94x | 2.62ms | 5.77ms |
| fs_minmax | PARITY | 509 | 455 | 0.89x | 1.84x | 4.36x | 13.06ms | 29.89ms |
| fs_nation | PARITY | 70 | 86 | 1.23x | 1.73x | 3.76x | 95.59ms | 225.32ms |
| fs_point | SLOW | 14189 | 20752 | 1.46x | 3.33x | 4.79x | 395us | 729us |
| fs_supplier | PARITY | 629 | 636 | 1.01x | 1.79x | 4.25x | 10.60ms | 24.04ms |
| fs_topk | PARITY | 71 | 41 | 0.58x | 1.83x | 40.28x | 112.45ms | 196.63ms |
| offline_fs_batch | FAIL: cluster unreachable after the case (   33. request failed wi | - | - | - | - | - | - | - |

## 4. Compiled interpreter at T=1: OFF avg / ON avg (> 1 = ON faster; noise band ±10%) and the ON arm's ndbinfo.jit deltas per request

| query | OFF/ON | rows executed/req | compiled/req | reused/req | fallback | reading |
|---|---:|---:|---:|---:|---:|---|
| offline_fs_join_body | 0.96x | 8017740 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_scan_agg | 0.99x | 6000000 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| offline_fs_multi | 0.96x | 3505941 | 0.00 | 18.00 | 0 | rows executed, no effect: the program is not where the time goes |
| offline_fs_batch | 1.00x | 3299997 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| offline_fs_wide | 0.99x | 3299997 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| tpch_q13 | 0.97x | 3299997 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| offline_fs_chain | 0.97x | 3299994 | 0.00 | 10.00 | 0 | rows executed, no effect: the program is not where the time goes |
| tpch_q22 | 0.99x | 3150000 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| offline_fs_scalar | 1.02x | 3149997 | 0.00 | 8.00 | 0 | rows executed, no effect: the program is not where the time goes |
| tpch_q2 | 1.03x | 1803900 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| tpch_q11 | 0.85x | 1600830 | 0.00 | 12.00 | 0 | ON slower |
| core_group_few | 0.94x | 1500000 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_group_many | 0.95x | 1500000 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| tpch_q15 | 0.95x | 463792 | 0.00 | 8.00 | 0 | rows executed, no effect: the program is not where the time goes |
| offline_fs_anti | 0.96x | 417268 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_scan_filter | 0.99x | 233001 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_snow1_batch100 | 0.84x | 100100 | 1.97 | 4.03 | 0 | ON slower |
| fs_nation | 0.96x | 86079 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_topk | 0.98x | 84694 | 0.00 | 10.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_batch1000 | 1.12x | 35999 | 0.00 | 4.00 | 0 | ON faster |
| fs_freshness | 0.95x | 20997 | 0.00 | 18.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_idx_range | 0.94x | 18640 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_supplier | 0.94x | 15704 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_minmax | 1.00x | 15704 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_dnf | 1.04x | 4081 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_batch100 | 1.07x | 3670 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_strkey_batch100 | 1.00x | 3596 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_batch | 0.97x | 2200 | 0.00 | 12.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_batch100_window | 1.00x | 1998 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_in_idx100 | 1.02x | 1011 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_avg_range | 1.00x | 999.80 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_batch10 | 1.01x | 287.27 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| core_in_pk100 | 0.99x | 100.00 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_point | 1.01x | 36.19 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_greatest | 1.02x | 36.19 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_strkey_point | 0.97x | 36.19 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_point | 1.01x | 21.00 | 0.00 | 8.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_window7d | 0.98x | 12.15 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_agg_filter | 1.00x | 9.38 | 0.00 | 4.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_composite_point | 1.14x | 3.58 | 0.00 | 4.00 | 0 | ON faster |
| fs_hw_snow1_point | 1.13x | 2.00 | 0.00 | 6.00 | 0 | ON faster |
| fs_hw_snow2_point | 0.94x | 2.00 | 0.00 | 6.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_snow2_left_chain | 1.00x | 2.00 | 0.00 | 6.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_snow2_left_single | 0.95x | 2.00 | 0.00 | 6.00 | 0 | rows executed, no effect: the program is not where the time goes |
| fs_hw_sessions_window2h | 0.89x | 0.88 | 0.00 | 4.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_floor | 0.94x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_history | 1.01x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_latest | 1.01x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| core_pk_lookup | 1.06x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| core_pass_range | 1.00x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_hw_floor | 1.33x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_hw_collect5 | 0.94x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_hw_collect5_cte | 0.95x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_hw_collect50 | 1.00x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |
| fs_hw_collect50_cte | 1.03x | 0.00 | 0.00 | 0.00 | 0 | no rows through interpreted programs (lookup / pass-through / no filter): cannot show |

## 5. Against the baseline (fs_ronsql/bench_results/2026-09-11-prod_build-run2, host mikaels-MacBook-Pro.local/arm64; NOT the same machine, informational): avg > +15% or p99 > +25% = REGRESSION, avg < -15% = IMPROVED

| query | engine | threads | arm | baseline avg | now avg | avg | p99 | verdict |
|---|---|---:|---|---:|---:|---:|---:|---|
| fs_hw_agg_batch10 | ronsql | 1 | OFF | 207.67ms | 215.86ms | +4% | +29% | REGRESSION |
| fs_hw_agg_batch10 | mysqld_nopush | 1 | OFF | 237us | 780us | +229% | +333% | REGRESSION |
| fs_hw_agg_batch10 | mysqld_nopush | 1 | ON | 236us | 785us | +233% | +347% | REGRESSION |
| fs_hw_agg_batch100 | ronsql | 1 | OFF | 611.94ms | 989.32ms | +62% | +88% | REGRESSION |
| fs_hw_agg_batch100 | ronsql | 1 | ON | 590.73ms | 924.63ms | +57% | +103% | REGRESSION |
| fs_hw_agg_batch100 | mysqld_nopush | 1 | OFF | 1.86ms | 7.67ms | +312% | +247% | REGRESSION |
| fs_hw_agg_batch100 | mysqld_nopush | 1 | ON | 1.92ms | 7.73ms | +303% | +249% | REGRESSION |
| fs_hw_agg_batch1000 | ronsql | 1 | OFF | 4.38s | 8.73s | +99% | +140% | REGRESSION |
| fs_hw_agg_batch1000 | ronsql | 1 | ON | 4.21s | 7.80s | +85% | +150% | REGRESSION |
| fs_hw_agg_batch1000 | mysqld_nopush | 1 | OFF | 42.77ms | 94.88ms | +122% | +170% | REGRESSION |
| fs_hw_agg_batch1000 | mysqld_nopush | 1 | ON | 43.75ms | 94.80ms | +117% | +152% | REGRESSION |
| fs_hw_agg_batch100_window | ronsql | 1 | OFF | 553.47ms | 981.11ms | +77% | +119% | REGRESSION |
| fs_hw_agg_batch100_window | ronsql | 1 | ON | 549.75ms | 978.54ms | +78% | +119% | REGRESSION |
| fs_hw_agg_batch100_window | mysqld_nopush | 1 | OFF | 1.33ms | 6.52ms | +390% | +247% | REGRESSION |
| fs_hw_agg_batch100_window | mysqld_nopush | 1 | ON | 1.35ms | 6.41ms | +375% | +258% | REGRESSION |
| fs_hw_agg_filter | ronsql | 1 | OFF | 106us | 76us | -28% | -20% | IMPROVED |
| fs_hw_agg_filter | ronsql | 1 | ON | 109us | 76us | -30% | -17% | IMPROVED |
| fs_hw_agg_filter | mysqld_nopush | 1 | OFF | 112us | 218us | +94% | +77% | REGRESSION |
| fs_hw_agg_filter | mysqld_nopush | 1 | ON | 115us | 227us | +98% | +626% | REGRESSION |
| fs_hw_agg_greatest | ronsql | 1 | OFF | 106us | 77us | -28% | -12% | IMPROVED |
| fs_hw_agg_greatest | ronsql | 1 | ON | 111us | 76us | -32% | -19% | IMPROVED |
| fs_hw_agg_greatest | mysqld_nopush | 1 | OFF | 111us | 210us | +90% | +585% | REGRESSION |
| fs_hw_agg_greatest | mysqld_nopush | 1 | ON | 110us | 216us | +97% | +600% | REGRESSION |
| fs_hw_agg_point | ronsql | 1 | OFF | 112us | 80us | -29% | -3% | IMPROVED |
| fs_hw_agg_point | ronsql | 1 | ON | 106us | 79us | -25% | -9% | IMPROVED |
| fs_hw_agg_point | mysqld_nopush | 1 | OFF | 113us | 210us | +85% | +591% | REGRESSION |
| fs_hw_agg_point | mysqld_nopush | 1 | ON | 110us | 219us | +99% | +580% | REGRESSION |
| fs_hw_agg_window7d | ronsql | 1 | OFF | 124us | 76us | -38% | -52% | IMPROVED |
| fs_hw_agg_window7d | ronsql | 1 | ON | 117us | 78us | -33% | -24% | IMPROVED |
| fs_hw_agg_window7d | mysqld_nopush | 1 | OFF | 113us | 221us | +96% | +571% | REGRESSION |
| fs_hw_agg_window7d | mysqld_nopush | 1 | ON | 113us | 217us | +92% | +110% | REGRESSION |
| fs_hw_collect5 | ronsql | 1 | OFF | 108us | 77us | -29% | -22% | IMPROVED |
| fs_hw_collect5 | ronsql | 1 | ON | 110us | 82us | -26% | -7% | IMPROVED |
| fs_hw_collect5 | mysqld_nopush | 1 | OFF | 116us | 210us | +82% | +622% | REGRESSION |
| fs_hw_collect5 | mysqld_nopush | 1 | ON | 115us | 205us | +78% | +571% | REGRESSION |
| fs_hw_collect50 | ronsql | 1 | OFF | 112us | 78us | -31% | -18% | IMPROVED |
| fs_hw_collect50 | ronsql | 1 | ON | 112us | 77us | -31% | -19% | IMPROVED |
| fs_hw_collect50 | mysqld_nopush | 1 | OFF | 129us | 213us | +66% | +189% | REGRESSION |
| fs_hw_collect50 | mysqld_nopush | 1 | ON | 119us | 216us | +82% | +543% | REGRESSION |
| fs_hw_collect5_cte | mysqld_nopush | 1 | OFF | 119us | 232us | +95% | +565% | REGRESSION |
| fs_hw_collect5_cte | mysqld_nopush | 1 | ON | 118us | 224us | +90% | +620% | REGRESSION |
| fs_hw_composite_point | ronsql | 1 | OFF | 108us | 93us | -14% | +521% | REGRESSION |
| fs_hw_composite_point | ronsql | 1 | ON | 111us | 81us | -27% | +596% | REGRESSION |
| fs_hw_composite_point | mysqld_nopush | 1 | OFF | 113us | 321us | +183% | +764% | REGRESSION |
| fs_hw_composite_point | mysqld_nopush | 1 | ON | 117us | 321us | +175% | +591% | REGRESSION |
| fs_hw_floor | ronsql | 1 | OFF | 102us | 140us | +37% | +561% | REGRESSION |
| fs_hw_floor | ronsql | 1 | ON | 98us | 105us | +7% | +703% | REGRESSION |
| fs_hw_floor | mysqld_nopush | 1 | OFF | 92us | 167us | +81% | +669% | REGRESSION |
| fs_hw_floor | mysqld_nopush | 1 | ON | 105us | 144us | +37% | +682% | REGRESSION |
| fs_hw_sessions_window2h | ronsql | 1 | OFF | 107us | 83us | -22% | +510% | REGRESSION |
| fs_hw_sessions_window2h | ronsql | 1 | ON | 109us | 93us | -15% | +634% | REGRESSION |
| fs_hw_sessions_window2h | mysqld_nopush | 1 | OFF | 106us | 280us | +163% | +684% | REGRESSION |
| fs_hw_sessions_window2h | mysqld_nopush | 1 | ON | 110us | 299us | +172% | +548% | REGRESSION |
| fs_hw_snow1_batch100 | ronsql | 1 | OFF | 19.97ms | 37.74ms | +89% | +131% | REGRESSION |
| fs_hw_snow1_batch100 | ronsql | 1 | ON | 21.89ms | 44.71ms | +104% | +148% | REGRESSION |
| fs_hw_snow1_batch100 | mysqld_nopush | 1 | OFF | 4.71ms | 44.99ms | +855% | +1169% | REGRESSION |
| fs_hw_snow1_batch100 | mysqld_nopush | 1 | ON | 4.86ms | 45.84ms | +843% | +982% | REGRESSION |
| fs_hw_snow1_point | ronsql | 1 | OFF | 519us | 227us | -56% | -69% | IMPROVED |
| fs_hw_snow1_point | ronsql | 1 | ON | 493us | 201us | -59% | -60% | IMPROVED |
| fs_hw_snow1_point | mysqld_nopush | 1 | OFF | 125us | 1.11ms | +785% | +1208% | REGRESSION |
| fs_hw_snow1_point | mysqld_nopush | 1 | ON | 126us | 1.16ms | +823% | +1153% | REGRESSION |
| fs_hw_snow2_left_chain | ronsql | 1 | OFF | 530us | 204us | -62% | -66% | IMPROVED |
| fs_hw_snow2_left_chain | ronsql | 1 | ON | 530us | 204us | -62% | -64% | IMPROVED |
| fs_hw_snow2_left_chain | mysqld_nopush | 1 | OFF | 171us | 968us | +466% | +776% | REGRESSION |
| fs_hw_snow2_left_chain | mysqld_nopush | 1 | ON | 168us | 941us | +459% | +843% | REGRESSION |
| fs_hw_snow2_left_single | ronsql | 1 | OFF | 531us | 192us | -64% | -70% | IMPROVED |
| fs_hw_snow2_left_single | ronsql | 1 | ON | 530us | 202us | -62% | -65% | IMPROVED |
| fs_hw_snow2_left_single | mysqld_nopush | 1 | OFF | 175us | 971us | +455% | +659% | REGRESSION |
| fs_hw_snow2_left_single | mysqld_nopush | 1 | ON | 173us | 931us | +437% | +783% | REGRESSION |
| fs_hw_snow2_point | ronsql | 1 | OFF | 526us | 196us | -63% | -65% | IMPROVED |
| fs_hw_snow2_point | ronsql | 1 | ON | 525us | 208us | -60% | -66% | IMPROVED |
| fs_hw_snow2_point | mysqld_nopush | 1 | OFF | 174us | 997us | +472% | +744% | REGRESSION |
| fs_hw_snow2_point | mysqld_nopush | 1 | ON | 173us | 970us | +462% | +852% | REGRESSION |
| fs_hw_strkey_batch100 | ronsql | 1 | OFF | 369.47ms | 742.91ms | +101% | +157% | REGRESSION |
| fs_hw_strkey_batch100 | ronsql | 1 | ON | 373.05ms | 743.44ms | +99% | +156% | REGRESSION |
| fs_hw_strkey_batch100 | mysqld_nopush | 1 | OFF | 3.92ms | 11.28ms | +188% | +157% | REGRESSION |
| fs_hw_strkey_batch100 | mysqld_nopush | 1 | ON | 3.90ms | 11.18ms | +187% | +165% | REGRESSION |
| fs_hw_strkey_point | ronsql | 1 | OFF | 111us | 79us | -29% | -12% | IMPROVED |
| fs_hw_strkey_point | ronsql | 1 | ON | 113us | 81us | -29% | -14% | IMPROVED |
| fs_hw_strkey_point | mysqld_nopush | 1 | OFF | 120us | 220us | +84% | +554% | REGRESSION |
| fs_hw_strkey_point | mysqld_nopush | 1 | ON | 120us | 209us | +74% | +542% | REGRESSION |

SUMMARY queries=56 fail=1 critical=9 slow=1 parity=45 tmax_fail=1 missing_cases=52 regressions=59 (cross-host, informational)
