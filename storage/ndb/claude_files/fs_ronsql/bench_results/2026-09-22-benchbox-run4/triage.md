# RonSQL performance triage

run: build=/home/mikael/mysql_trees/rondb_2604/prod_build sf=1.0 threads=[1, 8] engines=ronsql,mysqld_nopush arms=OFF,ON baseline=storage/ndb/claude_files/fs_ronsql/bench_results/2026-09-11-prod_build-run2

MySQL reference engine: `mysqld_nopush`; compiler arm for the engine comparison: `OFF`. Latency ratio = RonSQL avg / MySQL avg at T=1; throughput ratio = MySQL q/s / RonSQL q/s at T=8 (both > 1 = RonSQL behind). Classes: PARITY <= 1.25x, SLOW <= 3.00x, CRITICAL above; FAIL = the RonSQL case did not run.

(Recorded as pasted on 2026-09-22 from the benchmark computer; produced by the first version of `ronsql_bench_triage.py`. The baseline is the 2026-09-11 laptop run, so the `baseline` column and section 5 compare machines, not code — see `m3_plan.md` §6. The T=8 `N/A` rows are cases the run never reached: it stopped at `offline_fs_batch` T=8.)

## 1. Needs work (28 of 56 RonSQL-capable queries)

| # | query | latency T=1 | throughput T=8 | RonSQL / MySQL avg | RonSQL q/s / MySQL q/s | top phase | rows/req | plan pins | baseline |
|---:|---|---|---|---:|---:|---|---:|---|---|
| 1 | fs_hw_hash_point | FAIL: [semantic] Caught exception: Failed to get table. Note that RonSQL only supports | N/A | - / - | - / - | - | - | - | - |
| 2 | core_in_pk100 | CRITICAL (793.03x) | N/A | 398.46ms / 502us | - / - | firstbatch 398.23ms | 0.0 | - | - |
| 3 | fs_hw_agg_batch10 | CRITICAL (276.76x) | N/A | 215.86ms / 780us | - / - | firstbatch 215.63ms | 0.0 | - | REGRESSION ronsql T1 OFF avg +4% p99 +29%; REGRESSION mysqld_nopush T1 OFF avg +229% p99 +333%; REGRESSION mysqld_nopush T1 ON avg +233% p99 +347% |
| 4 | fs_hw_agg_batch100_window | CRITICAL (150.48x) | N/A | 981.11ms / 6.52ms | - / - | firstbatch 980.79ms | 0.0 | - | REGRESSION ronsql T1 OFF avg +77% p99 +119%; REGRESSION ronsql T1 ON avg +78% p99 +119%; REGRESSION mysqld_nopush T1 OFF avg +390% p99 +247%; REGRESSION mysqld_nopush T1 ON avg +375% p99 +258% |
| 5 | fs_hw_agg_batch100 | CRITICAL (128.99x) | N/A | 989.32ms / 7.67ms | - / - | firstbatch 988.99ms | 0.0 | - | REGRESSION ronsql T1 OFF avg +62% p99 +88%; REGRESSION ronsql T1 ON avg +57% p99 +103%; REGRESSION mysqld_nopush T1 OFF avg +312% p99 +247%; REGRESSION mysqld_nopush T1 ON avg +303% p99 +249% |
| 6 | fs_hw_agg_batch1000 | CRITICAL (92.02x) | N/A | 8.73s / 94.88ms | - / - | firstbatch 8.73s | 0.0 | - | REGRESSION ronsql T1 OFF avg +99% p99 +140%; REGRESSION ronsql T1 ON avg +85% p99 +150%; REGRESSION mysqld_nopush T1 OFF avg +122% p99 +170%; REGRESSION mysqld_nopush T1 ON avg +117% p99 +152% |
| 7 | core_in_idx100 | CRITICAL (81.80x) | N/A | 403.25ms / 4.93ms | - / - | firstbatch 402.95ms | 0.0 | - | - |
| 8 | fs_hw_strkey_batch100 | CRITICAL (65.86x) | N/A | 742.91ms / 11.28ms | - / - | firstbatch 742.59ms | 0.0 | - | REGRESSION ronsql T1 OFF avg +101% p99 +157%; REGRESSION ronsql T1 ON avg +99% p99 +156%; REGRESSION mysqld_nopush T1 OFF avg +188% p99 +157%; REGRESSION mysqld_nopush T1 ON avg +187% p99 +165% |
| 9 | tpch_q22 | CRITICAL (5.05x) | N/A | 1.10s / 216.91ms | - / - | firstbatch 1.10s | 0.0 | - | - |
| 10 | tpch_q2 | CRITICAL (4.10x) | N/A | 1.12s / 273.78ms | - / - | firstbatch 1.12s | 0.0 | - | - |
| 11 | fs_point | PARITY (0.97x) | SLOW (1.46x) | 225us / 230us | 14189 / 20752 | firstbatch 156us | 0.0 | - | - |
| 12 | fs_floor | SLOW (1.38x) | SLOW (1.31x) | 180us / 130us | 42778 / 56146 | firstbatch 148us | 0.0 | - | - |
| 13 | fs_hw_snow1_batch100 | PARITY (0.84x) | N/A | 37.74ms / 44.99ms | - / - | firstbatch 37.45ms | 88.9 | - | REGRESSION ronsql T1 OFF avg +89% p99 +131%; REGRESSION ronsql T1 ON avg +104% p99 +148%; REGRESSION mysqld_nopush T1 OFF avg +855% p99 +1169%; REGRESSION mysqld_nopush T1 ON avg +843% p99 +982% |
| 14 | fs_hw_floor | PARITY (0.84x) | N/A | 140us / 167us | - / - | firstbatch 110us | 0.0 | - | REGRESSION ronsql T1 OFF avg +37% p99 +561%; REGRESSION ronsql T1 ON avg +7% p99 +703%; REGRESSION mysqld_nopush T1 OFF avg +81% p99 +669%; REGRESSION mysqld_nopush T1 ON avg +37% p99 +682% |
| 15 | fs_hw_agg_point | PARITY (0.38x) | N/A | 80us / 210us | - / - | firstbatch 44us | 0.0 | - | IMPROVED ronsql T1 OFF avg -29% p99 -3%; IMPROVED ronsql T1 ON avg -25% p99 -9%; REGRESSION mysqld_nopush T1 OFF avg +85% p99 +591%; REGRESSION mysqld_nopush T1 ON avg +99% p99 +580% |
| 16 | fs_hw_collect5 | PARITY (0.37x) | N/A | 77us / 210us | - / - | firstbatch 38us | 4.2 | - | IMPROVED ronsql T1 OFF avg -29% p99 -22%; IMPROVED ronsql T1 ON avg -26% p99 -7%; REGRESSION mysqld_nopush T1 OFF avg +82% p99 +622%; REGRESSION mysqld_nopush T1 ON avg +78% p99 +571% |
| 17 | fs_hw_agg_greatest | PARITY (0.36x) | N/A | 77us / 210us | - / - | firstbatch 42us | 0.0 | - | IMPROVED ronsql T1 OFF avg -28% p99 -12%; IMPROVED ronsql T1 ON avg -32% p99 -19%; REGRESSION mysqld_nopush T1 OFF avg +90% p99 +585%; REGRESSION mysqld_nopush T1 ON avg +97% p99 +600% |
| 18 | fs_hw_collect50 | PARITY (0.36x) | N/A | 78us / 213us | - / - | firstbatch 36us | 17.6 | - | IMPROVED ronsql T1 OFF avg -31% p99 -18%; IMPROVED ronsql T1 ON avg -31% p99 -19%; REGRESSION mysqld_nopush T1 OFF avg +66% p99 +189%; REGRESSION mysqld_nopush T1 ON avg +82% p99 +543% |
| 19 | fs_hw_strkey_point | PARITY (0.36x) | N/A | 79us / 220us | - / - | firstbatch 45us | 0.0 | - | IMPROVED ronsql T1 OFF avg -29% p99 -12%; IMPROVED ronsql T1 ON avg -29% p99 -14%; REGRESSION mysqld_nopush T1 OFF avg +84% p99 +554%; REGRESSION mysqld_nopush T1 ON avg +74% p99 +542% |
| 20 | fs_hw_agg_filter | PARITY (0.35x) | N/A | 76us / 218us | - / - | firstbatch 41us | 0.0 | - | IMPROVED ronsql T1 OFF avg -28% p99 -20%; IMPROVED ronsql T1 ON avg -30% p99 -17%; REGRESSION mysqld_nopush T1 OFF avg +94% p99 +77%; REGRESSION mysqld_nopush T1 ON avg +98% p99 +626% |
| 21 | fs_hw_agg_window7d | PARITY (0.35x) | N/A | 76us / 221us | - / - | firstbatch 41us | 0.0 | - | IMPROVED ronsql T1 OFF avg -38% p99 -52%; IMPROVED ronsql T1 ON avg -33% p99 -24%; REGRESSION mysqld_nopush T1 OFF avg +96% p99 +571%; REGRESSION mysqld_nopush T1 ON avg +92% p99 +110% |
| 22 | fs_hw_collect5_cte | PARITY (0.33x) | N/A | 76us / 232us | - / - | firstbatch 38us | 4.2 | - | REGRESSION mysqld_nopush T1 OFF avg +95% p99 +565%; REGRESSION mysqld_nopush T1 ON avg +90% p99 +620% |
| 23 | fs_hw_sessions_window2h | PARITY (0.30x) | N/A | 83us / 280us | - / - | firstbatch 50us | 0.0 | - | REGRESSION ronsql T1 OFF avg -22% p99 +510%; REGRESSION ronsql T1 ON avg -15% p99 +634%; REGRESSION mysqld_nopush T1 OFF avg +163% p99 +684%; REGRESSION mysqld_nopush T1 ON avg +172% p99 +548% |
| 24 | fs_hw_composite_point | PARITY (0.29x) | N/A | 93us / 321us | - / - | firstbatch 60us | 0.0 | - | REGRESSION ronsql T1 OFF avg -14% p99 +521%; REGRESSION ronsql T1 ON avg -27% p99 +596%; REGRESSION mysqld_nopush T1 OFF avg +183% p99 +764%; REGRESSION mysqld_nopush T1 ON avg +175% p99 +591% |
| 25 | fs_hw_snow2_left_chain | PARITY (0.21x) | N/A | 204us / 968us | - / - | firstbatch 138us | 0.8 | - | IMPROVED ronsql T1 OFF avg -62% p99 -66%; IMPROVED ronsql T1 ON avg -62% p99 -64%; REGRESSION mysqld_nopush T1 OFF avg +466% p99 +776%; REGRESSION mysqld_nopush T1 ON avg +459% p99 +843% |
| 26 | fs_hw_snow1_point | PARITY (0.20x) | N/A | 227us / 1.11ms | - / - | firstbatch 128us | 0.9 | - | IMPROVED ronsql T1 OFF avg -56% p99 -69%; IMPROVED ronsql T1 ON avg -59% p99 -60%; REGRESSION mysqld_nopush T1 OFF avg +785% p99 +1208%; REGRESSION mysqld_nopush T1 ON avg +823% p99 +1153% |
| 27 | fs_hw_snow2_left_single | PARITY (0.20x) | N/A | 192us / 971us | - / - | firstbatch 126us | 1.0 | - | IMPROVED ronsql T1 OFF avg -64% p99 -70%; IMPROVED ronsql T1 ON avg -62% p99 -65%; REGRESSION mysqld_nopush T1 OFF avg +455% p99 +659%; REGRESSION mysqld_nopush T1 ON avg +437% p99 +783% |
| 28 | fs_hw_snow2_point | PARITY (0.20x) | N/A | 196us / 997us | - / - | firstbatch 130us | 0.8 | - | IMPROVED ronsql T1 OFF avg -63% p99 -65%; IMPROVED ronsql T1 ON avg -60% p99 -66%; REGRESSION mysqld_nopush T1 OFF avg +472% p99 +744%; REGRESSION mysqld_nopush T1 ON avg +462% p99 +852% |

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

(Only the `fs` category was reached at T=8; every other row was `N/A`.)

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
| offline_fs_batch | FAIL | - | - | - | - | - | - | - |

## 4. Compiled interpreter (OFF avg / ON avg at T=1; > 1 = ON faster; noise band ±10%)

faster with ON: fs_hw_floor 1.33x, fs_hw_agg_batch1000 1.12x, fs_hw_snow1_point 1.13x, fs_hw_composite_point 1.14x

slower with ON: tpch_q11 0.85x, fs_hw_snow1_batch100 0.84x, fs_hw_sessions_window2h 0.89x

## 5. Against the baseline (2026-09-11-prod_build-run2, the laptop): cross-machine, informational

RonSQL, T=1, OFF: every fs_hw point shape improved 22–38 % (e.g. agg_point 112 → 80 µs, collect50 112 → 78 µs), the snowflakes 56–64 % (snow1_point 519 → 227 µs, snow2_point 526 → 196 µs); the scan-bound entries are 62–101 % slower (agg_batch100 612 → 989 ms, agg_batch1000 4.38 → 8.73 s, strkey_batch100 369 → 743 ms, snow1_batch100 20 → 38 ms); fs_hw_floor 102 → 140 µs; p99 +510–703 % on fs_hw_floor, composite_point, sessions_window2h. MySQL nopush, T=1: +66–99 % on every point shape (113 → 210 µs), +122–390 % on the batch entries, +437–855 % on the snowflakes and snow1_batch100 (125 µs → 1.11 ms, 4.7 → 45 ms).

SUMMARY queries=56 fail=1 critical=9 slow=1 parity=45 regressions=59
