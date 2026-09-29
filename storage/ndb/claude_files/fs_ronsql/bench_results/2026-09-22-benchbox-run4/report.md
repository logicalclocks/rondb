# RonSQL / MySQL / compiled-interpreter benchmark matrix

build=/home/mikael/mysql_trees/rondb_2604/prod_build sf=1 threads=[1, 8, 32] engines=ronsql,mysqld_nopush compiler=OFF,ON order=query-major repeat=1 cpubind=/home/mikael/mysql_trees/rondb_2604/mysql-test/suite/ronsqlcrunch/census.cnf client_cpus=15-16 host=localhost.localdomain (Linux x86_64) started 2026-09-22T20:17:28

FAILED cases: off_ronsql_fs_hw_hash_point_T1 ([semantic] Caught exception: Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.), off_mysqld_nopush_fs_hw_hash_point_T1 (warmup query failed: query failed: Error 1146 (42S02): Table 'fs_bench.transactions_hash_1' doesn't exist), on_ronsql_fs_hw_hash_point_T1 ([semantic] Caught exception: Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.), on_mysqld_nopush_fs_hw_hash_point_T1 (warmup query failed: query failed: Error 1146 (42S02): Table 'fs_bench.transactions_hash_1' doesn't exist), off_ronsql_offline_fs_batch_T8 (cluster unreachable after the case (   33. request failed with status 500: ): ERROR 1296 (HY000) at line 1: Got error 157 'Connection to NDB failed' from NDBINFO)

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 1 thread

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_floor | 180us | 190us | 0.94x | 5294 | 5028 | 130us | 123us | 1.06x | 7676 | 8111 |
| fs_point | 225us | 222us | 1.01x | 4262 | 4334 | 230us | 234us | 0.99x | 4333 | 4273 |
| fs_batch | 640us | 663us | 0.97x | 1526 | 1476 | 7.36ms | 7.95ms | 0.93x | 136 | 126 |
| fs_freshness | 2.67ms | 2.80ms | 0.95x | 372 | 354 | 122ms | 112ms | 1.09x | 8 | 9 |
| fs_supplier | 2.84ms | 3.01ms | 0.94x | 351 | 331 | 6.68ms | 6.82ms | 0.98x | 150 | 147 |
| fs_nation | 24.61ms | 25.66ms | 0.96x | 41 | 39 | 43.56ms | 46.68ms | 0.93x | 23 | 21 |
| fs_topk | 25.90ms | 26.45ms | 0.98x | 39 | 38 | 990ms | 1105ms | 0.90x | 1 | 1 |
| fs_minmax | 3.60ms | 3.60ms | 1.00x | 277 | 277 | 9.58ms | 9.57ms | 1.00x | 104 | 105 |
| fs_dnf | 971us | 932us | 1.04x | 1017 | 1058 | 112ms | 119ms | 0.95x | 9 | 8 |
| fs_history | 1.35ms | 1.34ms | 1.01x | 718 | 723 | 1.98ms | 2.02ms | 0.98x | 505 | 494 |
| fs_latest | 579us | 572us | 1.01x | 1693 | 1713 | 626us | 624us | 1.00x | 1596 | 1601 |
| offline_fs_batch | 1538ms | 1544ms | 1.00x | 1 | 1 | 78726ms | 67232ms | 1.17x | 0 | 0 |
| offline_fs_multi | 1464ms | 1522ms | 0.96x | 1 | 1 | 41914ms | 48632ms | 0.86x | 0 | 0 |
| offline_fs_join_body | 2000ms | 2084ms | 0.96x | 0 | 0 | 11555ms | 11449ms | 1.01x | 0 | 0 |
| offline_fs_anti | 206ms | 215ms | 0.96x | 5 | 5 | 218ms | 223ms | 0.98x | 5 | 4 |
| offline_fs_wide | 2032ms | 2047ms | 0.99x | 0 | 0 | 82285ms | 72995ms | 1.13x | 0 | 0 |
| offline_fs_chain | 1119ms | 1154ms | 0.97x | 1 | 1 | 2174ms | 2095ms | 1.04x | 0 | 0 |
| offline_fs_scalar | 1125ms | 1103ms | 1.02x | 1 | 1 | 2102ms | 1978ms | 1.06x | 0 | 1 |
| tpch_q2 | 1123ms | 1094ms | 1.03x | 1 | 1 | 274ms | 278ms | 0.99x | 4 | 4 |
| tpch_q11 | 102ms | 121ms | 0.85x | 10 | 8 | 2043ms | 2271ms | 0.90x | 0 | 0 |
| tpch_q13 | 1111ms | 1146ms | 0.97x | 1 | 1 | 1486ms | 1425ms | 1.04x | 1 | 1 |
| tpch_q15 | 54.92ms | 57.68ms | 0.95x | 18 | 17 | 157ms | 156ms | 1.01x | 6 | 6 |
| tpch_q22 | 1096ms | 1104ms | 0.99x | 1 | 1 | 217ms | 223ms | 0.97x | 5 | 4 |
| core_pk_lookup | 404us | 380us | 1.06x | 2417 | 2565 | 439us | 454us | 0.97x | 2276 | 2199 |
| core_in_pk100 | 398ms | 403ms | 0.99x | 3 | 2 | 502us | 502us | 1.00x | 1965 | 1967 |
| core_in_idx100 | 403ms | 393ms | 1.02x | 2 | 3 | 4.93ms | 5.01ms | 0.98x | 202 | 199 |
| core_idx_range | 2.49ms | 2.64ms | 0.94x | 400 | 377 | 6.13ms | 6.16ms | 1.00x | 163 | 162 |
| core_avg_range | 257us | 258us | 1.00x | 3732 | 3727 | 616us | 613us | 1.00x | 1623 | 1630 |
| core_pass_range | 492us | 492us | 1.00x | 1903 | 1903 | 978us | 981us | 1.00x | 1022 | 1019 |
| core_scan_agg | 642ms | 650ms | 0.99x | 2 | 2 | 1986ms | 1319ms | 1.51x | 0 | 1 |
| core_scan_filter | 319ms | 321ms | 0.99x | 3 | 3 | 680ms | 661ms | 1.03x | 1 | 2 |
| core_group_few | 160ms | 170ms | 0.94x | 6 | 6 | 618ms | 609ms | 1.02x | 2 | 2 |
| core_group_many | 1139ms | 1203ms | 0.95x | 1 | 1 | 2221ms | 2039ms | 1.09x | 0 | 0 |
| fs_hw_floor | 140us | 105us | 1.33x | 6714 | 8789 | 167us | 144us | 1.16x | 5967 | 6911 |
| fs_hw_agg_point | 80us | 79us | 1.01x | 11094 | 11263 | 210us | 219us | 0.96x | 4757 | 4550 |
| fs_hw_agg_window7d | 76us | 78us | 0.98x | 11427 | 11254 | 221us | 217us | 1.02x | 4516 | 4583 |
| fs_hw_agg_greatest | 77us | 76us | 1.02x | 11493 | 11701 | 210us | 216us | 0.98x | 4738 | 4631 |
| fs_hw_agg_filter | 76us | 76us | 1.00x | 11509 | 11575 | 218us | 227us | 0.96x | 4580 | 4398 |
| fs_hw_agg_batch10 | 216ms | 214ms | 1.01x | 5 | 5 | 780us | 785us | 0.99x | 1279 | 1271 |
| fs_hw_agg_batch100 | 989ms | 925ms | 1.07x | 1 | 1 | 7.67ms | 7.73ms | 0.99x | 130 | 129 |
| fs_hw_agg_batch1000 | 8731ms | 7798ms | 1.12x | 0 | 0 | 94.88ms | 94.80ms | 1.00x | 11 | 11 |
| fs_hw_agg_batch100_window | 981ms | 979ms | 1.00x | 1 | 1 | 6.52ms | 6.41ms | 1.02x | 153 | 156 |
| fs_hw_collect5 | 77us | 82us | 0.94x | 11488 | 10864 | 210us | 205us | 1.03x | 4739 | 4876 |
| fs_hw_collect5_cte | 76us | 81us | 0.95x | 11588 | 11036 | 232us | 224us | 1.03x | 4293 | 4446 |
| fs_hw_collect50 | 78us | 77us | 1.00x | 11391 | 11422 | 213us | 216us | 0.99x | 4686 | 4622 |
| fs_hw_collect50_cte | 84us | 82us | 1.03x | 10625 | 10852 | 236us | 236us | 1.00x | 4222 | 4229 |
| fs_hw_snow1_point | 227us | 201us | 1.13x | 4227 | 4745 | 1.11ms | 1.16ms | 0.96x | 903 | 862 |
| fs_hw_snow2_point | 196us | 208us | 0.94x | 4901 | 4601 | 997us | 970us | 1.03x | 1002 | 1029 |
| fs_hw_snow1_batch100 | 37.74ms | 44.71ms | 0.84x | 26 | 22 | 44.99ms | 45.84ms | 0.98x | 22 | 22 |
| fs_hw_snow2_left_chain | 204us | 204us | 1.00x | 4691 | 4689 | 968us | 941us | 1.03x | 1032 | 1061 |
| fs_hw_snow2_left_single | 192us | 202us | 0.95x | 4934 | 4727 | 971us | 931us | 1.04x | 1029 | 1073 |
| fs_hw_strkey_point | 79us | 81us | 0.97x | 11184 | 10918 | 220us | 209us | 1.05x | 4525 | 4776 |
| fs_hw_strkey_batch100 | 743ms | 743ms | 1.00x | 1 | 1 | 11.28ms | 11.18ms | 1.01x | 88 | 89 |
| fs_hw_composite_point | 93us | 81us | 1.14x | 9594 | 10841 | 321us | 321us | 1.00x | 3100 | 3108 |
| fs_hw_hash_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_sessions_window2h | 83us | 93us | 0.89x | 10585 | 9602 | 280us | 299us | 0.94x | 3560 | 3338 |
| cte_tpch_q2 | - | - | - | - | - | 100749ms | 84166ms | 1.20x | 0 | 0 |
| cte_tpch_q11 | - | - | - | - | - | 302ms | 310ms | 0.97x | 3 | 3 |
| cte_tpch_q13 | - | - | - | - | - | 2049ms | 2089ms | 0.98x | 0 | 0 |
| cte_tpch_q15 | - | - | - | - | - | 135ms | 136ms | 0.99x | 7 | 7 |
| cte_tpch_q22 | - | - | - | - | - | 2189ms | 2076ms | 1.05x | 0 | 0 |
| fs_hw_collect5_twin | - | - | - | - | - | 302us | 300us | 1.01x | 3306 | 3328 |
| fs_hw_snow1_twin | - | - | - | - | - | 488us | 514us | 0.95x | 2047 | 1942 |
| fs_hw_snow2_twin | - | - | - | - | - | 382us | 389us | 0.98x | 2609 | 2565 |

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 8 threads

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_floor | 173us | 199us | 0.87x | 42778 | 35399 | 141us | 142us | 0.99x | 56146 | 55733 |
| fs_point | 395us | 365us | 1.08x | 14189 | 20575 | 380us | 381us | 1.00x | 20752 | 20672 |
| fs_batch | 1.90ms | 1.97ms | 0.96x | 4148 | 4005 | 3.09ms | 3.10ms | 1.00x | 2385 | 2479 |
| fs_freshness | 10.70ms | 12.14ms | 0.88x | 635 | 656 | 23.93ms | 24.29ms | 0.99x | 289 | 287 |
| fs_supplier | 10.60ms | 11.37ms | 0.93x | 629 | 700 | 12.40ms | 12.40ms | 1.00x | 636 | 637 |
| fs_nation | 95.59ms | 105ms | 0.91x | 70 | 76 | 91.04ms | 90.69ms | 1.00x | 86 | 87 |
| fs_topk | 112ms | 115ms | 0.98x | 71 | 69 | 191ms | 192ms | 0.99x | 41 | 38 |
| fs_minmax | 13.06ms | 13.21ms | 0.99x | 509 | 505 | 17.41ms | 17.43ms | 1.00x | 455 | 455 |
| fs_dnf | 3.58ms | 3.64ms | 0.98x | 2221 | 1828 | 13.53ms | 13.60ms | 0.99x | 588 | 586 |
| fs_history | 1.91ms | 1.91ms | 1.00x | 3958 | 3944 | 4.85ms | 4.88ms | 0.99x | 1635 | 1622 |
| fs_latest | 2.62ms | 2.70ms | 0.97x | 2664 | 2929 | 2.58ms | 2.64ms | 0.98x | 3089 | 3021 |
| offline_fs_batch | - | - | - | - | - | - | - | - | - | - |
| offline_fs_multi | - | - | - | - | - | - | - | - | - | - |
| offline_fs_join_body | - | - | - | - | - | - | - | - | - | - |
| offline_fs_anti | - | - | - | - | - | - | - | - | - | - |
| offline_fs_wide | - | - | - | - | - | - | - | - | - | - |
| offline_fs_chain | - | - | - | - | - | - | - | - | - | - |
| offline_fs_scalar | - | - | - | - | - | - | - | - | - | - |
| tpch_q2 | - | - | - | - | - | - | - | - | - | - |
| tpch_q11 | - | - | - | - | - | - | - | - | - | - |
| tpch_q13 | - | - | - | - | - | - | - | - | - | - |
| tpch_q15 | - | - | - | - | - | - | - | - | - | - |
| tpch_q22 | - | - | - | - | - | - | - | - | - | - |
| core_pk_lookup | - | - | - | - | - | - | - | - | - | - |
| core_in_pk100 | - | - | - | - | - | - | - | - | - | - |
| core_in_idx100 | - | - | - | - | - | - | - | - | - | - |
| core_idx_range | - | - | - | - | - | - | - | - | - | - |
| core_avg_range | - | - | - | - | - | - | - | - | - | - |
| core_pass_range | - | - | - | - | - | - | - | - | - | - |
| core_scan_agg | - | - | - | - | - | - | - | - | - | - |
| core_scan_filter | - | - | - | - | - | - | - | - | - | - |
| core_group_few | - | - | - | - | - | - | - | - | - | - |
| core_group_many | - | - | - | - | - | - | - | - | - | - |
| fs_hw_floor | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_window7d | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_greatest | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_filter | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch10 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch100 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch1000 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch100_window | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5_cte | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect50 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect50_cte | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_batch100 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_left_chain | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_left_single | - | - | - | - | - | - | - | - | - | - |
| fs_hw_strkey_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_strkey_batch100 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_composite_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_hash_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_sessions_window2h | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q2 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q11 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q13 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q15 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q22 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5_twin | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_twin | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_twin | - | - | - | - | - | - | - | - | - | - |

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 32 threads

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_floor | - | - | - | - | - | - | - | - | - | - |
| fs_point | - | - | - | - | - | - | - | - | - | - |
| fs_batch | - | - | - | - | - | - | - | - | - | - |
| fs_freshness | - | - | - | - | - | - | - | - | - | - |
| fs_supplier | - | - | - | - | - | - | - | - | - | - |
| fs_nation | - | - | - | - | - | - | - | - | - | - |
| fs_topk | - | - | - | - | - | - | - | - | - | - |
| fs_minmax | - | - | - | - | - | - | - | - | - | - |
| fs_dnf | - | - | - | - | - | - | - | - | - | - |
| fs_history | - | - | - | - | - | - | - | - | - | - |
| fs_latest | - | - | - | - | - | - | - | - | - | - |
| offline_fs_batch | - | - | - | - | - | - | - | - | - | - |
| offline_fs_multi | - | - | - | - | - | - | - | - | - | - |
| offline_fs_join_body | - | - | - | - | - | - | - | - | - | - |
| offline_fs_anti | - | - | - | - | - | - | - | - | - | - |
| offline_fs_wide | - | - | - | - | - | - | - | - | - | - |
| offline_fs_chain | - | - | - | - | - | - | - | - | - | - |
| offline_fs_scalar | - | - | - | - | - | - | - | - | - | - |
| tpch_q2 | - | - | - | - | - | - | - | - | - | - |
| tpch_q11 | - | - | - | - | - | - | - | - | - | - |
| tpch_q13 | - | - | - | - | - | - | - | - | - | - |
| tpch_q15 | - | - | - | - | - | - | - | - | - | - |
| tpch_q22 | - | - | - | - | - | - | - | - | - | - |
| core_pk_lookup | - | - | - | - | - | - | - | - | - | - |
| core_in_pk100 | - | - | - | - | - | - | - | - | - | - |
| core_in_idx100 | - | - | - | - | - | - | - | - | - | - |
| core_idx_range | - | - | - | - | - | - | - | - | - | - |
| core_avg_range | - | - | - | - | - | - | - | - | - | - |
| core_pass_range | - | - | - | - | - | - | - | - | - | - |
| core_scan_agg | - | - | - | - | - | - | - | - | - | - |
| core_scan_filter | - | - | - | - | - | - | - | - | - | - |
| core_group_few | - | - | - | - | - | - | - | - | - | - |
| core_group_many | - | - | - | - | - | - | - | - | - | - |
| fs_hw_floor | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_window7d | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_greatest | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_filter | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch10 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch100 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch1000 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch100_window | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5_cte | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect50 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect50_cte | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_batch100 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_left_chain | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_left_single | - | - | - | - | - | - | - | - | - | - |
| fs_hw_strkey_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_strkey_batch100 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_composite_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_hash_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_sessions_window2h | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q2 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q11 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q13 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q15 | - | - | - | - | - | - | - | - | - | - |
| cte_tpch_q22 | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5_twin | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_twin | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_twin | - | - | - | - | - | - | - | - | - | - |

## B. RonSQL vs MySQL server (avg latency; ratio = mysqld / ronsql, >1 = RonSQL faster)

| query | threads | compiler | ronsql | mysqld_nopush | mysqld_nopush/ronsql |
|---|---:|---|---:|---:|---:|
| fs_floor | 1 | OFF | 180us | 130us | 0.72x |
| fs_floor | 1 | ON | 190us | 123us | 0.65x |
| fs_floor | 8 | OFF | 173us | 141us | 0.81x |
| fs_floor | 8 | ON | 199us | 142us | 0.71x |
| fs_point | 1 | OFF | 225us | 230us | 1.03x |
| fs_point | 1 | ON | 222us | 234us | 1.05x |
| fs_point | 8 | OFF | 395us | 380us | 0.96x |
| fs_point | 8 | ON | 365us | 381us | 1.05x |
| fs_batch | 1 | OFF | 640us | 7.36ms | 11.51x |
| fs_batch | 1 | ON | 663us | 7.95ms | 12.00x |
| fs_batch | 8 | OFF | 1.90ms | 3.09ms | 1.63x |
| fs_batch | 8 | ON | 1.97ms | 3.10ms | 1.57x |
| fs_freshness | 1 | OFF | 2.67ms | 122ms | 45.76x |
| fs_freshness | 1 | ON | 2.80ms | 112ms | 39.95x |
| fs_freshness | 8 | OFF | 10.70ms | 23.93ms | 2.24x |
| fs_freshness | 8 | ON | 12.14ms | 24.29ms | 2.00x |
| fs_supplier | 1 | OFF | 2.84ms | 6.68ms | 2.35x |
| fs_supplier | 1 | ON | 3.01ms | 6.82ms | 2.27x |
| fs_supplier | 8 | OFF | 10.60ms | 12.40ms | 1.17x |
| fs_supplier | 8 | ON | 11.37ms | 12.40ms | 1.09x |
| fs_nation | 1 | OFF | 24.61ms | 43.56ms | 1.77x |
| fs_nation | 1 | ON | 25.66ms | 46.68ms | 1.82x |
| fs_nation | 8 | OFF | 95.59ms | 91.04ms | 0.95x |
| fs_nation | 8 | ON | 105ms | 90.69ms | 0.87x |
| fs_topk | 1 | OFF | 25.90ms | 990ms | 38.21x |
| fs_topk | 1 | ON | 26.45ms | 1105ms | 41.77x |
| fs_topk | 8 | OFF | 112ms | 191ms | 1.69x |
| fs_topk | 8 | ON | 115ms | 192ms | 1.67x |
| fs_minmax | 1 | OFF | 3.60ms | 9.58ms | 2.66x |
| fs_minmax | 1 | ON | 3.60ms | 9.57ms | 2.66x |
| fs_minmax | 8 | OFF | 13.06ms | 17.41ms | 1.33x |
| fs_minmax | 8 | ON | 13.21ms | 17.43ms | 1.32x |
| fs_dnf | 1 | OFF | 971us | 112ms | 115.71x |
| fs_dnf | 1 | ON | 932us | 119ms | 127.35x |
| fs_dnf | 8 | OFF | 3.58ms | 13.53ms | 3.78x |
| fs_dnf | 8 | ON | 3.64ms | 13.60ms | 3.74x |
| fs_history | 1 | OFF | 1.35ms | 1.98ms | 1.47x |
| fs_history | 1 | ON | 1.34ms | 2.02ms | 1.51x |
| fs_history | 8 | OFF | 1.91ms | 4.85ms | 2.54x |
| fs_history | 8 | ON | 1.91ms | 4.88ms | 2.55x |
| fs_latest | 1 | OFF | 579us | 626us | 1.08x |
| fs_latest | 1 | ON | 572us | 624us | 1.09x |
| fs_latest | 8 | OFF | 2.62ms | 2.58ms | 0.98x |
| fs_latest | 8 | ON | 2.70ms | 2.64ms | 0.98x |
| offline_fs_batch | 1 | OFF | 1538ms | 78726ms | 51.17x |
| offline_fs_batch | 1 | ON | 1544ms | 67232ms | 43.53x |
| offline_fs_multi | 1 | OFF | 1464ms | 41914ms | 28.63x |
| offline_fs_multi | 1 | ON | 1522ms | 48632ms | 31.95x |
| offline_fs_join_body | 1 | OFF | 2000ms | 11555ms | 5.78x |
| offline_fs_join_body | 1 | ON | 2084ms | 11449ms | 5.49x |
| offline_fs_anti | 1 | OFF | 206ms | 218ms | 1.06x |
| offline_fs_anti | 1 | ON | 215ms | 223ms | 1.03x |
| offline_fs_wide | 1 | OFF | 2032ms | 82285ms | 40.50x |
| offline_fs_wide | 1 | ON | 2047ms | 72995ms | 35.66x |
| offline_fs_chain | 1 | OFF | 1119ms | 2174ms | 1.94x |
| offline_fs_chain | 1 | ON | 1154ms | 2095ms | 1.82x |
| offline_fs_scalar | 1 | OFF | 1125ms | 2102ms | 1.87x |
| offline_fs_scalar | 1 | ON | 1103ms | 1978ms | 1.79x |
| tpch_q2 | 1 | OFF | 1123ms | 274ms | 0.24x |
| tpch_q2 | 1 | ON | 1094ms | 278ms | 0.25x |
| tpch_q11 | 1 | OFF | 102ms | 2043ms | 19.94x |
| tpch_q11 | 1 | ON | 121ms | 2271ms | 18.83x |
| tpch_q13 | 1 | OFF | 1111ms | 1486ms | 1.34x |
| tpch_q13 | 1 | ON | 1146ms | 1425ms | 1.24x |
| tpch_q15 | 1 | OFF | 54.92ms | 157ms | 2.86x |
| tpch_q15 | 1 | ON | 57.68ms | 156ms | 2.70x |
| tpch_q22 | 1 | OFF | 1096ms | 217ms | 0.20x |
| tpch_q22 | 1 | ON | 1104ms | 223ms | 0.20x |
| core_pk_lookup | 1 | OFF | 404us | 439us | 1.09x |
| core_pk_lookup | 1 | ON | 380us | 454us | 1.19x |
| core_in_pk100 | 1 | OFF | 398ms | 502us | 0.00x |
| core_in_pk100 | 1 | ON | 403ms | 502us | 0.00x |
| core_in_idx100 | 1 | OFF | 403ms | 4.93ms | 0.01x |
| core_in_idx100 | 1 | ON | 393ms | 5.01ms | 0.01x |
| core_idx_range | 1 | OFF | 2.49ms | 6.13ms | 2.46x |
| core_idx_range | 1 | ON | 2.64ms | 6.16ms | 2.33x |
| core_avg_range | 1 | OFF | 257us | 616us | 2.39x |
| core_avg_range | 1 | ON | 258us | 613us | 2.38x |
| core_pass_range | 1 | OFF | 492us | 978us | 1.99x |
| core_pass_range | 1 | ON | 492us | 981us | 1.99x |
| core_scan_agg | 1 | OFF | 642ms | 1986ms | 3.09x |
| core_scan_agg | 1 | ON | 650ms | 1319ms | 2.03x |
| core_scan_filter | 1 | OFF | 319ms | 680ms | 2.13x |
| core_scan_filter | 1 | ON | 321ms | 661ms | 2.06x |
| core_group_few | 1 | OFF | 160ms | 618ms | 3.85x |
| core_group_few | 1 | ON | 170ms | 609ms | 3.57x |
| core_group_many | 1 | OFF | 1139ms | 2221ms | 1.95x |
| core_group_many | 1 | ON | 1203ms | 2039ms | 1.70x |
| fs_hw_floor | 1 | OFF | 140us | 167us | 1.20x |
| fs_hw_floor | 1 | ON | 105us | 144us | 1.38x |
| fs_hw_agg_point | 1 | OFF | 80us | 210us | 2.63x |
| fs_hw_agg_point | 1 | ON | 79us | 219us | 2.78x |
| fs_hw_agg_window7d | 1 | OFF | 76us | 221us | 2.89x |
| fs_hw_agg_window7d | 1 | ON | 78us | 217us | 2.79x |
| fs_hw_agg_greatest | 1 | OFF | 77us | 210us | 2.74x |
| fs_hw_agg_greatest | 1 | ON | 76us | 216us | 2.85x |
| fs_hw_agg_filter | 1 | OFF | 76us | 218us | 2.85x |
| fs_hw_agg_filter | 1 | ON | 76us | 227us | 2.98x |
| fs_hw_agg_batch10 | 1 | OFF | 216ms | 780us | 0.00x |
| fs_hw_agg_batch10 | 1 | ON | 214ms | 785us | 0.00x |
| fs_hw_agg_batch100 | 1 | OFF | 989ms | 7.67ms | 0.01x |
| fs_hw_agg_batch100 | 1 | ON | 925ms | 7.73ms | 0.01x |
| fs_hw_agg_batch1000 | 1 | OFF | 8731ms | 94.88ms | 0.01x |
| fs_hw_agg_batch1000 | 1 | ON | 7798ms | 94.80ms | 0.01x |
| fs_hw_agg_batch100_window | 1 | OFF | 981ms | 6.52ms | 0.01x |
| fs_hw_agg_batch100_window | 1 | ON | 979ms | 6.41ms | 0.01x |
| fs_hw_collect5 | 1 | OFF | 77us | 210us | 2.74x |
| fs_hw_collect5 | 1 | ON | 82us | 205us | 2.50x |
| fs_hw_collect5_cte | 1 | OFF | 76us | 232us | 3.04x |
| fs_hw_collect5_cte | 1 | ON | 81us | 224us | 2.79x |
| fs_hw_collect50 | 1 | OFF | 78us | 213us | 2.74x |
| fs_hw_collect50 | 1 | ON | 77us | 216us | 2.79x |
| fs_hw_collect50_cte | 1 | OFF | 84us | 236us | 2.82x |
| fs_hw_collect50_cte | 1 | ON | 82us | 236us | 2.88x |
| fs_hw_snow1_point | 1 | OFF | 227us | 1.11ms | 4.89x |
| fs_hw_snow1_point | 1 | ON | 201us | 1.16ms | 5.77x |
| fs_hw_snow2_point | 1 | OFF | 196us | 997us | 5.08x |
| fs_hw_snow2_point | 1 | ON | 208us | 970us | 4.66x |
| fs_hw_snow1_batch100 | 1 | OFF | 37.74ms | 44.99ms | 1.19x |
| fs_hw_snow1_batch100 | 1 | ON | 44.71ms | 45.84ms | 1.03x |
| fs_hw_snow2_left_chain | 1 | OFF | 204us | 968us | 4.75x |
| fs_hw_snow2_left_chain | 1 | ON | 204us | 941us | 4.62x |
| fs_hw_snow2_left_single | 1 | OFF | 192us | 971us | 5.05x |
| fs_hw_snow2_left_single | 1 | ON | 202us | 931us | 4.60x |
| fs_hw_strkey_point | 1 | OFF | 79us | 220us | 2.79x |
| fs_hw_strkey_point | 1 | ON | 81us | 209us | 2.58x |
| fs_hw_strkey_batch100 | 1 | OFF | 743ms | 11.28ms | 0.02x |
| fs_hw_strkey_batch100 | 1 | ON | 743ms | 11.18ms | 0.02x |
| fs_hw_composite_point | 1 | OFF | 93us | 321us | 3.46x |
| fs_hw_composite_point | 1 | ON | 81us | 321us | 3.95x |
| fs_hw_sessions_window2h | 1 | OFF | 83us | 280us | 3.36x |
| fs_hw_sessions_window2h | 1 | ON | 93us | 299us | 3.21x |
| cte_tpch_q2 | 1 | OFF | - | 100749ms | - |
| cte_tpch_q2 | 1 | ON | - | 84166ms | - |
| cte_tpch_q11 | 1 | OFF | - | 302ms | - |
| cte_tpch_q11 | 1 | ON | - | 310ms | - |
| cte_tpch_q13 | 1 | OFF | - | 2049ms | - |
| cte_tpch_q13 | 1 | ON | - | 2089ms | - |
| cte_tpch_q15 | 1 | OFF | - | 135ms | - |
| cte_tpch_q15 | 1 | ON | - | 136ms | - |
| cte_tpch_q22 | 1 | OFF | - | 2189ms | - |
| cte_tpch_q22 | 1 | ON | - | 2076ms | - |
| fs_hw_collect5_twin | 1 | OFF | - | 302us | - |
| fs_hw_collect5_twin | 1 | ON | - | 300us | - |
| fs_hw_snow1_twin | 1 | OFF | - | 488us | - |
| fs_hw_snow1_twin | 1 | ON | - | 514us | - |
| fs_hw_snow2_twin | 1 | OFF | - | 382us | - |
| fs_hw_snow2_twin | 1 | ON | - | 389us | - |

## C. RonSQL — where the time goes (server-side phases, avg per request, 1 thread)

client = end-to-end latency seen by rondb-cli; http+client = client - prepare - execute (RDRS HTTP handling, JSON, network); firstbatch = data-node execution until the first result row (single-table: the whole DoAggregation); load = NDB dictionary lookups; rows = result rows drained per request.

| query | compiler | client | prepare | execute | http+client | parse | analyze | load | plan | compile | ndbprep | send | firstbatch | drain | print | rows | top |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fs_floor | OFF | 180us | 2us | 149us | 28us | 1us | 0us | 1us | 0us | 0us | 0us | - | 148us | - | 0us | 0.0 | firstbatch 148us |
| fs_floor | ON | 190us | 2us | 160us | 28us | 1us | 0us | 1us | 0us | 0us | 0us | - | 159us | - | 0us | 0.0 | firstbatch 159us |
| fs_point | OFF | 225us | 11us | 180us | 33us | 6us | 0us | 3us | 0us | 1us | 2us | 7us | 156us | 0us | 1us | 0.0 | firstbatch 156us |
| fs_point | ON | 222us | 10us | 180us | 32us | 5us | 0us | 3us | 0us | 1us | 2us | 7us | 157us | 0us | 1us | 0.0 | firstbatch 157us |
| fs_batch | OFF | 640us | 15us | 584us | 41us | 8us | 0us | 5us | 0us | 1us | 2us | 10us | 504us | 0us | 50us | 0.0 | firstbatch 504us |
| fs_batch | ON | 663us | 16us | 610us | 37us | 9us | 0us | 5us | 0us | 2us | 2us | 10us | 527us | 0us | 50us | 0.0 | firstbatch 527us |
| fs_freshness | OFF | 2.67ms | 18us | 2.61ms | 42us | 9us | 0us | 7us | 0us | 1us | 3us | 13us | 2.56ms | 0us | 14us | 0.0 | firstbatch 2.56ms |
| fs_freshness | ON | 2.80ms | 17us | 2.74ms | 43us | 9us | 0us | 7us | 0us | 1us | 2us | 13us | 2.70ms | 0us | 14us | 0.0 | firstbatch 2.70ms |
| fs_supplier | OFF | 2.84ms | 11us | 2.79ms | 39us | 6us | 0us | 4us | 0us | 1us | 2us | 9us | 2.76ms | 0us | 1us | 0.0 | firstbatch 2.76ms |
| fs_supplier | ON | 3.01ms | 17us | 2.94ms | 53us | 9us | 0us | 6us | 0us | 2us | 2us | 14us | 2.91ms | 0us | 2us | 0.0 | firstbatch 2.91ms |
| fs_nation | OFF | 24.61ms | 20us | 24.49ms | 100us | 10us | 0us | 8us | 0us | 2us | 3us | 16us | 24.44ms | 0us | 8us | 0.0 | firstbatch 24.44ms |
| fs_nation | ON | 25.66ms | 47us | 25.45ms | 163us | 21us | 1us | 18us | 0us | 5us | 5us | 34us | 25.36ms | 0us | 14us | 0.0 | firstbatch 25.36ms |
| fs_topk | OFF | 25.90ms | 35us | 25.66ms | 205us | 16us | 1us | 15us | 0us | 2us | 4us | 24us | 21.94ms | 3.50ms | - | 1382.1 | firstbatch 21.94ms |
| fs_topk | ON | 26.45ms | 20us | 26.29ms | 140us | 11us | 0us | 7us | 0us | 1us | 3us | 13us | 22.66ms | 3.45ms | - | 1381.6 | firstbatch 22.66ms |
| fs_minmax | OFF | 3.60ms | 21us | 3.52ms | 59us | 12us | 0us | 6us | 0us | 2us | 2us | 14us | 3.48ms | 0us | 3us | 0.0 | firstbatch 3.48ms |
| fs_minmax | ON | 3.60ms | 21us | 3.53ms | 49us | 12us | 0us | 6us | 0us | 2us | 2us | 13us | 3.49ms | 0us | 3us | 0.0 | firstbatch 3.49ms |
| fs_dnf | OFF | 971us | 16us | 915us | 40us | 9us | 0us | 5us | 0us | 1us | 2us | 11us | 874us | 0us | 11us | 0.0 | firstbatch 874us |
| fs_dnf | ON | 932us | 14us | 881us | 37us | 8us | 0us | 5us | 0us | 1us | 2us | 10us | 845us | 0us | 10us | 0.0 | firstbatch 845us |
| fs_history | OFF | 1.35ms | 6us | 1.30ms | 44us | 3us | 0us | 2us | 0us | 0us | 1us | 5us | 346us | 320us | - | 1999.9 | firstbatch 346us |
| fs_history | ON | 1.34ms | 6us | 1.30ms | 34us | 3us | 0us | 2us | 0us | 0us | 1us | 5us | 342us | 320us | - | 1999.9 | firstbatch 342us |
| fs_latest | OFF | 579us | 4us | 546us | 29us | 2us | 0us | 2us | 0us | 0us | 0us | 4us | 484us | 31us | - | 100.0 | firstbatch 484us |
| fs_latest | ON | 572us | 4us | 540us | 29us | 1us | 0us | 2us | 0us | 0us | 0us | 4us | 478us | 31us | - | 100.0 | firstbatch 478us |
| offline_fs_batch | OFF | 1538ms | 54us | 1538ms | 246us | 28us | 1us | 13us | - | 7us | 5us | 45us | 1538ms | 0us | 23us | 0.0 | firstbatch 1538ms |
| offline_fs_batch | ON | 1544ms | 39us | 1544ms | 261us | 20us | 1us | 9us | - | 6us | 5us | 36us | 1544ms | 0us | 18us | 0.0 | firstbatch 1544ms |
| offline_fs_multi | OFF | 1464ms | 128us | 1464ms | 292us | 31us | 1us | 84us | - | 8us | 6us | 78us | 1463ms | 0us | 30us | 0.0 | firstbatch 1463ms |
| offline_fs_multi | ON | 1522ms | 128us | 1522ms | 242us | 33us | 1us | 82us | 0us | 8us | 6us | 77us | 1521ms | 0us | 29us | 0.0 | firstbatch 1521ms |
| offline_fs_join_body | OFF | 2000ms | 41us | 1999ms | 229us | 21us | 1us | 13us | - | 5us | 4us | 72us | 1999ms | - | 19us | 0.0 | firstbatch 1999ms |
| offline_fs_join_body | ON | 2084ms | 41us | 2083ms | 859us | 21us | 1us | 11us | - | 5us | 4us | 37us | 2083ms | - | 20us | 0.0 | firstbatch 2083ms |
| offline_fs_anti | OFF | 206ms | 46us | 206ms | 144us | 18us | 1us | 20us | 0us | 5us | 4us | 27us | 206ms | 0us | 21us | 0.0 | firstbatch 206ms |
| offline_fs_anti | ON | 215ms | 59us | 215ms | 191us | 22us | 1us | 27us | 0us | 6us | 5us | 37us | 215ms | 0us | 21us | 0.0 | firstbatch 215ms |
| offline_fs_wide | OFF | 2032ms | 65us | 2031ms | 275us | 36us | 1us | 15us | - | 9us | 5us | 54us | 2031ms | 0us | 31us | 0.0 | firstbatch 2031ms |
| offline_fs_wide | ON | 2047ms | 55us | 2047ms | 235us | 29us | 0us | 13us | - | 9us | 5us | 36us | 2047ms | - | 37us | 0.0 | firstbatch 2047ms |
| offline_fs_chain | OFF | 1119ms | 57us | 1119ms | 263us | 28us | 1us | 16us | 0us | 8us | 5us | 61us | 1119ms | - | 15us | 0.0 | firstbatch 1119ms |
| offline_fs_chain | ON | 1154ms | 57us | 1154ms | 283us | 26us | 1us | 15us | 0us | 10us | 6us | 37us | 1153ms | 0us | 14us | 0.0 | firstbatch 1153ms |
| offline_fs_scalar | OFF | 1125ms | 32us | 1124ms | 258us | 15us | 1us | 8us | 0us | 5us | 4us | 47us | 1124ms | 0us | 9us | 0.0 | firstbatch 1124ms |
| offline_fs_scalar | ON | 1103ms | 48us | 1102ms | 252us | 23us | 0us | 11us | - | 7us | 5us | 36us | 1102ms | 0us | 12us | 0.0 | firstbatch 1102ms |
| tpch_q2 | OFF | 1123ms | 76us | 1123ms | 184us | 23us | 1us | 42us | - | 7us | 4us | 52us | 1123ms | - | 28us | 0.0 | firstbatch 1123ms |
| tpch_q2 | ON | 1094ms | 77us | 1094ms | 253us | 19us | 1us | 49us | - | 6us | 4us | 52us | 1094ms | - | 22us | 0.0 | firstbatch 1094ms |
| tpch_q11 | OFF | 102ms | 41us | 102ms | 169us | 17us | 1us | 17us | 0us | 5us | 4us | 26us | 102ms | 0us | 5us | 0.0 | firstbatch 102ms |
| tpch_q11 | ON | 121ms | 43us | 120ms | 187us | 18us | 1us | 18us | 0us | 5us | 5us | 27us | 120ms | 0us | 6us | 0.0 | firstbatch 120ms |
| tpch_q13 | OFF | 1111ms | 40us | 1111ms | 260us | 19us | 1us | 11us | 0us | 6us | 4us | 37us | 1110ms | 0us | 5us | 0.0 | firstbatch 1110ms |
| tpch_q13 | ON | 1146ms | 41us | 1145ms | 269us | 20us | 1us | 11us | - | 6us | 4us | 41us | 1145ms | - | 6us | 0.0 | firstbatch 1145ms |
| tpch_q15 | OFF | 54.92ms | 40us | 54.68ms | 200us | 16us | 1us | 16us | 0us | 5us | 4us | 22us | 54.61ms | 0us | 10us | 0.0 | firstbatch 54.61ms |
| tpch_q15 | ON | 57.68ms | 36us | 57.44ms | 204us | 15us | 0us | 15us | 0us | 4us | 4us | 23us | 57.37ms | 0us | 8us | 0.0 | firstbatch 57.37ms |
| tpch_q22 | OFF | 1096ms | 93us | 1096ms | 267us | 29us | 1us | 51us | - | 8us | 5us | 55us | 1095ms | 0us | 17us | 0.0 | firstbatch 1095ms |
| tpch_q22 | ON | 1104ms | 94us | 1104ms | 246us | 20us | 1us | 65us | - | 5us | 4us | 37us | 1103ms | 0us | 14us | 0.0 | firstbatch 1103ms |
| core_pk_lookup | OFF | 404us | 3us | 373us | 28us | 1us | 0us | 2us | 0us | 0us | 0us | - | 372us | - | - | 1.0 | firstbatch 372us |
| core_pk_lookup | ON | 380us | 3us | 349us | 28us | 1us | 0us | 2us | 0us | 0us | 0us | - | 348us | - | - | 1.0 | firstbatch 348us |
| core_in_pk100 | OFF | 398ms | 36us | 398ms | 174us | 19us | 1us | 8us | 3us | 4us | 8us | - | 398ms | - | 10us | 0.0 | firstbatch 398ms |
| core_in_pk100 | ON | 403ms | 61us | 402ms | 209us | 20us | 1us | 31us | 3us | 3us | 8us | - | 402ms | - | 10us | 0.0 | firstbatch 402ms |
| core_in_idx100 | OFF | 403ms | 57us | 403ms | 173us | 21us | 2us | 25us | 3us | 4us | 9us | - | 403ms | - | 47us | 0.0 | firstbatch 403ms |
| core_in_idx100 | ON | 393ms | 49us | 393ms | 171us | 20us | 2us | 18us | 3us | 5us | 9us | - | 393ms | - | 46us | 0.0 | firstbatch 393ms |
| core_idx_range | OFF | 2.49ms | 5us | 2.45ms | 35us | 2us | 0us | 2us | 0us | 0us | 1us | - | 2.45ms | - | 1us | 0.0 | firstbatch 2.45ms |
| core_idx_range | ON | 2.64ms | 4us | 2.60ms | 36us | 2us | 0us | 2us | 0us | 0us | 1us | - | 2.60ms | - | 1us | 0.0 | firstbatch 2.60ms |
| core_avg_range | OFF | 257us | 5us | 221us | 31us | 2us | 0us | 2us | 0us | 1us | 1us | - | 219us | - | 1us | 0.0 | firstbatch 219us |
| core_avg_range | ON | 258us | 5us | 223us | 30us | 2us | 0us | 2us | 0us | 1us | 0us | - | 221us | - | 1us | 0.0 | firstbatch 221us |
| core_pass_range | OFF | 492us | 4us | 456us | 33us | 2us | 0us | 2us | 0us | 0us | 1us | 4us | 176us | 274us | - | 999.8 | drain 274us |
| core_pass_range | ON | 492us | 4us | 455us | 33us | 2us | 0us | 2us | 0us | 0us | 1us | 4us | 176us | 273us | - | 999.8 | drain 273us |
| core_scan_agg | OFF | 642ms | 58us | 642ms | 252us | 12us | 1us | 36us | 1us | 5us | 2us | - | 642ms | - | 15us | 0.0 | firstbatch 642ms |
| core_scan_agg | ON | 650ms | 39us | 650ms | 231us | 10us | 0us | 21us | 1us | 4us | 1us | - | 650ms | - | 12us | 0.0 | firstbatch 650ms |
| core_scan_filter | OFF | 319ms | 51us | 319ms | 259us | 15us | 1us | 24us | 3us | 5us | 7us | - | 319ms | - | 12us | 0.0 | firstbatch 319ms |
| core_scan_filter | ON | 321ms | 41us | 321ms | 249us | 12us | 1us | 19us | 2us | 4us | 7us | - | 321ms | - | 11us | 0.0 | firstbatch 321ms |
| core_group_few | OFF | 160ms | 22us | 160ms | 248us | 8us | 0us | 8us | 1us | 4us | 1us | - | 160ms | - | 11us | 0.0 | firstbatch 160ms |
| core_group_few | ON | 170ms | 24us | 170ms | 216us | 8us | 1us | 9us | 1us | 4us | 1us | - | 170ms | - | 11us | 0.0 | firstbatch 170ms |
| core_group_many | OFF | 1139ms | 69us | 1130ms | 8.94ms | 9us | 0us | 52us | 1us | 4us | 2us | - | 1041ms | - | 77.22ms | 0.0 | firstbatch 1041ms |
| core_group_many | ON | 1203ms | 55us | 1194ms | 8.86ms | 10us | 1us | 37us | 1us | 4us | 2us | - | 1106ms | - | 76.74ms | 0.0 | firstbatch 1106ms |
| fs_hw_floor | OFF | 140us | 2us | 111us | 27us | 1us | 0us | 1us | 0us | 0us | 0us | - | 110us | - | 0us | 0.0 | firstbatch 110us |
| fs_hw_floor | ON | 105us | 2us | 76us | 27us | 1us | 0us | 1us | 0us | 0us | 0us | - | 75us | - | 0us | 0.0 | firstbatch 75us |
| fs_hw_agg_point | OFF | 80us | 4us | 46us | 29us | 2us | 0us | 1us | 0us | 0us | 1us | - | 44us | - | 0us | 0.0 | firstbatch 44us |
| fs_hw_agg_point | ON | 79us | 4us | 47us | 28us | 2us | 0us | 1us | 0us | 0us | 0us | - | 45us | - | 0us | 0.0 | firstbatch 45us |
| fs_hw_agg_window7d | OFF | 76us | 5us | 43us | 29us | 2us | 0us | 1us | 0us | 0us | 1us | - | 41us | - | 0us | 0.0 | firstbatch 41us |
| fs_hw_agg_window7d | ON | 78us | 5us | 44us | 29us | 2us | 0us | 1us | 0us | 0us | 1us | - | 42us | - | 0us | 0.0 | firstbatch 42us |
| fs_hw_agg_greatest | OFF | 77us | 5us | 43us | 29us | 2us | 0us | 1us | 0us | 1us | 1us | - | 42us | - | 0us | 0.0 | firstbatch 42us |
| fs_hw_agg_greatest | ON | 76us | 4us | 43us | 28us | 2us | 0us | 1us | 0us | 1us | 0us | - | 42us | - | 0us | 0.0 | firstbatch 42us |
| fs_hw_agg_filter | OFF | 76us | 5us | 43us | 29us | 2us | 0us | 1us | 0us | 0us | 1us | - | 41us | - | 0us | 0.0 | firstbatch 41us |
| fs_hw_agg_filter | ON | 76us | 5us | 42us | 29us | 3us | 0us | 1us | 0us | 0us | 1us | - | 40us | - | 0us | 0.0 | firstbatch 40us |
| fs_hw_agg_batch10 | OFF | 216ms | 32us | 216ms | 178us | 12us | 1us | 13us | 1us | 3us | 5us | - | 216ms | - | 6us | 0.0 | firstbatch 216ms |
| fs_hw_agg_batch10 | ON | 214ms | 27us | 214ms | 193us | 10us | 1us | 10us | 1us | 3us | 5us | - | 214ms | - | 6us | 0.0 | firstbatch 214ms |
| fs_hw_agg_batch100 | OFF | 989ms | 71us | 989ms | 219us | 23us | 2us | 37us | 3us | 4us | 9us | - | 989ms | - | 22us | 0.0 | firstbatch 989ms |
| fs_hw_agg_batch100 | ON | 925ms | 54us | 924ms | 256us | 19us | 1us | 26us | 3us | 3us | 9us | - | 924ms | - | 16us | 0.0 | firstbatch 924ms |
| fs_hw_agg_batch1000 | OFF | 8731ms | 236us | 8730ms | 294us | 106us | 19us | 74us | 28us | 5us | 48us | - | 8730ms | - | 165us | 0.0 | firstbatch 8730ms |
| fs_hw_agg_batch1000 | ON | 7798ms | 352us | 7797ms | 288us | 124us | 27us | 157us | 35us | 6us | 60us | - | 7797ms | - | 195us | 0.0 | firstbatch 7797ms |
| fs_hw_agg_batch100_window | OFF | 981ms | 69us | 981ms | 221us | 19us | 2us | 39us | 4us | 4us | 11us | - | 981ms | - | 16us | 0.0 | firstbatch 981ms |
| fs_hw_agg_batch100_window | ON | 979ms | 79us | 978ms | 221us | 20us | 2us | 48us | 3us | 4us | 12us | - | 978ms | - | 20us | 0.0 | firstbatch 978ms |
| fs_hw_collect5 | OFF | 77us | 3us | 45us | 29us | 2us | 0us | 1us | 0us | 0us | 1us | 4us | 38us | 1us | - | 4.2 | firstbatch 38us |
| fs_hw_collect5 | ON | 82us | 3us | 51us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | 4us | 45us | 1us | - | 4.2 | firstbatch 45us |
| fs_hw_collect5_cte | OFF | 76us | 4us | 44us | 28us | 3us | 0us | 1us | 0us | 0us | 1us | 4us | 38us | 1us | - | 4.2 | firstbatch 38us |
| fs_hw_collect5_cte | ON | 81us | 5us | 47us | 29us | 3us | 0us | 1us | 0us | 0us | 1us | 4us | 40us | 1us | - | 4.2 | firstbatch 40us |
| fs_hw_collect50 | OFF | 78us | 3us | 46us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | 4us | 36us | 5us | - | 17.6 | firstbatch 36us |
| fs_hw_collect50 | ON | 77us | 3us | 46us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | 4us | 36us | 5us | - | 17.6 | firstbatch 36us |
| fs_hw_collect50_cte | OFF | 84us | 4us | 51us | 28us | 3us | 0us | 1us | 0us | 0us | 1us | 4us | 41us | 5us | - | 17.6 | firstbatch 41us |
| fs_hw_collect50_cte | ON | 82us | 5us | 49us | 28us | 3us | 0us | 1us | 0us | 0us | 1us | 4us | 39us | 5us | - | 17.6 | firstbatch 39us |
| fs_hw_snow1_point | OFF | 227us | 12us | 173us | 42us | 5us | 0us | 5us | 0us | 1us | 3us | 16us | 128us | 8us | - | 0.9 | firstbatch 128us |
| fs_hw_snow1_point | ON | 201us | 9us | 157us | 35us | 4us | 0us | 3us | 0us | 0us | 2us | 12us | 122us | 8us | - | 0.9 | firstbatch 122us |
| fs_hw_snow2_point | OFF | 196us | 8us | 158us | 30us | 5us | 0us | 3us | 0us | 0us | 2us | 10us | 130us | 5us | - | 0.8 | firstbatch 130us |
| fs_hw_snow2_point | ON | 208us | 10us | 166us | 33us | 5us | 0us | 4us | 0us | 0us | 2us | 13us | 132us | 5us | - | 0.8 | firstbatch 132us |
| fs_hw_snow1_batch100 | OFF | 37.74ms | 32us | 37.61ms | 98us | 19us | 0us | 11us | 0us | 1us | 3us | 20us | 37.45ms | 105us | - | 88.9 | firstbatch 37.45ms |
| fs_hw_snow1_batch100 | ON | 44.71ms | 33us | 44.56ms | 117us | 19us | 0us | 12us | 0us | 1us | 3us | 19us | 44.38ms | 123us | - | 89.0 | firstbatch 44.38ms |
| fs_hw_snow2_left_chain | OFF | 204us | 7us | 168us | 29us | 4us | 0us | 3us | 0us | 0us | 2us | 11us | 138us | 6us | - | 0.8 | firstbatch 138us |
| fs_hw_snow2_left_chain | ON | 204us | 8us | 163us | 32us | 4us | 0us | 3us | 0us | 0us | 2us | 14us | 129us | 6us | - | 0.8 | firstbatch 129us |
| fs_hw_snow2_left_single | OFF | 192us | 8us | 155us | 29us | 4us | 0us | 3us | 0us | 0us | 2us | 10us | 126us | 6us | - | 1.0 | firstbatch 126us |
| fs_hw_snow2_left_single | ON | 202us | 9us | 162us | 31us | 5us | 0us | 3us | 0us | 0us | 2us | 12us | 128us | 7us | - | 1.0 | firstbatch 128us |
| fs_hw_strkey_point | OFF | 79us | 4us | 46us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 45us | - | 0us | 0.0 | firstbatch 45us |
| fs_hw_strkey_point | ON | 81us | 4us | 48us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 47us | - | 0us | 0.0 | firstbatch 47us |
| fs_hw_strkey_batch100 | OFF | 743ms | 64us | 743ms | 186us | 26us | 1us | 27us | 3us | 3us | 10us | - | 743ms | - | 43us | 0.0 | firstbatch 743ms |
| fs_hw_strkey_batch100 | ON | 743ms | 56us | 743ms | 174us | 22us | 1us | 25us | 3us | 3us | 9us | - | 743ms | - | 32us | 0.0 | firstbatch 743ms |
| fs_hw_composite_point | OFF | 93us | 4us | 61us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 60us | - | 0us | 0.0 | firstbatch 60us |
| fs_hw_composite_point | ON | 81us | 4us | 50us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 48us | - | 0us | 0.0 | firstbatch 48us |
| fs_hw_sessions_window2h | OFF | 83us | 4us | 51us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 50us | - | 0us | 0.0 | firstbatch 50us |
| fs_hw_sessions_window2h | ON | 93us | 4us | 61us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 60us | - | 0us | 0.0 | firstbatch 60us |

## D. MySQL server — where the time goes (1 thread)

ndb wait = Ndb_api_wait_nanos_count per request (time the mysqld connection waited for data nodes), corrected by the idle baseline (1.00 s/s of background NDB API waiting measured before the matrix); mysqld = client latency - ndb wait (parsing, optimizer, row processing, result transfer); batches / rows = scan batches and rows received from NDB per request; pushed = pushed (SPJ) queries executed per request. Counters include the warmup request and are approximate at short case durations.

| query | engine | compiler | client | ndb wait | mysqld | batches/req | rows/req | pushed/req | KB recv/req |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| fs_floor | mysqld_nopush | OFF | 130us | 72us | 58us | 3.0 | 3 | 0.00 | 0.4 |
| fs_floor | mysqld_nopush | ON | 123us | 64us | 59us | 3.0 | 3 | 0.00 | 0.4 |
| fs_point | mysqld_nopush | OFF | 230us | 42us | 189us | 3.7 | 11 | 0.00 | 0.5 |
| fs_point | mysqld_nopush | ON | 234us | 43us | 191us | 3.7 | 11 | 0.00 | 0.5 |
| fs_batch | mysqld_nopush | OFF | 7.36ms | 6.36ms | 1000us | 7.4 | 1104 | 0.00 | 54.5 |
| fs_batch | mysqld_nopush | ON | 7.95ms | 6.92ms | 1.03ms | 7.4 | 1109 | 0.00 | 54.7 |
| fs_freshness | mysqld_nopush | OFF | 122ms | 115ms | 7.34ms | 18.7 | 10580 | 0.00 | 473.7 |
| fs_freshness | mysqld_nopush | ON | 112ms | 105ms | 6.92ms | 18.6 | 10563 | 0.00 | 473.3 |
| fs_supplier | mysqld_nopush | OFF | 6.68ms | 1.23ms | 5.45ms | 12.0 | 7950 | 0.00 | 424.1 |
| fs_supplier | mysqld_nopush | ON | 6.82ms | 1.28ms | 5.54ms | 12.0 | 7950 | 0.00 | 424.1 |
| fs_nation | mysqld_nopush | OFF | 43.56ms | 8.43ms | 35.13ms | 49.0 | 45362 | 0.00 | 2398.2 |
| fs_nation | mysqld_nopush | ON | 46.68ms | 8.52ms | 38.16ms | 49.0 | 45370 | 0.00 | 2398.5 |
| fs_topk | mysqld_nopush | OFF | 990ms | 882ms | 108ms | 41.3 | 42152 | 0.00 | 2200.4 |
| fs_topk | mysqld_nopush | ON | 1105ms | 974ms | 131ms | 41.8 | 42175 | 0.00 | 2198.5 |
| fs_minmax | mysqld_nopush | OFF | 9.58ms | 1.31ms | 8.27ms | 12.0 | 7950 | 0.00 | 483.0 |
| fs_minmax | mysqld_nopush | ON | 9.57ms | 1.35ms | 8.22ms | 12.0 | 7951 | 0.00 | 483.0 |
| fs_dnf | mysqld_nopush | OFF | 112ms | 109ms | 3.51ms | 4.0 | 2251 | 0.00 | 232.0 |
| fs_dnf | mysqld_nopush | ON | 119ms | 115ms | 4.06ms | 4.0 | 2245 | 0.00 | 232.2 |
| fs_history | mysqld_nopush | OFF | 1.98ms | 562us | 1.42ms | 4.0 | 2001 | 0.00 | 109.5 |
| fs_history | mysqld_nopush | ON | 2.02ms | 594us | 1.43ms | 4.0 | 2001 | 0.00 | 109.5 |
| fs_latest | mysqld_nopush | OFF | 626us | 462us | 164us | 4.0 | 3960 | 0.00 | 170.3 |
| fs_latest | mysqld_nopush | ON | 624us | 458us | 166us | 4.0 | 3960 | 0.00 | 170.3 |
| offline_fs_batch | mysqld_nopush | OFF | 78726ms | 72151ms | 6575ms | 1517.0 | 1650003 | 0.00 | 72712.4 |
| offline_fs_batch | mysqld_nopush | ON | 67232ms | 70272ms | 0us | 1517.0 | 1650003 | 0.00 | 72710.6 |
| offline_fs_multi | mysqld_nopush | OFF | 41914ms | 39530ms | 2384ms | 1653.7 | 1722308 | 0.00 | 69584.1 |
| offline_fs_multi | mysqld_nopush | ON | 48632ms | 44245ms | 4387ms | 1653.7 | 1722314 | 0.00 | 69586.3 |
| offline_fs_join_body | mysqld_nopush | OFF | 11555ms | 8735ms | 2821ms | 1.3 | 7510001 | 0.00 | 393301.7 |
| offline_fs_join_body | mysqld_nopush | ON | 11449ms | 8868ms | 2582ms | 0.7 | 7510001 | 0.00 | 393301.6 |
| offline_fs_anti | mysqld_nopush | OFF | 218ms | 40.75ms | 178ms | 289.0 | 283634 | 0.00 | 11023.4 |
| offline_fs_anti | mysqld_nopush | ON | 223ms | 41.45ms | 181ms | 289.0 | 283634 | 0.00 | 11023.7 |
| offline_fs_wide | mysqld_nopush | OFF | 82285ms | 76036ms | 6249ms | 1517.0 | 1650003 | 0.00 | 78571.8 |
| offline_fs_wide | mysqld_nopush | ON | 72995ms | 76086ms | 0us | 1517.0 | 1650003 | 0.00 | 78571.8 |
| offline_fs_chain | mysqld_nopush | OFF | 2174ms | 1750ms | 423ms | 1517.0 | 1500000 | 0.00 | 58637.1 |
| offline_fs_chain | mysqld_nopush | ON | 2095ms | 1740ms | 355ms | 1517.0 | 1500000 | 0.00 | 58635.2 |
| offline_fs_scalar | mysqld_nopush | OFF | 2102ms | 1743ms | 359ms | 1517.0 | 1500000 | 0.00 | 58635.2 |
| offline_fs_scalar | mysqld_nopush | ON | 1978ms | 1710ms | 268ms | 1517.0 | 1500000 | 0.00 | 58635.2 |
| tpch_q2 | mysqld_nopush | OFF | 274ms | 182ms | 91.77ms | 3.8 | 50364 | 0.00 | 4234.9 |
| tpch_q2 | mysqld_nopush | ON | 278ms | 184ms | 93.35ms | 3.8 | 50364 | 0.00 | 4234.9 |
| tpch_q11 | mysqld_nopush | OFF | 2043ms | 1312ms | 731ms | 3.3 | 4800003 | 0.00 | 353168.1 |
| tpch_q11 | mysqld_nopush | ON | 2271ms | 1378ms | 893ms | 3.3 | 4800003 | 0.00 | 353168.1 |
| tpch_q13 | mysqld_nopush | OFF | 1486ms | 1133ms | 353ms | 0.0 | 1650000 | 0.00 | 145950.2 |
| tpch_q13 | mysqld_nopush | ON | 1425ms | 1181ms | 245ms | 0.0 | 1650000 | 0.00 | 145950.2 |
| tpch_q15 | mysqld_nopush | OFF | 157ms | 46.93ms | 110ms | 231.1 | 226897 | 0.00 | 14187.6 |
| tpch_q15 | mysqld_nopush | ON | 156ms | 47.54ms | 108ms | 231.1 | 226897 | 0.00 | 14187.6 |
| tpch_q22 | mysqld_nopush | OFF | 217ms | 188ms | 28.52ms | 140.7 | 331229 | 0.00 | 24097.8 |
| tpch_q22 | mysqld_nopush | ON | 223ms | 192ms | 31.13ms | 140.9 | 331229 | 0.00 | 24097.6 |
| core_pk_lookup | mysqld_nopush | OFF | 439us | 342us | 97us | 0.0 | 2 | 0.00 | 0.1 |
| core_pk_lookup | mysqld_nopush | ON | 454us | 352us | 102us | 0.0 | 2 | 0.00 | 0.1 |
| core_in_pk100 | mysqld_nopush | OFF | 502us | 168us | 334us | 0.0 | 101 | 0.00 | 7.1 |
| core_in_pk100 | mysqld_nopush | ON | 502us | 171us | 331us | 0.0 | 101 | 0.00 | 7.1 |
| core_in_idx100 | mysqld_nopush | OFF | 4.93ms | 952us | 3.98ms | 4.0 | 1003 | 0.00 | 47.1 |
| core_in_idx100 | mysqld_nopush | ON | 5.01ms | 1.03ms | 3.98ms | 4.0 | 1003 | 0.00 | 47.1 |
| core_idx_range | mysqld_nopush | OFF | 6.13ms | 2.83ms | 3.30ms | 20.0 | 18640 | 0.00 | 728.7 |
| core_idx_range | mysqld_nopush | ON | 6.16ms | 2.83ms | 3.33ms | 20.0 | 18640 | 0.00 | 728.7 |
| core_avg_range | mysqld_nopush | OFF | 616us | 240us | 376us | 4.0 | 1001 | 0.00 | 43.1 |
| core_avg_range | mysqld_nopush | ON | 613us | 239us | 374us | 4.0 | 1001 | 0.00 | 43.1 |
| core_pass_range | mysqld_nopush | OFF | 978us | 308us | 670us | 4.0 | 1001 | 0.00 | 50.9 |
| core_pass_range | mysqld_nopush | ON | 981us | 309us | 672us | 4.0 | 1001 | 0.00 | 50.9 |
| core_scan_agg | mysqld_nopush | OFF | 1986ms | 1010ms | 976ms | 6075.0 | 6007924 | 0.00 | 234727.1 |
| core_scan_agg | mysqld_nopush | ON | 1319ms | 481ms | 838ms | 6075.0 | 6007924 | 0.00 | 234727.1 |
| core_scan_filter | mysqld_nopush | OFF | 680ms | 437ms | 243ms | 1675.7 | 1634287 | 0.00 | 134108.5 |
| core_scan_filter | mysqld_nopush | ON | 661ms | 422ms | 239ms | 1673.6 | 1634287 | 0.00 | 134108.4 |
| core_group_few | mysqld_nopush | OFF | 618ms | 151ms | 467ms | 1517.0 | 1500000 | 0.00 | 58635.2 |
| core_group_few | mysqld_nopush | ON | 609ms | 151ms | 457ms | 1517.0 | 1500000 | 0.00 | 58635.2 |
| core_group_many | mysqld_nopush | OFF | 2221ms | 1755ms | 466ms | 1517.0 | 1500000 | 0.00 | 64496.4 |
| core_group_many | mysqld_nopush | ON | 2039ms | 1709ms | 329ms | 1517.0 | 1500000 | 0.00 | 64494.6 |
| fs_hw_floor | mysqld_nopush | OFF | 167us | 107us | 61us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_floor | mysqld_nopush | ON | 144us | 85us | 59us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_agg_point | mysqld_nopush | OFF | 210us | 56us | 154us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_point | mysqld_nopush | ON | 219us | 64us | 155us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_window7d | mysqld_nopush | OFF | 221us | 55us | 166us | 2.4 | 13 | 0.00 | 0.7 |
| fs_hw_agg_window7d | mysqld_nopush | ON | 217us | 48us | 169us | 2.4 | 13 | 0.00 | 0.8 |
| fs_hw_agg_greatest | mysqld_nopush | OFF | 210us | 57us | 154us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_greatest | mysqld_nopush | ON | 216us | 64us | 151us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_filter | mysqld_nopush | OFF | 218us | 46us | 172us | 2.9 | 29 | 0.00 | 1.5 |
| fs_hw_agg_filter | mysqld_nopush | ON | 227us | 57us | 170us | 2.9 | 29 | 0.00 | 1.5 |
| fs_hw_agg_batch10 | mysqld_nopush | OFF | 780us | 153us | 627us | 4.0 | 364 | 0.00 | 17.2 |
| fs_hw_agg_batch10 | mysqld_nopush | ON | 785us | 155us | 630us | 4.0 | 364 | 0.00 | 17.2 |
| fs_hw_agg_batch100 | mysqld_nopush | OFF | 7.67ms | 2.42ms | 5.25ms | 5.2 | 3621 | 0.00 | 169.8 |
| fs_hw_agg_batch100 | mysqld_nopush | ON | 7.73ms | 2.53ms | 5.20ms | 5.2 | 3615 | 0.00 | 169.6 |
| fs_hw_agg_batch1000 | mysqld_nopush | OFF | 94.88ms | 86.06ms | 8.82ms | 56.2 | 36343 | 0.00 | 1704.4 |
| fs_hw_agg_batch1000 | mysqld_nopush | ON | 94.80ms | 86.16ms | 8.64ms | 56.3 | 36305 | 0.00 | 1702.6 |
| fs_hw_agg_batch100_window | mysqld_nopush | OFF | 6.52ms | 1.59ms | 4.93ms | 8.0 | 1937 | 0.00 | 91.0 |
| fs_hw_agg_batch100_window | mysqld_nopush | ON | 6.41ms | 1.50ms | 4.91ms | 8.0 | 1935 | 0.00 | 90.9 |
| fs_hw_collect5 | mysqld_nopush | OFF | 210us | 64us | 147us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5 | mysqld_nopush | ON | 205us | 60us | 145us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld_nopush | OFF | 232us | 60us | 172us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld_nopush | ON | 224us | 54us | 171us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld_nopush | OFF | 213us | 59us | 154us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld_nopush | ON | 216us | 58us | 157us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50_cte | mysqld_nopush | OFF | 236us | 52us | 184us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50_cte | mysqld_nopush | ON | 236us | 52us | 184us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_point | mysqld_nopush | OFF | 1.11ms | 958us | 152us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_point | mysqld_nopush | ON | 1.16ms | 1.01ms | 149us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld_nopush | OFF | 997us | 818us | 179us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld_nopush | ON | 970us | 793us | 177us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_batch100 | mysqld_nopush | OFF | 44.99ms | 43.83ms | 1.16ms | 0.0 | 209 | 0.00 | 13.5 |
| fs_hw_snow1_batch100 | mysqld_nopush | ON | 45.84ms | 44.53ms | 1.31ms | 0.0 | 205 | 0.00 | 13.4 |
| fs_hw_snow2_left_chain | mysqld_nopush | OFF | 968us | 792us | 176us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_chain | mysqld_nopush | ON | 941us | 765us | 177us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld_nopush | OFF | 971us | 800us | 171us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld_nopush | ON | 931us | 759us | 172us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_strkey_point | mysqld_nopush | OFF | 220us | 59us | 161us | 3.1 | 37 | 0.00 | 1.8 |
| fs_hw_strkey_point | mysqld_nopush | ON | 209us | 54us | 154us | 3.1 | 37 | 0.00 | 1.8 |
| fs_hw_strkey_batch100 | mysqld_nopush | OFF | 11.28ms | 2.31ms | 8.97ms | 5.3 | 3634 | 0.00 | 198.8 |
| fs_hw_strkey_batch100 | mysqld_nopush | ON | 11.18ms | 2.25ms | 8.93ms | 5.3 | 3633 | 0.00 | 198.7 |
| fs_hw_composite_point | mysqld_nopush | OFF | 321us | 73us | 248us | 1.9 | 4 | 0.00 | 0.3 |
| fs_hw_composite_point | mysqld_nopush | ON | 321us | 75us | 245us | 1.9 | 4 | 0.00 | 0.3 |
| fs_hw_sessions_window2h | mysqld_nopush | OFF | 280us | 87us | 193us | 0.9 | 2 | 0.00 | 0.2 |
| fs_hw_sessions_window2h | mysqld_nopush | ON | 299us | 106us | 193us | 0.9 | 2 | 0.00 | 0.2 |
| cte_tpch_q2 | mysqld_nopush | OFF | 100749ms | 90678ms | 10071ms | 811.3 | 1000008 | 0.00 | 59414.1 |
| cte_tpch_q2 | mysqld_nopush | ON | 84166ms | 90706ms | 0us | 810.0 | 1000006 | 0.00 | 59413.9 |
| cte_tpch_q11 | mysqld_nopush | OFF | 302ms | 79.49ms | 223ms | 814.1 | 800415 | 0.00 | 25033.6 |
| cte_tpch_q11 | mysqld_nopush | ON | 310ms | 79.19ms | 231ms | 814.0 | 800415 | 0.00 | 25033.7 |
| cte_tpch_q13 | mysqld_nopush | OFF | 2049ms | 1629ms | 420ms | 1670.0 | 1650000 | 0.00 | 50436.3 |
| cte_tpch_q13 | mysqld_nopush | ON | 2089ms | 1746ms | 343ms | 1670.0 | 1650000 | 0.00 | 50436.3 |
| cte_tpch_q15 | mysqld_nopush | OFF | 135ms | 46.38ms | 88.13ms | 231.0 | 226896 | 0.00 | 12414.7 |
| cte_tpch_q15 | mysqld_nopush | ON | 136ms | 47.65ms | 88.19ms | 231.1 | 226896 | 0.00 | 12414.9 |
| cte_tpch_q22 | mysqld_nopush | OFF | 2189ms | 1769ms | 420ms | 1657.7 | 1636411 | 0.00 | 51716.0 |
| cte_tpch_q22 | mysqld_nopush | ON | 2076ms | 1747ms | 328ms | 1658.2 | 1636412 | 0.00 | 51716.1 |
| fs_hw_collect5_twin | mysqld_nopush | OFF | 302us | 70us | 232us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_twin | mysqld_nopush | ON | 300us | 66us | 234us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_twin | mysqld_nopush | OFF | 488us | 364us | 123us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_twin | mysqld_nopush | ON | 514us | 392us | 122us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_twin | mysqld_nopush | OFF | 382us | 249us | 133us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_twin | mysqld_nopush | ON | 389us | 259us | 130us | 0.0 | 3 | 0.00 | 0.2 |

## E. ndbinfo.jit deltas per case, compiler ON, 1 thread (per request incl. warmup)

| query | engine | compiled/req | reused/req | fallback | rows executed/req | compile us/req | compile share |
|---|---|---:|---:|---:|---:|---:|---:|
| fs_floor | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_floor | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_point | ronsql | 0.00 | 8.00 | 0 | 21 | 0.0 | 0.0% |
| fs_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_batch | ronsql | 0.00 | 12.00 | 0 | 2200 | 0.0 | 0.0% |
| fs_batch | mysqld_nopush | 1.83 | 5.56 | 0 | 1084 | 2.3 | 0.0% |
| fs_freshness | ronsql | 0.00 | 18.00 | 0 | 20997 | 0.0 | 0.0% |
| fs_freshness | mysqld_nopush | 1.63 | 8.88 | 0 | 10317 | 3.4 | 0.0% |
| fs_supplier | ronsql | 0.00 | 12.00 | 0 | 15704 | 0.0 | 0.0% |
| fs_supplier | mysqld_nopush | 0.00 | 4.00 | 0 | 7547 | 0.0 | 0.0% |
| fs_nation | ronsql | 0.00 | 12.00 | 0 | 86079 | 0.0 | 0.0% |
| fs_nation | mysqld_nopush | 0.00 | 4.00 | 0 | 39347 | 0.0 | 0.0% |
| fs_topk | ronsql | 0.00 | 10.00 | 0 | 84694 | 0.0 | 0.0% |
| fs_topk | mysqld_nopush | 0.00 | 4.00 | 0 | 39347 | 0.0 | 0.0% |
| fs_minmax | ronsql | 0.00 | 12.00 | 0 | 15704 | 0.0 | 0.0% |
| fs_minmax | mysqld_nopush | 0.00 | 4.00 | 0 | 7547 | 0.0 | 0.0% |
| fs_dnf | ronsql | 0.00 | 12.00 | 0 | 4081 | 0.0 | 0.0% |
| fs_dnf | mysqld_nopush | 0.96 | 3.04 | 0 | 2007 | 2.0 | 0.0% |
| fs_history | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_history | mysqld_nopush | 0.97 | 3.03 | 0 | 2000 | 0.9 | 0.0% |
| fs_latest | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_latest | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| offline_fs_batch | ronsql | 0.00 | 12.00 | 0 | 3299997 | 0.0 | 0.0% |
| offline_fs_batch | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| offline_fs_multi | ronsql | 0.00 | 18.00 | 0 | 3505941 | 0.0 | 0.0% |
| offline_fs_multi | mysqld_nopush | 0.00 | 4.00 | 0 | 133634 | 0.0 | 0.0% |
| offline_fs_join_body | ronsql | 0.00 | 12.00 | 0 | 8017740 | 0.0 | 0.0% |
| offline_fs_join_body | mysqld_nopush | 0.00 | 3040.00 | 0 | 7500000 | 0.0 | 0.0% |
| offline_fs_anti | ronsql | 0.00 | 12.00 | 0 | 417268 | 0.0 | 0.0% |
| offline_fs_anti | mysqld_nopush | 0.00 | 4.00 | 0 | 133634 | 0.0 | 0.0% |
| offline_fs_wide | ronsql | 0.00 | 12.00 | 0 | 3299997 | 0.0 | 0.0% |
| offline_fs_wide | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| offline_fs_chain | ronsql | 0.00 | 10.00 | 0 | 3299994 | 0.0 | 0.0% |
| offline_fs_chain | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| offline_fs_scalar | ronsql | 0.00 | 8.00 | 0 | 3149997 | 0.0 | 0.0% |
| offline_fs_scalar | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| tpch_q2 | ronsql | 0.00 | 12.00 | 0 | 1803900 | 0.0 | 0.0% |
| tpch_q2 | mysqld_nopush | 0.00 | 10476.00 | 0 | 213080 | 0.0 | 0.0% |
| tpch_q11 | ronsql | 0.00 | 12.00 | 0 | 1600830 | 0.0 | 0.0% |
| tpch_q11 | mysqld_nopush | 0.00 | 8.00 | 0 | 1600000 | 0.0 | 0.0% |
| tpch_q13 | ronsql | 0.00 | 12.00 | 0 | 3299997 | 0.0 | 0.0% |
| tpch_q13 | mysqld_nopush | 0.00 | 600004.00 | 0 | 1650000 | 0.0 | 0.0% |
| tpch_q15 | ronsql | 0.00 | 8.00 | 0 | 463792 | 0.0 | 0.0% |
| tpch_q15 | mysqld_nopush | 0.00 | 4.00 | 0 | 226896 | 0.0 | 0.0% |
| tpch_q22 | ronsql | 0.00 | 12.00 | 0 | 3150000 | 0.0 | 0.0% |
| tpch_q22 | mysqld_nopush | 0.00 | 152.00 | 0 | 426596 | 0.0 | 0.0% |
| core_pk_lookup | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| core_pk_lookup | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| core_in_pk100 | ronsql | 0.00 | 4.00 | 0 | 100 | 0.0 | 0.0% |
| core_in_pk100 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| core_in_idx100 | ronsql | 0.00 | 4.00 | 0 | 1011 | 0.0 | 0.0% |
| core_in_idx100 | mysqld_nopush | 1.00 | 3.00 | 0 | 1001 | 13.9 | 0.3% |
| core_idx_range | ronsql | 0.00 | 4.00 | 0 | 18640 | 0.0 | 0.0% |
| core_idx_range | mysqld_nopush | 0.00 | 4.00 | 0 | 18640 | 0.0 | 0.0% |
| core_avg_range | ronsql | 0.00 | 4.00 | 0 | 1000 | 0.0 | 0.0% |
| core_avg_range | mysqld_nopush | 1.00 | 3.00 | 0 | 1000 | 0.8 | 0.1% |
| core_pass_range | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| core_pass_range | mysqld_nopush | 1.00 | 3.00 | 0 | 1000 | 0.7 | 0.1% |
| core_scan_agg | ronsql | 0.00 | 4.00 | 0 | 6000000 | 0.0 | 0.0% |
| core_scan_agg | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| core_scan_filter | ronsql | 0.00 | 4.00 | 0 | 233001 | 0.0 | 0.0% |
| core_scan_filter | mysqld_nopush | 0.00 | 4.00 | 0 | 6000000 | 0.0 | 0.0% |
| core_group_few | ronsql | 0.00 | 4.00 | 0 | 1500000 | 0.0 | 0.0% |
| core_group_few | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| core_group_many | ronsql | 0.00 | 4.00 | 0 | 1500000 | 0.0 | 0.0% |
| core_group_many | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_floor | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_floor | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_point | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_window7d | ronsql | 0.00 | 4.00 | 0 | 12 | 0.0 | 0.0% |
| fs_hw_agg_window7d | mysqld_nopush | 0.99 | 3.01 | 0 | 12 | 0.7 | 0.3% |
| fs_hw_agg_greatest | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_greatest | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_filter | ronsql | 0.00 | 4.00 | 0 | 9 | 0.0 | 0.0% |
| fs_hw_agg_filter | mysqld_nopush | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | ronsql | 0.00 | 4.00 | 0 | 287 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | mysqld_nopush | 1.00 | 3.00 | 0 | 363 | 1.5 | 0.2% |
| fs_hw_agg_batch100 | ronsql | 0.00 | 4.00 | 0 | 3670 | 0.0 | 0.0% |
| fs_hw_agg_batch100 | mysqld_nopush | 1.00 | 3.00 | 0 | 3613 | 9.4 | 0.1% |
| fs_hw_agg_batch1000 | ronsql | 0.00 | 4.00 | 0 | 35999 | 0.0 | 0.0% |
| fs_hw_agg_batch1000 | mysqld_nopush | 0.00 | 0.00 | 3296 | 0 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | ronsql | 0.00 | 4.00 | 0 | 1998 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | mysqld_nopush | 1.00 | 7.00 | 0 | 1932 | 9.6 | 0.2% |
| fs_hw_collect5 | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5_cte | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5_cte | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50_cte | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50_cte | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_point | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow1_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_point | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_batch100 | ronsql | 1.97 | 4.03 | 0 | 100100 | 19.3 | 0.0% |
| fs_hw_snow1_batch100 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_strkey_point | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_strkey_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_strkey_batch100 | ronsql | 0.00 | 4.00 | 0 | 3596 | 0.0 | 0.0% |
| fs_hw_strkey_batch100 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_composite_point | ronsql | 0.00 | 4.00 | 0 | 4 | 0.0 | 0.0% |
| fs_hw_composite_point | mysqld_nopush | 0.98 | 3.02 | 0 | 4 | 0.7 | 0.2% |
| fs_hw_sessions_window2h | ronsql | 0.00 | 4.00 | 0 | 1 | 0.0 | 0.0% |
| fs_hw_sessions_window2h | mysqld_nopush | 0.99 | 3.01 | 0 | 1 | 0.7 | 0.2% |
| cte_tpch_q2 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| cte_tpch_q11 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| cte_tpch_q13 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| cte_tpch_q15 | mysqld_nopush | 0.00 | 4.00 | 0 | 226896 | 0.0 | 0.0% |
| cte_tpch_q22 | mysqld_nopush | 0.00 | 4.00 | 0 | 150000 | 0.0 | 0.0% |
| fs_hw_collect5_twin | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_twin | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_twin | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |

## F. Throughput scaling (q/s) across thread counts

| query | engine | compiler | T=1 | T=8 | T=32 | T32/T1 |
|---|---|---|---:|---:|---:|---:|
| fs_floor | ronsql | OFF | 5294 | 42778 | - | - |
| fs_floor | ronsql | ON | 5028 | 35399 | - | - |
| fs_floor | mysqld_nopush | OFF | 7676 | 56146 | - | - |
| fs_floor | mysqld_nopush | ON | 8111 | 55733 | - | - |
| fs_point | ronsql | OFF | 4262 | 14189 | - | - |
| fs_point | ronsql | ON | 4334 | 20575 | - | - |
| fs_point | mysqld_nopush | OFF | 4333 | 20752 | - | - |
| fs_point | mysqld_nopush | ON | 4273 | 20672 | - | - |
| fs_batch | ronsql | OFF | 1526 | 4148 | - | - |
| fs_batch | ronsql | ON | 1476 | 4005 | - | - |
| fs_batch | mysqld_nopush | OFF | 136 | 2385 | - | - |
| fs_batch | mysqld_nopush | ON | 126 | 2479 | - | - |
| fs_freshness | ronsql | OFF | 372 | 635 | - | - |
| fs_freshness | ronsql | ON | 354 | 656 | - | - |
| fs_freshness | mysqld_nopush | OFF | 8 | 289 | - | - |
| fs_freshness | mysqld_nopush | ON | 9 | 287 | - | - |
| fs_supplier | ronsql | OFF | 351 | 629 | - | - |
| fs_supplier | ronsql | ON | 331 | 700 | - | - |
| fs_supplier | mysqld_nopush | OFF | 150 | 636 | - | - |
| fs_supplier | mysqld_nopush | ON | 147 | 637 | - | - |
| fs_nation | ronsql | OFF | 41 | 70 | - | - |
| fs_nation | ronsql | ON | 39 | 76 | - | - |
| fs_nation | mysqld_nopush | OFF | 23 | 86 | - | - |
| fs_nation | mysqld_nopush | ON | 21 | 87 | - | - |
| fs_topk | ronsql | OFF | 39 | 71 | - | - |
| fs_topk | ronsql | ON | 38 | 69 | - | - |
| fs_topk | mysqld_nopush | OFF | 1 | 41 | - | - |
| fs_topk | mysqld_nopush | ON | 1 | 38 | - | - |
| fs_minmax | ronsql | OFF | 277 | 509 | - | - |
| fs_minmax | ronsql | ON | 277 | 505 | - | - |
| fs_minmax | mysqld_nopush | OFF | 104 | 455 | - | - |
| fs_minmax | mysqld_nopush | ON | 105 | 455 | - | - |
| fs_dnf | ronsql | OFF | 1017 | 2221 | - | - |
| fs_dnf | ronsql | ON | 1058 | 1828 | - | - |
| fs_dnf | mysqld_nopush | OFF | 9 | 588 | - | - |
| fs_dnf | mysqld_nopush | ON | 8 | 586 | - | - |
| fs_history | ronsql | OFF | 718 | 3958 | - | - |
| fs_history | ronsql | ON | 723 | 3944 | - | - |
| fs_history | mysqld_nopush | OFF | 505 | 1635 | - | - |
| fs_history | mysqld_nopush | ON | 494 | 1622 | - | - |
| fs_latest | ronsql | OFF | 1693 | 2664 | - | - |
| fs_latest | ronsql | ON | 1713 | 2929 | - | - |
| fs_latest | mysqld_nopush | OFF | 1596 | 3089 | - | - |
| fs_latest | mysqld_nopush | ON | 1601 | 3021 | - | - |
| offline_fs_batch | ronsql | OFF | 1 | - | - | - |
| offline_fs_batch | ronsql | ON | 1 | - | - | - |
| offline_fs_batch | mysqld_nopush | OFF | 0 | - | - | - |
| offline_fs_batch | mysqld_nopush | ON | 0 | - | - | - |
| offline_fs_multi | ronsql | OFF | 1 | - | - | - |
| offline_fs_multi | ronsql | ON | 1 | - | - | - |
| offline_fs_multi | mysqld_nopush | OFF | 0 | - | - | - |
| offline_fs_multi | mysqld_nopush | ON | 0 | - | - | - |
| offline_fs_join_body | ronsql | OFF | 0 | - | - | - |
| offline_fs_join_body | ronsql | ON | 0 | - | - | - |
| offline_fs_join_body | mysqld_nopush | OFF | 0 | - | - | - |
| offline_fs_join_body | mysqld_nopush | ON | 0 | - | - | - |
| offline_fs_anti | ronsql | OFF | 5 | - | - | - |
| offline_fs_anti | ronsql | ON | 5 | - | - | - |
| offline_fs_anti | mysqld_nopush | OFF | 5 | - | - | - |
| offline_fs_anti | mysqld_nopush | ON | 4 | - | - | - |
| offline_fs_wide | ronsql | OFF | 0 | - | - | - |
| offline_fs_wide | ronsql | ON | 0 | - | - | - |
| offline_fs_wide | mysqld_nopush | OFF | 0 | - | - | - |
| offline_fs_wide | mysqld_nopush | ON | 0 | - | - | - |
| offline_fs_chain | ronsql | OFF | 1 | - | - | - |
| offline_fs_chain | ronsql | ON | 1 | - | - | - |
| offline_fs_chain | mysqld_nopush | OFF | 0 | - | - | - |
| offline_fs_chain | mysqld_nopush | ON | 0 | - | - | - |
| offline_fs_scalar | ronsql | OFF | 1 | - | - | - |
| offline_fs_scalar | ronsql | ON | 1 | - | - | - |
| offline_fs_scalar | mysqld_nopush | OFF | 0 | - | - | - |
| offline_fs_scalar | mysqld_nopush | ON | 1 | - | - | - |
| tpch_q2 | ronsql | OFF | 1 | - | - | - |
| tpch_q2 | ronsql | ON | 1 | - | - | - |
| tpch_q2 | mysqld_nopush | OFF | 4 | - | - | - |
| tpch_q2 | mysqld_nopush | ON | 4 | - | - | - |
| tpch_q11 | ronsql | OFF | 10 | - | - | - |
| tpch_q11 | ronsql | ON | 8 | - | - | - |
| tpch_q11 | mysqld_nopush | OFF | 0 | - | - | - |
| tpch_q11 | mysqld_nopush | ON | 0 | - | - | - |
| tpch_q13 | ronsql | OFF | 1 | - | - | - |
| tpch_q13 | ronsql | ON | 1 | - | - | - |
| tpch_q13 | mysqld_nopush | OFF | 1 | - | - | - |
| tpch_q13 | mysqld_nopush | ON | 1 | - | - | - |
| tpch_q15 | ronsql | OFF | 18 | - | - | - |
| tpch_q15 | ronsql | ON | 17 | - | - | - |
| tpch_q15 | mysqld_nopush | OFF | 6 | - | - | - |
| tpch_q15 | mysqld_nopush | ON | 6 | - | - | - |
| tpch_q22 | ronsql | OFF | 1 | - | - | - |
| tpch_q22 | ronsql | ON | 1 | - | - | - |
| tpch_q22 | mysqld_nopush | OFF | 5 | - | - | - |
| tpch_q22 | mysqld_nopush | ON | 4 | - | - | - |
| core_pk_lookup | ronsql | OFF | 2417 | - | - | - |
| core_pk_lookup | ronsql | ON | 2565 | - | - | - |
| core_pk_lookup | mysqld_nopush | OFF | 2276 | - | - | - |
| core_pk_lookup | mysqld_nopush | ON | 2199 | - | - | - |
| core_in_pk100 | ronsql | OFF | 3 | - | - | - |
| core_in_pk100 | ronsql | ON | 2 | - | - | - |
| core_in_pk100 | mysqld_nopush | OFF | 1965 | - | - | - |
| core_in_pk100 | mysqld_nopush | ON | 1967 | - | - | - |
| core_in_idx100 | ronsql | OFF | 2 | - | - | - |
| core_in_idx100 | ronsql | ON | 3 | - | - | - |
| core_in_idx100 | mysqld_nopush | OFF | 202 | - | - | - |
| core_in_idx100 | mysqld_nopush | ON | 199 | - | - | - |
| core_idx_range | ronsql | OFF | 400 | - | - | - |
| core_idx_range | ronsql | ON | 377 | - | - | - |
| core_idx_range | mysqld_nopush | OFF | 163 | - | - | - |
| core_idx_range | mysqld_nopush | ON | 162 | - | - | - |
| core_avg_range | ronsql | OFF | 3732 | - | - | - |
| core_avg_range | ronsql | ON | 3727 | - | - | - |
| core_avg_range | mysqld_nopush | OFF | 1623 | - | - | - |
| core_avg_range | mysqld_nopush | ON | 1630 | - | - | - |
| core_pass_range | ronsql | OFF | 1903 | - | - | - |
| core_pass_range | ronsql | ON | 1903 | - | - | - |
| core_pass_range | mysqld_nopush | OFF | 1022 | - | - | - |
| core_pass_range | mysqld_nopush | ON | 1019 | - | - | - |
| core_scan_agg | ronsql | OFF | 2 | - | - | - |
| core_scan_agg | ronsql | ON | 2 | - | - | - |
| core_scan_agg | mysqld_nopush | OFF | 0 | - | - | - |
| core_scan_agg | mysqld_nopush | ON | 1 | - | - | - |
| core_scan_filter | ronsql | OFF | 3 | - | - | - |
| core_scan_filter | ronsql | ON | 3 | - | - | - |
| core_scan_filter | mysqld_nopush | OFF | 1 | - | - | - |
| core_scan_filter | mysqld_nopush | ON | 2 | - | - | - |
| core_group_few | ronsql | OFF | 6 | - | - | - |
| core_group_few | ronsql | ON | 6 | - | - | - |
| core_group_few | mysqld_nopush | OFF | 2 | - | - | - |
| core_group_few | mysqld_nopush | ON | 2 | - | - | - |
| core_group_many | ronsql | OFF | 1 | - | - | - |
| core_group_many | ronsql | ON | 1 | - | - | - |
| core_group_many | mysqld_nopush | OFF | 0 | - | - | - |
| core_group_many | mysqld_nopush | ON | 0 | - | - | - |
| fs_hw_floor | ronsql | OFF | 6714 | - | - | - |
| fs_hw_floor | ronsql | ON | 8789 | - | - | - |
| fs_hw_floor | mysqld_nopush | OFF | 5967 | - | - | - |
| fs_hw_floor | mysqld_nopush | ON | 6911 | - | - | - |
| fs_hw_agg_point | ronsql | OFF | 11094 | - | - | - |
| fs_hw_agg_point | ronsql | ON | 11263 | - | - | - |
| fs_hw_agg_point | mysqld_nopush | OFF | 4757 | - | - | - |
| fs_hw_agg_point | mysqld_nopush | ON | 4550 | - | - | - |
| fs_hw_agg_window7d | ronsql | OFF | 11427 | - | - | - |
| fs_hw_agg_window7d | ronsql | ON | 11254 | - | - | - |
| fs_hw_agg_window7d | mysqld_nopush | OFF | 4516 | - | - | - |
| fs_hw_agg_window7d | mysqld_nopush | ON | 4583 | - | - | - |
| fs_hw_agg_greatest | ronsql | OFF | 11493 | - | - | - |
| fs_hw_agg_greatest | ronsql | ON | 11701 | - | - | - |
| fs_hw_agg_greatest | mysqld_nopush | OFF | 4738 | - | - | - |
| fs_hw_agg_greatest | mysqld_nopush | ON | 4631 | - | - | - |
| fs_hw_agg_filter | ronsql | OFF | 11509 | - | - | - |
| fs_hw_agg_filter | ronsql | ON | 11575 | - | - | - |
| fs_hw_agg_filter | mysqld_nopush | OFF | 4580 | - | - | - |
| fs_hw_agg_filter | mysqld_nopush | ON | 4398 | - | - | - |
| fs_hw_agg_batch10 | ronsql | OFF | 5 | - | - | - |
| fs_hw_agg_batch10 | ronsql | ON | 5 | - | - | - |
| fs_hw_agg_batch10 | mysqld_nopush | OFF | 1279 | - | - | - |
| fs_hw_agg_batch10 | mysqld_nopush | ON | 1271 | - | - | - |
| fs_hw_agg_batch100 | ronsql | OFF | 1 | - | - | - |
| fs_hw_agg_batch100 | ronsql | ON | 1 | - | - | - |
| fs_hw_agg_batch100 | mysqld_nopush | OFF | 130 | - | - | - |
| fs_hw_agg_batch100 | mysqld_nopush | ON | 129 | - | - | - |
| fs_hw_agg_batch1000 | ronsql | OFF | 0 | - | - | - |
| fs_hw_agg_batch1000 | ronsql | ON | 0 | - | - | - |
| fs_hw_agg_batch1000 | mysqld_nopush | OFF | 11 | - | - | - |
| fs_hw_agg_batch1000 | mysqld_nopush | ON | 11 | - | - | - |
| fs_hw_agg_batch100_window | ronsql | OFF | 1 | - | - | - |
| fs_hw_agg_batch100_window | ronsql | ON | 1 | - | - | - |
| fs_hw_agg_batch100_window | mysqld_nopush | OFF | 153 | - | - | - |
| fs_hw_agg_batch100_window | mysqld_nopush | ON | 156 | - | - | - |
| fs_hw_collect5 | ronsql | OFF | 11488 | - | - | - |
| fs_hw_collect5 | ronsql | ON | 10864 | - | - | - |
| fs_hw_collect5 | mysqld_nopush | OFF | 4739 | - | - | - |
| fs_hw_collect5 | mysqld_nopush | ON | 4876 | - | - | - |
| fs_hw_collect5_cte | ronsql | OFF | 11588 | - | - | - |
| fs_hw_collect5_cte | ronsql | ON | 11036 | - | - | - |
| fs_hw_collect5_cte | mysqld_nopush | OFF | 4293 | - | - | - |
| fs_hw_collect5_cte | mysqld_nopush | ON | 4446 | - | - | - |
| fs_hw_collect50 | ronsql | OFF | 11391 | - | - | - |
| fs_hw_collect50 | ronsql | ON | 11422 | - | - | - |
| fs_hw_collect50 | mysqld_nopush | OFF | 4686 | - | - | - |
| fs_hw_collect50 | mysqld_nopush | ON | 4622 | - | - | - |
| fs_hw_collect50_cte | ronsql | OFF | 10625 | - | - | - |
| fs_hw_collect50_cte | ronsql | ON | 10852 | - | - | - |
| fs_hw_collect50_cte | mysqld_nopush | OFF | 4222 | - | - | - |
| fs_hw_collect50_cte | mysqld_nopush | ON | 4229 | - | - | - |
| fs_hw_snow1_point | ronsql | OFF | 4227 | - | - | - |
| fs_hw_snow1_point | ronsql | ON | 4745 | - | - | - |
| fs_hw_snow1_point | mysqld_nopush | OFF | 903 | - | - | - |
| fs_hw_snow1_point | mysqld_nopush | ON | 862 | - | - | - |
| fs_hw_snow2_point | ronsql | OFF | 4901 | - | - | - |
| fs_hw_snow2_point | ronsql | ON | 4601 | - | - | - |
| fs_hw_snow2_point | mysqld_nopush | OFF | 1002 | - | - | - |
| fs_hw_snow2_point | mysqld_nopush | ON | 1029 | - | - | - |
| fs_hw_snow1_batch100 | ronsql | OFF | 26 | - | - | - |
| fs_hw_snow1_batch100 | ronsql | ON | 22 | - | - | - |
| fs_hw_snow1_batch100 | mysqld_nopush | OFF | 22 | - | - | - |
| fs_hw_snow1_batch100 | mysqld_nopush | ON | 22 | - | - | - |
| fs_hw_snow2_left_chain | ronsql | OFF | 4691 | - | - | - |
| fs_hw_snow2_left_chain | ronsql | ON | 4689 | - | - | - |
| fs_hw_snow2_left_chain | mysqld_nopush | OFF | 1032 | - | - | - |
| fs_hw_snow2_left_chain | mysqld_nopush | ON | 1061 | - | - | - |
| fs_hw_snow2_left_single | ronsql | OFF | 4934 | - | - | - |
| fs_hw_snow2_left_single | ronsql | ON | 4727 | - | - | - |
| fs_hw_snow2_left_single | mysqld_nopush | OFF | 1029 | - | - | - |
| fs_hw_snow2_left_single | mysqld_nopush | ON | 1073 | - | - | - |
| fs_hw_strkey_point | ronsql | OFF | 11184 | - | - | - |
| fs_hw_strkey_point | ronsql | ON | 10918 | - | - | - |
| fs_hw_strkey_point | mysqld_nopush | OFF | 4525 | - | - | - |
| fs_hw_strkey_point | mysqld_nopush | ON | 4776 | - | - | - |
| fs_hw_strkey_batch100 | ronsql | OFF | 1 | - | - | - |
| fs_hw_strkey_batch100 | ronsql | ON | 1 | - | - | - |
| fs_hw_strkey_batch100 | mysqld_nopush | OFF | 88 | - | - | - |
| fs_hw_strkey_batch100 | mysqld_nopush | ON | 89 | - | - | - |
| fs_hw_composite_point | ronsql | OFF | 9594 | - | - | - |
| fs_hw_composite_point | ronsql | ON | 10841 | - | - | - |
| fs_hw_composite_point | mysqld_nopush | OFF | 3100 | - | - | - |
| fs_hw_composite_point | mysqld_nopush | ON | 3108 | - | - | - |
| fs_hw_sessions_window2h | ronsql | OFF | 10585 | - | - | - |
| fs_hw_sessions_window2h | ronsql | ON | 9602 | - | - | - |
| fs_hw_sessions_window2h | mysqld_nopush | OFF | 3560 | - | - | - |
| fs_hw_sessions_window2h | mysqld_nopush | ON | 3338 | - | - | - |
| cte_tpch_q2 | mysqld_nopush | OFF | 0 | - | - | - |
| cte_tpch_q2 | mysqld_nopush | ON | 0 | - | - | - |
| cte_tpch_q11 | mysqld_nopush | OFF | 3 | - | - | - |
| cte_tpch_q11 | mysqld_nopush | ON | 3 | - | - | - |
| cte_tpch_q13 | mysqld_nopush | OFF | 0 | - | - | - |
| cte_tpch_q13 | mysqld_nopush | ON | 0 | - | - | - |
| cte_tpch_q15 | mysqld_nopush | OFF | 7 | - | - | - |
| cte_tpch_q15 | mysqld_nopush | ON | 7 | - | - | - |
| cte_tpch_q22 | mysqld_nopush | OFF | 0 | - | - | - |
| cte_tpch_q22 | mysqld_nopush | ON | 0 | - | - | - |
| fs_hw_collect5_twin | mysqld_nopush | OFF | 3306 | - | - | - |
| fs_hw_collect5_twin | mysqld_nopush | ON | 3328 | - | - | - |
| fs_hw_snow1_twin | mysqld_nopush | OFF | 2047 | - | - | - |
| fs_hw_snow1_twin | mysqld_nopush | ON | 1942 | - | - | - |
| fs_hw_snow2_twin | mysqld_nopush | OFF | 2609 | - | - | - |
| fs_hw_snow2_twin | mysqld_nopush | ON | 2565 | - | - | - |

## G. Hopsworks serving path: RonSQL template vs MySQL production twin (avg latency; ratio = twin / ronsql, >1 = RonSQL path faster)

template = the Hopsworks RonSQL statement on RonSQL; same text = that statement on the MySQL server; twin = the production MySQL statement Hopsworks runs on the SQL path (ROW_NUMBER window / nested join).

| entry | threads | compiler | ronsql template | mysqld_nopush same text | mysqld_nopush twin | twin/ronsql |
|---|---:|---|---:|---:|---:|---:|
| fs_hw_collect5 | 1 | OFF | 77us | 210us | 302us | 3.92x |
| fs_hw_collect5 | 1 | ON | 82us | 205us | 300us | 3.66x |
| fs_hw_collect5 | 8 | OFF | - | - | - | - |
| fs_hw_collect5 | 8 | ON | - | - | - | - |
| fs_hw_collect5 | 32 | OFF | - | - | - | - |
| fs_hw_collect5 | 32 | ON | - | - | - | - |
| fs_hw_snow1_point | 1 | OFF | 227us | 1.11ms | 488us | 2.15x |
| fs_hw_snow1_point | 1 | ON | 201us | 1.16ms | 514us | 2.56x |
| fs_hw_snow1_point | 8 | OFF | - | - | - | - |
| fs_hw_snow1_point | 8 | ON | - | - | - | - |
| fs_hw_snow1_point | 32 | OFF | - | - | - | - |
| fs_hw_snow1_point | 32 | ON | - | - | - | - |
| fs_hw_snow2_point | 1 | OFF | 196us | 997us | 382us | 1.95x |
| fs_hw_snow2_point | 1 | ON | 208us | 970us | 389us | 1.87x |
| fs_hw_snow2_point | 8 | OFF | - | - | - | - |
| fs_hw_snow2_point | 8 | ON | - | - | - | - |
| fs_hw_snow2_point | 32 | OFF | - | - | - | - |
| fs_hw_snow2_point | 32 | ON | - | - | - | - |
