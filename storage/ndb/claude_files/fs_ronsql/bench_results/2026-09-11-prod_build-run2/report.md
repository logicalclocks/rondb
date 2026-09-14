# RonSQL / MySQL / compiled-interpreter benchmark matrix

build=/Users/mikael/mysql_trees/rondb_1121_fs_ronsql/prod_build sf=1 threads=[1, 8] engines=ronsql,mysqld_nopush compiler=OFF,ON order=query-major repeat=1 cpubind=- client_cpus=- host=mikaels-MacBook-Pro.local (Darwin arm64) started 2026-09-11T11:52:05

FAILED cases: off_ronsql_fs_hw_collect5_cte_T1 (Error handling: RPE), on_ronsql_fs_hw_collect5_cte_T1 (Error handling: RPE), off_ronsql_fs_hw_hash_point_T1 (Caught exception: Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.), off_mysqld_nopush_fs_hw_hash_point_T1 (warmup query failed: query failed: Error 1146 (42S02): Table 'fs_bench.transactions_hash_1' doesn't exist), on_ronsql_fs_hw_hash_point_T1 (Caught exception: Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.), on_mysqld_nopush_fs_hw_hash_point_T1 (warmup query failed: query failed: Error 1146 (42S02): Table 'fs_bench.transactions_hash_1' doesn't exist), off_ronsql_fs_hw_collect5_cte_T8 (Error handling: RPE), on_ronsql_fs_hw_collect5_cte_T8 (Error handling: RPE), off_ronsql_fs_hw_hash_point_T8 (Caught exception: Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.), off_mysqld_nopush_fs_hw_hash_point_T8 (warmup query failed: query failed: Error 1146 (42S02): Table 'fs_bench.transactions_hash_1' doesn't exist), on_ronsql_fs_hw_hash_point_T8 (Caught exception: Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.), on_mysqld_nopush_fs_hw_hash_point_T8 (warmup query failed: query failed: Error 1146 (42S02): Table 'fs_bench.transactions_hash_1' doesn't exist)

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 1 thread

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | 102us | 98us | 1.04x | 9370 | 9722 | 92us | 105us | 0.88x | 10819 | 9496 |
| fs_hw_agg_point | 112us | 106us | 1.06x | 8540 | 9022 | 113us | 110us | 1.03x | 8770 | 9021 |
| fs_hw_agg_window7d | 124us | 117us | 1.06x | 7715 | 8172 | 113us | 113us | 1.00x | 8779 | 8776 |
| fs_hw_agg_greatest | 106us | 111us | 0.95x | 9005 | 8568 | 111us | 110us | 1.01x | 8967 | 9069 |
| fs_hw_agg_filter | 106us | 109us | 0.97x | 9010 | 8760 | 112us | 115us | 0.98x | 8865 | 8675 |
| fs_hw_agg_batch10 | 208ms | 219ms | 0.95x | 5 | 5 | 237us | 236us | 1.00x | 4198 | 4210 |
| fs_hw_agg_batch100 | 612ms | 591ms | 1.04x | 2 | 2 | 1.86ms | 1.92ms | 0.97x | 536 | 520 |
| fs_hw_agg_batch1000 | 4381ms | 4206ms | 1.04x | 0 | 0 | 42.77ms | 43.75ms | 0.98x | 23 | 23 |
| fs_hw_agg_batch100_window | 553ms | 550ms | 1.01x | 2 | 2 | 1.33ms | 1.35ms | 0.99x | 750 | 736 |
| fs_hw_collect5 | 108us | 110us | 0.98x | 8833 | 8655 | 116us | 115us | 1.00x | 8603 | 8639 |
| fs_hw_collect5_cte | - | - | - | - | - | 119us | 118us | 1.01x | 8335 | 8432 |
| fs_hw_collect50 | 112us | 112us | 1.00x | 8468 | 8473 | 129us | 119us | 1.08x | 7743 | 8385 |
| fs_hw_snow1_point | 519us | 493us | 1.05x | 1900 | 2007 | 125us | 126us | 1.00x | 7919 | 7902 |
| fs_hw_snow2_point | 526us | 525us | 1.00x | 1880 | 1885 | 174us | 173us | 1.01x | 5708 | 5765 |
| fs_hw_snow1_batch100 | 19.97ms | 21.89ms | 0.91x | 50 | 46 | 4.71ms | 4.86ms | 0.97x | 212 | 205 |
| fs_hw_snow2_left_chain | 530us | 530us | 1.00x | 1864 | 1866 | 171us | 168us | 1.02x | 5823 | 5914 |
| fs_hw_snow2_left_single | 531us | 530us | 1.00x | 1861 | 1865 | 175us | 173us | 1.01x | 5687 | 5743 |
| fs_hw_strkey_point | 111us | 113us | 0.98x | 8597 | 8422 | 120us | 120us | 1.00x | 8289 | 8303 |
| fs_hw_strkey_batch100 | 369ms | 373ms | 0.99x | 3 | 3 | 3.92ms | 3.90ms | 1.01x | 254 | 255 |
| fs_hw_composite_point | 108us | 111us | 0.97x | 8822 | 8590 | 113us | 117us | 0.97x | 8721 | 8484 |
| fs_hw_hash_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_sessions_window2h | 107us | 109us | 0.98x | 8885 | 8695 | 106us | 110us | 0.97x | 9295 | 9007 |
| fs_hw_collect5_twin | - | - | - | - | - | 142us | 143us | 0.99x | 7013 | 6945 |
| fs_hw_snow1_twin | - | - | - | - | - | 126us | 127us | 0.99x | 7896 | 7849 |
| fs_hw_snow2_twin | - | - | - | - | - | 144us | 149us | 0.97x | 6893 | 6672 |

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 8 threads

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | 271us | 277us | 0.98x | 28258 | 27646 | 260us | 259us | 1.01x | 30471 | 30641 |
| fs_hw_agg_point | 299us | 304us | 0.98x | 25627 | 25173 | 288us | 287us | 1.00x | 27461 | 27590 |
| fs_hw_agg_window7d | 298us | 301us | 0.99x | 25733 | 25463 | 286us | 289us | 0.99x | 27623 | 27264 |
| fs_hw_agg_greatest | 298us | 298us | 1.00x | 25681 | 25790 | 289us | 286us | 1.01x | 27450 | 27658 |
| fs_hw_agg_filter | 300us | 299us | 1.00x | 25575 | 25537 | 293us | 295us | 0.99x | 27005 | 26814 |
| fs_hw_agg_batch10 | 825ms | 986ms | 0.84x | 10 | 8 | 557us | 570us | 0.98x | 14226 | 13920 |
| fs_hw_agg_batch100 | 3033ms | 2856ms | 1.06x | 3 | 3 | 4.90ms | 4.98ms | 0.98x | 1619 | 1594 |
| fs_hw_agg_batch1000 | 26584ms | 23108ms | 1.15x | 0 | 0 | 153ms | 144ms | 1.06x | 51 | 55 |
| fs_hw_agg_batch100_window | 3199ms | 3227ms | 0.99x | 2 | 2 | 3.68ms | 3.82ms | 0.96x | 2159 | 2073 |
| fs_hw_collect5 | 288us | 291us | 0.99x | 26653 | 26324 | 293us | 288us | 1.02x | 26982 | 27464 |
| fs_hw_collect5_cte | - | - | - | - | - | 299us | 305us | 0.98x | 26503 | 25945 |
| fs_hw_collect50 | 293us | 291us | 1.01x | 26066 | 26327 | 299us | 297us | 1.01x | 26479 | 26607 |
| fs_hw_snow1_point | 872us | 869us | 1.00x | 9075 | 9100 | 316us | 317us | 1.00x | 25181 | 25052 |
| fs_hw_snow2_point | 910us | 919us | 0.99x | 8688 | 8606 | 409us | 415us | 0.98x | 19428 | 19152 |
| fs_hw_snow1_batch100 | 90.27ms | 110ms | 0.82x | 88 | 72 | 12.67ms | 11.75ms | 1.08x | 629 | 678 |
| fs_hw_snow2_left_chain | 937us | 925us | 1.01x | 8441 | 8544 | 415us | 409us | 1.02x | 19086 | 19429 |
| fs_hw_snow2_left_single | 904us | 906us | 1.00x | 8740 | 8725 | 406us | 407us | 1.00x | 19550 | 19527 |
| fs_hw_strkey_point | 296us | 299us | 0.99x | 25887 | 25718 | 292us | 295us | 0.99x | 27159 | 26709 |
| fs_hw_strkey_batch100 | 2166ms | 2423ms | 0.89x | 4 | 3 | 6.09ms | 6.02ms | 1.01x | 1296 | 1316 |
| fs_hw_composite_point | 287us | 291us | 0.99x | 26609 | 26294 | 278us | 282us | 0.99x | 28467 | 27999 |
| fs_hw_hash_point | - | - | - | - | - | - | - | - | - | - |
| fs_hw_sessions_window2h | 288us | 289us | 1.00x | 26509 | 26526 | 272us | 277us | 0.98x | 29024 | 28485 |
| fs_hw_collect5_twin | - | - | - | - | - | 321us | 325us | 0.99x | 24676 | 24388 |
| fs_hw_snow1_twin | - | - | - | - | - | 296us | 297us | 1.00x | 26844 | 26721 |
| fs_hw_snow2_twin | - | - | - | - | - | 347us | 345us | 1.01x | 22882 | 23034 |

## B. RonSQL vs MySQL server (avg latency; ratio = mysqld / ronsql, >1 = RonSQL faster)

| query | threads | compiler | ronsql | mysqld_nopush | mysqld_nopush/ronsql |
|---|---:|---|---:|---:|---:|
| fs_hw_floor | 1 | OFF | 102us | 92us | 0.90x |
| fs_hw_floor | 1 | ON | 98us | 105us | 1.07x |
| fs_hw_floor | 8 | OFF | 271us | 260us | 0.96x |
| fs_hw_floor | 8 | ON | 277us | 259us | 0.93x |
| fs_hw_agg_point | 1 | OFF | 112us | 113us | 1.01x |
| fs_hw_agg_point | 1 | ON | 106us | 110us | 1.04x |
| fs_hw_agg_point | 8 | OFF | 299us | 288us | 0.97x |
| fs_hw_agg_point | 8 | ON | 304us | 287us | 0.94x |
| fs_hw_agg_window7d | 1 | OFF | 124us | 113us | 0.91x |
| fs_hw_agg_window7d | 1 | ON | 117us | 113us | 0.97x |
| fs_hw_agg_window7d | 8 | OFF | 298us | 286us | 0.96x |
| fs_hw_agg_window7d | 8 | ON | 301us | 289us | 0.96x |
| fs_hw_agg_greatest | 1 | OFF | 106us | 111us | 1.05x |
| fs_hw_agg_greatest | 1 | ON | 111us | 110us | 0.98x |
| fs_hw_agg_greatest | 8 | OFF | 298us | 289us | 0.97x |
| fs_hw_agg_greatest | 8 | ON | 298us | 286us | 0.96x |
| fs_hw_agg_filter | 1 | OFF | 106us | 112us | 1.06x |
| fs_hw_agg_filter | 1 | ON | 109us | 115us | 1.05x |
| fs_hw_agg_filter | 8 | OFF | 300us | 293us | 0.98x |
| fs_hw_agg_filter | 8 | ON | 299us | 295us | 0.99x |
| fs_hw_agg_batch10 | 1 | OFF | 208ms | 237us | 0.00x |
| fs_hw_agg_batch10 | 1 | ON | 219ms | 236us | 0.00x |
| fs_hw_agg_batch10 | 8 | OFF | 825ms | 557us | 0.00x |
| fs_hw_agg_batch10 | 8 | ON | 986ms | 570us | 0.00x |
| fs_hw_agg_batch100 | 1 | OFF | 612ms | 1.86ms | 0.00x |
| fs_hw_agg_batch100 | 1 | ON | 591ms | 1.92ms | 0.00x |
| fs_hw_agg_batch100 | 8 | OFF | 3033ms | 4.90ms | 0.00x |
| fs_hw_agg_batch100 | 8 | ON | 2856ms | 4.98ms | 0.00x |
| fs_hw_agg_batch1000 | 1 | OFF | 4381ms | 42.77ms | 0.01x |
| fs_hw_agg_batch1000 | 1 | ON | 4206ms | 43.75ms | 0.01x |
| fs_hw_agg_batch1000 | 8 | OFF | 26584ms | 153ms | 0.01x |
| fs_hw_agg_batch1000 | 8 | ON | 23108ms | 144ms | 0.01x |
| fs_hw_agg_batch100_window | 1 | OFF | 553ms | 1.33ms | 0.00x |
| fs_hw_agg_batch100_window | 1 | ON | 550ms | 1.35ms | 0.00x |
| fs_hw_agg_batch100_window | 8 | OFF | 3199ms | 3.68ms | 0.00x |
| fs_hw_agg_batch100_window | 8 | ON | 3227ms | 3.82ms | 0.00x |
| fs_hw_collect5 | 1 | OFF | 108us | 116us | 1.07x |
| fs_hw_collect5 | 1 | ON | 110us | 115us | 1.04x |
| fs_hw_collect5 | 8 | OFF | 288us | 293us | 1.02x |
| fs_hw_collect5 | 8 | ON | 291us | 288us | 0.99x |
| fs_hw_collect5_cte | 1 | OFF | - | 119us | - |
| fs_hw_collect5_cte | 1 | ON | - | 118us | - |
| fs_hw_collect5_cte | 8 | OFF | - | 299us | - |
| fs_hw_collect5_cte | 8 | ON | - | 305us | - |
| fs_hw_collect50 | 1 | OFF | 112us | 129us | 1.14x |
| fs_hw_collect50 | 1 | ON | 112us | 119us | 1.06x |
| fs_hw_collect50 | 8 | OFF | 293us | 299us | 1.02x |
| fs_hw_collect50 | 8 | ON | 291us | 297us | 1.02x |
| fs_hw_snow1_point | 1 | OFF | 519us | 125us | 0.24x |
| fs_hw_snow1_point | 1 | ON | 493us | 126us | 0.26x |
| fs_hw_snow1_point | 8 | OFF | 872us | 316us | 0.36x |
| fs_hw_snow1_point | 8 | ON | 869us | 317us | 0.36x |
| fs_hw_snow2_point | 1 | OFF | 526us | 174us | 0.33x |
| fs_hw_snow2_point | 1 | ON | 525us | 173us | 0.33x |
| fs_hw_snow2_point | 8 | OFF | 910us | 409us | 0.45x |
| fs_hw_snow2_point | 8 | ON | 919us | 415us | 0.45x |
| fs_hw_snow1_batch100 | 1 | OFF | 19.97ms | 4.71ms | 0.24x |
| fs_hw_snow1_batch100 | 1 | ON | 21.89ms | 4.86ms | 0.22x |
| fs_hw_snow1_batch100 | 8 | OFF | 90.27ms | 12.67ms | 0.14x |
| fs_hw_snow1_batch100 | 8 | ON | 110ms | 11.75ms | 0.11x |
| fs_hw_snow2_left_chain | 1 | OFF | 530us | 171us | 0.32x |
| fs_hw_snow2_left_chain | 1 | ON | 530us | 168us | 0.32x |
| fs_hw_snow2_left_chain | 8 | OFF | 937us | 415us | 0.44x |
| fs_hw_snow2_left_chain | 8 | ON | 925us | 409us | 0.44x |
| fs_hw_snow2_left_single | 1 | OFF | 531us | 175us | 0.33x |
| fs_hw_snow2_left_single | 1 | ON | 530us | 173us | 0.33x |
| fs_hw_snow2_left_single | 8 | OFF | 904us | 406us | 0.45x |
| fs_hw_snow2_left_single | 8 | ON | 906us | 407us | 0.45x |
| fs_hw_strkey_point | 1 | OFF | 111us | 120us | 1.08x |
| fs_hw_strkey_point | 1 | ON | 113us | 120us | 1.06x |
| fs_hw_strkey_point | 8 | OFF | 296us | 292us | 0.98x |
| fs_hw_strkey_point | 8 | ON | 299us | 295us | 0.99x |
| fs_hw_strkey_batch100 | 1 | OFF | 369ms | 3.92ms | 0.01x |
| fs_hw_strkey_batch100 | 1 | ON | 373ms | 3.90ms | 0.01x |
| fs_hw_strkey_batch100 | 8 | OFF | 2166ms | 6.09ms | 0.00x |
| fs_hw_strkey_batch100 | 8 | ON | 2423ms | 6.02ms | 0.00x |
| fs_hw_composite_point | 1 | OFF | 108us | 113us | 1.05x |
| fs_hw_composite_point | 1 | ON | 111us | 117us | 1.05x |
| fs_hw_composite_point | 8 | OFF | 287us | 278us | 0.97x |
| fs_hw_composite_point | 8 | ON | 291us | 282us | 0.97x |
| fs_hw_sessions_window2h | 1 | OFF | 107us | 106us | 0.99x |
| fs_hw_sessions_window2h | 1 | ON | 109us | 110us | 1.00x |
| fs_hw_sessions_window2h | 8 | OFF | 288us | 272us | 0.95x |
| fs_hw_sessions_window2h | 8 | ON | 289us | 277us | 0.96x |
| fs_hw_collect5_twin | 1 | OFF | - | 142us | - |
| fs_hw_collect5_twin | 1 | ON | - | 143us | - |
| fs_hw_collect5_twin | 8 | OFF | - | 321us | - |
| fs_hw_collect5_twin | 8 | ON | - | 325us | - |
| fs_hw_snow1_twin | 1 | OFF | - | 126us | - |
| fs_hw_snow1_twin | 1 | ON | - | 127us | - |
| fs_hw_snow1_twin | 8 | OFF | - | 296us | - |
| fs_hw_snow1_twin | 8 | ON | - | 297us | - |
| fs_hw_snow2_twin | 1 | OFF | - | 144us | - |
| fs_hw_snow2_twin | 1 | ON | - | 149us | - |
| fs_hw_snow2_twin | 8 | OFF | - | 347us | - |
| fs_hw_snow2_twin | 8 | ON | - | 345us | - |

## C. RonSQL — where the time goes (server-side phases, avg per request, 1 thread)

client = end-to-end latency seen by rondb-cli; http+client = client - prepare - execute (RDRS HTTP handling, JSON, network); firstbatch = data-node execution until the first result row (single-table: the whole DoAggregation); load = NDB dictionary lookups; rows = result rows drained per request.

| query | compiler | client | prepare | execute | http+client | parse | analyze | load | plan | compile | ndbprep | send | firstbatch | drain | print | rows | top |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fs_hw_floor | OFF | 102us | 2us | 69us | 31us | 1us | 0us | 1us | 0us | 0us | 0us | - | 69us | - | 0us | 0.0 | firstbatch 69us |
| fs_hw_floor | ON | 98us | 2us | 67us | 29us | 1us | 0us | 1us | 0us | 0us | 0us | - | 66us | - | 0us | 0.0 | firstbatch 66us |
| fs_hw_agg_point | OFF | 112us | 4us | 74us | 34us | 2us | 0us | 1us | 0us | 0us | 0us | - | 73us | - | 0us | 0.0 | firstbatch 73us |
| fs_hw_agg_point | ON | 106us | 4us | 73us | 29us | 2us | 0us | 1us | 0us | 0us | 0us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_window7d | OFF | 124us | 4us | 85us | 34us | 2us | 0us | 1us | 0us | 0us | 3us | - | 82us | - | 0us | 0.0 | firstbatch 82us |
| fs_hw_agg_window7d | ON | 117us | 4us | 78us | 35us | 2us | 0us | 1us | 0us | 0us | 3us | - | 74us | - | 0us | 0.0 | firstbatch 74us |
| fs_hw_agg_greatest | OFF | 106us | 4us | 73us | 29us | 2us | 0us | 1us | 0us | 0us | 0us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_greatest | ON | 111us | 4us | 75us | 32us | 2us | 0us | 1us | 0us | 0us | 0us | - | 74us | - | 0us | 0.0 | firstbatch 74us |
| fs_hw_agg_filter | OFF | 106us | 4us | 72us | 29us | 2us | 0us | 1us | 0us | 0us | 1us | - | 71us | - | 0us | 0.0 | firstbatch 71us |
| fs_hw_agg_filter | ON | 109us | 4us | 74us | 31us | 2us | 0us | 1us | 0us | 0us | 1us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_batch10 | OFF | 208ms | 15us | 208ms | 135us | 6us | 0us | 4us | 1us | 2us | 4us | - | 208ms | - | 5us | 0.0 | firstbatch 208ms |
| fs_hw_agg_batch10 | ON | 219ms | 18us | 219ms | 132us | 8us | 0us | 5us | 1us | 2us | 5us | - | 219ms | - | 5us | 0.0 | firstbatch 219ms |
| fs_hw_agg_batch100 | OFF | 612ms | 46us | 612ms | 164us | 12us | 1us | 27us | 2us | 2us | 9us | - | 612ms | - | 22us | 0.0 | firstbatch 612ms |
| fs_hw_agg_batch100 | ON | 591ms | 25us | 591ms | 185us | 12us | 1us | 5us | 2us | 2us | 9us | - | 590ms | - | 17us | 0.0 | firstbatch 590ms |
| fs_hw_agg_batch1000 | OFF | 4381ms | 150us | 4380ms | 180us | 52us | 9us | 69us | 15us | 3us | 38us | - | 4380ms | - | 133us | 0.0 | firstbatch 4380ms |
| fs_hw_agg_batch1000 | ON | 4206ms | 132us | 4206ms | 198us | 58us | 10us | 38us | 23us | 2us | 35us | - | 4206ms | - | 140us | 0.0 | firstbatch 4206ms |
| fs_hw_agg_batch100_window | OFF | 553ms | 25us | 553ms | 165us | 13us | 1us | 6us | 2us | 2us | 13us | - | 553ms | - | 16us | 0.0 | firstbatch 553ms |
| fs_hw_agg_batch100_window | ON | 550ms | 26us | 550ms | 174us | 13us | 1us | 6us | 2us | 3us | 14us | - | 550ms | - | 18us | 0.0 | firstbatch 550ms |
| fs_hw_collect5 | OFF | 108us | 3us | 74us | 30us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 68us | 1us | - | 4.2 | firstbatch 68us |
| fs_hw_collect5 | ON | 110us | 3us | 76us | 31us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 69us | 1us | - | 4.2 | firstbatch 69us |
| fs_hw_collect50 | OFF | 112us | 3us | 77us | 32us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 68us | 5us | - | 17.6 | firstbatch 68us |
| fs_hw_collect50 | ON | 112us | 3us | 77us | 32us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 68us | 5us | - | 17.6 | firstbatch 68us |
| fs_hw_snow1_point | OFF | 519us | 6us | 465us | 48us | 3us | 0us | 2us | 0us | 0us | 1us | 9us | 443us | 5us | - | 0.9 | firstbatch 443us |
| fs_hw_snow1_point | ON | 493us | 5us | 443us | 44us | 3us | 0us | 2us | 0us | 0us | 1us | 8us | 423us | 4us | - | 0.9 | firstbatch 423us |
| fs_hw_snow2_point | OFF | 526us | 6us | 476us | 44us | 3us | 0us | 2us | 0us | 0us | 1us | 10us | 455us | 2us | - | 0.8 | firstbatch 455us |
| fs_hw_snow2_point | ON | 525us | 6us | 475us | 43us | 3us | 0us | 2us | 0us | 0us | 1us | 10us | 454us | 2us | - | 0.8 | firstbatch 454us |
| fs_hw_snow1_batch100 | OFF | 19.97ms | 18us | 19.85ms | 102us | 9us | 0us | 8us | 0us | 1us | 2us | 11us | 19.66ms | 161us | - | 89.0 | firstbatch 19.66ms |
| fs_hw_snow1_batch100 | ON | 21.89ms | 23us | 21.76ms | 107us | 11us | 0us | 10us | 0us | 1us | 2us | 13us | 21.55ms | 174us | - | 88.9 | firstbatch 21.55ms |
| fs_hw_snow2_left_chain | OFF | 530us | 6us | 479us | 45us | 3us | 0us | 2us | 0us | 0us | 1us | 10us | 459us | 2us | - | 0.8 | firstbatch 459us |
| fs_hw_snow2_left_chain | ON | 530us | 6us | 479us | 44us | 3us | 0us | 2us | 0us | 0us | 1us | 10us | 459us | 2us | - | 0.8 | firstbatch 459us |
| fs_hw_snow2_left_single | OFF | 531us | 7us | 479us | 45us | 4us | 0us | 3us | 0us | 0us | 1us | 10us | 457us | 4us | - | 1.0 | firstbatch 457us |
| fs_hw_snow2_left_single | ON | 530us | 7us | 479us | 45us | 4us | 0us | 2us | 0us | 0us | 1us | 10us | 457us | 3us | - | 1.0 | firstbatch 457us |
| fs_hw_strkey_point | OFF | 111us | 4us | 77us | 30us | 2us | 0us | 1us | 0us | 0us | 0us | - | 75us | - | 0us | 0.0 | firstbatch 75us |
| fs_hw_strkey_point | ON | 113us | 4us | 79us | 30us | 2us | 0us | 1us | 0us | 0us | 0us | - | 78us | - | 0us | 0.0 | firstbatch 78us |
| fs_hw_strkey_batch100 | OFF | 369ms | 29us | 369ms | 161us | 16us | 1us | 6us | 2us | 2us | 10us | - | 369ms | - | 29us | 0.0 | firstbatch 369ms |
| fs_hw_strkey_batch100 | ON | 373ms | 27us | 373ms | 153us | 15us | 2us | 5us | 2us | 3us | 10us | - | 373ms | - | 34us | 0.0 | firstbatch 373ms |
| fs_hw_composite_point | OFF | 108us | 3us | 74us | 31us | 2us | 0us | 1us | 0us | 0us | 3us | - | 70us | - | 0us | 0.0 | firstbatch 70us |
| fs_hw_composite_point | ON | 111us | 3us | 74us | 33us | 2us | 0us | 1us | 0us | 0us | 3us | - | 71us | - | 0us | 0.0 | firstbatch 71us |
| fs_hw_sessions_window2h | OFF | 107us | 3us | 73us | 30us | 1us | 0us | 1us | 0us | 0us | 3us | - | 70us | - | 0us | 0.0 | firstbatch 70us |
| fs_hw_sessions_window2h | ON | 109us | 4us | 74us | 32us | 1us | 0us | 1us | 0us | 0us | 3us | - | 70us | - | 0us | 0.0 | firstbatch 70us |

## D. MySQL server — where the time goes (1 thread)

ndb wait = Ndb_api_wait_nanos_count per request (time the mysqld connection waited for data nodes), corrected by the idle baseline (1.00 s/s of background NDB API waiting measured before the matrix); mysqld = client latency - ndb wait (parsing, optimizer, row processing, result transfer); batches / rows = scan batches and rows received from NDB per request; pushed = pushed (SPJ) queries executed per request. Counters include the warmup request and are approximate at short case durations.

| query | engine | compiler | client | ndb wait | mysqld | batches/req | rows/req | pushed/req | KB recv/req |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | mysqld_nopush | OFF | 92us | 65us | 27us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_floor | mysqld_nopush | ON | 105us | 71us | 34us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_agg_point | mysqld_nopush | OFF | 113us | 69us | 44us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_point | mysqld_nopush | ON | 110us | 70us | 40us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_window7d | mysqld_nopush | OFF | 113us | 67us | 46us | 2.4 | 13 | 0.00 | 0.7 |
| fs_hw_agg_window7d | mysqld_nopush | ON | 113us | 67us | 46us | 2.4 | 13 | 0.00 | 0.7 |
| fs_hw_agg_greatest | mysqld_nopush | OFF | 111us | 69us | 42us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_greatest | mysqld_nopush | ON | 110us | 67us | 43us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_filter | mysqld_nopush | OFF | 112us | 68us | 44us | 2.9 | 29 | 0.00 | 1.5 |
| fs_hw_agg_filter | mysqld_nopush | ON | 115us | 69us | 46us | 2.9 | 29 | 0.00 | 1.5 |
| fs_hw_agg_batch10 | mysqld_nopush | OFF | 237us | 133us | 104us | 4.0 | 364 | 0.00 | 17.2 |
| fs_hw_agg_batch10 | mysqld_nopush | ON | 236us | 133us | 103us | 4.0 | 364 | 0.00 | 17.2 |
| fs_hw_agg_batch100 | mysqld_nopush | OFF | 1.86ms | 1.16ms | 700us | 5.2 | 3610 | 0.00 | 169.4 |
| fs_hw_agg_batch100 | mysqld_nopush | ON | 1.92ms | 1.21ms | 706us | 5.2 | 3612 | 0.00 | 169.4 |
| fs_hw_agg_batch1000 | mysqld_nopush | OFF | 42.77ms | 38.52ms | 4.25ms | 56.2 | 36246 | 0.00 | 1699.9 |
| fs_hw_agg_batch1000 | mysqld_nopush | ON | 43.75ms | 39.45ms | 4.30ms | 56.2 | 36287 | 0.00 | 1701.8 |
| fs_hw_agg_batch100_window | mysqld_nopush | OFF | 1.33ms | 773us | 557us | 8.0 | 1934 | 0.00 | 90.8 |
| fs_hw_agg_batch100_window | mysqld_nopush | ON | 1.35ms | 799us | 551us | 8.0 | 1933 | 0.00 | 90.8 |
| fs_hw_collect5 | mysqld_nopush | OFF | 116us | 73us | 43us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5 | mysqld_nopush | ON | 115us | 73us | 42us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld_nopush | OFF | 119us | 71us | 48us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld_nopush | ON | 118us | 69us | 49us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld_nopush | OFF | 129us | 79us | 50us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld_nopush | ON | 119us | 72us | 46us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_point | mysqld_nopush | OFF | 125us | 76us | 49us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_point | mysqld_nopush | ON | 126us | 73us | 52us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld_nopush | OFF | 174us | 112us | 62us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld_nopush | ON | 173us | 112us | 60us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_batch100 | mysqld_nopush | OFF | 4.71ms | 4.16ms | 546us | 0.0 | 194 | 0.00 | 13.1 |
| fs_hw_snow1_batch100 | mysqld_nopush | ON | 4.86ms | 4.27ms | 594us | 0.0 | 194 | 0.00 | 13.1 |
| fs_hw_snow2_left_chain | mysqld_nopush | OFF | 171us | 113us | 58us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_chain | mysqld_nopush | ON | 168us | 110us | 59us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld_nopush | OFF | 175us | 113us | 62us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld_nopush | ON | 173us | 114us | 59us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_strkey_point | mysqld_nopush | OFF | 120us | 74us | 46us | 3.1 | 37 | 0.00 | 1.8 |
| fs_hw_strkey_point | mysqld_nopush | ON | 120us | 74us | 46us | 3.1 | 37 | 0.00 | 1.8 |
| fs_hw_strkey_batch100 | mysqld_nopush | OFF | 3.92ms | 1.18ms | 2.74ms | 5.2 | 3621 | 0.00 | 198.1 |
| fs_hw_strkey_batch100 | mysqld_nopush | ON | 3.90ms | 1.16ms | 2.74ms | 5.2 | 3621 | 0.00 | 198.1 |
| fs_hw_composite_point | mysqld_nopush | OFF | 113us | 64us | 49us | 1.9 | 4 | 0.00 | 0.3 |
| fs_hw_composite_point | mysqld_nopush | ON | 117us | 68us | 48us | 1.9 | 4 | 0.00 | 0.3 |
| fs_hw_sessions_window2h | mysqld_nopush | OFF | 106us | 65us | 42us | 0.9 | 2 | 0.00 | 0.2 |
| fs_hw_sessions_window2h | mysqld_nopush | ON | 110us | 65us | 45us | 0.9 | 2 | 0.00 | 0.2 |
| fs_hw_collect5_twin | mysqld_nopush | OFF | 142us | 70us | 71us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_twin | mysqld_nopush | ON | 143us | 70us | 73us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_twin | mysqld_nopush | OFF | 126us | 79us | 47us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_twin | mysqld_nopush | ON | 127us | 81us | 46us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_twin | mysqld_nopush | OFF | 144us | 95us | 50us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_twin | mysqld_nopush | ON | 149us | 98us | 51us | 0.0 | 3 | 0.00 | 0.2 |

## E. ndbinfo.jit deltas per case, compiler ON, 1 thread (per request incl. warmup)

| query | engine | compiled/req | reused/req | fallback | rows executed/req | compile us/req | compile share |
|---|---|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_floor | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_point | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_window7d | ronsql | 0.00 | 4.00 | 0 | 12 | 0.0 | 0.0% |
| fs_hw_agg_window7d | mysqld_nopush | 0.99 | 3.01 | 0 | 12 | 1.1 | 1.0% |
| fs_hw_agg_greatest | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_greatest | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_filter | ronsql | 0.00 | 4.00 | 0 | 9 | 0.0 | 0.0% |
| fs_hw_agg_filter | mysqld_nopush | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | ronsql | 0.00 | 4.00 | 0 | 368 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | mysqld_nopush | 1.00 | 3.00 | 0 | 363 | 2.0 | 0.8% |
| fs_hw_agg_batch100 | ronsql | 0.00 | 4.00 | 0 | 3535 | 0.0 | 0.0% |
| fs_hw_agg_batch100 | mysqld_nopush | 1.00 | 3.00 | 0 | 3611 | 13.1 | 0.7% |
| fs_hw_agg_batch1000 | ronsql | 0.00 | 4.00 | 0 | 35999 | 0.0 | 0.0% |
| fs_hw_agg_batch1000 | mysqld_nopush | 0.00 | 0.00 | 3584 | 0 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | ronsql | 0.00 | 4.00 | 0 | 1998 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | mysqld_nopush | 1.00 | 7.00 | 0 | 1931 | 12.3 | 0.9% |
| fs_hw_collect5 | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5_cte | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_point | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow1_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_point | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_batch100 | ronsql | 1.97 | 4.03 | 0 | 100100 | 21.5 | 0.1% |
| fs_hw_snow1_batch100 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_strkey_point | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_strkey_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_strkey_batch100 | ronsql | 0.00 | 4.00 | 0 | 3593 | 0.0 | 0.0% |
| fs_hw_strkey_batch100 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_composite_point | ronsql | 0.00 | 4.00 | 0 | 4 | 0.0 | 0.0% |
| fs_hw_composite_point | mysqld_nopush | 0.98 | 3.02 | 0 | 4 | 1.3 | 1.1% |
| fs_hw_sessions_window2h | ronsql | 0.00 | 4.00 | 0 | 1 | 0.0 | 0.0% |
| fs_hw_sessions_window2h | mysqld_nopush | 0.99 | 3.01 | 0 | 1 | 1.4 | 1.3% |
| fs_hw_collect5_twin | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_twin | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_twin | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |

## F. Throughput scaling (q/s) across thread counts

| query | engine | compiler | T=1 | T=8 | T8/T1 |
|---|---|---|---:|---:|---:|
| fs_hw_floor | ronsql | OFF | 9370 | 28258 | 3.02x |
| fs_hw_floor | ronsql | ON | 9722 | 27646 | 2.84x |
| fs_hw_floor | mysqld_nopush | OFF | 10819 | 30471 | 2.82x |
| fs_hw_floor | mysqld_nopush | ON | 9496 | 30641 | 3.23x |
| fs_hw_agg_point | ronsql | OFF | 8540 | 25627 | 3.00x |
| fs_hw_agg_point | ronsql | ON | 9022 | 25173 | 2.79x |
| fs_hw_agg_point | mysqld_nopush | OFF | 8770 | 27461 | 3.13x |
| fs_hw_agg_point | mysqld_nopush | ON | 9021 | 27590 | 3.06x |
| fs_hw_agg_window7d | ronsql | OFF | 7715 | 25733 | 3.34x |
| fs_hw_agg_window7d | ronsql | ON | 8172 | 25463 | 3.12x |
| fs_hw_agg_window7d | mysqld_nopush | OFF | 8779 | 27623 | 3.15x |
| fs_hw_agg_window7d | mysqld_nopush | ON | 8776 | 27264 | 3.11x |
| fs_hw_agg_greatest | ronsql | OFF | 9005 | 25681 | 2.85x |
| fs_hw_agg_greatest | ronsql | ON | 8568 | 25790 | 3.01x |
| fs_hw_agg_greatest | mysqld_nopush | OFF | 8967 | 27450 | 3.06x |
| fs_hw_agg_greatest | mysqld_nopush | ON | 9069 | 27658 | 3.05x |
| fs_hw_agg_filter | ronsql | OFF | 9010 | 25575 | 2.84x |
| fs_hw_agg_filter | ronsql | ON | 8760 | 25537 | 2.92x |
| fs_hw_agg_filter | mysqld_nopush | OFF | 8865 | 27005 | 3.05x |
| fs_hw_agg_filter | mysqld_nopush | ON | 8675 | 26814 | 3.09x |
| fs_hw_agg_batch10 | ronsql | OFF | 5 | 10 | 2.00x |
| fs_hw_agg_batch10 | ronsql | ON | 5 | 8 | 1.75x |
| fs_hw_agg_batch10 | mysqld_nopush | OFF | 4198 | 14226 | 3.39x |
| fs_hw_agg_batch10 | mysqld_nopush | ON | 4210 | 13920 | 3.31x |
| fs_hw_agg_batch100 | ronsql | OFF | 2 | 3 | 1.60x |
| fs_hw_agg_batch100 | ronsql | ON | 2 | 3 | 1.57x |
| fs_hw_agg_batch100 | mysqld_nopush | OFF | 536 | 1619 | 3.02x |
| fs_hw_agg_batch100 | mysqld_nopush | ON | 520 | 1594 | 3.07x |
| fs_hw_agg_batch1000 | ronsql | OFF | 0 | 0 | 1.26x |
| fs_hw_agg_batch1000 | ronsql | ON | 0 | 0 | 1.33x |
| fs_hw_agg_batch1000 | mysqld_nopush | OFF | 23 | 51 | 2.20x |
| fs_hw_agg_batch1000 | mysqld_nopush | ON | 23 | 55 | 2.41x |
| fs_hw_agg_batch100_window | ronsql | OFF | 2 | 2 | 1.33x |
| fs_hw_agg_batch100_window | ronsql | ON | 2 | 2 | 1.31x |
| fs_hw_agg_batch100_window | mysqld_nopush | OFF | 750 | 2159 | 2.88x |
| fs_hw_agg_batch100_window | mysqld_nopush | ON | 736 | 2073 | 2.82x |
| fs_hw_collect5 | ronsql | OFF | 8833 | 26653 | 3.02x |
| fs_hw_collect5 | ronsql | ON | 8655 | 26324 | 3.04x |
| fs_hw_collect5 | mysqld_nopush | OFF | 8603 | 26982 | 3.14x |
| fs_hw_collect5 | mysqld_nopush | ON | 8639 | 27464 | 3.18x |
| fs_hw_collect5_cte | mysqld_nopush | OFF | 8335 | 26503 | 3.18x |
| fs_hw_collect5_cte | mysqld_nopush | ON | 8432 | 25945 | 3.08x |
| fs_hw_collect50 | ronsql | OFF | 8468 | 26066 | 3.08x |
| fs_hw_collect50 | ronsql | ON | 8473 | 26327 | 3.11x |
| fs_hw_collect50 | mysqld_nopush | OFF | 7743 | 26479 | 3.42x |
| fs_hw_collect50 | mysqld_nopush | ON | 8385 | 26607 | 3.17x |
| fs_hw_snow1_point | ronsql | OFF | 1900 | 9075 | 4.78x |
| fs_hw_snow1_point | ronsql | ON | 2007 | 9100 | 4.53x |
| fs_hw_snow1_point | mysqld_nopush | OFF | 7919 | 25181 | 3.18x |
| fs_hw_snow1_point | mysqld_nopush | ON | 7902 | 25052 | 3.17x |
| fs_hw_snow2_point | ronsql | OFF | 1880 | 8688 | 4.62x |
| fs_hw_snow2_point | ronsql | ON | 1885 | 8606 | 4.56x |
| fs_hw_snow2_point | mysqld_nopush | OFF | 5708 | 19428 | 3.40x |
| fs_hw_snow2_point | mysqld_nopush | ON | 5765 | 19152 | 3.32x |
| fs_hw_snow1_batch100 | ronsql | OFF | 50 | 88 | 1.76x |
| fs_hw_snow1_batch100 | ronsql | ON | 46 | 72 | 1.59x |
| fs_hw_snow1_batch100 | mysqld_nopush | OFF | 212 | 629 | 2.97x |
| fs_hw_snow1_batch100 | mysqld_nopush | ON | 205 | 678 | 3.30x |
| fs_hw_snow2_left_chain | ronsql | OFF | 1864 | 8441 | 4.53x |
| fs_hw_snow2_left_chain | ronsql | ON | 1866 | 8544 | 4.58x |
| fs_hw_snow2_left_chain | mysqld_nopush | OFF | 5823 | 19086 | 3.28x |
| fs_hw_snow2_left_chain | mysqld_nopush | ON | 5914 | 19429 | 3.29x |
| fs_hw_snow2_left_single | ronsql | OFF | 1861 | 8740 | 4.70x |
| fs_hw_snow2_left_single | ronsql | ON | 1865 | 8725 | 4.68x |
| fs_hw_snow2_left_single | mysqld_nopush | OFF | 5687 | 19550 | 3.44x |
| fs_hw_snow2_left_single | mysqld_nopush | ON | 5743 | 19527 | 3.40x |
| fs_hw_strkey_point | ronsql | OFF | 8597 | 25887 | 3.01x |
| fs_hw_strkey_point | ronsql | ON | 8422 | 25718 | 3.05x |
| fs_hw_strkey_point | mysqld_nopush | OFF | 8289 | 27159 | 3.28x |
| fs_hw_strkey_point | mysqld_nopush | ON | 8303 | 26709 | 3.22x |
| fs_hw_strkey_batch100 | ronsql | OFF | 3 | 4 | 1.34x |
| fs_hw_strkey_batch100 | ronsql | ON | 3 | 3 | 1.21x |
| fs_hw_strkey_batch100 | mysqld_nopush | OFF | 254 | 1296 | 5.09x |
| fs_hw_strkey_batch100 | mysqld_nopush | ON | 255 | 1316 | 5.15x |
| fs_hw_composite_point | ronsql | OFF | 8822 | 26609 | 3.02x |
| fs_hw_composite_point | ronsql | ON | 8590 | 26294 | 3.06x |
| fs_hw_composite_point | mysqld_nopush | OFF | 8721 | 28467 | 3.26x |
| fs_hw_composite_point | mysqld_nopush | ON | 8484 | 27999 | 3.30x |
| fs_hw_sessions_window2h | ronsql | OFF | 8885 | 26509 | 2.98x |
| fs_hw_sessions_window2h | ronsql | ON | 8695 | 26526 | 3.05x |
| fs_hw_sessions_window2h | mysqld_nopush | OFF | 9295 | 29024 | 3.12x |
| fs_hw_sessions_window2h | mysqld_nopush | ON | 9007 | 28485 | 3.16x |
| fs_hw_collect5_twin | mysqld_nopush | OFF | 7013 | 24676 | 3.52x |
| fs_hw_collect5_twin | mysqld_nopush | ON | 6945 | 24388 | 3.51x |
| fs_hw_snow1_twin | mysqld_nopush | OFF | 7896 | 26844 | 3.40x |
| fs_hw_snow1_twin | mysqld_nopush | ON | 7849 | 26721 | 3.40x |
| fs_hw_snow2_twin | mysqld_nopush | OFF | 6893 | 22882 | 3.32x |
| fs_hw_snow2_twin | mysqld_nopush | ON | 6672 | 23034 | 3.45x |

## G. Hopsworks serving path: RonSQL template vs MySQL production twin (avg latency; ratio = twin / ronsql, >1 = RonSQL path faster)

template = the Hopsworks RonSQL statement on RonSQL; same text = that statement on the MySQL server; twin = the production MySQL statement Hopsworks runs on the SQL path (ROW_NUMBER window / nested join).

| entry | threads | compiler | ronsql template | mysqld_nopush same text | mysqld_nopush twin | twin/ronsql |
|---|---:|---|---:|---:|---:|---:|
| fs_hw_collect5 | 1 | OFF | 108us | 116us | 142us | 1.31x |
| fs_hw_collect5 | 1 | ON | 110us | 115us | 143us | 1.30x |
| fs_hw_collect5 | 8 | OFF | 288us | 293us | 321us | 1.12x |
| fs_hw_collect5 | 8 | ON | 291us | 288us | 325us | 1.11x |
