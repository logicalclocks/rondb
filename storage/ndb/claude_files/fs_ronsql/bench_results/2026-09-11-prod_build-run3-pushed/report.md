# RonSQL / MySQL / compiled-interpreter benchmark matrix

build=/Users/mikael/mysql_trees/rondb_1121_fs_ronsql/prod_build sf=0.1 threads=[1, 8] engines=mysqld compiler=OFF,ON order=query-major repeat=1 cpubind=- client_cpus=- host=mikaels-MacBook-Pro.local (Darwin arm64) started 2026-09-11T12:51:43

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 1 thread

| query | mysqld OFF avg | mysqld ON avg | mysqld ON/OFF | mysqld OFF q/s | mysqld ON q/s |
|---|---:|---:|---:|---:|---:|
| fs_hw_floor | 106us | 103us | 1.03x | 9428 | 9680 |
| fs_hw_agg_window7d | 119us | 119us | 1.00x | 8320 | 8316 |
| fs_hw_agg_greatest | 119us | 120us | 0.99x | 8332 | 8289 |
| fs_hw_agg_batch10 | 972us | 1.00ms | 0.97x | 1027 | 997 |
| fs_hw_agg_batch100 | 10.38ms | 10.26ms | 1.01x | 96 | 97 |
| fs_hw_agg_batch1000 | 172ms | 174ms | 0.99x | 6 | 6 |
| fs_hw_agg_batch100_window | 10.06ms | 10.08ms | 1.00x | 99 | 99 |
| fs_hw_collect5 | 113us | 118us | 0.96x | 8775 | 8456 |
| fs_hw_collect50 | 116us | 117us | 1.00x | 8544 | 8535 |
| fs_hw_snow1_point | 124us | 122us | 1.01x | 8027 | 8119 |
| fs_hw_snow2_point | 168us | 171us | 0.98x | 5931 | 5823 |
| fs_hw_snow1_batch100 | 8.62ms | 8.76ms | 0.98x | 116 | 114 |
| fs_hw_snow2_left_chain | 169us | 167us | 1.01x | 5892 | 5964 |
| fs_hw_snow2_left_single | 168us | 169us | 1.00x | 5911 | 5896 |
| fs_hw_composite_point | 117us | 115us | 1.02x | 8473 | 8621 |
| fs_hw_sessions_window2h | 115us | 113us | 1.01x | 8650 | 8728 |
| fs_hw_collect5_twin | 146us | 146us | 1.00x | 6809 | 6827 |
| fs_hw_snow1_twin | 119us | 125us | 0.96x | 8327 | 7967 |
| fs_hw_snow2_twin | 149us | 144us | 1.03x | 6664 | 6893 |

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 8 threads

| query | mysqld OFF avg | mysqld ON avg | mysqld ON/OFF | mysqld OFF q/s | mysqld ON q/s |
|---|---:|---:|---:|---:|---:|
| fs_hw_floor | 257us | 254us | 1.01x | 30986 | 31198 |
| fs_hw_agg_window7d | 290us | 292us | 0.99x | 27182 | 27064 |
| fs_hw_agg_greatest | 282us | 283us | 1.00x | 28082 | 27953 |
| fs_hw_agg_batch10 | 2.16ms | 2.19ms | 0.99x | 3662 | 3599 |
| fs_hw_agg_batch100 | 23.64ms | 23.61ms | 1.00x | 335 | 327 |
| fs_hw_agg_batch1000 | 541ms | 542ms | 1.00x | 15 | 15 |
| fs_hw_agg_batch100_window | 22.56ms | 22.85ms | 0.99x | 346 | 339 |
| fs_hw_collect5 | 289us | 284us | 1.02x | 27449 | 27839 |
| fs_hw_collect50 | 300us | 311us | 0.96x | 26347 | 25271 |
| fs_hw_snow1_point | 321us | 317us | 1.01x | 24622 | 25045 |
| fs_hw_snow2_point | 407us | 422us | 0.96x | 19502 | 18811 |
| fs_hw_snow1_batch100 | 21.44ms | 21.26ms | 1.01x | 372 | 376 |
| fs_hw_snow2_left_chain | 409us | 412us | 0.99x | 19389 | 19285 |
| fs_hw_snow2_left_single | 409us | 412us | 0.99x | 19433 | 19303 |
| fs_hw_composite_point | 289us | 292us | 0.99x | 27359 | 27105 |
| fs_hw_sessions_window2h | 285us | 290us | 0.98x | 27758 | 27150 |
| fs_hw_collect5_twin | 322us | 327us | 0.99x | 24513 | 23969 |
| fs_hw_snow1_twin | 299us | 300us | 1.00x | 26505 | 26477 |
| fs_hw_snow2_twin | 350us | 351us | 1.00x | 22720 | 22639 |

## D. MySQL server — where the time goes (1 thread)

ndb wait = Ndb_api_wait_nanos_count per request (time the mysqld connection waited for data nodes), corrected by the idle baseline (1.00 s/s of background NDB API waiting measured before the matrix); mysqld = client latency - ndb wait (parsing, optimizer, row processing, result transfer); batches / rows = scan batches and rows received from NDB per request; pushed = pushed (SPJ) queries executed per request. Counters include the warmup request and are approximate at short case durations.

| query | engine | compiler | client | ndb wait | mysqld | batches/req | rows/req | pushed/req | KB recv/req |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | mysqld | OFF | 106us | 74us | 32us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_floor | mysqld | ON | 103us | 70us | 33us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_agg_window7d | mysqld | OFF | 119us | 73us | 46us | 4.0 | 5 | 0.00 | 0.6 |
| fs_hw_agg_window7d | mysqld | ON | 119us | 72us | 47us | 4.0 | 5 | 0.00 | 0.6 |
| fs_hw_agg_greatest | mysqld | OFF | 119us | 71us | 48us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_greatest | mysqld | ON | 120us | 74us | 46us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_batch10 | mysqld | OFF | 972us | 830us | 141us | 31.3 | 32 | 0.00 | 3.8 |
| fs_hw_agg_batch10 | mysqld | ON | 1.00ms | 847us | 153us | 31.3 | 32 | 0.00 | 3.8 |
| fs_hw_agg_batch100 | mysqld | OFF | 10.38ms | 8.92ms | 1.46ms | 313.1 | 321 | 0.00 | 38.1 |
| fs_hw_agg_batch100 | mysqld | ON | 10.26ms | 8.83ms | 1.43ms | 313.1 | 321 | 0.00 | 38.1 |
| fs_hw_agg_batch1000 | mysqld | OFF | 172ms | 123ms | 48.56ms | 3132.0 | 3264 | 0.00 | 382.1 |
| fs_hw_agg_batch1000 | mysqld | ON | 174ms | 125ms | 48.53ms | 3132.0 | 3264 | 0.00 | 382.1 |
| fs_hw_agg_batch100_window | mysqld | OFF | 10.06ms | 8.56ms | 1.50ms | 293.9 | 302 | 0.00 | 36.4 |
| fs_hw_agg_batch100_window | mysqld | ON | 10.08ms | 8.61ms | 1.47ms | 293.9 | 302 | 0.00 | 36.4 |
| fs_hw_collect5 | mysqld | OFF | 113us | 72us | 41us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5 | mysqld | ON | 118us | 75us | 43us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld | OFF | 116us | 69us | 47us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld | ON | 117us | 72us | 45us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_point | mysqld | OFF | 124us | 74us | 49us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_point | mysqld | ON | 122us | 73us | 49us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld | OFF | 168us | 109us | 59us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld | ON | 171us | 112us | 59us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_batch100 | mysqld | OFF | 8.62ms | 7.71ms | 915us | 0.0 | 195 | 0.00 | 13.1 |
| fs_hw_snow1_batch100 | mysqld | ON | 8.76ms | 7.79ms | 965us | 0.0 | 195 | 0.00 | 13.1 |
| fs_hw_snow2_left_chain | mysqld | OFF | 169us | 110us | 59us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_chain | mysqld | ON | 167us | 111us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld | OFF | 168us | 113us | 55us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld | ON | 169us | 111us | 57us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_composite_point | mysqld | OFF | 117us | 69us | 47us | 4.0 | 5 | 0.00 | 0.4 |
| fs_hw_composite_point | mysqld | ON | 115us | 70us | 45us | 4.0 | 5 | 0.00 | 0.4 |
| fs_hw_sessions_window2h | mysqld | OFF | 115us | 68us | 47us | 4.0 | 5 | 0.00 | 0.4 |
| fs_hw_sessions_window2h | mysqld | ON | 113us | 70us | 43us | 4.0 | 5 | 0.00 | 0.4 |
| fs_hw_collect5_twin | mysqld | OFF | 146us | 71us | 75us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_twin | mysqld | ON | 146us | 75us | 70us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_twin | mysqld | OFF | 119us | 76us | 43us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_twin | mysqld | ON | 125us | 80us | 45us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_twin | mysqld | OFF | 149us | 99us | 50us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_twin | mysqld | ON | 144us | 95us | 49us | 0.0 | 3 | 0.00 | 0.2 |

## E. ndbinfo.jit deltas per case, compiler ON, 1 thread (per request incl. warmup)

| query | engine | compiled/req | reused/req | fallback | rows executed/req | compile us/req | compile share |
|---|---|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_window7d | mysqld | 0.00 | 4.00 | 0 | 12 | 0.0 | 0.0% |
| fs_hw_agg_greatest | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | mysqld | 0.00 | 40.00 | 0 | 363 | 0.0 | 0.0% |
| fs_hw_agg_batch100 | mysqld | 0.00 | 400.00 | 0 | 3636 | 0.0 | 0.0% |
| fs_hw_agg_batch1000 | mysqld | 0.00 | 4000.00 | 0 | 36611 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | mysqld | 0.00 | 400.00 | 0 | 1973 | 0.0 | 0.0% |
| fs_hw_collect5 | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_point | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_point | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_batch100 | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_composite_point | mysqld | 0.00 | 4.00 | 0 | 4 | 0.0 | 0.0% |
| fs_hw_sessions_window2h | mysqld | 0.00 | 4.00 | 0 | 1 | 0.0 | 0.0% |
| fs_hw_collect5_twin | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_twin | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_twin | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |

## F. Throughput scaling (q/s) across thread counts

| query | engine | compiler | T=1 | T=8 | T8/T1 |
|---|---|---|---:|---:|---:|
| fs_hw_floor | mysqld | OFF | 9428 | 30986 | 3.29x |
| fs_hw_floor | mysqld | ON | 9680 | 31198 | 3.22x |
| fs_hw_agg_window7d | mysqld | OFF | 8320 | 27182 | 3.27x |
| fs_hw_agg_window7d | mysqld | ON | 8316 | 27064 | 3.25x |
| fs_hw_agg_greatest | mysqld | OFF | 8332 | 28082 | 3.37x |
| fs_hw_agg_greatest | mysqld | ON | 8289 | 27953 | 3.37x |
| fs_hw_agg_batch10 | mysqld | OFF | 1027 | 3662 | 3.56x |
| fs_hw_agg_batch10 | mysqld | ON | 997 | 3599 | 3.61x |
| fs_hw_agg_batch100 | mysqld | OFF | 96 | 335 | 3.48x |
| fs_hw_agg_batch100 | mysqld | ON | 97 | 327 | 3.36x |
| fs_hw_agg_batch1000 | mysqld | OFF | 6 | 15 | 2.51x |
| fs_hw_agg_batch1000 | mysqld | ON | 6 | 15 | 2.52x |
| fs_hw_agg_batch100_window | mysqld | OFF | 99 | 346 | 3.48x |
| fs_hw_agg_batch100_window | mysqld | ON | 99 | 339 | 3.42x |
| fs_hw_collect5 | mysqld | OFF | 8775 | 27449 | 3.13x |
| fs_hw_collect5 | mysqld | ON | 8456 | 27839 | 3.29x |
| fs_hw_collect50 | mysqld | OFF | 8544 | 26347 | 3.08x |
| fs_hw_collect50 | mysqld | ON | 8535 | 25271 | 2.96x |
| fs_hw_snow1_point | mysqld | OFF | 8027 | 24622 | 3.07x |
| fs_hw_snow1_point | mysqld | ON | 8119 | 25045 | 3.08x |
| fs_hw_snow2_point | mysqld | OFF | 5931 | 19502 | 3.29x |
| fs_hw_snow2_point | mysqld | ON | 5823 | 18811 | 3.23x |
| fs_hw_snow1_batch100 | mysqld | OFF | 116 | 372 | 3.21x |
| fs_hw_snow1_batch100 | mysqld | ON | 114 | 376 | 3.29x |
| fs_hw_snow2_left_chain | mysqld | OFF | 5892 | 19389 | 3.29x |
| fs_hw_snow2_left_chain | mysqld | ON | 5964 | 19285 | 3.23x |
| fs_hw_snow2_left_single | mysqld | OFF | 5911 | 19433 | 3.29x |
| fs_hw_snow2_left_single | mysqld | ON | 5896 | 19303 | 3.27x |
| fs_hw_composite_point | mysqld | OFF | 8473 | 27359 | 3.23x |
| fs_hw_composite_point | mysqld | ON | 8621 | 27105 | 3.14x |
| fs_hw_sessions_window2h | mysqld | OFF | 8650 | 27758 | 3.21x |
| fs_hw_sessions_window2h | mysqld | ON | 8728 | 27150 | 3.11x |
| fs_hw_collect5_twin | mysqld | OFF | 6809 | 24513 | 3.60x |
| fs_hw_collect5_twin | mysqld | ON | 6827 | 23969 | 3.51x |
| fs_hw_snow1_twin | mysqld | OFF | 8327 | 26505 | 3.18x |
| fs_hw_snow1_twin | mysqld | ON | 7967 | 26477 | 3.32x |
| fs_hw_snow2_twin | mysqld | OFF | 6664 | 22720 | 3.41x |
| fs_hw_snow2_twin | mysqld | ON | 6893 | 22639 | 3.28x |
