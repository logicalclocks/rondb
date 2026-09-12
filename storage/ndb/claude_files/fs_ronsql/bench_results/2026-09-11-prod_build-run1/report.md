# RonSQL / MySQL / compiled-interpreter benchmark matrix

build=/Users/mikael/mysql_trees/rondb_1121_fs_ronsql/prod_build sf=1 threads=[1, 8] engines=ronsql,mysqld,mysqld_nopush compiler=OFF,ON order=query-major repeat=1 cpubind=- client_cpus=- host=mikaels-MacBook-Pro.local (Darwin arm64) started 2026-09-11T11:15:34

FAILED cases: off_mysqld_fs_hw_agg_point_T1 (warmup query failed: error iterating rows: Error 1296 (HY000): Got error 4120 'Scan already complete' from NDBCLUSTER), on_mysqld_fs_hw_agg_point_T1 (warmup query failed: error iterating rows: Error 1296 (HY000): Got error 4120 'Scan already complete' from NDBCLUSTER), off_mysqld_fs_hw_agg_filter_T1 (warmup query failed: error iterating rows: Error 1296 (HY000): Got error 4120 'Scan already complete' from NDBCLUSTER), on_mysqld_fs_hw_agg_filter_T1 (warmup query failed: error iterating rows: Error 1296 (HY000): Got error 4120 'Scan already complete' from NDBCLUSTER), off_ronsql_fs_hw_collect5_cte_T1 (Error handling: RPE), on_ronsql_fs_hw_collect5_cte_T1 (Error handling: RPE), off_mysqld_fs_hw_strkey_point_T1 (warmup query failed: error iterating rows: Error 1296 (HY000): Got error 4120 'Scan already complete' from NDBCLUSTER), on_mysqld_fs_hw_strkey_point_T1 (warmup query failed: error iterating rows: Error 1296 (HY000): Got error 4120 'Scan already complete' from NDBCLUSTER)

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 1 thread

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld OFF avg | mysqld ON avg | mysqld ON/OFF | mysqld OFF q/s | mysqld ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | 99us | 99us | 1.00x | 9691 | 9661 | 95us | 99us | 0.96x | 10506 | 10083 | 100us | 96us | 1.04x | 10009 | 10436 |
| fs_hw_agg_point | 104us | 109us | 0.96x | 9133 | 8754 | - | - | - | - | - | 111us | 111us | 1.00x | 8973 | 8989 |
| fs_hw_agg_window7d | 120us | 111us | 1.08x | 7969 | 8583 | 126us | 110us | 1.14x | 7856 | 8979 | 112us | 114us | 0.98x | 8861 | 8697 |
| fs_hw_agg_greatest | 105us | 109us | 0.97x | 9094 | 8800 | 110us | 113us | 0.97x | 9043 | 8789 | 111us | 108us | 1.02x | 8991 | 9203 |
| fs_hw_agg_filter | 103us | 111us | 0.94x | 9213 | 8629 | - | - | - | - | - | 116us | 125us | 0.93x | 8548 | 7923 |
| fs_hw_agg_batch10 | 204ms | 206ms | 0.99x | 5 | 5 | 988us | 992us | 1.00x | 1010 | 1006 | 251us | 241us | 1.04x | 3954 | 4123 |
| fs_hw_agg_batch100 | 572ms | 567ms | 1.01x | 2 | 2 | 10.09ms | 10.33ms | 0.98x | 99 | 97 | 1.81ms | 1.86ms | 0.97x | 551 | 536 |
| fs_hw_agg_batch1000 | 4384ms | 4245ms | 1.03x | 0 | 0 | 169ms | 170ms | 1.00x | 6 | 6 | 42.32ms | 42.99ms | 0.98x | 24 | 23 |
| fs_hw_agg_batch100_window | 545ms | 545ms | 1.00x | 2 | 2 | 10.11ms | 10.10ms | 1.00x | 99 | 99 | 1.22ms | 1.28ms | 0.95x | 815 | 774 |
| fs_hw_collect5 | 104us | 103us | 1.01x | 9132 | 9227 | 113us | 109us | 1.04x | 8795 | 9104 | 110us | 110us | 1.00x | 9059 | 9060 |
| fs_hw_collect5_cte | - | - | - | - | - | 121us | 125us | 0.97x | 8231 | 7974 | 119us | 116us | 1.03x | 8380 | 8594 |
| fs_hw_collect50 | 116us | 112us | 1.03x | 8227 | 8496 | 118us | 117us | 1.01x | 8450 | 8511 | 121us | 116us | 1.04x | 8211 | 8564 |
| fs_hw_snow1_point | 483us | 484us | 1.00x | 2046 | 2044 | 120us | 120us | 1.00x | 8291 | 8266 | 119us | 119us | 1.00x | 8349 | 8349 |
| fs_hw_snow2_point | 512us | 516us | 0.99x | 1933 | 1918 | 172us | 168us | 1.02x | 5777 | 5915 | 166us | 171us | 0.97x | 6014 | 5806 |
| fs_hw_snow1_batch100 | 19.60ms | 22.23ms | 0.88x | 51 | 45 | 8.17ms | 8.14ms | 1.00x | 122 | 123 | 4.45ms | 4.40ms | 1.01x | 225 | 227 |
| fs_hw_snow2_left_chain | 513us | 512us | 1.00x | 1927 | 1931 | 164us | 162us | 1.01x | 6072 | 6149 | 164us | 165us | 0.99x | 6070 | 6035 |
| fs_hw_snow2_left_single | 511us | 525us | 0.97x | 1935 | 1882 | 164us | 165us | 0.99x | 6066 | 6027 | 163us | 163us | 1.00x | 6097 | 6090 |
| fs_hw_strkey_point | 108us | 112us | 0.96x | 8822 | 8511 | - | - | - | - | - | 119us | 121us | 0.98x | 8353 | 8211 |
| fs_hw_strkey_batch100 | 381ms | - | - | 3 | - | - | - | - | - | - | - | - | - | - | - |

## A. Latency (avg) and throughput per engine, compiler OFF vs ON — 8 threads

| query | ronsql OFF avg | ronsql ON avg | ronsql ON/OFF | ronsql OFF q/s | ronsql ON q/s | mysqld OFF avg | mysqld ON avg | mysqld ON/OFF | mysqld OFF q/s | mysqld ON q/s | mysqld_nopush OFF avg | mysqld_nopush ON avg | mysqld_nopush ON/OFF | mysqld_nopush OFF q/s | mysqld_nopush ON q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_point | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_window7d | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_greatest | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_filter | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch10 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch100 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch1000 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_agg_batch100_window | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect5_cte | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_collect50 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_point | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_point | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow1_batch100 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_left_chain | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_snow2_left_single | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_strkey_point | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| fs_hw_strkey_batch100 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |

## B. RonSQL vs MySQL server (avg latency; ratio = mysqld / ronsql, >1 = RonSQL faster)

| query | threads | compiler | ronsql | mysqld | mysqld/ronsql | mysqld_nopush | mysqld_nopush/ronsql |
|---|---:|---|---:|---:|---:|---:|---:|
| fs_hw_floor | 1 | OFF | 99us | 95us | 0.96x | 100us | 1.01x |
| fs_hw_floor | 1 | ON | 99us | 99us | 1.00x | 96us | 0.97x |
| fs_hw_agg_point | 1 | OFF | 104us | - | - | 111us | 1.06x |
| fs_hw_agg_point | 1 | ON | 109us | - | - | 111us | 1.01x |
| fs_hw_agg_window7d | 1 | OFF | 120us | 126us | 1.05x | 112us | 0.93x |
| fs_hw_agg_window7d | 1 | ON | 111us | 110us | 0.99x | 114us | 1.03x |
| fs_hw_agg_greatest | 1 | OFF | 105us | 110us | 1.05x | 111us | 1.05x |
| fs_hw_agg_greatest | 1 | ON | 109us | 113us | 1.04x | 108us | 1.00x |
| fs_hw_agg_filter | 1 | OFF | 103us | - | - | 116us | 1.12x |
| fs_hw_agg_filter | 1 | ON | 111us | - | - | 125us | 1.13x |
| fs_hw_agg_batch10 | 1 | OFF | 204ms | 988us | 0.00x | 251us | 0.00x |
| fs_hw_agg_batch10 | 1 | ON | 206ms | 992us | 0.00x | 241us | 0.00x |
| fs_hw_agg_batch100 | 1 | OFF | 572ms | 10.09ms | 0.02x | 1.81ms | 0.00x |
| fs_hw_agg_batch100 | 1 | ON | 567ms | 10.33ms | 0.02x | 1.86ms | 0.00x |
| fs_hw_agg_batch1000 | 1 | OFF | 4384ms | 169ms | 0.04x | 42.32ms | 0.01x |
| fs_hw_agg_batch1000 | 1 | ON | 4245ms | 170ms | 0.04x | 42.99ms | 0.01x |
| fs_hw_agg_batch100_window | 1 | OFF | 545ms | 10.11ms | 0.02x | 1.22ms | 0.00x |
| fs_hw_agg_batch100_window | 1 | ON | 545ms | 10.10ms | 0.02x | 1.28ms | 0.00x |
| fs_hw_collect5 | 1 | OFF | 104us | 113us | 1.08x | 110us | 1.05x |
| fs_hw_collect5 | 1 | ON | 103us | 109us | 1.06x | 110us | 1.06x |
| fs_hw_collect5_cte | 1 | OFF | - | 121us | - | 119us | - |
| fs_hw_collect5_cte | 1 | ON | - | 125us | - | 116us | - |
| fs_hw_collect50 | 1 | OFF | 116us | 118us | 1.02x | 121us | 1.05x |
| fs_hw_collect50 | 1 | ON | 112us | 117us | 1.04x | 116us | 1.04x |
| fs_hw_snow1_point | 1 | OFF | 483us | 120us | 0.25x | 119us | 0.25x |
| fs_hw_snow1_point | 1 | ON | 484us | 120us | 0.25x | 119us | 0.25x |
| fs_hw_snow2_point | 1 | OFF | 512us | 172us | 0.34x | 166us | 0.32x |
| fs_hw_snow2_point | 1 | ON | 516us | 168us | 0.33x | 171us | 0.33x |
| fs_hw_snow1_batch100 | 1 | OFF | 19.60ms | 8.17ms | 0.42x | 4.45ms | 0.23x |
| fs_hw_snow1_batch100 | 1 | ON | 22.23ms | 8.14ms | 0.37x | 4.40ms | 0.20x |
| fs_hw_snow2_left_chain | 1 | OFF | 513us | 164us | 0.32x | 164us | 0.32x |
| fs_hw_snow2_left_chain | 1 | ON | 512us | 162us | 0.32x | 165us | 0.32x |
| fs_hw_snow2_left_single | 1 | OFF | 511us | 164us | 0.32x | 163us | 0.32x |
| fs_hw_snow2_left_single | 1 | ON | 525us | 165us | 0.31x | 163us | 0.31x |
| fs_hw_strkey_point | 1 | OFF | 108us | - | - | 119us | 1.10x |
| fs_hw_strkey_point | 1 | ON | 112us | - | - | 121us | 1.08x |
| fs_hw_strkey_batch100 | 1 | OFF | 381ms | - | - | - | - |

## C. RonSQL — where the time goes (server-side phases, avg per request, 1 thread)

client = end-to-end latency seen by rondb-cli; http+client = client - prepare - execute (RDRS HTTP handling, JSON, network); firstbatch = data-node execution until the first result row (single-table: the whole DoAggregation); load = NDB dictionary lookups; rows = result rows drained per request.

| query | compiler | client | prepare | execute | http+client | parse | analyze | load | plan | compile | ndbprep | send | firstbatch | drain | print | rows | top |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fs_hw_floor | OFF | 99us | 2us | 67us | 30us | 1us | 0us | 1us | 0us | 0us | 0us | - | 66us | - | 0us | 0.0 | firstbatch 66us |
| fs_hw_floor | ON | 99us | 2us | 68us | 29us | 1us | 0us | 1us | 0us | 0us | 0us | - | 67us | - | 0us | 0.0 | firstbatch 67us |
| fs_hw_agg_point | OFF | 104us | 4us | 72us | 29us | 2us | 0us | 1us | 0us | 0us | 0us | - | 70us | - | 0us | 0.0 | firstbatch 70us |
| fs_hw_agg_point | ON | 109us | 4us | 74us | 32us | 2us | 0us | 1us | 0us | 0us | 0us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_window7d | OFF | 120us | 4us | 82us | 34us | 2us | 0us | 1us | 0us | 0us | 3us | - | 78us | - | 0us | 0.0 | firstbatch 78us |
| fs_hw_agg_window7d | ON | 111us | 4us | 75us | 32us | 2us | 0us | 1us | 0us | 0us | 3us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_greatest | OFF | 105us | 4us | 73us | 28us | 2us | 0us | 1us | 0us | 0us | 0us | - | 71us | - | 0us | 0.0 | firstbatch 71us |
| fs_hw_agg_greatest | ON | 109us | 4us | 73us | 31us | 2us | 0us | 1us | 0us | 0us | 0us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_filter | OFF | 103us | 4us | 71us | 28us | 2us | 0us | 1us | 0us | 0us | 1us | - | 69us | - | 0us | 0.0 | firstbatch 69us |
| fs_hw_agg_filter | ON | 111us | 4us | 74us | 32us | 2us | 0us | 1us | 0us | 0us | 1us | - | 72us | - | 0us | 0.0 | firstbatch 72us |
| fs_hw_agg_batch10 | OFF | 204ms | 26us | 204ms | 144us | 8us | 0us | 13us | 1us | 2us | 7us | - | 204ms | - | 5us | 0.0 | firstbatch 204ms |
| fs_hw_agg_batch10 | ON | 206ms | 25us | 206ms | 155us | 8us | 1us | 12us | 1us | 2us | 8us | - | 206ms | - | 6us | 0.0 | firstbatch 206ms |
| fs_hw_agg_batch100 | OFF | 572ms | 47us | 572ms | 173us | 13us | 1us | 26us | 2us | 3us | 18us | - | 572ms | - | 18us | 0.0 | firstbatch 572ms |
| fs_hw_agg_batch100 | ON | 567ms | 24us | 567ms | 186us | 13us | 1us | 5us | 2us | 2us | 20us | - | 567ms | - | 17us | 0.0 | firstbatch 567ms |
| fs_hw_agg_batch1000 | OFF | 4384ms | 148us | 4384ms | 192us | 50us | 10us | 69us | 15us | 2us | 47us | - | 4384ms | - | 126us | 0.0 | firstbatch 4384ms |
| fs_hw_agg_batch1000 | ON | 4245ms | 148us | 4244ms | 202us | 50us | 10us | 69us | 16us | 2us | 55us | - | 4244ms | - | 132us | 0.0 | firstbatch 4244ms |
| fs_hw_agg_batch100_window | OFF | 545ms | 28us | 545ms | 172us | 13us | 1us | 5us | 6us | 2us | 27us | - | 545ms | - | 18us | 0.0 | firstbatch 545ms |
| fs_hw_agg_batch100_window | ON | 545ms | 26us | 545ms | 174us | 13us | 2us | 5us | 2us | 2us | 23us | - | 545ms | - | 18us | 0.0 | firstbatch 545ms |
| fs_hw_collect5 | OFF | 104us | 3us | 73us | 29us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 67us | 1us | - | 4.2 | firstbatch 67us |
| fs_hw_collect5 | ON | 103us | 3us | 71us | 29us | 1us | 0us | 1us | 0us | 0us | 1us | 3us | 65us | 1us | - | 4.2 | firstbatch 65us |
| fs_hw_collect50 | OFF | 116us | 3us | 80us | 32us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 71us | 5us | - | 17.6 | firstbatch 71us |
| fs_hw_collect50 | ON | 112us | 3us | 77us | 32us | 1us | 0us | 1us | 0us | 0us | 1us | 4us | 67us | 5us | - | 17.6 | firstbatch 67us |
| fs_hw_snow1_point | OFF | 483us | 5us | 435us | 43us | 2us | 0us | 2us | 0us | 0us | 1us | 8us | 415us | 4us | - | 0.9 | firstbatch 415us |
| fs_hw_snow1_point | ON | 484us | 5us | 435us | 43us | 2us | 0us | 2us | 0us | 0us | 1us | 8us | 415us | 4us | - | 0.9 | firstbatch 415us |
| fs_hw_snow2_point | OFF | 512us | 6us | 462us | 43us | 3us | 0us | 2us | 0us | 0us | 1us | 9us | 442us | 3us | - | 0.8 | firstbatch 442us |
| fs_hw_snow2_point | ON | 516us | 6us | 467us | 43us | 3us | 0us | 2us | 0us | 0us | 1us | 9us | 446us | 2us | - | 0.8 | firstbatch 446us |
| fs_hw_snow1_batch100 | OFF | 19.60ms | 19us | 19.49ms | 91us | 9us | 0us | 8us | 0us | 1us | 2us | 11us | 19.30ms | 159us | - | 89.0 | firstbatch 19.30ms |
| fs_hw_snow1_batch100 | ON | 22.23ms | 19us | 22.11ms | 101us | 9us | 0us | 8us | 0us | 1us | 1us | 12us | 21.92ms | 160us | - | 89.0 | firstbatch 21.92ms |
| fs_hw_snow2_left_chain | OFF | 513us | 6us | 465us | 43us | 3us | 0us | 2us | 0us | 0us | 1us | 10us | 444us | 2us | - | 0.8 | firstbatch 444us |
| fs_hw_snow2_left_chain | ON | 512us | 6us | 464us | 43us | 3us | 0us | 2us | 0us | 0us | 1us | 9us | 444us | 2us | - | 0.8 | firstbatch 444us |
| fs_hw_snow2_left_single | OFF | 511us | 7us | 462us | 43us | 3us | 0us | 2us | 0us | 0us | 1us | 9us | 441us | 3us | - | 1.0 | firstbatch 441us |
| fs_hw_snow2_left_single | ON | 525us | 7us | 474us | 44us | 4us | 0us | 3us | 0us | 0us | 1us | 10us | 453us | 3us | - | 1.0 | firstbatch 453us |
| fs_hw_strkey_point | OFF | 108us | 4us | 75us | 29us | 2us | 0us | 1us | 0us | 0us | 0us | - | 74us | - | 0us | 0.0 | firstbatch 74us |
| fs_hw_strkey_point | ON | 112us | 4us | 79us | 30us | 2us | 0us | 1us | 0us | 0us | 0us | - | 77us | - | 0us | 0.0 | firstbatch 77us |
| fs_hw_strkey_batch100 | OFF | 381ms | 26us | 381ms | 164us | 14us | 2us | 5us | 2us | 2us | 10us | - | 381ms | - | 29us | 0.0 | firstbatch 381ms |

## D. MySQL server — where the time goes (1 thread)

ndb wait = Ndb_api_wait_nanos_count per request (time the mysqld connection waited for data nodes), corrected by the idle baseline (1.00 s/s of background NDB API waiting measured before the matrix); mysqld = client latency - ndb wait (parsing, optimizer, row processing, result transfer); batches / rows = scan batches and rows received from NDB per request; pushed = pushed (SPJ) queries executed per request. Counters include the warmup request and are approximate at short case durations.

| query | engine | compiler | client | ndb wait | mysqld | batches/req | rows/req | pushed/req | KB recv/req |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | mysqld | OFF | 95us | 65us | 30us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_floor | mysqld | ON | 99us | 69us | 29us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_floor | mysqld_nopush | OFF | 100us | 68us | 31us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_floor | mysqld_nopush | ON | 96us | 67us | 29us | 4.0 | 4 | 0.00 | 0.5 |
| fs_hw_agg_point | mysqld_nopush | OFF | 111us | 69us | 42us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_point | mysqld_nopush | ON | 111us | 69us | 42us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_window7d | mysqld | OFF | 126us | 80us | 47us | 4.0 | 5 | 0.00 | 0.6 |
| fs_hw_agg_window7d | mysqld | ON | 110us | 68us | 42us | 4.0 | 5 | 0.00 | 0.6 |
| fs_hw_agg_window7d | mysqld_nopush | OFF | 112us | 66us | 46us | 2.4 | 13 | 0.00 | 0.7 |
| fs_hw_agg_window7d | mysqld_nopush | ON | 114us | 69us | 45us | 2.4 | 13 | 0.00 | 0.7 |
| fs_hw_agg_greatest | mysqld | OFF | 110us | 67us | 43us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_greatest | mysqld | ON | 113us | 70us | 43us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_greatest | mysqld_nopush | OFF | 111us | 67us | 44us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_greatest | mysqld_nopush | ON | 108us | 68us | 40us | 3.1 | 37 | 0.00 | 1.5 |
| fs_hw_agg_filter | mysqld_nopush | OFF | 116us | 70us | 46us | 2.9 | 29 | 0.00 | 1.5 |
| fs_hw_agg_filter | mysqld_nopush | ON | 125us | 77us | 49us | 2.9 | 29 | 0.00 | 1.5 |
| fs_hw_agg_batch10 | mysqld | OFF | 988us | 833us | 156us | 31.3 | 32 | 0.00 | 3.8 |
| fs_hw_agg_batch10 | mysqld | ON | 992us | 839us | 154us | 31.3 | 32 | 0.00 | 3.8 |
| fs_hw_agg_batch10 | mysqld_nopush | OFF | 251us | 141us | 110us | 4.0 | 364 | 0.00 | 17.2 |
| fs_hw_agg_batch10 | mysqld_nopush | ON | 241us | 137us | 104us | 4.0 | 364 | 0.00 | 17.2 |
| fs_hw_agg_batch100 | mysqld | OFF | 10.09ms | 8.67ms | 1.42ms | 312.9 | 324 | 0.00 | 38.1 |
| fs_hw_agg_batch100 | mysqld | ON | 10.33ms | 8.82ms | 1.51ms | 313.1 | 323 | 0.00 | 38.1 |
| fs_hw_agg_batch100 | mysqld_nopush | OFF | 1.81ms | 1.13ms | 677us | 5.2 | 3607 | 0.00 | 169.2 |
| fs_hw_agg_batch100 | mysqld_nopush | ON | 1.86ms | 1.17ms | 685us | 5.2 | 3612 | 0.00 | 169.4 |
| fs_hw_agg_batch1000 | mysqld | OFF | 169ms | 120ms | 48.64ms | 3132.0 | 3264 | 0.00 | 382.1 |
| fs_hw_agg_batch1000 | mysqld | ON | 170ms | 122ms | 47.45ms | 3132.0 | 3284 | 0.00 | 382.7 |
| fs_hw_agg_batch1000 | mysqld_nopush | OFF | 42.32ms | 38.24ms | 4.08ms | 56.2 | 36272 | 0.00 | 1701.1 |
| fs_hw_agg_batch1000 | mysqld_nopush | ON | 42.99ms | 38.89ms | 4.10ms | 56.2 | 36272 | 0.00 | 1701.1 |
| fs_hw_agg_batch100_window | mysqld | OFF | 10.11ms | 8.64ms | 1.47ms | 293.8 | 304 | 0.00 | 36.5 |
| fs_hw_agg_batch100_window | mysqld | ON | 10.10ms | 8.60ms | 1.50ms | 293.9 | 304 | 0.00 | 36.5 |
| fs_hw_agg_batch100_window | mysqld_nopush | OFF | 1.22ms | 697us | 523us | 8.0 | 1933 | 0.00 | 90.8 |
| fs_hw_agg_batch100_window | mysqld_nopush | ON | 1.28ms | 755us | 525us | 8.0 | 1933 | 0.00 | 90.8 |
| fs_hw_collect5 | mysqld | OFF | 113us | 72us | 41us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5 | mysqld | ON | 109us | 69us | 40us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5 | mysqld_nopush | OFF | 110us | 69us | 40us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5 | mysqld_nopush | ON | 110us | 71us | 39us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld | OFF | 121us | 71us | 50us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld | ON | 125us | 75us | 50us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld_nopush | OFF | 119us | 72us | 47us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect5_cte | mysqld_nopush | ON | 116us | 69us | 47us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld | OFF | 118us | 70us | 47us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld | ON | 117us | 71us | 46us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld_nopush | OFF | 121us | 74us | 47us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_collect50 | mysqld_nopush | ON | 116us | 71us | 45us | 3.1 | 37 | 0.00 | 1.9 |
| fs_hw_snow1_point | mysqld | OFF | 120us | 71us | 49us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_point | mysqld | ON | 120us | 70us | 50us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_point | mysqld_nopush | OFF | 119us | 71us | 48us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_point | mysqld_nopush | ON | 119us | 71us | 48us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld | OFF | 172us | 110us | 62us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld | ON | 168us | 110us | 59us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld_nopush | OFF | 166us | 107us | 58us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_point | mysqld_nopush | ON | 171us | 111us | 60us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow1_batch100 | mysqld | OFF | 8.17ms | 7.30ms | 865us | 0.0 | 196 | 0.00 | 13.2 |
| fs_hw_snow1_batch100 | mysqld | ON | 8.14ms | 7.28ms | 862us | 0.0 | 196 | 0.00 | 13.2 |
| fs_hw_snow1_batch100 | mysqld_nopush | OFF | 4.45ms | 3.92ms | 527us | 0.0 | 193 | 0.00 | 13.1 |
| fs_hw_snow1_batch100 | mysqld_nopush | ON | 4.40ms | 3.89ms | 512us | 0.0 | 192 | 0.00 | 13.1 |
| fs_hw_snow2_left_chain | mysqld | OFF | 164us | 107us | 57us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_chain | mysqld | ON | 162us | 106us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_chain | mysqld_nopush | OFF | 164us | 107us | 57us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_chain | mysqld_nopush | ON | 165us | 109us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld | OFF | 164us | 108us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld | ON | 165us | 109us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld_nopush | OFF | 163us | 107us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_snow2_left_single | mysqld_nopush | ON | 163us | 107us | 56us | 0.0 | 3 | 0.00 | 0.2 |
| fs_hw_strkey_point | mysqld_nopush | OFF | 119us | 74us | 45us | 3.1 | 37 | 0.00 | 1.8 |
| fs_hw_strkey_point | mysqld_nopush | ON | 121us | 74us | 47us | 3.1 | 37 | 0.00 | 1.8 |

## E. ndbinfo.jit deltas per case, compiler ON, 1 thread (per request incl. warmup)

| query | engine | compiled/req | reused/req | fallback | rows executed/req | compile us/req | compile share |
|---|---|---:|---:|---:|---:|---:|---:|
| fs_hw_floor | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_floor | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_floor | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_point | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_window7d | ronsql | 0.00 | 4.00 | 0 | 12 | 0.0 | 0.0% |
| fs_hw_agg_window7d | mysqld | 0.00 | 4.00 | 0 | 12 | 0.0 | 0.0% |
| fs_hw_agg_window7d | mysqld_nopush | 0.99 | 3.01 | 0 | 12 | 1.3 | 1.2% |
| fs_hw_agg_greatest | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_greatest | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_greatest | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_agg_filter | ronsql | 0.00 | 4.00 | 0 | 9 | 0.0 | 0.0% |
| fs_hw_agg_filter | mysqld_nopush | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | ronsql | 0.00 | 4.00 | 0 | 359 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | mysqld | 0.00 | 40.00 | 0 | 366 | 0.0 | 0.0% |
| fs_hw_agg_batch10 | mysqld_nopush | 1.00 | 3.00 | 0 | 363 | 2.0 | 0.8% |
| fs_hw_agg_batch100 | ronsql | 0.00 | 4.00 | 0 | 3567 | 0.0 | 0.0% |
| fs_hw_agg_batch100 | mysqld | 0.00 | 400.00 | 0 | 3666 | 0.0 | 0.0% |
| fs_hw_agg_batch100 | mysqld_nopush | 1.00 | 3.00 | 0 | 3610 | 13.0 | 0.7% |
| fs_hw_agg_batch1000 | ronsql | 0.00 | 4.00 | 0 | 35999 | 0.0 | 0.0% |
| fs_hw_agg_batch1000 | mysqld | 0.00 | 4000.00 | 0 | 36494 | 0.0 | 0.0% |
| fs_hw_agg_batch1000 | mysqld_nopush | 0.00 | 0.00 | 3744 | 0 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | ronsql | 0.00 | 4.00 | 0 | 1998 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | mysqld | 0.00 | 400.00 | 0 | 1986 | 0.0 | 0.0% |
| fs_hw_agg_batch100_window | mysqld_nopush | 1.00 | 7.00 | 0 | 1932 | 12.1 | 0.9% |
| fs_hw_collect5 | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5 | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5_cte | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect5_cte | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | ronsql | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_collect50 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_point | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow1_point | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_point | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_point | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_batch100 | ronsql | 1.97 | 4.03 | 0 | 100100 | 20.0 | 0.1% |
| fs_hw_snow1_batch100 | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow1_batch100 | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_chain | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | ronsql | 0.00 | 6.00 | 0 | 2 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | mysqld | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_snow2_left_single | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |
| fs_hw_strkey_point | ronsql | 0.00 | 4.00 | 0 | 36 | 0.0 | 0.0% |
| fs_hw_strkey_point | mysqld_nopush | 0.00 | 0.00 | 0 | 0 | 0.0 | 0.0% |

## F. Throughput scaling (q/s) across thread counts

| query | engine | compiler | T=1 | T=8 | T8/T1 |
|---|---|---|---:|---:|---:|
| fs_hw_floor | ronsql | OFF | 9691 | - | - |
| fs_hw_floor | ronsql | ON | 9661 | - | - |
| fs_hw_floor | mysqld | OFF | 10506 | - | - |
| fs_hw_floor | mysqld | ON | 10083 | - | - |
| fs_hw_floor | mysqld_nopush | OFF | 10009 | - | - |
| fs_hw_floor | mysqld_nopush | ON | 10436 | - | - |
| fs_hw_agg_point | ronsql | OFF | 9133 | - | - |
| fs_hw_agg_point | ronsql | ON | 8754 | - | - |
| fs_hw_agg_point | mysqld_nopush | OFF | 8973 | - | - |
| fs_hw_agg_point | mysqld_nopush | ON | 8989 | - | - |
| fs_hw_agg_window7d | ronsql | OFF | 7969 | - | - |
| fs_hw_agg_window7d | ronsql | ON | 8583 | - | - |
| fs_hw_agg_window7d | mysqld | OFF | 7856 | - | - |
| fs_hw_agg_window7d | mysqld | ON | 8979 | - | - |
| fs_hw_agg_window7d | mysqld_nopush | OFF | 8861 | - | - |
| fs_hw_agg_window7d | mysqld_nopush | ON | 8697 | - | - |
| fs_hw_agg_greatest | ronsql | OFF | 9094 | - | - |
| fs_hw_agg_greatest | ronsql | ON | 8800 | - | - |
| fs_hw_agg_greatest | mysqld | OFF | 9043 | - | - |
| fs_hw_agg_greatest | mysqld | ON | 8789 | - | - |
| fs_hw_agg_greatest | mysqld_nopush | OFF | 8991 | - | - |
| fs_hw_agg_greatest | mysqld_nopush | ON | 9203 | - | - |
| fs_hw_agg_filter | ronsql | OFF | 9213 | - | - |
| fs_hw_agg_filter | ronsql | ON | 8629 | - | - |
| fs_hw_agg_filter | mysqld_nopush | OFF | 8548 | - | - |
| fs_hw_agg_filter | mysqld_nopush | ON | 7923 | - | - |
| fs_hw_agg_batch10 | ronsql | OFF | 5 | - | - |
| fs_hw_agg_batch10 | ronsql | ON | 5 | - | - |
| fs_hw_agg_batch10 | mysqld | OFF | 1010 | - | - |
| fs_hw_agg_batch10 | mysqld | ON | 1006 | - | - |
| fs_hw_agg_batch10 | mysqld_nopush | OFF | 3954 | - | - |
| fs_hw_agg_batch10 | mysqld_nopush | ON | 4123 | - | - |
| fs_hw_agg_batch100 | ronsql | OFF | 2 | - | - |
| fs_hw_agg_batch100 | ronsql | ON | 2 | - | - |
| fs_hw_agg_batch100 | mysqld | OFF | 99 | - | - |
| fs_hw_agg_batch100 | mysqld | ON | 97 | - | - |
| fs_hw_agg_batch100 | mysqld_nopush | OFF | 551 | - | - |
| fs_hw_agg_batch100 | mysqld_nopush | ON | 536 | - | - |
| fs_hw_agg_batch1000 | ronsql | OFF | 0 | - | - |
| fs_hw_agg_batch1000 | ronsql | ON | 0 | - | - |
| fs_hw_agg_batch1000 | mysqld | OFF | 6 | - | - |
| fs_hw_agg_batch1000 | mysqld | ON | 6 | - | - |
| fs_hw_agg_batch1000 | mysqld_nopush | OFF | 24 | - | - |
| fs_hw_agg_batch1000 | mysqld_nopush | ON | 23 | - | - |
| fs_hw_agg_batch100_window | ronsql | OFF | 2 | - | - |
| fs_hw_agg_batch100_window | ronsql | ON | 2 | - | - |
| fs_hw_agg_batch100_window | mysqld | OFF | 99 | - | - |
| fs_hw_agg_batch100_window | mysqld | ON | 99 | - | - |
| fs_hw_agg_batch100_window | mysqld_nopush | OFF | 815 | - | - |
| fs_hw_agg_batch100_window | mysqld_nopush | ON | 774 | - | - |
| fs_hw_collect5 | ronsql | OFF | 9132 | - | - |
| fs_hw_collect5 | ronsql | ON | 9227 | - | - |
| fs_hw_collect5 | mysqld | OFF | 8795 | - | - |
| fs_hw_collect5 | mysqld | ON | 9104 | - | - |
| fs_hw_collect5 | mysqld_nopush | OFF | 9059 | - | - |
| fs_hw_collect5 | mysqld_nopush | ON | 9060 | - | - |
| fs_hw_collect5_cte | mysqld | OFF | 8231 | - | - |
| fs_hw_collect5_cte | mysqld | ON | 7974 | - | - |
| fs_hw_collect5_cte | mysqld_nopush | OFF | 8380 | - | - |
| fs_hw_collect5_cte | mysqld_nopush | ON | 8594 | - | - |
| fs_hw_collect50 | ronsql | OFF | 8227 | - | - |
| fs_hw_collect50 | ronsql | ON | 8496 | - | - |
| fs_hw_collect50 | mysqld | OFF | 8450 | - | - |
| fs_hw_collect50 | mysqld | ON | 8511 | - | - |
| fs_hw_collect50 | mysqld_nopush | OFF | 8211 | - | - |
| fs_hw_collect50 | mysqld_nopush | ON | 8564 | - | - |
| fs_hw_snow1_point | ronsql | OFF | 2046 | - | - |
| fs_hw_snow1_point | ronsql | ON | 2044 | - | - |
| fs_hw_snow1_point | mysqld | OFF | 8291 | - | - |
| fs_hw_snow1_point | mysqld | ON | 8266 | - | - |
| fs_hw_snow1_point | mysqld_nopush | OFF | 8349 | - | - |
| fs_hw_snow1_point | mysqld_nopush | ON | 8349 | - | - |
| fs_hw_snow2_point | ronsql | OFF | 1933 | - | - |
| fs_hw_snow2_point | ronsql | ON | 1918 | - | - |
| fs_hw_snow2_point | mysqld | OFF | 5777 | - | - |
| fs_hw_snow2_point | mysqld | ON | 5915 | - | - |
| fs_hw_snow2_point | mysqld_nopush | OFF | 6014 | - | - |
| fs_hw_snow2_point | mysqld_nopush | ON | 5806 | - | - |
| fs_hw_snow1_batch100 | ronsql | OFF | 51 | - | - |
| fs_hw_snow1_batch100 | ronsql | ON | 45 | - | - |
| fs_hw_snow1_batch100 | mysqld | OFF | 122 | - | - |
| fs_hw_snow1_batch100 | mysqld | ON | 123 | - | - |
| fs_hw_snow1_batch100 | mysqld_nopush | OFF | 225 | - | - |
| fs_hw_snow1_batch100 | mysqld_nopush | ON | 227 | - | - |
| fs_hw_snow2_left_chain | ronsql | OFF | 1927 | - | - |
| fs_hw_snow2_left_chain | ronsql | ON | 1931 | - | - |
| fs_hw_snow2_left_chain | mysqld | OFF | 6072 | - | - |
| fs_hw_snow2_left_chain | mysqld | ON | 6149 | - | - |
| fs_hw_snow2_left_chain | mysqld_nopush | OFF | 6070 | - | - |
| fs_hw_snow2_left_chain | mysqld_nopush | ON | 6035 | - | - |
| fs_hw_snow2_left_single | ronsql | OFF | 1935 | - | - |
| fs_hw_snow2_left_single | ronsql | ON | 1882 | - | - |
| fs_hw_snow2_left_single | mysqld | OFF | 6066 | - | - |
| fs_hw_snow2_left_single | mysqld | ON | 6027 | - | - |
| fs_hw_snow2_left_single | mysqld_nopush | OFF | 6097 | - | - |
| fs_hw_snow2_left_single | mysqld_nopush | ON | 6090 | - | - |
| fs_hw_strkey_point | ronsql | OFF | 8822 | - | - |
| fs_hw_strkey_point | ronsql | ON | 8511 | - | - |
| fs_hw_strkey_point | mysqld_nopush | OFF | 8353 | - | - |
| fs_hw_strkey_point | mysqld_nopush | ON | 8211 | - | - |
| fs_hw_strkey_batch100 | ronsql | OFF | 3 | - | - |
