# ronsql_jit slice 3 — strict JIT compile arming. ERROR_INSERT 4064
# (DBTUP: non-join aggregation programs must compile) + 5120 (DBLQH
# proxy: every join-agg leaf must compile). Program-LEVEL only — the
# per-row fallbacks (NULL loads, negative SUB replays, GL null rows)
# stay legal, so whole corpus tests can arm this. Cmvmi routes
# `all error N` by range (4xxx DBTUP, 5xxx DBLQH), hence two commands;
# `all error 0` broadcasts the clear to every block.
--exec $NDB_MGM -e "all error 4064" >> $MYSQL_TMP_DIR/jit_strict_mgm.log
--exec $NDB_MGM -e "all error 5120" >> $MYSQL_TMP_DIR/jit_strict_mgm.log
