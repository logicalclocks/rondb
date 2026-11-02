# ndb_spj_shapes_init.inc -- start a test that uses ndb_spj_shapes_check.inc.
#
# Resets the failure counter and list, sets the defaults of the
# ndb_spj_shapes_check.inc inputs, and pins the session settings the
# checks rely on.  Source it once, after include/have_ndb.inc.

--disable_query_log
--let $spj_nfail= 0
--let $spj_fail_list=
--let $spj_ordered= 0
--let $spj_agg= 0
--let $spj_min_pushed= 2
--let $spj_expect_agg= 1
SET SESSION ndb_join_pushdown = ON;
SET SESSION ndb_join_pushdown_aggregate = OFF;
SET SESSION ndb_join_pushdown_aggregate_outer_join = OFF;
SET SESSION ndb_pushdown_aggregate = OFF;
SET SESSION cte_max_recursion_depth = 100000;
--enable_query_log
