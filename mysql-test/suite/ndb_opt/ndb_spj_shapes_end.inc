# ndb_spj_shapes_end.inc -- finish a test that uses ndb_spj_shapes_check.inc.
#
# Restores the session defaults.  If any shape failed (wrong result, SQL
# error, or not pushed as required), echoes the failed shapes and dies;
# mysqltest shows the echoed line as the last result line before the
# failure ('--die' itself does not expand variables).

--disable_query_log
SET SESSION ndb_join_pushdown = DEFAULT;
SET SESSION ndb_join_pushdown_aggregate = DEFAULT;
SET SESSION ndb_join_pushdown_aggregate_outer_join = DEFAULT;
SET SESSION ndb_pushdown_aggregate = DEFAULT;
SET SESSION cte_max_recursion_depth = DEFAULT;
--enable_query_log
if ($spj_nfail)
{
  --echo # FAILED shapes ($spj_nfail):$spj_fail_list
  --die SPJ shape check failed, see the '# FAILED' lines of the result
}
