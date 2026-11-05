# ndb_spj_shapes_dmlcmp.inc -- compare the two tables written by the same
# DML statement, run once without ($dml_off) and once with ($dml_on)
# ndb_join_pushdown (used by ndb_spj_shapes_variants.test).
#
# Input:
#   $dml_label  shape name without spaces
#   $dml_off    table written with ndb_join_pushdown = OFF
#   $dml_on     table written with ndb_join_pushdown = ON
#   $dml_exec   1 if Ndb_pushed_queries_executed grew during the ON run
# Prints "# <label>: rows=<rows in $dml_off> exec=<e>"; if the tables
# differ as multisets, prints "#   FAILED: ..." and adds the label to
# $spj_fail_list (ndb_spj_shapes_end.inc dies naming it).

--disable_query_log
--let $dml_rows= `SELECT COUNT(*) FROM $dml_off`
--let $dml_diff= `SELECT (SELECT COUNT(*) FROM (SELECT * FROM $dml_off EXCEPT ALL SELECT * FROM $dml_on) AS x) + (SELECT COUNT(*) FROM (SELECT * FROM $dml_on EXCEPT ALL SELECT * FROM $dml_off) AS y)`
--echo # $dml_label: rows=$dml_rows exec=$dml_exec
if ($dml_diff)
{
  --echo #   FAILED: $dml_diff row(s) differ between the runs without and with pushdown
  --inc $spj_nfail
  --let $spj_fail_list= $spj_fail_list $dml_label
}
--enable_query_log
