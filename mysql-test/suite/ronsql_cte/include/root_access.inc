# root_access.inc (RONDB-1124 J-9, T-13, J-22) — EXPLAIN $QUERY and print
# what its join plan says about how the query runs:
#   * the "Root access:" line of each join plan: the NDB API operation the
#     root runs as (readTuple, scanIndex, scanTable, lookupCte, scanCte),
#     its filter, and why a lookup was not taken where the WHERE binds the
#     whole key;
#   * each anti-join operation ([ANTI]);
#   * each "Aggregation leaf" mark (an aggregating query only).
# EXPLAIN and execution take the root access from one decision
# (decide_root_access), so the printed line is what the query runs.
#
# $QUERY is kept for the ronsql_compare.inc that usually follows.
--let $ra_query=$QUERY
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/root_access_explain.txt
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -o -E 'Root access: .*|\[ANTI\] .*|Aggregation leaf' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $QUERY=$ra_query
