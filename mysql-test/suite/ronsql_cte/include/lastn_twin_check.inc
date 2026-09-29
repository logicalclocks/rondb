# lastn_twin_check.inc — WP-J J0 (ronsql_fs_support_plan.md): prove on
# MySQL that the grouped "one group per row" form of a last-N aggregate
# returns exactly what the natural statement returns.
#
# $LASTN_NATURAL_FILE holds the natural statement (a non-aggregating CTE
# body with ORDER BY + LIMIT, aggregated or joined by the main query),
# $LASTN_J0_FILE the J0 rewrite (every output column a GROUP BY key, the
# full primary key first, plus a dummy COUNT(*)).  Both run on MySQL; the
# sorted results must be identical (--diff_files is fatal).  The caller runs
# ronsql_compare.inc on the J0 file, which closes the chain
#   RonSQL(J0) = MySQL(J0) = MySQL(natural).
#
# Prints the natural statement and its MySQL result.  Removes the natural
# file; the J0 file is left for the caller (ronsql_compare.inc removes it
# as its QUERY_FILE).
--echo == Natural statement ==
--cat_file $LASTN_NATURAL_FILE
--exec $MYSQL --default-character-set=utf8mb4 --init-command-add='SET time_zone = "+00:00";' -D test < $LASTN_NATURAL_FILE > $MYSQL_TMP_DIR/lastn_nat.out
--exec $MYSQL --default-character-set=utf8mb4 --init-command-add='SET time_zone = "+00:00";' -D test < $LASTN_J0_FILE > $MYSQL_TMP_DIR/lastn_j0.out
--exec (head -n 1 $MYSQL_TMP_DIR/lastn_nat.out && tail -n +2 $MYSQL_TMP_DIR/lastn_nat.out | sort) > $MYSQL_TMP_DIR/lastn_nat.sorted
--exec (head -n 1 $MYSQL_TMP_DIR/lastn_j0.out && tail -n +2 $MYSQL_TMP_DIR/lastn_j0.out | sort) > $MYSQL_TMP_DIR/lastn_j0.sorted
--echo == Natural statement result (MySQL) ==
--cat_file $MYSQL_TMP_DIR/lastn_nat.sorted
--diff_files $MYSQL_TMP_DIR/lastn_nat.sorted $MYSQL_TMP_DIR/lastn_j0.sorted
--echo == J0 form: same result on MySQL ==
--remove_file $MYSQL_TMP_DIR/lastn_nat.out
--remove_file $MYSQL_TMP_DIR/lastn_j0.out
--remove_file $MYSQL_TMP_DIR/lastn_nat.sorted
--remove_file $MYSQL_TMP_DIR/lastn_j0.sorted
--remove_file $LASTN_NATURAL_FILE
