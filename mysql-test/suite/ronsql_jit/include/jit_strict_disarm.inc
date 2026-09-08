# Disarm the slice-3 strict-compile inserts (see jit_strict_arm.inc).
--exec $NDB_MGM -e "all error 0" >> $MYSQL_TMP_DIR/jit_strict_mgm.log
--remove_file $MYSQL_TMP_DIR/jit_strict_mgm.log
