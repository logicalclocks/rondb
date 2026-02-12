# suite/ndb_ttl_purge/include/ttl_purge_cleanup_tmp.inc
#
# Clean up all temp files created by ttl_purge test infrastructure.
# Must be sourced at the end of every test that uses:
#   - ttl_purge_json_parse.inc (creates *.json, *.inc, *.txt)
#   - ttl_purge_wait_table_detected.inc (creates *.json, *.txt, *.inc)
#   - ttl_purge_wait_table_removed.inc (creates *.json, *.txt, *.inc)
#   - direct curl output to $MYSQL_TMP_DIR

--remove_files_wildcard $MYSQL_TMP_DIR *.json
--remove_files_wildcard $MYSQL_TMP_DIR *.inc
--remove_files_wildcard $MYSQL_TMP_DIR *.txt
