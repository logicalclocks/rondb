# ==== Purpose ====
#
# Run one churn round for ttl_nr_copy_churn:
#   1. CALL test.churn_run(15) on con_churn -- the SAFE channels (live-row
#      ZWRITE, fresh insert, trailing delete) that essentially never hit
#      handler-bypassing fatal errors, so the burst always makes progress
#      (@churn_i advances) through any restart window;
#   2. six DIRECT statements driving the fatal-prone rowid-less channels:
#      INSERT-over-expired (D1/ZINSERT_TTL) and ttl-ignored DELETE
#      (purge shape) on the high-PK pool H plus an already-copied-region
#      lap on pool A, with the matching re-expiring/re-creating REPLACEs
#      (D2/ZWRITE).
#
# Why the split (measured, not theoretical): during an active copy-fragment
# an operation on an expired row hits the known 626-on-expired transient
# (DbtupExecQuery.cpp original_op_type gate); NDB 626 maps to
# HA_ERR_KEY_NOT_FOUND, which MySQL marks FATAL (handler::is_fatal_error,
# sql/handler.cc) and raises as an uncatchable ER_KEY_NOT_FOUND (1032)
# "Can't find record" -- stored-program condition handlers CANNOT swallow
# it, so keeping these statements inside the procedure kills the whole CALL
# at its first statement for the entire copy window.  As top-level
# mysqltest statements each one gets its own --error tolerance instead, so
# every channel keeps firing at the copy window on every round.
#
# Error policy: expected-transient failures are counted, never fatal to the
# test.  Every failed statement's warning stack is scanned for the 899
# signature ("Rowid already allocated"); benign transient 899s are possible
# on a healthy cluster around abort/TC-takeover windows, so signatures are
# only tallied ($_sig899_fatal, plus forensics lines in the counters file).
# The cycle end delivers the persistence verdict (strict sweep with retries
# + the replica-consistency invariant include).
#
# Uses (caller-scoped): connection con_churn, procedure test.churn_run,
# table test.churn_t, round counter $_r (incremented here), counters
# $_call_fatal, $_direct_err, $_sig899_fatal (incremented here).
# Returns on connection default with query/result/warnings logs enabled.

--inc $_r
connection con_churn;
--disable_query_log
--disable_result_log
--disable_warnings

# --- 1. safe-channel burst (progress guaranteed) -------------------------
--error 0,1030,1032,1180,1296
CALL test.churn_run(15);
if ($mysql_errno)
{
  --inc $_call_fatal
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 call round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}

# --- 2. fatal-prone rowid-less channels, individually tolerated ----------
# H pool (copied last): INSERT over expired PK -> dup-convert ZINSERT_TTL
--error 0,1030,1032,1062,1180,1205,1296,1297
eval INSERT INTO test.churn_t VALUES (100001 + ($_r MOD 800), NOW(), 11);
if ($mysql_errno)
{
  --let $_e = $mysql_errno
  --inc $_direct_err
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 direct H-insert errno=$_e round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}
# H pool: ZWRITE re-expiring the same PK (keeps the pool expired)
--error 0,1030,1032,1062,1180,1205,1296,1297
eval REPLACE INTO test.churn_t VALUES (100001 + ($_r MOD 800), NOW() - INTERVAL 1 HOUR, 12);
if ($mysql_errno)
{
  --let $_e = $mysql_errno
  --inc $_direct_err
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 direct H-replace errno=$_e round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}
# H pool: ttl-ignored DELETE of an expired PK (purge shape)
--error 0,1030,1032,1062,1180,1205,1296,1297
eval DELETE FROM test.churn_t WHERE id = 100001 + (($_r + 400) MOD 800);
if ($mysql_errno)
{
  --let $_e = $mysql_errno
  --inc $_direct_err
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 direct H-delete errno=$_e round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}
# H pool: ZWRITE re-creating the deleted PK as expired (write->insert)
--error 0,1030,1032,1062,1180,1205,1296,1297
eval REPLACE INTO test.churn_t VALUES (100001 + (($_r + 400) MOD 800), NOW() - INTERVAL 1 HOUR, 13);
if ($mysql_errno)
{
  --let $_e = $mysql_errno
  --inc $_direct_err
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 direct H-recreate errno=$_e round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}
# A pool (already-copied region): INSERT over expired + re-expire
--error 0,1030,1032,1062,1180,1205,1296,1297
eval INSERT INTO test.churn_t VALUES (1 + ($_r MOD 800), NOW(), 14);
if ($mysql_errno)
{
  --let $_e = $mysql_errno
  --inc $_direct_err
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 direct A-insert errno=$_e round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}
--error 0,1030,1032,1062,1180,1205,1296,1297
eval REPLACE INTO test.churn_t VALUES (1 + ($_r MOD 800), NOW() - INTERVAL 1 HOUR, 15);
if ($mysql_errno)
{
  --let $_e = $mysql_errno
  --inc $_direct_err
  --let $_bw1 = query_get_value(SHOW WARNINGS, Message, 1)
  --let $_bw2 = query_get_value(SHOW WARNINGS, Message, 2)
  --let $_bw3 = query_get_value(SHOW WARNINGS, Message, 3)
  if (`SELECT "$_bw1 $_bw2 $_bw3" LIKE "%Rowid already allocated%"`)
  {
    --inc $_sig899_fatal
    --exec echo "sig899 direct A-replace errno=$_e round=$_r: $_bw1 | $_bw2 | $_bw3" >> $MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters
  }
}

--enable_query_log
--enable_result_log
--enable_warnings
connection default;
