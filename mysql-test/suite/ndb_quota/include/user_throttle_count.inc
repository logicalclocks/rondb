# Runs $throttle_stmt $throttle_count times on the current connection and
# sets $throttled to the number of statements refused with the temporary
# error a rate limit gives SQL (1297). Any other error is echoed, so a
# statement failing for another reason cannot pass as throttled.
--disable_query_log
--disable_result_log
let $throttled = 0;
let $_throttle_left = $throttle_count;
while ($_throttle_left)
{
  --disable_abort_on_error
  eval $throttle_stmt;
  --enable_abort_on_error
  if ($mysql_errno == 1297)
  {
    inc $throttled;
  }
  if ($mysql_errno != 1297)
  {
    if ($mysql_errno)
    {
      --echo UNEXPECTED error $mysql_errno from: $throttle_stmt
    }
  }
  dec $_throttle_left;
}
--enable_result_log
--enable_query_log
