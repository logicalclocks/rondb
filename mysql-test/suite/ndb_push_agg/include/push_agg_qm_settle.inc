# push_agg_qm_settle.inc -- wait until QUERY_MEMORY in use, summed over the
# data nodes, reads the same 6 times in a row 0.5 s apart, and return it in
# $pa_qm (32 KB pages). Dies if it has not settled after 60 s.

--disable_query_log
--let $pa_qm_sql = SELECT CAST(SUM(used) AS SIGNED) FROM ndbinfo.resources WHERE resource_name = 'QUERY_MEMORY'
--let $pa_qm = `$pa_qm_sql`
--let $pa_qm_stable = 0
--let $pa_qm_polls = 0
while ($pa_qm_stable < 6)
{
  --sleep 0.5
  --let $pa_qm_now = `$pa_qm_sql`
  if ($pa_qm_now == $pa_qm)
  {
    --inc $pa_qm_stable
  }
  if ($pa_qm_now != $pa_qm)
  {
    --let $pa_qm_stable = 0
    --let $pa_qm = $pa_qm_now
  }
  --inc $pa_qm_polls
  if ($pa_qm_polls >= 120)
  {
    --die QUERY_MEMORY did not settle within 60 seconds
  }
}
--enable_query_log
