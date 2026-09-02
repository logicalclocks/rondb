# ==== Purpose ====
#
# Assert that every fragment of an NDB table has IDENTICAL physical row
# counts (fixed_elem_count and var_elem_count) on all data nodes hosting a
# replica of that fragment, using ndbinfo.memory_per_fragment.
#
# Rationale (TTL / error 899 investigation): backup replicas normally apply
# the same operations in the same per-row order as the primary, so the
# per-fragment element counts stay in lockstep.  A difference between the
# replicas of one fragment means the replica row-presence/rowid layouts have
# FORKED (the birth condition of recurring NDB error 899 "Rowid already
# allocated" on TTL tables, where rowid-less ZINSERT_TTL/ZWRITE forwarding
# silently absorbs the divergence instead of failing).  Expired-but-unpurged
# TTL rows are physically present and counted on all replicas, so the
# equality is EXACT -- no tolerance is applied.
#
# The check is retried for a while before failing: ndbinfo reads are live
# and a concurrently committing transaction (commits run backup-first) or a
# just-restarted node whose ndbinfo rows are not yet visible can make a
# single sample differ transiently.  A genuine fork is permanent and will
# still be there on the last retry.  Call this on a quiesced table for an
# immediate verdict.
#
# ==== Usage ====
#
# --let $ndb_db = test
# --let $ndb_table = t1
# [--let $ndb_expected_replicas = 2]
# [--let $ndb_check_children = 1]
# [--let $ndb_consistency_retries = 60]
# [--let $ndb_dump_on_fail = 1]
# --source include/ndb_assert_frag_replica_consistency.inc
#
# Parameters:
#
# $ndb_db, $ndb_table
#   Database and table name of the NDB table to check (mandatory).
#
# $ndb_expected_replicas
#   Number of data nodes that must report each fragment (default 2, i.e.
#   NoOfReplicas=2).  Fragments reported by fewer nodes fail the assert:
#   this guards against a silent pass when a node's ndbinfo rows are
#   missing (e.g. node not yet started), which would make the equality
#   comparison vacuous.
#
# $ndb_check_children
#   If set, also check the table's child objects (unique hash index
#   fragments and BLOB part tables), which must be replica-identical for
#   the same reason.  Ordered indexes are always EXCLUDED: TUX index
#   fragments are rebuilt locally on restart and their internal node
#   counts are not required to match across replicas.  Default: off
#   (main table only).
#
# $ndb_consistency_retries
#   Number of 0.5s-spaced retries before declaring failure (default 60,
#   i.e. up to ~30s).
#
# $ndb_dump_on_fail
#   If set, on failure additionally dump the table's per-row rowids via
#   ndb_select_all --rowid --gci to MYSQLTEST_VARDIR/tmp/ for offline
#   layout analysis.  Note ndb_select_all is a normal scan, so on a TTL
#   table the dump contains only non-expired rows.
#
# On success prints a single deterministic PASS line.  On failure prints
# the full per-node per-fragment counts and dies.
#
# This include is dependency-free (no other includes) so it can be used
# from any suite (ndb, ndb_ttl, ndb_ttl_purge, ...).

if (!$ndb_db)
{
  --die ERROR IN TEST: ndb_assert_frag_replica_consistency.inc requires $ndb_db
}
if (!$ndb_table)
{
  --die ERROR IN TEST: ndb_assert_frag_replica_consistency.inc requires $ndb_table
}
if (!$ndb_expected_replicas)
{
  --let $ndb_expected_replicas = 2
}
if (!$ndb_consistency_retries)
{
  --let $ndb_consistency_retries = 60
}

--disable_query_log
--disable_result_log

--let $_narc_fq = $ndb_db/def/$ndb_table

# Scope: the main table's fragments, optionally plus non-ordered-index
# children (unique hash indexes, BLOB part tables).
--let $_narc_scope = fq_name = '$_narc_fq'
if ($ndb_check_children)
{
  --let $_narc_scope = (fq_name = '$_narc_fq' OR (parent_fq_name = '$_narc_fq' AND type NOT LIKE '%Ordered index%'))
}

# The consistency predicate, evaluated as one boolean:
#  - no (fq_name, fragment) may have fewer/more than $ndb_expected_replicas
#    reporting nodes, or more than one distinct fixed/var element count
#    across its nodes, and
#  - at least one fragment of the main table must be visible at all
#    (guards against a vacuous pass while ndbinfo has no rows for it).
# SUM() over block_instance collapses the (single) LDM instance row per
# node and fragment.
--let $_narc_ok = `SELECT (SELECT COUNT(*) FROM (SELECT fq_name, fragment_num FROM (SELECT fq_name, node_id, fragment_num, SUM(fixed_elem_count) AS fec, SUM(var_elem_count) AS vec FROM ndbinfo.memory_per_fragment WHERE $_narc_scope GROUP BY fq_name, node_id, fragment_num) _narc_per_node GROUP BY fq_name, fragment_num HAVING COUNT(*) <> $ndb_expected_replicas OR COUNT(DISTINCT fec) <> 1 OR COUNT(DISTINCT vec) <> 1) _narc_bad) = 0 AND (SELECT COUNT(*) FROM ndbinfo.memory_per_fragment WHERE fq_name = '$_narc_fq') > 0`

--let $_narc_tries = $ndb_consistency_retries
while (!$_narc_ok)
{
  --dec $_narc_tries
  if (!$_narc_tries)
  {
    --enable_query_log
    --enable_result_log
    --echo ######## ndb_assert_frag_replica_consistency FAILED for $ndb_db.$ndb_table ########
    --echo # Divergent (or incompletely reported) per-node per-fragment counts follow.
    --echo # Every fragment must be reported by exactly $ndb_expected_replicas nodes with identical
    --echo # fixed_elem_count and var_elem_count.  A count difference between the
    --echo # replicas of one fragment is a replica rowid-layout fork (the error 899
    --echo # seed condition).
    --vertical_results
    eval SELECT fq_name, node_id, fragment_num,
                SUM(fixed_elem_count) AS fixed_elem_count,
                SUM(var_elem_count) AS var_elem_count
         FROM ndbinfo.memory_per_fragment
         WHERE $_narc_scope
         GROUP BY fq_name, node_id, fragment_num
         ORDER BY fq_name, fragment_num, node_id;
    --horizontal_results
    if ($ndb_dump_on_fail)
    {
      --echo # Writing rowid dump (visible rows only on TTL tables) to
      --echo # MYSQLTEST_VARDIR/log/narc_$ndb_table.rowid.dump
      --error 0,1,2
      --exec $NDB_SELECT_ALL -d $ndb_db $ndb_table --rowid --gci > $MYSQLTEST_VARDIR/log/narc_$ndb_table.rowid.dump 2>&1
    }
    --die Replica fixed/var_elem_count divergence (or missing replica) detected -- possible rowid layout fork
  }
  --sleep 0.5
  --let $_narc_ok = `SELECT (SELECT COUNT(*) FROM (SELECT fq_name, fragment_num FROM (SELECT fq_name, node_id, fragment_num, SUM(fixed_elem_count) AS fec, SUM(var_elem_count) AS vec FROM ndbinfo.memory_per_fragment WHERE $_narc_scope GROUP BY fq_name, node_id, fragment_num) _narc_per_node GROUP BY fq_name, fragment_num HAVING COUNT(*) <> $ndb_expected_replicas OR COUNT(DISTINCT fec) <> 1 OR COUNT(DISTINCT vec) <> 1) _narc_bad) = 0 AND (SELECT COUNT(*) FROM ndbinfo.memory_per_fragment WHERE fq_name = '$_narc_fq') > 0`
}

--enable_query_log
--enable_result_log
--echo # ndb_assert_frag_replica_consistency: PASS for $ndb_db.$ndb_table (all fragments identical on $ndb_expected_replicas replicas)

# Reset optional parameters so later calls re-default cleanly.
--let $ndb_expected_replicas =
--let $ndb_check_children =
--let $ndb_consistency_retries =
--let $ndb_dump_on_fail =
--let $_narc_fq =
--let $_narc_scope =
--let $_narc_ok =
--let $_narc_tries =
