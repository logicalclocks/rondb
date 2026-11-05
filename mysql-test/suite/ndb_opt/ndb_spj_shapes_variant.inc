# ndb_spj_shapes_variant.inc -- pushed joins on one table variant (used by
# ndb_spj_shapes_variants.test).
#
# Input:
#   $vt_name    short name, used in table names and labels
#   $vt_pk      primary key clause with a leading comma, e.g.
#               ", PRIMARY KEY (id)", or empty for a hidden primary key
#   $vt_opts    table options after ENGINE=NDB (COMMENT, PARTITION BY ...)
#   $vt_on      ON clause joining p to the variant cv_<name> by its full
#               primary key (empty: no PK lookup shape for this variant)
#   $vt_expire  1: half of the rows have a timestamp two hours in the past
#               (expired in a TTL=3600@ts table)
#   $vt_frags   optional: the number of fragments $vt_opts must give the
#               table. NDB only warns about a COMMENT it does not support
#               and uses the defaults, so the test stops if the count
#               differs. Reset at the end of this file.
#
# The variant table cv_<name> has the same 300 rows in every variant: ids
# 1..300 without multiples of 7, b = id % 40 (ordered index), v = id % 13,
# u = id + 1000 (nullable unique index, NULL for multiples of 5).
# Checks: PK lookup child, unique-index lookup child, ordered-index scan
# child, and the variant as the root with a lookup and a scan child.

--disable_query_log
eval CREATE TABLE cv_$vt_name (
  id INT NOT NULL,
  b INT NOT NULL,
  v INT NOT NULL,
  u INT NULL,
  ts DATETIME NOT NULL,
  KEY ix_b (b),
  UNIQUE KEY uk_u (u)
  $vt_pk
) ENGINE=NDB $vt_opts;
eval INSERT INTO cv_$vt_name
  SELECT n, n % 40, n % 13, IF(n % 5 = 0, NULL, n + 1000),
         IF($vt_expire AND n % 2 = 0, NOW() - INTERVAL 2 HOUR,
            NOW() + INTERVAL 1 DAY)
  FROM seq WHERE n <= 300 AND n % 7 <> 0;
--disable_result_log
eval ANALYZE TABLE cv_$vt_name;
--enable_result_log
if ($vt_frags)
{
  let $vt_frag_cnt= `SELECT COUNT(DISTINCT fragment_num)
                       FROM ndbinfo.operations_per_fragment
                      WHERE fq_name = 'test/def/cv_$vt_name'`;
  if (`SELECT $vt_frag_cnt <> $vt_frags`)
  {
    --echo cv_$vt_name has $vt_frag_cnt fragments, expected $vt_frags
    --die Test setup: the table options of a variant were not applied
  }
}
--enable_query_log

if ($vt_on)
{
  let $spj_label= V-$vt_name-pk-lookup-left;
  let $spj_query= SELECT p.id, cv_$vt_name.v
    FROM p LEFT JOIN cv_$vt_name ON $vt_on;
  --source suite/ndb_opt/ndb_spj_shapes_check.inc
}

let $spj_label= V-$vt_name-unique-lookup-left;
let $spj_query= SELECT p.id, cv_$vt_name.v
  FROM p LEFT JOIN cv_$vt_name ON cv_$vt_name.u = p.pu;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

let $spj_label= V-$vt_name-index-scan;
let $spj_query= SELECT p.id, cv_$vt_name.id
  FROM p STRAIGHT_JOIN cv_$vt_name FORCE INDEX (ix_b) ON cv_$vt_name.b = p.b;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

let $spj_label= V-$vt_name-root-lookup-child;
let $spj_query= SELECT cv_$vt_name.id, p.id
  FROM cv_$vt_name STRAIGHT_JOIN p ON p.id = cv_$vt_name.v;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

let $spj_label= V-$vt_name-root-scan-child-left;
let $spj_query= SELECT cv_$vt_name.id, p.id
  FROM cv_$vt_name LEFT JOIN p FORCE INDEX (ix_b) ON p.b = cv_$vt_name.b;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

--disable_query_log
eval DROP TABLE cv_$vt_name;
--enable_query_log
let $vt_frags= ;
