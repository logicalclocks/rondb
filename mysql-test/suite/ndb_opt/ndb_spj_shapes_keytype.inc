# ndb_spj_shapes_keytype.inc -- pushed joins linked on a key of one
# data type (used by ndb_spj_shapes_keys.test).
#
# Input:
#   $kt_name  short name of the type, used in table names and labels
#   $kt_type  the SQL column type
#   $kt_expr  expression over seq.n giving a key value; it must be
#             injective for n % 50 in 0..49 (n % 50 is the domain)
#
# Parent pk_<name> has 300 rows, the key is NULL in every 23rd row.
# Child ck_<name> has the key values of n = 1..40 as PK k and, again, as
# the ordered-index column k2, so the parent values with n % 50 in
# {0, 41..49} have no match.  Three checks: PK lookup (LEFT JOIN),
# ordered-index scan (inner join), and ordered-index scan child of a PK
# lookup child (key passed down two levels).

--disable_query_log
eval CREATE TABLE pk_$kt_name (
  id INT NOT NULL PRIMARY KEY,
  k $kt_type NULL
) ENGINE=NDB;
eval CREATE TABLE ck_$kt_name (
  k $kt_type NOT NULL PRIMARY KEY,
  k2 $kt_type NOT NULL,
  val INT NOT NULL,
  KEY ix_k2 (k2)
) ENGINE=NDB;
eval INSERT INTO pk_$kt_name
  SELECT n, IF(n % 23 = 0, NULL, $kt_expr) FROM seq WHERE n <= 300;
eval INSERT INTO ck_$kt_name
  SELECT $kt_expr, $kt_expr, n FROM seq WHERE n <= 40;
--disable_result_log
eval ANALYZE TABLE pk_$kt_name, ck_$kt_name;
--enable_result_log
--enable_query_log

let $spj_label= K-$kt_name-lookup-left;
let $spj_query= SELECT pk_$kt_name.id, ck_$kt_name.val
  FROM pk_$kt_name LEFT JOIN ck_$kt_name ON ck_$kt_name.k = pk_$kt_name.k;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

let $spj_label= K-$kt_name-scan-inner;
let $spj_query= SELECT pk_$kt_name.id, ck_$kt_name.val
  FROM pk_$kt_name STRAIGHT_JOIN ck_$kt_name FORCE INDEX (ix_k2)
    ON ck_$kt_name.k2 = pk_$kt_name.k;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

let $spj_label= K-$kt_name-two-levels;
let $spj_query= SELECT pk_$kt_name.id, c1.val, c2.val
  FROM pk_$kt_name
    STRAIGHT_JOIN ck_$kt_name AS c1 ON c1.k = pk_$kt_name.k
    STRAIGHT_JOIN ck_$kt_name AS c2 FORCE INDEX (ix_k2) ON c2.k2 = c1.k;
--source suite/ndb_opt/ndb_spj_shapes_check.inc

--disable_query_log
eval DROP TABLE pk_$kt_name, ck_$kt_name;
--enable_query_log
