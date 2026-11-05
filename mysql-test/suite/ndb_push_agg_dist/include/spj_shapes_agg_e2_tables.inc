# spj_shapes_agg_e2_tables.inc -- the tables of the large-root shapes of
# the ndb_spj_shapes_agg tests (E-2, E-6c): a 60000-row root e2_t1, three
# index-scan children each in e2_t2, and the lookup leaf e2_t3.  Needs
# seq from spj_shapes_agg_tables.inc.  Drop them with
# DROP TABLE e2_t1, e2_t2, e2_t3.

SET SESSION ndb_use_transactions = 0;
CREATE TABLE e2_t1 (
  id INT NOT NULL PRIMARY KEY,
  a INT NOT NULL
) ENGINE=NDB;
INSERT INTO e2_t1
  SELECT (s1.n - 1) * 1000 + s2.n, (s1.n - 1) * 1000 + s2.n
  FROM seq s1, seq s2 WHERE s1.n <= 60 AND s2.n <= 1000;
CREATE TABLE e2_t2 (
  id INT NOT NULL PRIMARY KEY,
  a INT NOT NULL,
  c INT NOT NULL,
  KEY ix_a (a)
) ENGINE=NDB;
INSERT INTO e2_t2
  SELECT ((s1.n - 1) * 1000 + s2.n) * 3 + s3.n - 1,
         (s1.n - 1) * 1000 + s2.n,
         (((s1.n - 1) * 1000 + s2.n) * 7 + s3.n * 13) % 1000
  FROM seq s1, seq s2, seq s3
  WHERE s1.n <= 60 AND s2.n <= 1000 AND s3.n <= 3;
CREATE TABLE e2_t3 (
  c INT NOT NULL PRIMARY KEY,
  g INT NOT NULL
) ENGINE=NDB;
INSERT INTO e2_t3 SELECT n - 1, n % 17 FROM seq WHERE n <= 1000;
SET SESSION ndb_use_transactions = DEFAULT;

ANALYZE TABLE e2_t1, e2_t2, e2_t3;
SELECT COUNT(*) FROM e2_t2;
