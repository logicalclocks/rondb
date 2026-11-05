# spj_shapes_agg_e3_tables.inc -- the tables of the E-3 shapes of the
# ndb_spj_shapes_agg tests: a 20000-row root r3 whose links into m2
# dangle for half of the rows, and m3.  Needs seq from
# spj_shapes_agg_tables.inc.  Drop them with DROP TABLE r3, m2, m3.

SET SESSION ndb_use_transactions = 0;
CREATE TABLE r3 (
  id INT NOT NULL PRIMARY KEY,
  a INT NOT NULL,
  g INT NOT NULL
) ENGINE=NDB;
INSERT INTO r3
  SELECT (s1.n - 1) * 1000 + s2.n, ((s1.n - 1) * 1000 + s2.n) % 4000 + 1, s2.n % 10
  FROM seq s1, seq s2 WHERE s1.n <= 20 AND s2.n <= 1000;
CREATE TABLE m2 (
  pk INT NOT NULL PRIMARY KEY,
  b INT NULL,
  v INT NOT NULL
) ENGINE=NDB;
INSERT INTO m2 SELECT n, IF(n % 10 = 1, NULL, (n * 3) % 3000 + 1), n % 9
  FROM seq WHERE n <= 4000 AND n % 2 = 1;
CREATE TABLE m3 (
  pk INT NOT NULL PRIMARY KEY,
  v INT NOT NULL
) ENGINE=NDB;
INSERT INTO m3 SELECT n, n % 5 FROM seq WHERE n <= 3000 AND n % 3 <> 0;
SET SESSION ndb_use_transactions = DEFAULT;

ANALYZE TABLE r3, m2, m3;
