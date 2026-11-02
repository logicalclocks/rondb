# spj_shapes_agg_tables.inc -- the tables most shapes of the
# ndb_spj_shapes_agg tests use: seq (1..5000), the 2000-row root d, its
# lookup children l1, l2, l3, the fact table f (scan child of d) and reg.
# Drop them with DROP TABLE seq, d, l1, l2, l3, f, reg.

CREATE TABLE seq (n INT NOT NULL PRIMARY KEY) ENGINE=NDB;
INSERT INTO seq
  WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5000)
  SELECT n FROM c;

# Root / dimension table, 2000 rows.  The name columns hold five variants
# of the logical group n % 30: 'grav007', 'GRAV007', 'gráv007', 'GRÁV007'
# and 'grav007  '.  Under utf8mb4_0900_ai_ci (NO PAD) the first four are
# equal; under the PAD SPACE collations all five are.  The accented
# variant is one byte longer and needs one more word.
CREATE TABLE d (
  id INT NOT NULL PRIMARY KEY,
  g INT NOT NULL,
  gn INT NULL,
  a INT NULL,
  b INT NULL,
  k INT NOT NULL,
  x INT NULL,
  nm VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  nm_gen VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  nm_sw VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  dbl DOUBLE NOT NULL,
  KEY ix_g (g),
  KEY ix_k (k),
  KEY ix_x (x)
) ENGINE=NDB;
INSERT INTO d
  SELECT n, n % 20, IF(n % 7 = 0, NULL, n % 5),
         IF(n % 9 = 0, NULL, (n * 13) % 1000 + 1),
         IF(n % 11 = 0, NULL, (n * 17) % 900 + 1),
         (n * 7919) % 10007,
         IF(n % 6 = 0, NULL, n % 25),
         w, w, w,
         IF(n % 3 = 0, -0e0, IF(n % 3 = 1, 0e0, 1.5e0))
  FROM (SELECT n, ELT(1 + n % 5,
                      CONCAT('grav', LPAD(n % 30, 3, '0')),
                      CONCAT('GRAV', LPAD(n % 30, 3, '0')),
                      CONCAT('gr', _utf8mb4 x'C3A1', 'v', LPAD(n % 30, 3, '0')),
                      CONCAT('GR', _utf8mb4 x'C381', 'V', LPAD(n % 30, 3, '0')),
                      CONCAT('grav', LPAD(n % 30, 3, '0'), '  ')) AS w
        FROM seq WHERE n <= 2000) AS dt;

# Lookup child of d.a; every third pk is missing.  s holds strings whose
# order under utf8mb4_0900_ai_ci differs from their byte order.
CREATE TABLE l1 (
  pk INT NOT NULL PRIMARY KEY,
  v INT NOT NULL,
  c INT NOT NULL,
  s VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  b INT NULL
) ENGINE=NDB;
INSERT INTO l1
  SELECT n, n % 10, n % 100,
         ELT(1 + n % 6, 'Bob', 'alice', 'Carl',
             CONCAT(_utf8mb4 x'C3A9', 'clair'), 'dough', 'Fudge'),
         IF(n % 8 = 0, NULL, (n * 11) % 900 + 1)
  FROM seq WHERE n <= 1000 AND n % 3 <> 0;

CREATE TABLE l2 (
  pk INT NOT NULL PRIMARY KEY,
  v INT NOT NULL,
  c INT NOT NULL
) ENGINE=NDB;
INSERT INTO l2 SELECT n, n % 13, (n * 5) % 500 + 1 FROM seq
  WHERE n <= 900 AND n % 4 <> 0;

CREATE TABLE l3 (
  pk INT NOT NULL PRIMARY KEY,
  v INT NOT NULL
) ENGINE=NDB;
INSERT INTO l3 SELECT n, n % 7 FROM seq WHERE n <= 500 AND n % 5 <> 0;

# Fact table, scan child of d on did: d.id % 6 rows per d row (0..5).
# price and disc are dyadic so that DOUBLE and DECIMAL arithmetic agree.
# big / ubig are near 2^62 / 2^63 for the rows of group d.g = 3 / 4.
CREATE TABLE f (
  id INT NOT NULL PRIMARY KEY,
  did INT NOT NULL,
  qty INT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  disc DECIMAL(4,2) NOT NULL,
  r DOUBLE NOT NULL,
  big BIGINT NOT NULL,
  ubig BIGINT UNSIGNED NOT NULL,
  dec38 DECIMAL(38,6) NOT NULL,
  y INT NULL,
  s VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  fdbl DOUBLE NOT NULL,
  KEY ix_did (did),
  KEY ix_y (y)
) ENGINE=NDB;
INSERT INTO f
  SELECT s1.n * 10 + s2.n, s1.n,
         (s1.n * 7 + s2.n * 3) % 50,
         ((s1.n * 37 + s2.n * 11) % 400) * 0.25 + 1,
         ELT(1 + (s1.n + s2.n) % 3, 0.00, 0.25, 0.50),
         ((s1.n + s2.n) % 40) * 0.25,
         IF(s1.n % 20 = 3, 4611686018427387904 + s1.n * 10 + s2.n, s1.n),
         IF(s1.n % 20 = 4, 9223372036854775808 + s1.n * 10 + s2.n, s1.n),
         CAST(12345678901234567890123456.123456 AS DECIMAL(38,6)) * (1 + s2.n % 3)
           + s1.n / 1000,
         IF((s1.n + s2.n) % 5 = 0, NULL, (s1.n + s2.n) % 25),
         ELT(1 + (s1.n + s2.n) % 5,
             CONCAT('grav', LPAD(s1.n % 30, 3, '0')),
             CONCAT('GRAV', LPAD(s1.n % 30, 3, '0')),
             CONCAT('gr', _utf8mb4 x'C3A1', 'v', LPAD(s1.n % 30, 3, '0')),
             CONCAT('GR', _utf8mb4 x'C381', 'V', LPAD(s1.n % 30, 3, '0')),
             CONCAT('grav', LPAD(s1.n % 30, 3, '0'), '  ')),
         IF(s2.n % 3 = 0, -0e0, IF(s2.n % 3 = 1, 0e0, 2.5e0))
  FROM seq s1, seq s2
  WHERE s1.n <= 2000 AND s2.n <= 5 AND s2.n <= s1.n % 6;

# Small root for the inner + left 3-table shape (Q-5)
CREATE TABLE reg (
  id INT NOT NULL PRIMARY KEY,
  region VARCHAR(10) NOT NULL
) ENGINE=NDB;
INSERT INTO reg SELECT n - 1, ELT(1 + n % 4, 'north', 'south', 'east', 'west')
  FROM seq WHERE n <= 20;

ANALYZE TABLE d, l1, l2, l3, f, reg;
SELECT COUNT(*) FROM f;
