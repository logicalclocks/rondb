# Deterministic data load for the data-driven CTE suite.
#
# Uses a digits cross-join numbers helper (_seq, 1..10000) + INSERT..SELECT so
# every value is computed server-side from the row number with MOD/ELT/DATE_ADD
# arithmetic -- no RAND, fully reproducible so the .result Expected blocks stay
# stable.  Both the mysql client and RonSQL read the same stored NDB bytes.
#
# Row counts: region 5, nation 25, customer 300 (>256 groups for GROUP BY
# o_custkey -> crosses the 256-row API batch boundary), supplier 50, part 100,
# orders 1500 (5 per custkey), lineitem 3000 (2 per order).  Sequential PKs hash
# across all node groups, so multi-node redistribution is exercised.

--disable_query_log
--disable_warnings

# Load with a fixed UTC session so TIMESTAMP storage (evlog.e_stamp /
# e_stamp6) is deterministic across machines — TIMESTAMP literals are
# converted from the session time zone to UTC at store time.  DATE / DATETIME
# / TIME are timezone-independent, so this does not affect the other columns.
SET time_zone = '+00:00';

# --- deterministic numbers helper (non-NDB, dropped at end of load) ---
CREATE TABLE _d (d INT) ENGINE=MEMORY;
INSERT INTO _d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE _seq (n INT NOT NULL PRIMARY KEY) ENGINE=MEMORY;
INSERT INTO _seq SELECT a.d*1000 + b.d*100 + c.d*10 + e.d + 1
  FROM _d a, _d b, _d c, _d e;

# --- dimension + fact tables ---
INSERT INTO region VALUES
  (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDEAST');

INSERT INTO nation
  SELECT n-1, CONCAT('NATION', n-1), MOD(n-1,5)
  FROM _seq WHERE n <= 25;

INSERT INTO customer
  SELECT n, MOD(n,25),
         ELT(MOD(n,5)+1,'AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'),
         n*17.25 + 100.50
  FROM _seq WHERE n <= 300;

INSERT INTO supplier
  SELECT n, MOD(n,25), n*31.40 + 50.00, n*100000, n*3
  FROM _seq WHERE n <= 50;

INSERT INTO part
  SELECT n, CONCAT('part#', n),
         ELT(MOD(n,5)+1,'BRAND#11','BRAND#22','BRAND#33','BRAND#44','BRAND#55'),
         MOD(n,50)+1, n*9.10 + 900.00
  FROM _seq WHERE n <= 100;

INSERT INTO orders
  SELECT n, MOD(n-1,300)+1,
         DATE_ADD('1995-01-01', INTERVAL MOD(n,1000) DAY),
         ELT(MOD(n,3)+1,'O','F','P'),
         n*13.37, MOD(n,5),
         IF(MOD(n,7)=0, NULL, MOD(n,40)+1)
  FROM _seq WHERE n <= 1500;

INSERT INTO lineitem
  SELECT o.o_orderkey, ln.k,
         MOD(o.o_orderkey*7 + ln.k, 100)+1,
         MOD(o.o_orderkey*3 + ln.k, 50)+1,
         MOD(o.o_orderkey + ln.k, 50)+1,
         (o.o_orderkey + ln.k)*7.77,
         MOD(o.o_orderkey,10)*0.01,
         MOD(o.o_orderkey,8)*0.01,
         DATE_ADD('1995-01-02', INTERVAL MOD(o.o_orderkey*2 + ln.k, 1000) DAY),
         ELT(MOD(o.o_orderkey + ln.k, 3)+1,'N','R','A')
  FROM orders o JOIN (SELECT 1 AS k UNION ALL SELECT 2 AS k) ln;

# --- temporal coverage (evlog): 300 rows, 60 groups (e_group = MOD(n,60)).
# All values computed deterministically from n with well-spread temporal
# ranges so MIN/MAX is non-trivial; e_logged / e_split carry sub-second
# fractions to exercise fsp>0 width handling.  Both engines read the same
# stored NDB bytes, so the RonSQL-vs-MySQL diff is self-consistent. ---
INSERT INTO evlog
  SELECT n,
         MOD(n,60),
         1990 + MOD(n*7,40),
         DATE_ADD('2001-01-01 00:00:00', INTERVAL n*137 MINUTE),
         DATE_ADD(DATE_ADD('2001-06-15 12:00:00', INTERVAL n*89 SECOND),
                  INTERVAL MOD(n*123457,1000000) MICROSECOND),
         SEC_TO_TIME(MOD(n*271,86400)),
         SEC_TO_TIME(MOD(n*331,86400) + MOD(n*97,1000)*0.001),
         DATE_ADD('2001-01-01 00:00:00', INTERVAL n*157 MINUTE),
         DATE_ADD(DATE_ADD('2001-03-10 08:00:00', INTERVAL n*53 SECOND),
                  INTERVAL MOD(n*98765,1000000) MICROSECOND)
  FROM _seq WHERE n <= 300;

DROP TABLE _seq;
DROP TABLE _d;

--enable_warnings
--enable_query_log
