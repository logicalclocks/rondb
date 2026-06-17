# Shared schema for the data-driven CTE suite (TPC-H-lite).
#
# Chosen for: broad column-type coverage (signed/unsigned int widths, FLOAT,
# DOUBLE, DECIMAL scale-2 and scale-0, CHAR, VARCHAR, DATE), a >256-group
# dimension (customer / orders.o_custkey) to cross the 256-row API batch
# boundary, and natural spread across all node groups (sequential PK hashing).
#
# All tables ENGINE=NDB.  Paired with cte_data.inc (deterministic load) and
# dropped by cte_drop.inc.

--disable_query_log
--disable_warnings

CREATE TABLE region (
  r_regionkey TINYINT NOT NULL,
  r_name      CHAR(12) NOT NULL,
  PRIMARY KEY USING HASH (r_regionkey)
) ENGINE=NDB;

CREATE TABLE nation (
  n_nationkey TINYINT NOT NULL,
  n_name      CHAR(15) NOT NULL,
  n_regionkey TINYINT NOT NULL,
  PRIMARY KEY USING HASH (n_nationkey)
) ENGINE=NDB;
CREATE INDEX idx_n_regionkey ON nation (n_regionkey);

CREATE TABLE customer (
  c_custkey    INT NOT NULL,
  c_nationkey  TINYINT NOT NULL,
  c_mktsegment VARCHAR(12) NOT NULL,
  c_acctbal    DECIMAL(12,2) NOT NULL,
  PRIMARY KEY USING HASH (c_custkey)
) ENGINE=NDB;
CREATE INDEX idx_c_nationkey ON customer (c_nationkey);
CREATE INDEX idx_c_mktsegment ON customer (c_mktsegment);

CREATE TABLE supplier (
  s_suppkey     INT NOT NULL,
  s_nationkey   TINYINT NOT NULL,
  s_acctbal     DECIMAL(12,2) NOT NULL,
  s_total_sales BIGINT UNSIGNED NOT NULL,
  s_margin      DECIMAL(8,0) NOT NULL,
  PRIMARY KEY USING HASH (s_suppkey)
) ENGINE=NDB;
CREATE INDEX idx_s_nationkey ON supplier (s_nationkey);

CREATE TABLE part (
  p_partkey    INT NOT NULL,
  p_name       VARCHAR(40) NOT NULL,
  p_brand      CHAR(10) NOT NULL,
  p_size       MEDIUMINT NOT NULL,
  p_retailprice DECIMAL(12,2) NOT NULL,
  PRIMARY KEY USING HASH (p_partkey)
) ENGINE=NDB;
CREATE INDEX idx_p_brand ON part (p_brand);
CREATE INDEX idx_p_size ON part (p_size);

CREATE TABLE orders (
  o_orderkey    INT NOT NULL,
  o_custkey     INT NOT NULL,
  o_orderdate   DATE NOT NULL,
  o_orderstatus CHAR(1) NOT NULL,
  o_totalprice  DECIMAL(12,2) NOT NULL,
  o_shippriority SMALLINT UNSIGNED NOT NULL,
  o_clerk       INT NULL,
  PRIMARY KEY USING HASH (o_orderkey)
) ENGINE=NDB;
CREATE INDEX idx_o_custkey ON orders (o_custkey);
CREATE INDEX idx_o_orderdate ON orders (o_orderdate);
CREATE INDEX idx_o_status_date ON orders (o_orderstatus, o_orderdate);
# Nullable indexed column: ~1/7 of rows are NULL (o_orderkey % 7 == 0).
# Enables NULL-semantics tests (MIN/MAX/COUNT ignore NULL, IS NULL filters)
# and the nullable-MIN/MAX-via-index probe.
CREATE INDEX idx_o_clerk ON orders (o_clerk);

CREATE TABLE lineitem (
  l_orderkey     INT NOT NULL,
  l_linenumber   TINYINT NOT NULL,
  l_partkey      INT NOT NULL,
  l_suppkey      INT NOT NULL,
  l_quantity     INT NOT NULL,
  l_extendedprice DECIMAL(12,2) NOT NULL,
  l_discount     FLOAT NOT NULL,
  l_tax          DOUBLE NOT NULL,
  l_shipdate     DATE NOT NULL,
  l_returnflag   CHAR(1) NOT NULL,
  PRIMARY KEY USING HASH (l_orderkey, l_linenumber)
) ENGINE=NDB;
CREATE INDEX idx_l_partkey ON lineitem (l_partkey);
CREATE INDEX idx_l_shipdate ON lineitem (l_shipdate);

# Temporal-type coverage for MIN/MAX pushdown (D17 + temporal extension):
# YEAR, DATETIME (fsp 0), DATETIME(6) (max fractional width), TIME (fsp 0),
# TIME(3) (fractional).  Dedicated table so the shared orders/lineitem stay
# untouched.  e_group (60 distinct) is the CTE GROUP BY key.  TIMESTAMP is
# intentionally absent — TIMESTAMP2 MIN/MAX is deferred (timezone semantics).
# Named evlog (not "events" — EVENTS is a reserved keyword in RonSQL).
CREATE TABLE evlog (
  e_id       INT NOT NULL,
  e_group    INT NOT NULL,
  e_year     YEAR NOT NULL,
  e_modified DATETIME NOT NULL,
  e_logged   DATETIME(6) NOT NULL,
  e_elapsed  TIME NOT NULL,
  e_split    TIME(3) NOT NULL,
  PRIMARY KEY USING HASH (e_id)
) ENGINE=NDB;
CREATE INDEX idx_e_group ON evlog (e_group);

--enable_warnings
--enable_query_log
