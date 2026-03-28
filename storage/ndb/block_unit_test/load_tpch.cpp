/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * load_tpch — Load all 8 TPC-H tables into NDB at a given scale factor.
 *
 * Creates tables via MySQL (ENGINE=NDB), loads data via NDB API batch inserts.
 * Data is deterministic (hash-based, no rand()) for reproducible benchmarks.
 *
 * Usage: load_tpch -c <connect_string> -m <mysql_port> [options]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <mysql.h>
#include <decimal_utils.hpp>

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

static double elapsedMs(TimePoint start, TimePoint end)
{
  return std::chrono::duration<double, std::milli>(end - start).count();
}

/* ------------------------------------------------------------------ */
/* Deterministic hash for data generation                              */
/* ------------------------------------------------------------------ */

static Uint32 detHash(Uint32 a, Uint32 b)
{
  Uint32 h = a * 2654435761u + b;
  h ^= h >> 16;
  h *= 0x45d9f3b;
  h ^= h >> 16;
  return h;
}

/* ------------------------------------------------------------------ */
/* Helpers: DECIMAL and VARCHAR setValue for NDB API                    */
/* ------------------------------------------------------------------ */

static void
setDecimal(NdbOperation *op, const char *col, const char *str)
{
  /* DECIMAL(15,2): 13 integral (9+4 → 4+2 bytes) + 2 frac (1 byte) = 7 bytes */
  const int PREC = 15, SCALE = 2, BIN_LEN = 7;
  char bin[BIN_LEN];
  int rc = decimal_str2bin(str, (int)strlen(str), PREC, SCALE, bin, BIN_LEN);
  (void)rc;
  assert(rc == E_DEC_OK || rc == E_DEC_TRUNCATED);
  op->setValue(col, bin, BIN_LEN);
}

static void
setVarchar(NdbOperation *op, const char *col, const char *str)
{
  /* VARCHAR in NDB requires 1-byte length prefix */
  size_t len = strlen(str);
  char buf[256];
  assert(len < sizeof(buf));
  buf[0] = (char)len;
  memcpy(buf + 1, str, len);
  op->setValue(col, buf, (Uint32)(1 + len));
}

/* ------------------------------------------------------------------ */
/* TPC-H reference data                                                */
/* ------------------------------------------------------------------ */

static const char *REGION_NAMES[] = {
  "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"
};

static const char *NATION_NAMES[] = {
  "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
  "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
  "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
  "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
  "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"
};

static const Uint32 NATION_REGION[] = {
  0, 1, 1, 1, 4, 0, 3, 3, 2, 2,
  4, 4, 2, 4, 0, 0, 0, 1, 2, 3,
  4, 2, 3, 3, 1
};

static const char *COLOR_WORDS[] = {
  "almond", "antique", "aquamarine", "azure", "beige",
  "bisque", "black", "blanched", "blue", "blush",
  "brown", "burlywood", "burnished", "chartreuse", "chiffon",
  "chocolate", "coral", "cornflower", "cornsilk", "cream",
  "cyan", "dark", "deep", "dim", "dodger",
  "drab", "firebrick", "floral", "forest", "frosted",
  "gainsboro", "ghost", "goldenrod", "green", "grey",
  "honeydew", "hot", "indian", "ivory", "khaki",
  "lace", "lavender", "lawn", "lemon", "light",
  "lime", "linen", "magenta", "maroon", "medium",
  "metallic", "midnight", "mint", "misty", "moccasin",
  "navajo", "navy", "olive", "orange", "orchid",
  "pale", "papaya", "peach", "peru", "pink",
  "plum", "powder", "puff", "purple", "red",
  "rose", "rosy", "royal", "saddle", "salmon",
  "sandy", "seashell", "sienna", "sky", "slate",
  "smoke", "snow", "spring", "steel", "tan",
  "thistle", "tomato", "turquoise", "violet", "wheat",
  "white", "yellow"
};
static const Uint32 NUM_COLORS = 92;

static const char *PRIORITY_NAMES[] = {
  "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"
};

static const char *SHIPMODE_NAMES[] = {
  "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"
};

/* ------------------------------------------------------------------ */
/* MySQL helpers                                                       */
/* ------------------------------------------------------------------ */

static int
sqlExec(MYSQL *conn, const char *query)
{
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL failed: %s\n  query: %.200s...\n",
            mysql_error(conn), query);
    return -1;
  }
  return 0;
}

static MYSQL *
connectMysql(int mysqlPort)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) {
    fprintf(stderr, "mysql_init failed\n");
    return nullptr;
  }
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         "test", mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

/* ------------------------------------------------------------------ */
/* Scale factor parameters                                             */
/* ------------------------------------------------------------------ */

struct ScaleParams {
  Uint32 numSuppliers;
  Uint32 numParts;
  Uint32 numCustomers;
  Uint32 numOrders;
  Uint32 linesPerOrder;
};

static ScaleParams
computeScale(double sf)
{
  ScaleParams p;
  p.numSuppliers = std::max(1u, (Uint32)(10000 * sf));
  p.numParts     = std::max(1u, (Uint32)(200000 * sf));
  p.numCustomers = std::max(1u, (Uint32)(150000 * sf));
  p.numOrders    = std::max(1u, (Uint32)(1500000 * sf));
  p.linesPerOrder = 4;
  return p;
}

/* ------------------------------------------------------------------ */
/* Table creation                                                      */
/* ------------------------------------------------------------------ */

static int
sqlExecTimed(MYSQL *conn, const char *label, const char *query)
{
  auto t0 = Clock::now();
  printf("  [DDL] %s ...", label); fflush(stdout);
  int rc = sqlExec(conn, query);
  double ms = elapsedMs(t0, Clock::now());
  printf(" %.0f ms%s\n", ms, rc != 0 ? " FAILED" : "");
  fflush(stdout);
  return rc;
}

static int
createTables(MYSQL *conn)
{
  printf("  Dropping old tables...\n"); fflush(stdout);
  sqlExecTimed(conn, "DROP tpch_lineitem", "DROP TABLE IF EXISTS tpch_lineitem");
  sqlExecTimed(conn, "DROP tpch_orders",   "DROP TABLE IF EXISTS tpch_orders");
  sqlExecTimed(conn, "DROP tpch_partsupp", "DROP TABLE IF EXISTS tpch_partsupp");
  sqlExecTimed(conn, "DROP tpch_customer", "DROP TABLE IF EXISTS tpch_customer");
  sqlExecTimed(conn, "DROP tpch_supplier", "DROP TABLE IF EXISTS tpch_supplier");
  sqlExecTimed(conn, "DROP tpch_part",     "DROP TABLE IF EXISTS tpch_part");
  sqlExecTimed(conn, "DROP tpch_nation",   "DROP TABLE IF EXISTS tpch_nation");
  sqlExecTimed(conn, "DROP tpch_region",   "DROP TABLE IF EXISTS tpch_region");
  printf("  Creating tables...\n"); fflush(stdout);

  if (sqlExecTimed(conn, "CREATE tpch_region",
    "CREATE TABLE tpch_region ("
    "  r_regionkey INT NOT NULL PRIMARY KEY,"
    "  r_name CHAR(25) NOT NULL,"
    "  r_comment VARCHAR(152)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_nation",
    "CREATE TABLE tpch_nation ("
    "  n_nationkey INT NOT NULL PRIMARY KEY,"
    "  n_name CHAR(25) NOT NULL,"
    "  n_regionkey INT NOT NULL,"
    "  n_comment VARCHAR(152)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_supplier",
    "CREATE TABLE tpch_supplier ("
    "  s_suppkey INT NOT NULL PRIMARY KEY,"
    "  s_name CHAR(25) NOT NULL,"
    "  s_address VARCHAR(40) NOT NULL,"
    "  s_nationkey INT NOT NULL,"
    "  s_phone CHAR(15) NOT NULL,"
    "  s_acctbal DECIMAL(15,2) NOT NULL,"
    "  s_comment VARCHAR(101)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_part",
    "CREATE TABLE tpch_part ("
    "  p_partkey INT NOT NULL PRIMARY KEY,"
    "  p_name VARCHAR(55) NOT NULL,"
    "  p_mfgr CHAR(25) NOT NULL,"
    "  p_brand CHAR(10) NOT NULL,"
    "  p_type VARCHAR(25) NOT NULL,"
    "  p_size INT NOT NULL,"
    "  p_container CHAR(10) NOT NULL,"
    "  p_retailprice DECIMAL(15,2) NOT NULL,"
    "  p_comment VARCHAR(23)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_partsupp",
    "CREATE TABLE tpch_partsupp ("
    "  ps_partkey INT NOT NULL,"
    "  ps_suppkey INT NOT NULL,"
    "  ps_availqty INT NOT NULL,"
    "  ps_supplycost DECIMAL(15,2) NOT NULL,"
    "  ps_comment VARCHAR(199),"
    "  PRIMARY KEY (ps_partkey, ps_suppkey)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_customer",
    "CREATE TABLE tpch_customer ("
    "  c_custkey INT NOT NULL PRIMARY KEY,"
    "  c_name VARCHAR(25) NOT NULL,"
    "  c_address VARCHAR(40) NOT NULL,"
    "  c_nationkey INT NOT NULL,"
    "  c_phone CHAR(15) NOT NULL,"
    "  c_acctbal DECIMAL(15,2) NOT NULL,"
    "  c_mktsegment CHAR(10) NOT NULL,"
    "  c_comment VARCHAR(117)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_orders",
    "CREATE TABLE tpch_orders ("
    "  o_orderkey INT NOT NULL PRIMARY KEY,"
    "  o_custkey INT NOT NULL,"
    "  o_orderstatus CHAR(1) NOT NULL,"
    "  o_totalprice DECIMAL(15,2) NOT NULL,"
    "  o_orderdate DATE NOT NULL,"
    "  o_orderyear INT NOT NULL,"
    "  o_orderpriority CHAR(15) NOT NULL,"
    "  o_clerk CHAR(15) NOT NULL,"
    "  o_shippriority INT NOT NULL,"
    "  o_comment VARCHAR(79)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  if (sqlExecTimed(conn, "CREATE tpch_lineitem",
    "CREATE TABLE tpch_lineitem ("
    "  l_orderkey INT NOT NULL,"
    "  l_linenumber INT NOT NULL,"
    "  l_partkey INT NOT NULL,"
    "  l_suppkey INT NOT NULL,"
    "  l_quantity DECIMAL(15,2) NOT NULL,"
    "  l_extendedprice DECIMAL(15,2) NOT NULL,"
    "  l_discount DECIMAL(15,2) NOT NULL,"
    "  l_tax DECIMAL(15,2) NOT NULL,"
    "  l_returnflag CHAR(1) NOT NULL,"
    "  l_linestatus CHAR(1) NOT NULL,"
    "  l_shipdate DATE NOT NULL,"
    "  l_commitdate DATE NOT NULL,"
    "  l_receiptdate DATE NOT NULL,"
    "  l_shipinstruct CHAR(25) NOT NULL,"
    "  l_shipmode CHAR(10) NOT NULL,"
    "  l_comment VARCHAR(44),"
    "  PRIMARY KEY (l_orderkey, l_linenumber)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  // Indexes on foreign-key columns for pushdown join aggregation
  if (sqlExecTimed(conn, "CREATE INDEX supplier_nationkey",
    "CREATE INDEX idx_supplier_nationkey ON tpch_supplier (s_nationkey)"
    ) != 0) return -1;
  if (sqlExecTimed(conn, "CREATE INDEX customer_nationkey",
    "CREATE INDEX idx_customer_nationkey ON tpch_customer (c_nationkey)"
    ) != 0) return -1;
  if (sqlExecTimed(conn, "CREATE INDEX orders_custkey",
    "CREATE INDEX idx_orders_custkey ON tpch_orders (o_custkey)"
    ) != 0) return -1;
  if (sqlExecTimed(conn, "CREATE INDEX lineitem_suppkey",
    "CREATE INDEX idx_lineitem_suppkey ON tpch_lineitem (l_suppkey)"
    ) != 0) return -1;

  return 0;
}

static void
dropTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_lineitem");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_orders");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_partsupp");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_customer");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_supplier");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_part");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_nation");
  sqlExec(conn, "DROP TABLE IF EXISTS tpch_region");
}

/* ------------------------------------------------------------------ */
/* NDB packed DATE helper                                              */
/* ------------------------------------------------------------------ */

static void
setNdbDate(NdbOperation *op, const char *colName, int year, int month, int day)
{
  Uint32 packed = ((Uint32)year << 9) | ((Uint32)month << 5) | (Uint32)day;
  char buf[4] = {0};
  buf[0] = (char)(packed & 0xFF);
  buf[1] = (char)((packed >> 8) & 0xFF);
  buf[2] = (char)((packed >> 16) & 0xFF);
  op->setValue(colName, buf);
}

/* ------------------------------------------------------------------ */
/* NDB CHAR column helper — space-pad to column width                  */
/* ------------------------------------------------------------------ */

static void
setChar(NdbOperation *op, const char *colName,
        const char *value, Uint32 charLen)
{
  char buf[64];
  require(charLen < sizeof(buf));
  memset(buf, ' ', charLen);
  Uint32 slen = (Uint32)strlen(value);
  if (slen > charLen) slen = charLen;
  memcpy(buf, value, slen);
  op->setValue(colName, buf);
}

/* ------------------------------------------------------------------ */
/* Data loading functions                                              */
/* ------------------------------------------------------------------ */

static int
loadRegion(Ndb *ndb)
{
  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable("tpch_region");
  if (tab == nullptr) return -1;

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) return -1;

  for (Uint32 i = 0; i < 5; i++) {
    NdbOperation *op = tx->getNdbOperation(tab);
    op->insertTuple();
    op->equal("r_regionkey", (Int32)i);
    setChar(op, "r_name", REGION_NAMES[i], 25);
    setVarchar(op, "r_comment", "");
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    fprintf(stderr, "insert region: %s\n", tx->getNdbError().message);
    tx->close();
    return -1;
  }
  tx->close();
  printf("  REGION: 5 rows\n");
  return 0;
}

static int
loadNation(Ndb *ndb)
{
  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable("tpch_nation");
  if (tab == nullptr) return -1;

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) return -1;

  for (Uint32 i = 0; i < 25; i++) {
    NdbOperation *op = tx->getNdbOperation(tab);
    op->insertTuple();
    op->equal("n_nationkey", (Int32)i);
    setChar(op, "n_name", NATION_NAMES[i], 25);
    op->setValue("n_regionkey", (Int32)NATION_REGION[i]);
    setVarchar(op, "n_comment", "");
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    fprintf(stderr, "insert nation: %s\n", tx->getNdbError().message);
    tx->close();
    return -1;
  }
  tx->close();
  printf("  NATION: 25 rows\n");
  return 0;
}

static int
loadSupplier(Ndb *ndb, Uint32 count)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("tpch_supplier");
  if (tab == nullptr) return -1;

  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < count; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 e = std::min(s + BATCH, count);
    for (Uint32 i = s; i < e; i++) {
      Uint32 suppkey = i + 1;
      NdbOperation *op = tx->getNdbOperation(tab);
      op->insertTuple();
      op->equal("s_suppkey", (Int32)suppkey);
      char name[26];
      snprintf(name, sizeof(name), "Supplier#%09u", suppkey);
      setChar(op, "s_name", name, 25);
      setVarchar(op, "s_address", "addr");
      op->setValue("s_nationkey", (Int32)(suppkey % 25));
      op->setValue("s_phone", "000-000-0000000");
      char bal[20];
      snprintf(bal, sizeof(bal), "%d.%02d",
               (int)(detHash(suppkey, 100) % 10000),
               (int)(detHash(suppkey, 101) % 100));
      setDecimal(op, "s_acctbal", bal);
      setVarchar(op, "s_comment", "");
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert supplier [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (e % 100000 == 0 || e == count))
      printf("  SUPPLIER: %u / %u\n", e, count);
  }
  if (!verbose) printf("  SUPPLIER: %u rows\n", count);
  return 0;
}

static int
loadPart(Ndb *ndb, Uint32 count)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("tpch_part");
  if (tab == nullptr) return -1;

  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < count; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 e = std::min(s + BATCH, count);
    for (Uint32 i = s; i < e; i++) {
      Uint32 partkey = i + 1;
      NdbOperation *op = tx->getNdbOperation(tab);
      op->insertTuple();
      op->equal("p_partkey", (Int32)partkey);

      /* Build p_name: 5 color words, force "green" for ~14% of parts */
      char pname[56];
      int pos = 0;
      for (int w = 0; w < 5; w++) {
        Uint32 cidx;
        if (w == 0 && (partkey % 7 == 0)) {
          cidx = 33;  /* "green" index in COLOR_WORDS */
        } else {
          cidx = detHash(partkey, 200 + w) % NUM_COLORS;
        }
        if (w > 0) pname[pos++] = ' ';
        int len = snprintf(pname + pos, sizeof(pname) - pos, "%s",
                           COLOR_WORDS[cidx]);
        pos += len;
      }
      pname[pos] = '\0';
      setVarchar(op, "p_name", pname);

      char mfgr[26];
      snprintf(mfgr, sizeof(mfgr), "Manufacturer#%u",
               (detHash(partkey, 300) % 5) + 1);
      setChar(op, "p_mfgr", mfgr, 25);
      char brand[11];
      snprintf(brand, sizeof(brand), "Brand#%u%u",
               (detHash(partkey, 301) % 5) + 1,
               (detHash(partkey, 302) % 5) + 1);
      setChar(op, "p_brand", brand, 10);
      setVarchar(op, "p_type", "STANDARD");
      op->setValue("p_size", (Int32)((detHash(partkey, 303) % 50) + 1));
      setChar(op, "p_container", "SM CASE", 10);

      char price[20];
      snprintf(price, sizeof(price), "%u.%02u",
               (900 + partkey / 10) % 2001,
               (detHash(partkey, 304) % 100));
      setDecimal(op, "p_retailprice", price);
      setVarchar(op, "p_comment", "");
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert part [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (e % 100000 == 0 || e == count))
      printf("  PART: %u / %u\n", e, count);
  }
  if (!verbose) printf("  PART: %u rows\n", count);
  return 0;
}

static int
loadPartSupp(Ndb *ndb, Uint32 numParts, Uint32 numSuppliers)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("tpch_partsupp");
  if (tab == nullptr) return -1;

  const Uint32 BATCH = 500;
  Uint32 totalRows = numParts * 4;
  Uint32 loaded = 0;

  for (Uint32 p = 0; p < numParts; ) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 batchEnd = std::min(p + BATCH / 4, numParts);
    for (; p < batchEnd; p++) {
      Uint32 partkey = p + 1;
      for (Uint32 s = 0; s < 4; s++) {
        Uint32 suppkey = ((partkey + s * (numSuppliers / 4 + 1)) % numSuppliers) + 1;
        NdbOperation *op = tx->getNdbOperation(tab);
        op->insertTuple();
        op->equal("ps_partkey", (Int32)partkey);
        op->equal("ps_suppkey", (Int32)suppkey);
        op->setValue("ps_availqty",
                     (Int32)(detHash(partkey, 400 + s) % 10000));
        char cost[20];
        snprintf(cost, sizeof(cost), "%u.%02u",
                 detHash(partkey, 410 + s) % 1000,
                 detHash(partkey, 420 + s) % 100);
        setDecimal(op, "ps_supplycost", cost);
        setVarchar(op, "ps_comment", "");
        loaded++;
      }
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert partsupp: %s\n", tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (loaded % 100000 < BATCH || p >= numParts))
      printf("  PARTSUPP: %u / %u\n", loaded, totalRows);
  }
  if (!verbose) printf("  PARTSUPP: %u rows\n", totalRows);
  return 0;
}

static int
loadCustomer(Ndb *ndb, Uint32 count)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("tpch_customer");
  if (tab == nullptr) return -1;

  static const char *MKTSEGMENTS[] = {
    "AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"
  };

  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < count; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 e = std::min(s + BATCH, count);
    for (Uint32 i = s; i < e; i++) {
      Uint32 custkey = i + 1;
      NdbOperation *op = tx->getNdbOperation(tab);
      op->insertTuple();
      op->equal("c_custkey", (Int32)custkey);
      char name[26];
      snprintf(name, sizeof(name), "Customer#%09u", custkey);
      setVarchar(op, "c_name", name);
      setVarchar(op, "c_address", "addr");
      op->setValue("c_nationkey", (Int32)(detHash(custkey, 500) % 25));
      op->setValue("c_phone", "000-000-0000000");
      char bal[20];
      snprintf(bal, sizeof(bal), "%d.%02d",
               (int)(detHash(custkey, 501) % 10000) - 999,
               (int)(detHash(custkey, 502) % 100));
      setDecimal(op, "c_acctbal", bal);
      setChar(op, "c_mktsegment", MKTSEGMENTS[detHash(custkey, 503) % 5], 10);
      setVarchar(op, "c_comment", "");
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert customer [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (e % 100000 == 0 || e == count))
      printf("  CUSTOMER: %u / %u\n", e, count);
  }
  if (!verbose) printf("  CUSTOMER: %u rows\n", count);
  return 0;
}

static int
loadOrders(Ndb *ndb, Uint32 count, Uint32 numCustomers)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("tpch_orders");
  if (tab == nullptr) return -1;

  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < count; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 e = std::min(s + BATCH, count);
    for (Uint32 i = s; i < e; i++) {
      Uint32 orderkey = i + 1;
      NdbOperation *op = tx->getNdbOperation(tab);
      op->insertTuple();
      op->equal("o_orderkey", (Int32)orderkey);
      op->setValue("o_custkey",
                   (Int32)((detHash(orderkey, 600) % numCustomers) + 1));
      op->setValue("o_orderstatus", "O");
      char price[20];
      snprintf(price, sizeof(price), "%u.%02u",
               detHash(orderkey, 601) % 500000,
               detHash(orderkey, 602) % 100);
      setDecimal(op, "o_totalprice", price);

      /* Order date: 1992-01-01 + (orderkey % 2557) days
       * This gives dates from 1992-01-01 to ~1998-12-31 (7 years) */
      Uint32 epochDay = orderkey % 2557;
      struct tm t = {};
      t.tm_year = 92;
      t.tm_mon = 0;
      t.tm_mday = 1 + (int)epochDay;
      t.tm_isdst = -1;
      mktime(&t);
      int year = t.tm_year + 1900;
      int month = t.tm_mon + 1;
      int day = t.tm_mday;

      setNdbDate(op, "o_orderdate", year, month, day);
      op->setValue("o_orderyear", (Int32)year);
      setChar(op, "o_orderpriority", PRIORITY_NAMES[detHash(orderkey, 603) % 5], 15);
      char clerk[16];
      snprintf(clerk, sizeof(clerk), "Clerk#%09u",
               (detHash(orderkey, 604) % 1000) + 1);
      setChar(op, "o_clerk", clerk, 15);
      op->setValue("o_shippriority", (Int32)0);
      setVarchar(op, "o_comment", "");
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert orders [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (e % 100000 == 0 || e == count))
      printf("  ORDERS: %u / %u\n", e, count);
  }
  if (!verbose) printf("  ORDERS: %u rows\n", count);
  return 0;
}

static int
loadLineitemRange(Ndb *ndb, Uint32 startOrder, Uint32 endOrder,
                  Uint32 linesPerOrder,
                  Uint32 numParts, Uint32 numSuppliers,
                  int threadIdx)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("tpch_lineitem");
  if (tab == nullptr) return -1;

  Uint32 rangeOrders = endOrder - startOrder;
  Uint32 rangeRows = rangeOrders * linesPerOrder;
  const Uint32 BATCH = 500;
  Uint32 loaded = 0;

  for (Uint32 o = startOrder; o < endOrder; ) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 batchEnd = std::min(o + BATCH / linesPerOrder, endOrder);
    if (batchEnd == o) batchEnd = o + 1;
    for (; o < batchEnd; o++) {
      Uint32 orderkey = o + 1;
      for (Uint32 ln = 1; ln <= linesPerOrder; ln++) {
        Uint32 partkey = (detHash(orderkey, ln) % numParts) + 1;
        /* Pick a valid supplier for this part (same formula as PARTSUPP) */
        Uint32 sidx = detHash(orderkey, 700 + ln) % 4;
        Uint32 suppkey =
          ((partkey + sidx * (numSuppliers / 4 + 1)) % numSuppliers) + 1;

        NdbOperation *op = tx->getNdbOperation(tab);
        op->insertTuple();
        op->equal("l_orderkey", (Int32)orderkey);
        op->equal("l_linenumber", (Int32)ln);
        op->setValue("l_partkey", (Int32)partkey);
        op->setValue("l_suppkey", (Int32)suppkey);

        Uint32 qty = (detHash(orderkey, 710 + ln) % 50) + 1;
        char qtyStr[20];
        snprintf(qtyStr, sizeof(qtyStr), "%u.00", qty);
        setDecimal(op, "l_quantity", qtyStr);

        Uint32 priceWhole = (900 + partkey / 10) % 2001;
        char priceStr[20];
        snprintf(priceStr, sizeof(priceStr), "%u.%02u",
                 priceWhole * qty,
                 detHash(orderkey, 720 + ln) % 100);
        setDecimal(op, "l_extendedprice", priceStr);

        char discStr[20];
        snprintf(discStr, sizeof(discStr), "0.%02u",
                 detHash(orderkey, 730 + ln) % 11);
        setDecimal(op, "l_discount", discStr);

        char taxStr[20];
        snprintf(taxStr, sizeof(taxStr), "0.%02u",
                 detHash(orderkey, 740 + ln) % 9);
        setDecimal(op, "l_tax", taxStr);

        op->setValue("l_returnflag", "N");
        op->setValue("l_linestatus", "O");

        /* Ship date: order date + 1..121 days */
        Uint32 baseDay = orderkey % 2557;
        Uint32 shipDelay = (detHash(orderkey, 750 + ln) % 121) + 1;
        Uint32 commitDelay = (detHash(orderkey, 760 + ln) % 90) + 1;
        Uint32 receiptDelay = shipDelay + (detHash(orderkey, 770 + ln) % 30);

        auto setDateFromEpoch = [op](const char *col, Uint32 epochDay) {
          struct tm t = {};
          t.tm_year = 92;
          t.tm_mon = 0;
          t.tm_mday = 1 + (int)epochDay;
          t.tm_isdst = -1;
          mktime(&t);
          int year = t.tm_year + 1900;
          int month = t.tm_mon + 1;
          int day = t.tm_mday;
          Uint32 packed = ((Uint32)year << 9) | ((Uint32)month << 5) |
                          (Uint32)day;
          char buf[4] = {0};
          buf[0] = (char)(packed & 0xFF);
          buf[1] = (char)((packed >> 8) & 0xFF);
          buf[2] = (char)((packed >> 16) & 0xFF);
          op->setValue(col, buf);
        };

        setDateFromEpoch("l_shipdate", baseDay + shipDelay);
        setDateFromEpoch("l_commitdate", baseDay + commitDelay);
        setDateFromEpoch("l_receiptdate", baseDay + receiptDelay);
        setChar(op, "l_shipinstruct", "NONE", 25);
        setChar(op, "l_shipmode",
                SHIPMODE_NAMES[detHash(orderkey, 780 + ln) % 7], 10);
        setVarchar(op, "l_comment", "");
        loaded++;
      }
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert lineitem[%d]: %s (loaded=%u)\n",
              threadIdx, tx->getNdbError().message, loaded);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (loaded % 100000 < BATCH * linesPerOrder || o >= endOrder))
      printf("  LINEITEM[%d]: %u / %u\n", threadIdx, loaded, rangeRows);
  }
  if (!verbose)
    printf("  LINEITEM[%d]: %u rows (orders %u..%u)\n",
           threadIdx, rangeRows, startOrder + 1, endOrder);
  return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
  const char *connectString = nullptr;
  int mysqlPort = 3306;
  double scaleFactor = 1.0;
  bool dropOnly = false;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n"
        "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
        "  -m <port>            MySQL port (default: 3306)\n"
        "  --sf <N>             Scale factor (default: 1)\n"
        "  --drop-only          Only drop tables\n"
        "  -v, --verbose        Verbose output\n"
        "  -h, --help           Show help\n",
        argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--sf") == 0 && i + 1 < argc) {
      scaleFactor = atof(argv[++i]);
    } else if (strcmp(argv[i], "--drop-only") == 0) {
      dropOnly = true;
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      return 1;
    }
  }

  if (connectString == nullptr) connectString = "localhost:1186";

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ScaleParams sp = computeScale(scaleFactor);
  Uint32 totalLineItems = sp.numOrders * sp.linesPerOrder;

  printf("load_tpch: TPC-H data loader for NDB\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Scale factor: %.3f\n", scaleFactor);
  printf("  Suppliers: %u  Parts: %u  Customers: %u\n",
         sp.numSuppliers, sp.numParts, sp.numCustomers);
  printf("  Orders: %u  Lines/order: %u  Total lineitems: %u\n\n",
         sp.numOrders, sp.linesPerOrder, totalLineItems);

  ndb_init();
  int result = 0;

  do {
    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      result = 1; break;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      result = 1; break;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init: %s\n", ndb.getNdbError().message);
      result = 1; break;
    }

    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      result = 1; break;
    }

    if (dropOnly) {
      printf("Dropping tables...\n");
      dropTables(conn);
      printf("Done.\n");
      mysql_close(conn);
      break;
    }

    printf("Creating tables...\n");
    if (createTables(conn) != 0) {
      mysql_close(conn); result = 1; break;
    }

    printf("Loading data (parallel)...\n");
    auto tLoad = Clock::now();

    /*
     * Parallel loading strategy:
     *   Data generation is purely deterministic (no cross-table reads),
     *   and NDB has no enforced FK constraints, so all tables can be
     *   loaded concurrently.  Each thread gets its own Ndb object from
     *   the shared Ndb_cluster_connection.
     *
     *   Group 1: region, nation, supplier, part, customer (independent)
     *   Group 2: partsupp, orders, lineitem (independent of each other)
     *
     *   We load all 8 in one wave for maximum parallelism.
     */

    struct ThreadResult {
      const char *table;
      int rc;
    };

    auto makeNdb = [&con]() -> Ndb * {
      Ndb *n = new Ndb(&con, "test");
      if (n->init() != 0) {
        fprintf(stderr, "Ndb::init: %s\n", n->getNdbError().message);
        delete n;
        return nullptr;
      }
      return n;
    };

    const int NUM_THREADS = 10;  /* 7 tables + 3 lineitem shards */
    ThreadResult results[NUM_THREADS] = {};

    auto runLoad = [&](int idx, const char *name,
                       std::function<int(Ndb *)> fn) {
      results[idx].table = name;
      Ndb *n = makeNdb();
      if (n == nullptr) {
        results[idx].rc = -1;
        return;
      }
      results[idx].rc = fn(n);
      delete n;
    };

    std::thread threads[NUM_THREADS];

    threads[0] = std::thread(runLoad, 0, "REGION",
      [](Ndb *n) { return loadRegion(n); });
    threads[1] = std::thread(runLoad, 1, "NATION",
      [](Ndb *n) { return loadNation(n); });
    threads[2] = std::thread(runLoad, 2, "SUPPLIER",
      [&sp](Ndb *n) { return loadSupplier(n, sp.numSuppliers); });
    threads[3] = std::thread(runLoad, 3, "PART",
      [&sp](Ndb *n) { return loadPart(n, sp.numParts); });
    threads[4] = std::thread(runLoad, 4, "CUSTOMER",
      [&sp](Ndb *n) { return loadCustomer(n, sp.numCustomers); });
    threads[5] = std::thread(runLoad, 5, "PARTSUPP",
      [&sp](Ndb *n) {
        return loadPartSupp(n, sp.numParts, sp.numSuppliers);
      });
    threads[6] = std::thread(runLoad, 6, "ORDERS",
      [&sp](Ndb *n) {
        return loadOrders(n, sp.numOrders, sp.numCustomers);
      });

    /* Split lineitem across 3 threads by order-key range */
    static const char *LI_NAMES[] = {
      "LINEITEM[0]", "LINEITEM[1]", "LINEITEM[2]"
    };
    const int LI_THREADS = 3;
    Uint32 ordersPerShard = sp.numOrders / LI_THREADS;
    for (int t = 0; t < LI_THREADS; t++) {
      Uint32 start = t * ordersPerShard;
      Uint32 end = (t == LI_THREADS - 1) ? sp.numOrders
                                          : (t + 1) * ordersPerShard;
      threads[7 + t] = std::thread(runLoad, 7 + t, LI_NAMES[t],
        [&sp, start, end, t](Ndb *n) {
          return loadLineitemRange(n, start, end, sp.linesPerOrder,
                                   sp.numParts, sp.numSuppliers, t);
        });
    }

    for (int i = 0; i < NUM_THREADS; i++)
      threads[i].join();

    for (int i = 0; i < NUM_THREADS; i++) {
      if (results[i].rc != 0) {
        fprintf(stderr, "Failed to load %s\n", results[i].table);
        result = 1;
      }
    }
    if (result != 0) { mysql_close(conn); break; }

    double loadMs = elapsedMs(tLoad, Clock::now());
    printf("\nAll data loaded in %.1f ms (%.1f s)\n", loadMs, loadMs / 1000.0);
    mysql_close(conn);
  } while (0);

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
