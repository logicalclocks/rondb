/*
   Copyright (c) 2025, 2026, Hopsworks and/or its affiliates.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/*
 * load_ronsql_large.cpp — deterministic large-data loader for the
 * ronsql_large MTR suite.
 *
 * Creates two tables via MySQL (ENGINE=NDB) and bulk-loads them via NDB API
 * batch inserts.  All values are computed deterministically from the row
 * number (no RAND), so the suite's .result files stay stable across machines
 * and across the 5 topology variants.
 *
 * The dataset is sized so the heavy aggregating-CTE queries genuinely exercise
 * (a) the >256-row API batch boundary (NUM_CUST = 20000 distinct GROUP BY
 * keys) and (b) cross-fragment / cross-node-group redistribution (orders are
 * keyed by a sequential PK that hashes across all fragments, while the GROUP
 * BY key lg_cust spreads each customer's rows over many fragments).
 *
 * Aggregates in the suite are restricted to INTEGER / DECIMAL / DATE columns
 * (never FLOAT/DOUBLE SUM), so RonSQL's partial-sum order matches MySQL
 * exactly under strict_diff — DECIMAL and integer addition are associative.
 *
 * Usage: load_ronsql_large -c <connect_string> -m <mysql_port> [--drop-only]
 *        Mirrors load_tpch's invocation contract; reachable from MTR as
 *        $NDB_PUSH_AGG_DIR/load_ronsql_large.
 */

#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <mysql.h>
#include <decimal_utils.hpp>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

static bool verbose = false;

/* Scale: 20000 customers, 100000 orders (5 per customer). */
static const Uint32 NUM_CUST   = 20000;
static const Uint32 NUM_ORDERS = 100000;

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
/* DECIMAL / VARCHAR / CHAR / DATE setValue helpers for NDB API         */
/* ------------------------------------------------------------------ */

/* Binary size of a DECIMAL(prec,scale) column in NDB's on-disk format. */
static int
decimalBinSize(int prec, int scale)
{
  static const int dig2bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
  int intg = prec - scale;
  int intg0 = intg / 9, intg1 = intg % 9;
  int frac0 = scale / 9, frac1 = scale % 9;
  return intg0 * 4 + dig2bytes[intg1] + frac0 * 4 + dig2bytes[frac1];
}

static void
setDecimal(NdbOperation *op, const char *col, const char *str,
           int prec, int scale)
{
  int bin_len = decimalBinSize(prec, scale);
  char bin[16];
  assert(bin_len <= (int)sizeof(bin));
  int rc = decimal_str2bin(str, (int)strlen(str), prec, scale, bin, bin_len);
  (void)rc;
  assert(rc == E_DEC_OK || rc == E_DEC_TRUNCATED);
  op->setValue(col, bin, bin_len);
}

static void
setVarchar(NdbOperation *op, const char *col, const char *str)
{
  /* VARCHAR in NDB requires a 1-byte length prefix */
  size_t len = strlen(str);
  char buf[256];
  assert(len < sizeof(buf));
  buf[0] = (char)len;
  memcpy(buf + 1, str, len);
  op->setValue(col, buf, (Uint32)(1 + len));
}

/* NDB packed DATE: ((year << 9) | (month << 5) | day), 3 little-endian bytes */
static void
setNdbDate(NdbOperation *op, const char *col, int year, int month, int day)
{
  Uint32 packed = ((Uint32)year << 9) | ((Uint32)month << 5) | (Uint32)day;
  char buf[4] = {0};
  buf[0] = (char)(packed & 0xFF);
  buf[1] = (char)((packed >> 8) & 0xFF);
  buf[2] = (char)((packed >> 16) & 0xFF);
  op->setValue(col, buf);
}

static const char *SEGMENTS[] = {
  "AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"
};
static const char STATUS[] = { 'O', 'F', 'P' };

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
  unsigned int timeout = 150;
  mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         "test", mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n"
            "  Hint: mysqld may still be in NDB setup — check the "
            "mysqld error log\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

static void
dropTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS lg_orders");
  sqlExec(conn, "DROP TABLE IF EXISTS lg_cust");
}

static int
createTables(MYSQL *conn)
{
  dropTables(conn);

  static const char *DDL[] = {
    "CREATE TABLE lg_cust ("
    "  lc_id      INT NOT NULL,"
    "  lc_region  INT NOT NULL,"
    "  lc_segment VARCHAR(12) NOT NULL,"
    "  lc_balance DECIMAL(12,2) NOT NULL,"
    "  PRIMARY KEY USING HASH (lc_id)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1",

    "CREATE INDEX idx_lc_region ON lg_cust (lc_region)",

    "CREATE TABLE lg_orders ("
    "  lg_id     INT NOT NULL,"
    "  lg_cust   INT NOT NULL,"
    "  lg_region INT NOT NULL,"
    "  lg_status CHAR(1) NOT NULL,"
    "  lg_qty    INT NOT NULL,"
    "  lg_amount DECIMAL(12,2) NOT NULL,"
    "  lg_date   DATE NOT NULL,"
    "  PRIMARY KEY USING HASH (lg_id)"
    ") ENGINE=NDB DEFAULT CHARSET=latin1",

    "CREATE INDEX idx_lg_cust ON lg_orders (lg_cust)",
    "CREATE INDEX idx_lg_region ON lg_orders (lg_region)",
    "CREATE INDEX idx_lg_date ON lg_orders (lg_date)",
  };
  for (size_t i = 0; i < sizeof(DDL) / sizeof(DDL[0]); i++) {
    printf("  [DDL] %.40s ...\n", DDL[i]); fflush(stdout);
    if (sqlExec(conn, DDL[i]) != 0) return -1;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* Data loading                                                        */
/* ------------------------------------------------------------------ */

static int
loadCust(Ndb *ndb)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("lg_cust");
  if (tab == nullptr) {
    fprintf(stderr, "getTable lg_cust: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < NUM_CUST; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 e = std::min(s + BATCH, NUM_CUST);
    for (Uint32 i = s; i < e; i++) {
      Uint32 id = i + 1;
      NdbOperation *op = tx->getNdbOperation(tab);
      op->insertTuple();
      op->equal("lc_id", (Int32)id);
      op->setValue("lc_region", (Int32)(id % 5));
      setVarchar(op, "lc_segment", SEGMENTS[detHash(id, 11) % 5]);
      Uint32 cents = detHash(id, 12) % 1000000;  /* up to 9999.99 */
      char bal[16];
      snprintf(bal, sizeof(bal), "%u.%02u", cents / 100, cents % 100);
      setDecimal(op, "lc_balance", bal, 12, 2);
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert lg_cust [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
  }
  printf("  lg_cust: %u rows\n", NUM_CUST); fflush(stdout);
  return 0;
}

static int
loadOrders(Ndb *ndb)
{
  const NdbDictionary::Table *tab =
    ndb->getDictionary()->getTable("lg_orders");
  if (tab == nullptr) {
    fprintf(stderr, "getTable lg_orders: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < NUM_ORDERS; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) return -1;
    Uint32 e = std::min(s + BATCH, NUM_ORDERS);
    for (Uint32 i = s; i < e; i++) {
      Uint32 id = i + 1;
      /* Even spread: each customer gets ~NUM_ORDERS/NUM_CUST orders. */
      Uint32 cust = (i % NUM_CUST) + 1;
      NdbOperation *op = tx->getNdbOperation(tab);
      op->insertTuple();
      op->equal("lg_id", (Int32)id);
      op->setValue("lg_cust", (Int32)cust);
      op->setValue("lg_region", (Int32)(cust % 5));
      char st[2] = { STATUS[id % 3], 0 };
      op->setValue("lg_status", st);
      op->setValue("lg_qty", (Int32)((detHash(id, 21) % 100) + 1));
      Uint32 cents = detHash(id, 22) % 1000000;  /* up to 9999.99 */
      char amt[16];
      snprintf(amt, sizeof(amt), "%u.%02u", cents / 100, cents % 100);
      setDecimal(op, "lg_amount", amt, 12, 2);
      int year  = 2018 + (int)(detHash(id, 23) % 5);   /* 2018..2022 */
      int month = 1 + (int)(detHash(id, 24) % 12);     /* 1..12 */
      int day   = 1 + (int)(detHash(id, 25) % 28);     /* 1..28 */
      setNdbDate(op, "lg_date", year, month, day);
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert lg_orders [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
    if (verbose && (e % 20000 == 0 || e == NUM_ORDERS))
      printf("  lg_orders: %u / %u\n", e, NUM_ORDERS);
  }
  printf("  lg_orders: %u rows\n", NUM_ORDERS); fflush(stdout);
  return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
  const char *connectString = nullptr;
  int mysqlPort = 3306;
  bool dropOnly = false;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n"
        "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
        "  -m <port>            MySQL port (default: 3306)\n"
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

  printf("load_ronsql_large: large CTE data loader for NDB\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Customers: %u  Orders: %u\n\n", NUM_CUST, NUM_ORDERS);

  ndb_init();
  int result = 0;

  do {
    printf("Connecting to management server..."); fflush(stdout);
    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "\nFailed to connect to management server\n");
      result = 1; break;
    }
    printf(" ok\n"); fflush(stdout);

    printf("Waiting for cluster to be ready..."); fflush(stdout);
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "\nCluster not ready\n");
      result = 1; break;
    }
    printf(" ok\n"); fflush(stdout);

    printf("Initializing NDB object..."); fflush(stdout);
    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "\nNdb::init: %s\n", ndb.getNdbError().message);
      result = 1; break;
    }
    printf(" ok\n"); fflush(stdout);

    printf("Connecting to MySQL on port %d...", mysqlPort); fflush(stdout);
    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "\nCannot connect to MySQL on port %d\n", mysqlPort);
      result = 1; break;
    }
    printf(" ok\n"); fflush(stdout);

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

    printf("Loading data...\n");
    if (loadCust(&ndb) != 0 || loadOrders(&ndb) != 0) {
      mysql_close(conn); result = 1; break;
    }
    mysql_close(conn);
    printf("Done.\n");
  } while (0);

  fflush(stdout);
  ndb_end(0);

  /* Emit the MTR pass/fail token directly to the saved real-stdout fd
     (matches load_tpch — avoids stdio buffering across the dup2 restore). */
  if (result == 0) {
    ssize_t written = write(mtr_fd, "PASSED\n", 7);
    if (written != 7) result = 1;
  } else {
    ssize_t written = write(mtr_fd, "FAILED\n", 7);
    if (written != 7) result = 1;
  }
  close(mtr_fd);

  return result;
}
