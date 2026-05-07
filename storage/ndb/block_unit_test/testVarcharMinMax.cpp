/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * testVarcharMinMax — Phase I.6 F.2 / F.3 isolated end-to-end test for
 * MIN / MAX over VARCHAR / CHAR / Longvarchar columns.
 *
 * Exercises the kernel string MIN/MAX path (per-(group, slot) val_ptr
 * state, F.2-K.4) + the AGG_CHAR_RESULT wire format (F.2-K.5) +
 * NdbAggregator API parse + Result::data_str (F.2-K.5d-1, K.5d-3).
 *
 * Single-table scalar aggregation only — no joins, no GROUP BY, no
 * CTE.  This is the simplest path that touches the new wire format
 * end-to-end.  GROUP BY and CTE_LOOKUP-fed string MIN/MAX have wider
 * surfaces (linked-attr string format, CTE delivery substitution)
 * tracked under the F.4 follow-up plan.
 *
 * Schema (created by this test):
 *   vctest_t (
 *     id INT NOT NULL PRIMARY KEY,
 *     grp INT NOT NULL,
 *     vname VARCHAR(20) NOT NULL,
 *     cname CHAR(8) NOT NULL
 *   ) ENGINE=NDB
 *
 * Test rows: 'Alice'/'A1', 'Bob'/'B2', 'Charlie'/'C3', 'Dave'/'D4',
 * 'Eve'/'E5'.  MIN(vname)='Alice', MAX(vname)='Eve';
 * MIN(cname)='A1', MAX(cname)='E5'.
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbAggregator.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>

#include <mysql.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

static MYSQL *connectMysql(int mysqlPort) {
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

static int runQuery(MYSQL *conn, const char *sql) {
  if (mysql_query(conn, sql) != 0) {
    fprintf(stderr, "SQL failed (%s): %s\n", sql, mysql_error(conn));
    return -1;
  }
  return 0;
}

static int setupSchema(MYSQL *conn) {
  if (runQuery(conn, "DROP TABLE IF EXISTS vctest_t") != 0) return -1;
  if (runQuery(conn,
        "CREATE TABLE vctest_t ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL,"
        "  vname VARCHAR(20) NOT NULL,"
        "  cname CHAR(8) NOT NULL"
        ") ENGINE=NDB"
        // Phase I.6 K.5d-1 covers single-source merge only — force
        // one partition so the API only sees one ProcessRes call;
        // multi-source string merge lands as K.5d-2.
        " PARTITION BY KEY() PARTITIONS 1") != 0) return -1;
  if (runQuery(conn,
        "INSERT INTO vctest_t VALUES "
        "  (1, 1, 'Alice',   'A1'),"
        "  (2, 1, 'Bob',     'B2'),"
        "  (3, 2, 'Charlie', 'C3'),"
        "  (4, 2, 'Dave',    'D4'),"
        "  (5, 2, 'Eve',     'E5')") != 0) return -1;
  return 0;
}

static void dropSchema(MYSQL *conn) {
  runQuery(conn, "DROP TABLE IF EXISTS vctest_t");
}

/*
 * Compare a Result::data_str payload to the expected bytes, ignoring
 * any CHAR space-padding past the expected length.  Returns 0 on
 * match, -1 on mismatch (and prints diagnostics).
 */
static int verifyString(const char *label, NdbAggregator::Result &result,
                        const char *expected) {
  if (result.is_null()) {
    fprintf(stderr, "FAIL %s: result is NULL, expected '%s'\n",
            label, expected);
    return -1;
  }
  Uint32 plen = 0;
  const char *p = result.data_str(&plen);
  Uint32 elen = (Uint32)strlen(expected);
  // CHAR may be space-padded to declared size; match the prefix.
  Uint32 effective = plen;
  while (effective > elen && p[effective - 1] == ' ') effective--;
  if (effective != elen || memcmp(p, expected, elen) != 0) {
    fprintf(stderr, "FAIL %s: got '%.*s' (len=%u), expected '%s'\n",
            label, (int)plen, p, plen, expected);
    return -1;
  }
  V("  OK  %s = '%s' (len=%u)\n", label, expected, plen);
  return 0;
}

static int runScalarTest(Ndb *ndb) {
  V("\n=== Test: scalar MIN/MAX over VARCHAR + CHAR ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  const NdbDictionary::Table *tab = dict->getTable("vctest_t");
  if (tab == nullptr) {
    fprintf(stderr, "Cannot find vctest_t: %s\n", dict->getNdbError().message);
    return -1;
  }
  const NdbDictionary::Column *vnameCol = tab->getColumn("vname");
  const NdbDictionary::Column *cnameCol = tab->getColumn("cname");
  if (vnameCol == nullptr || cnameCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /*
   * Build aggregation program: no GROUP BY, four aggregates over two
   * string columns.
   *   reg0 = vname (VARCHAR)
   *   reg1 = cname (CHAR)
   *   agg[0] = MIN(vname)   -> 'Alice'
   *   agg[1] = MAX(vname)   -> 'Eve'
   *   agg[2] = MIN(cname)   -> 'A1'
   *   agg[3] = MAX(cname)   -> 'E5'
   */
  NdbAggregator agg(tab);
  if (!agg.LoadColumn(vnameCol->getAttrId(), 0) ||
      !agg.Min(0, 0) ||
      !agg.Max(1, 0) ||
      !agg.LoadColumn(cnameCol->getAttrId(), 1) ||
      !agg.Min(2, 1) ||
      !agg.Max(3, 1) ||
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /*
   * Single-table scan + setAggregationCode + DoAggregation — the
   * legacy NdbScanOperation aggregation surface.  NdbQueryBuilder's
   * scanTable+setAggregation requires a non-root aggregate leaf
   * (i.e. multi-table query), which is overkill for an isolated
   * F.2 wire-path test.
   */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == nullptr) {
    fprintf(stderr, "getNdbScanOperation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    fprintf(stderr, "readTuples failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->setAggregationCode(&agg) != 0) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->DoAggregation() != 0) {
    fprintf(stderr, "DoAggregation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }

  int failures = 0;
  NdbAggregator::ResultRecord rec = agg.FetchResultRecord();
  if (rec.end()) {
    fprintf(stderr, "No aggregation result returned\n");
    failures = 1;
  } else {
    NdbAggregator::Result minV = rec.FetchAggregationResult();
    NdbAggregator::Result maxV = rec.FetchAggregationResult();
    NdbAggregator::Result minC = rec.FetchAggregationResult();
    NdbAggregator::Result maxC = rec.FetchAggregationResult();
    if (verifyString("MIN(vname)", minV, "Alice") != 0) failures++;
    if (verifyString("MAX(vname)", maxV, "Eve")   != 0) failures++;
    if (verifyString("MIN(cname)", minC, "A1")    != 0) failures++;
    if (verifyString("MAX(cname)", maxC, "E5")    != 0) failures++;
    NdbAggregator::Result end = rec.FetchAggregationResult();
    if (!end.end()) {
      fprintf(stderr, "FAIL scalar: expected end after 4 aggregate slots\n");
      failures++;
    }
  }

  trans->close();
  return failures == 0 ? 0 : -1;
}

static int runGroupByTest(Ndb *ndb) {
  V("\n=== Test: GROUP BY MIN/MAX over VARCHAR + CHAR ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  const NdbDictionary::Table *tab = dict->getTable("vctest_t");
  if (tab == nullptr) {
    fprintf(stderr, "Cannot find vctest_t: %s\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator agg(tab);
  if (!agg.GroupBy("grp") ||
      !agg.LoadColumn("vname", 0) ||
      !agg.Min(0, 0) ||
      !agg.Max(1, 0) ||
      !agg.LoadColumn("cname", 1) ||
      !agg.Min(2, 1) ||
      !agg.Max(3, 1) ||
      !agg.Finalize()) {
    fprintf(stderr, "GROUP BY aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == nullptr) {
    fprintf(stderr, "getNdbScanOperation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    fprintf(stderr, "readTuples failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->setAggregationCode(&agg) != 0) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->DoAggregation() != 0) {
    fprintf(stderr, "DoAggregation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }

  int failures = 0;
  bool seenGrp1 = false;
  bool seenGrp2 = false;
  while (true) {
    NdbAggregator::ResultRecord rec = agg.FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grp = rec.FetchGroupbyColumn();
    if (grp.is_null()) {
      fprintf(stderr, "FAIL group: GROUP BY column is NULL\n");
      failures++;
      continue;
    }
    Int32 groupNo = grp.data_int32();

    NdbAggregator::Result minV = rec.FetchAggregationResult();
    NdbAggregator::Result maxV = rec.FetchAggregationResult();
    NdbAggregator::Result minC = rec.FetchAggregationResult();
    NdbAggregator::Result maxC = rec.FetchAggregationResult();
    NdbAggregator::Result end = rec.FetchAggregationResult();
    if (!end.end()) {
      fprintf(stderr, "FAIL group %d: expected end after 4 aggregate slots\n",
              groupNo);
      failures++;
    }

    if (groupNo == 1) {
      seenGrp1 = true;
      if (verifyString("grp1 MIN(vname)", minV, "Alice") != 0) failures++;
      if (verifyString("grp1 MAX(vname)", maxV, "Bob")   != 0) failures++;
      if (verifyString("grp1 MIN(cname)", minC, "A1")    != 0) failures++;
      if (verifyString("grp1 MAX(cname)", maxC, "B2")    != 0) failures++;
    } else if (groupNo == 2) {
      seenGrp2 = true;
      if (verifyString("grp2 MIN(vname)", minV, "Charlie") != 0) failures++;
      if (verifyString("grp2 MAX(vname)", maxV, "Eve")     != 0) failures++;
      if (verifyString("grp2 MIN(cname)", minC, "C3")      != 0) failures++;
      if (verifyString("grp2 MAX(cname)", maxC, "E5")      != 0) failures++;
    } else {
      fprintf(stderr, "FAIL group: unexpected group %d\n", groupNo);
      failures++;
    }
  }

  if (!seenGrp1 || !seenGrp2) {
    fprintf(stderr, "FAIL group: missing expected groups, seen 1=%d 2=%d\n",
            seenGrp1, seenGrp2);
    failures++;
  }

  trans->close();
  return failures == 0 ? 0 : -1;
}

static void usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s [options]\n"
          "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
          "  -m <port>            MySQL port (default: 3306)\n"
          "  -v, --verbose        Verbose output\n"
          "  -h, --help           Show help\n",
          prog);
}

int main(int argc, char **argv) {
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }
  }

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout. */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("testVarcharMinMax: Phase I.6 F.2 string MIN/MAX kernel-API test\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);

  ndb_init();
  int result = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm: %s\n", connectString);
      ndb_end(0);
      return 1;
    }
    if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30s\n");
      ndb_end(0);
      return 1;
    }

    MYSQL *mysqlConn = connectMysql(mysqlPort);
    if (mysqlConn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      ndb_end(0);
      return 1;
    }

    if (setupSchema(mysqlConn) != 0) {
      mysql_close(mysqlConn);
      ndb_end(0);
      return 1;
    }

    {
      Ndb ndb(&clusterConn, "test");
      if (ndb.init() != 0) {
        fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
        dropSchema(mysqlConn);
        mysql_close(mysqlConn);
        ndb_end(0);
        return 1;
      }
      if (runScalarTest(&ndb) != 0) result = 1;
      if (runGroupByTest(&ndb) != 0) result = 1;
    }

    dropSchema(mysqlConn);
    mysql_close(mysqlConn);
  }

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  } else {
    write(mtr_fd, "FAILED\n", 7);
  }
  close(mtr_fd);

  return result;
}
