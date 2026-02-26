/*
   Copyright (c) 2025, Hopsworks and/or its affiliates.

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
 * ndbapi_vss_test.cpp
 *
 * Edge-case and boundary-condition tests for vector search pushdown.
 * Each test runs both pushdown and non-pushdown paths, then cross-validates.
 *
 * Test cases:
 *   1. top_n > table_size (100 vs 50)
 *   2. top_n = 1
 *   3. top_n = table_size (50 vs 50)
 *   4. Empty table (0 rows)
 *   5. Index scan with bounds only (no NdbScanFilter)
 *   6. Identical vectors (all rows same vector)
 *   7. Large top_n + large table (5000 vs 50000)
 *   8. Index scan with bounds + NdbScanFilter
 *   9. Index scan with empty range (bounds match nothing)
 *  10. Single row table (table_size=1, top_n=1)
 *  11. Zero vector target (all-zero query vector)
 *  12. Multiple iterations (3 targets, same table, no stale state)
 */

#ifdef _WIN32
#include <winsock2.h>
#else
#include <unistd.h>
#endif

#include <mysql.h>
#include <mysqld_error.h>
#include <NdbApi.hpp>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ndb_config.h>
#include <random>
#include <string>
#include <vector>
#include <queue>
#include <algorithm>

#include <AttributeHeader.hpp>
#include <NdbSleep.h>

#include <simsimd/simsimd.h>

/* ------------------------------------------------------------------ */
/*  Macros                                                            */
/* ------------------------------------------------------------------ */

#define PRINT_ERROR(code, msg)                                           \
  fprintf(stderr, "Error in %s, line: %d, code: %d, msg: %s.\n",        \
          __FILE__, __LINE__, code, msg)

#define MYSQLERROR(mysql)                                                \
  {                                                                      \
    PRINT_ERROR(mysql_errno(&(mysql)), mysql_error(&(mysql)));           \
    exit(-1);                                                            \
  }

#define APIERROR(error)                                                  \
  {                                                                      \
    PRINT_ERROR(error.code, error.message);                              \
    exit(-1);                                                            \
  }

/* ------------------------------------------------------------------ */
/*  Constants                                                         */
/* ------------------------------------------------------------------ */

#define DIMS 128
static const int BYTES_PER_VEC = DIMS * sizeof(float);

/* ------------------------------------------------------------------ */
/*  Global state                                                      */
/* ------------------------------------------------------------------ */

static std::mt19937 g_rng;
static float g_target_vec[DIMS];

static const char *TABLE_NAME = "vss_test_tbl";
static const char *INDEX_NAME = "index_val";
static const char *DB_NAME    = "test_ndb_vss_edge";

/* ------------------------------------------------------------------ */
/*  Config / arg parsing                                              */
/* ------------------------------------------------------------------ */

struct Config {
  std::string mysql_host = "127.0.0.1";
  std::string mysql_user = "root";
  std::string mysql_pwd  = "";
  int mysql_port = 3308;
};

static bool starts_with(const std::string &s, const std::string &prefix) {
  return s.size() >= prefix.size() &&
         std::equal(prefix.begin(), prefix.end(), s.begin());
}

static void parse_args(int argc, char **argv, Config &cfg) {
  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (starts_with(arg, "--mysql_host=")) {
      cfg.mysql_host = arg.substr(strlen("--mysql_host="));
    } else if (starts_with(arg, "--mysql_user=")) {
      cfg.mysql_user = arg.substr(strlen("--mysql_user="));
    } else if (starts_with(arg, "--mysql_pwd=")) {
      cfg.mysql_pwd = arg.substr(strlen("--mysql_pwd="));
    } else if (starts_with(arg, "--mysql_port=")) {
      cfg.mysql_port = std::stoi(arg.substr(strlen("--mysql_port=")));
    } else {
      fprintf(stderr, "[warn] Unknown argument: %s\n", arg.c_str());
    }
  }
}

/* ------------------------------------------------------------------ */
/*  Helpers: schema, insert, vector generation                        */
/* ------------------------------------------------------------------ */

static void ensure_schema_and_table(MYSQL &mysql) {
  {
    std::string sql = std::string("CREATE DATABASE IF NOT EXISTS `") +
                      DB_NAME + "`";
    if (mysql_query(&mysql, sql.c_str()) != 0) MYSQLERROR(mysql);
  }
  {
    std::string sql = std::string("USE `") + DB_NAME + "`";
    if (mysql_query(&mysql, sql.c_str()) != 0) MYSQLERROR(mysql);
  }

  std::string drop_sql = std::string("DROP TABLE IF EXISTS `") +
                          TABLE_NAME + "`";
  if (mysql_query(&mysql, drop_sql.c_str()) != 0) MYSQLERROR(mysql);

  char create_sql[512];
  snprintf(create_sql, sizeof(create_sql),
           "CREATE TABLE `%s` ("
           "  `pk`  INT NOT NULL,"
           "  `val` INT NOT NULL,"
           "  `vec` VARBINARY(%d) NOT NULL,"
           "  PRIMARY KEY (`pk`),"
           "  KEY `%s`(`val`)"
           ") ENGINE=NDBCLUSTER",
           TABLE_NAME, BYTES_PER_VEC, INDEX_NAME);

  if (mysql_query(&mysql, create_sql) != 0) MYSQLERROR(mysql);

  fprintf(stderr, "[test] Table %s.%s created (DIMS=%d)\n",
          DB_NAME, TABLE_NAME, DIMS);
}

static void gen_random_vec(std::vector<char> &out) {
  out.resize(BYTES_PER_VEC);
  float *f = reinterpret_cast<float *>(out.data());
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  for (int i = 0; i < DIMS; i++) {
    f[i] = dist(g_rng);
  }
}

static void gen_fixed_vec(std::vector<char> &out, float value) {
  out.resize(BYTES_PER_VEC);
  float *f = reinterpret_cast<float *>(out.data());
  for (int i = 0; i < DIMS; i++) {
    f[i] = value;
  }
}

static void init_target() {
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  for (int i = 0; i < DIMS; ++i) {
    g_target_vec[i] = dist(g_rng);
  }
}

static void truncate_table(MYSQL &mysql, Ndb *myNdb) {
  std::string sql = std::string("TRUNCATE TABLE `") + TABLE_NAME + "`";
  if (mysql_query(&mysql, sql.c_str()) != 0) MYSQLERROR(mysql);

  /* TRUNCATE on NDB drops+recreates the table, invalidating cached
   * dictionary objects. Force the NDB API to re-fetch table and index. */
  NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  myDict->invalidateIndex(INDEX_NAME, TABLE_NAME);
  myDict->invalidateTable(TABLE_NAME);
}

static void insert_rows(MYSQL &mysql, int n, bool identical_vectors = false,
                        int batch_size = 1000) {
  if (n <= 0) return;

  mysql_autocommit(&mysql, 0);

  char ins_sql[256];
  snprintf(ins_sql, sizeof(ins_sql),
           "INSERT INTO `%s` (pk, val, vec) VALUES (?,?,?)", TABLE_NAME);

  MYSQL_STMT *stmt = mysql_stmt_init(&mysql);
  mysql_stmt_prepare(stmt, ins_sql, strlen(ins_sql));

  MYSQL_BIND bind[3];
  memset(bind, 0, sizeof(bind));

  int pk, val;
  static std::vector<char> vec_buf(BYTES_PER_VEC);
  unsigned long vec_len = BYTES_PER_VEC;

  bind[0].buffer_type   = MYSQL_TYPE_LONG;
  bind[0].buffer        = &pk;
  bind[1].buffer_type   = MYSQL_TYPE_LONG;
  bind[1].buffer        = &val;
  bind[2].buffer_type   = MYSQL_TYPE_BLOB;
  bind[2].buffer        = vec_buf.data();
  bind[2].buffer_length = vec_len;
  bind[2].length        = &vec_len;

  mysql_stmt_bind_param(stmt, bind);

  /* Pre-generate the identical vector once if needed */
  std::vector<char> fixed_vec;
  if (identical_vectors) {
    gen_fixed_vec(fixed_vec, 0.5f);
  }

  for (pk = 1; pk <= n; pk++) {
    val = pk * 100;

    if (identical_vectors) {
      memcpy(vec_buf.data(), fixed_vec.data(), BYTES_PER_VEC);
    } else {
      gen_random_vec(vec_buf);
    }

    if (mysql_stmt_execute(stmt) != 0) {
      fprintf(stderr, "execute failed at pk=%d: %s\n",
              pk, mysql_stmt_error(stmt));
      exit(1);
    }

    if (pk % batch_size == 0)
      mysql_commit(&mysql);
  }

  mysql_commit(&mysql);
  mysql_stmt_close(stmt);
  mysql_autocommit(&mysql, 1);
}

/* ------------------------------------------------------------------ */
/*  Pushdown table scan vector search                                 */
/* ------------------------------------------------------------------ */

static int scan_vector_search(Ndb *myNdb, uint32_t top_n,
                              std::vector<Int32> &result_pks) {
  const int retryMax = 10;
  const NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Table *myTable = myDict->getTable(TABLE_NAME);
  if (myTable == NULL) APIERROR(myDict->getNdbError());

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      fprintf(stderr, "startTransaction failed: %s\n", e.message);
      return -1;
    }

    NdbScanOperation *myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL) {
      fprintf(stderr, "getNdbScanOperation failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead, 0, 0, 0) != 0) {
      APIERROR(myTrans->getNdbError());
    }

    NdbRecAttr *myRecAttr[3];
    myRecAttr[0] = myScanOp->getValue("pk");
    myRecAttr[1] = myScanOp->getValue("val");
    myRecAttr[2] = myScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      fprintf(stderr, "getValue failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    NdbAggregator aggregator(myTable);
    [[maybe_unused]] bool ret = aggregator.VectorSearch("vec", g_target_vec, DIMS, top_n);
    assert(ret);
    if (myScanOp->setAggregationCode(&aggregator) == -1) {
      fprintf(stderr, "setAggregationCode failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->DoVectorSearch(myRecAttr, 3) == -1) {
      NdbError err = myTrans->getNdbError();
      fprintf(stderr, "DoVectorSearch failed: %s\n", err.message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    result_pks.clear();
    while (aggregator.VecFetchNextResult()) {
      result_pks.push_back(myRecAttr[0]->int32_value());
    }

    myNdb->closeTransaction(myTrans);
    return 0;
  }

  fprintf(stderr, "scan_vector_search retried %d times, failing.\n",
          retryMax);
  return -1;
}

/* ------------------------------------------------------------------ */
/*  Non-pushdown table scan vector search                             */
/* ------------------------------------------------------------------ */

static int table_scan_regular(Ndb *myNdb, uint32_t top_n,
                              std::vector<Int32> &result_pks) {
  const int retryMax = 10;
  const NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Table *myTable = myDict->getTable(TABLE_NAME);
  if (myTable == NULL) APIERROR(myDict->getNdbError());

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      fprintf(stderr, "startTransaction failed: %s\n", e.message);
      return -1;
    }

    NdbScanOperation *myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL) {
      fprintf(stderr, "getNdbScanOperation failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead) != 0) {
      APIERROR(myTrans->getNdbError());
    }

    NdbRecAttr *myRecAttr[3];
    myRecAttr[0] = myScanOp->getValue("pk");
    myRecAttr[1] = myScanOp->getValue("val");
    myRecAttr[2] = myScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      fprintf(stderr, "getValue failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myTrans->execute(NdbTransaction::NoCommit) != 0) {
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    std::priority_queue<NdbAggregator::VectorSearchResult *,
                        std::vector<NdbAggregator::VectorSearchResult *>,
                        NdbAggregator::ByDistance>
        vec_results;
    int check = -1;
    double distance = 0;

    while ((check = myScanOp->nextResult(true)) == 0) {
      float *current = (float *)((char *)(myRecAttr[2]->aRef()) + 2);
      simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance);

      if (top_n != 0 &&
          (vec_results.size() < top_n ||
           distance < vec_results.top()->distance_)) {
        if (vec_results.size() == top_n) {
          NdbAggregator::VectorSearchResult *kickout = vec_results.top();
          vec_results.pop();
          delete kickout;
        }
        NdbAggregator::VectorSearchResult *candidate =
            new NdbAggregator::VectorSearchResult(distance, 2, myRecAttr);
        vec_results.push(candidate);
      }
    }

    /* Extract results in ascending-distance order */
    std::vector<NdbAggregator::VectorSearchResult *> vec_results_final;
    while (!vec_results.empty()) {
      vec_results_final.push_back(vec_results.top());
      vec_results.pop();
    }

    result_pks.clear();
    std::for_each(
        vec_results_final.rbegin(), vec_results_final.rend(),
        [&result_pks](NdbAggregator::VectorSearchResult *candidate) {
          result_pks.push_back(candidate->attrs_[0]->int32_value());
          delete candidate;
        });

    myNdb->closeTransaction(myTrans);
    return 0;
  }

  fprintf(stderr, "table_scan_regular retried %d times, failing.\n",
          retryMax);
  return -1;
}

/* ------------------------------------------------------------------ */
/*  Pushdown index scan with bounds only (no NdbScanFilter)           */
/* ------------------------------------------------------------------ */

static int index_scan_vector_search_bounds_only(
    Ndb *myNdb, uint32_t top_n, Uint32 low_bound, Uint32 high_bound,
    std::vector<Int32> &result_pks) {
  NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex =
      myDict->getIndex(INDEX_NAME, TABLE_NAME);
  if (myPIndex == NULL) APIERROR(myDict->getNdbError());

  NdbTransaction *myTrans = myNdb->startTransaction();
  if (myTrans == NULL) APIERROR(myNdb->getNdbError());

  NdbIndexScanOperation *myIndexScanOp =
      myTrans->getNdbIndexScanOperation(myPIndex);

  Uint32 scanFlags =
      NdbScanOperation::SF_OrderBy | NdbScanOperation::SF_MultiRange;

  if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                scanFlags) != 0) {
    APIERROR(myTrans->getNdbError());
  }

  /* Bounds only: val >= low_bound AND val < high_bound */
  if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundLE,
                              (char *)&low_bound)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundGT,
                              (char *)&high_bound)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->end_of_bound(0)) {
    APIERROR(myIndexScanOp->getNdbError());
  }

  NdbRecAttr *myRecAttr[3];
  myRecAttr[0] = myIndexScanOp->getValue("pk");
  myRecAttr[1] = myIndexScanOp->getValue("val");
  myRecAttr[2] = myIndexScanOp->getValue("vec");
  if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr ||
      myRecAttr[2] == nullptr) {
    fprintf(stderr, "getValue failed: %s\n",
            myTrans->getNdbError().message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  const NdbDictionary::Table *myTable = myDict->getTable(TABLE_NAME);
  if (myTable == NULL) APIERROR(myDict->getNdbError());

  NdbAggregator aggregator(myTable);
  [[maybe_unused]] bool ret = aggregator.VectorSearch("vec", g_target_vec, DIMS, top_n);
  assert(ret);
  if (myIndexScanOp->setAggregationCode(&aggregator) == -1) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            myTrans->getNdbError().message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  if (myIndexScanOp->DoVectorSearch(myRecAttr, 3) == -1) {
    NdbError err = myTrans->getNdbError();
    fprintf(stderr, "DoVectorSearch failed: %s\n", err.message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  result_pks.clear();
  while (aggregator.VecFetchNextResult()) {
    result_pks.push_back(myRecAttr[0]->int32_value());
  }

  myNdb->closeTransaction(myTrans);
  return 0;
}

/* ------------------------------------------------------------------ */
/*  Non-pushdown index scan with bounds only (no NdbScanFilter)       */
/* ------------------------------------------------------------------ */

static int index_scan_regular_bounds_only(
    Ndb *myNdb, uint32_t top_n, Uint32 low_bound, Uint32 high_bound,
    std::vector<Int32> &result_pks) {
  NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex =
      myDict->getIndex(INDEX_NAME, TABLE_NAME);
  if (myPIndex == NULL) APIERROR(myDict->getNdbError());

  const int retryMax = 10;

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      fprintf(stderr, "startTransaction failed: %s\n", e.message);
      return -1;
    }

    NdbIndexScanOperation *myIndexScanOp =
        myTrans->getNdbIndexScanOperation(myPIndex);

    Uint32 scanFlags =
        NdbScanOperation::SF_OrderBy | NdbScanOperation::SF_MultiRange;

    if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                  scanFlags) != 0) {
      APIERROR(myTrans->getNdbError());
    }

    if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundLE,
                                (char *)&low_bound)) {
      APIERROR(myTrans->getNdbError());
    }
    if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundGT,
                                (char *)&high_bound)) {
      APIERROR(myTrans->getNdbError());
    }
    if (myIndexScanOp->end_of_bound(0)) {
      APIERROR(myIndexScanOp->getNdbError());
    }

    NdbRecAttr *myRecAttr[3];
    myRecAttr[0] = myIndexScanOp->getValue("pk");
    myRecAttr[1] = myIndexScanOp->getValue("val");
    myRecAttr[2] = myIndexScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      fprintf(stderr, "getValue failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myTrans->execute(NdbTransaction::NoCommit) != 0) {
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    std::priority_queue<NdbAggregator::VectorSearchResult *,
                        std::vector<NdbAggregator::VectorSearchResult *>,
                        NdbAggregator::ByDistance>
        vec_results;
    int check = -1;
    double distance = 0;

    while ((check = myIndexScanOp->nextResult(true)) == 0) {
      float *current = (float *)((char *)(myRecAttr[2]->aRef()) + 2);
      simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance);

      if (top_n != 0 &&
          (vec_results.size() < top_n ||
           distance < vec_results.top()->distance_)) {
        if (vec_results.size() == top_n) {
          NdbAggregator::VectorSearchResult *kickout = vec_results.top();
          vec_results.pop();
          delete kickout;
        }
        NdbAggregator::VectorSearchResult *candidate =
            new NdbAggregator::VectorSearchResult(distance, 2, myRecAttr);
        vec_results.push(candidate);
      }
    }

    std::vector<NdbAggregator::VectorSearchResult *> vec_results_final;
    while (!vec_results.empty()) {
      vec_results_final.push_back(vec_results.top());
      vec_results.pop();
    }

    result_pks.clear();
    std::for_each(
        vec_results_final.rbegin(), vec_results_final.rend(),
        [&result_pks](NdbAggregator::VectorSearchResult *candidate) {
          result_pks.push_back(candidate->attrs_[0]->int32_value());
          delete candidate;
        });

    myNdb->closeTransaction(myTrans);
    return 0;
  }

  fprintf(stderr, "index_scan_regular_bounds_only retried %d times.\n",
          retryMax);
  return -1;
}

/* ------------------------------------------------------------------ */
/*  Pushdown index scan with bounds + NdbScanFilter                   */
/* ------------------------------------------------------------------ */

static int index_scan_vector_search_bounds_filter(
    Ndb *myNdb, uint32_t top_n, Uint32 low_bound, Uint32 high_bound,
    Uint32 pk_filter_lt, std::vector<Int32> &result_pks) {
  NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex =
      myDict->getIndex(INDEX_NAME, TABLE_NAME);
  if (myPIndex == NULL) APIERROR(myDict->getNdbError());

  NdbTransaction *myTrans = myNdb->startTransaction();
  if (myTrans == NULL) APIERROR(myNdb->getNdbError());

  NdbIndexScanOperation *myIndexScanOp =
      myTrans->getNdbIndexScanOperation(myPIndex);

  Uint32 scanFlags =
      NdbScanOperation::SF_OrderBy | NdbScanOperation::SF_MultiRange;

  if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                scanFlags) != 0) {
    APIERROR(myTrans->getNdbError());
  }

  if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundLE,
                              (char *)&low_bound)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundGT,
                              (char *)&high_bound)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->end_of_bound(0)) {
    APIERROR(myIndexScanOp->getNdbError());
  }

  /* NdbScanFilter: pk < pk_filter_lt */
  NdbScanFilter filter(myIndexScanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_LT, 0, &pk_filter_lt,
                 sizeof(pk_filter_lt)) < 0 ||
      filter.end() < 0) {
    fprintf(stderr, "NdbScanFilter setup failed: %s\n",
            myTrans->getNdbError().message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  NdbRecAttr *myRecAttr[3];
  myRecAttr[0] = myIndexScanOp->getValue("pk");
  myRecAttr[1] = myIndexScanOp->getValue("val");
  myRecAttr[2] = myIndexScanOp->getValue("vec");
  if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr ||
      myRecAttr[2] == nullptr) {
    fprintf(stderr, "getValue failed: %s\n",
            myTrans->getNdbError().message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  const NdbDictionary::Table *myTable = myDict->getTable(TABLE_NAME);
  if (myTable == NULL) APIERROR(myDict->getNdbError());

  NdbAggregator aggregator(myTable);
  [[maybe_unused]] bool ret = aggregator.VectorSearch("vec", g_target_vec, DIMS, top_n);
  assert(ret);
  if (myIndexScanOp->setAggregationCode(&aggregator) == -1) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            myTrans->getNdbError().message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  if (myIndexScanOp->DoVectorSearch(myRecAttr, 3) == -1) {
    NdbError err = myTrans->getNdbError();
    fprintf(stderr, "DoVectorSearch failed: %s\n", err.message);
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  result_pks.clear();
  while (aggregator.VecFetchNextResult()) {
    result_pks.push_back(myRecAttr[0]->int32_value());
  }

  myNdb->closeTransaction(myTrans);
  return 0;
}

/* ------------------------------------------------------------------ */
/*  Non-pushdown index scan with bounds + NdbScanFilter               */
/* ------------------------------------------------------------------ */

static int index_scan_regular_bounds_filter(
    Ndb *myNdb, uint32_t top_n, Uint32 low_bound, Uint32 high_bound,
    Uint32 pk_filter_lt, std::vector<Int32> &result_pks) {
  NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex =
      myDict->getIndex(INDEX_NAME, TABLE_NAME);
  if (myPIndex == NULL) APIERROR(myDict->getNdbError());

  const int retryMax = 10;

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      fprintf(stderr, "startTransaction failed: %s\n", e.message);
      return -1;
    }

    NdbIndexScanOperation *myIndexScanOp =
        myTrans->getNdbIndexScanOperation(myPIndex);

    Uint32 scanFlags =
        NdbScanOperation::SF_OrderBy | NdbScanOperation::SF_MultiRange;

    if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                  scanFlags) != 0) {
      APIERROR(myTrans->getNdbError());
    }

    if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundLE,
                                (char *)&low_bound)) {
      APIERROR(myTrans->getNdbError());
    }
    if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundGT,
                                (char *)&high_bound)) {
      APIERROR(myTrans->getNdbError());
    }
    if (myIndexScanOp->end_of_bound(0)) {
      APIERROR(myIndexScanOp->getNdbError());
    }

    /* NdbScanFilter: pk < pk_filter_lt */
    NdbScanFilter filter(myIndexScanOp);
    if (filter.begin(NdbScanFilter::AND) < 0 ||
        filter.cmp(NdbScanFilter::COND_LT, 0, &pk_filter_lt,
                   sizeof(pk_filter_lt)) < 0 ||
        filter.end() < 0) {
      fprintf(stderr, "NdbScanFilter setup failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    NdbRecAttr *myRecAttr[3];
    myRecAttr[0] = myIndexScanOp->getValue("pk");
    myRecAttr[1] = myIndexScanOp->getValue("val");
    myRecAttr[2] = myIndexScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      fprintf(stderr, "getValue failed: %s\n",
              myTrans->getNdbError().message);
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myTrans->execute(NdbTransaction::NoCommit) != 0) {
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    std::priority_queue<NdbAggregator::VectorSearchResult *,
                        std::vector<NdbAggregator::VectorSearchResult *>,
                        NdbAggregator::ByDistance>
        vec_results;
    int check = -1;
    double distance = 0;

    while ((check = myIndexScanOp->nextResult(true)) == 0) {
      float *current = (float *)((char *)(myRecAttr[2]->aRef()) + 2);
      simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance);

      if (top_n != 0 &&
          (vec_results.size() < top_n ||
           distance < vec_results.top()->distance_)) {
        if (vec_results.size() == top_n) {
          NdbAggregator::VectorSearchResult *kickout = vec_results.top();
          vec_results.pop();
          delete kickout;
        }
        NdbAggregator::VectorSearchResult *candidate =
            new NdbAggregator::VectorSearchResult(distance, 2, myRecAttr);
        vec_results.push(candidate);
      }
    }

    std::vector<NdbAggregator::VectorSearchResult *> vec_results_final;
    while (!vec_results.empty()) {
      vec_results_final.push_back(vec_results.top());
      vec_results.pop();
    }

    result_pks.clear();
    std::for_each(
        vec_results_final.rbegin(), vec_results_final.rend(),
        [&result_pks](NdbAggregator::VectorSearchResult *candidate) {
          result_pks.push_back(candidate->attrs_[0]->int32_value());
          delete candidate;
        });

    myNdb->closeTransaction(myTrans);
    return 0;
  }

  fprintf(stderr, "index_scan_regular_bounds_filter retried %d times.\n",
          retryMax);
  return -1;
}

/* ------------------------------------------------------------------ */
/*  PK-lookup for tie-breaking verification (DoubleCheck)             */
/* ------------------------------------------------------------------ */

static bool DoubleCheck(Ndb *myNdb, Int32 pk_1, Int32 pk_2) {
  double distance_1 = 0, distance_2 = 0;
  const NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
  const NdbDictionary::Table *myTable = myDict->getTable(TABLE_NAME);
  if (myTable == NULL) APIERROR(myDict->getNdbError());

  fprintf(stderr, "[doublecheck] pk %d vs %d\n", pk_1, pk_2);

  {
    NdbTransaction *myTransaction = myNdb->startTransaction();
    if (myTransaction == NULL) APIERROR(myNdb->getNdbError());
    NdbOperation *myOperation = myTransaction->getNdbOperation(myTable);
    if (myOperation == NULL) APIERROR(myTransaction->getNdbError());
    myOperation->readTuple(NdbOperation::LM_CommittedRead);
    myOperation->equal("pk", pk_1);
    NdbRecAttr *myRecAttr = myOperation->getValue("vec", NULL);
    if (myRecAttr == NULL) APIERROR(myTransaction->getNdbError());
    if (myTransaction->execute(NdbTransaction::Commit) == -1)
      APIERROR(myTransaction->getNdbError());
    float *current = (float *)((char *)(myRecAttr->aRef()) + 2);
    simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance_1);
    myNdb->closeTransaction(myTransaction);
  }

  {
    NdbTransaction *myTransaction = myNdb->startTransaction();
    if (myTransaction == NULL) APIERROR(myNdb->getNdbError());
    NdbOperation *myOperation = myTransaction->getNdbOperation(myTable);
    if (myOperation == NULL) APIERROR(myTransaction->getNdbError());
    myOperation->readTuple(NdbOperation::LM_CommittedRead);
    myOperation->equal("pk", pk_2);
    NdbRecAttr *myRecAttr = myOperation->getValue("vec", NULL);
    if (myRecAttr == NULL) APIERROR(myTransaction->getNdbError());
    if (myTransaction->execute(NdbTransaction::Commit) == -1)
      APIERROR(myTransaction->getNdbError());
    float *current = (float *)((char *)(myRecAttr->aRef()) + 2);
    simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance_2);
    myNdb->closeTransaction(myTransaction);
  }

  if (distance_1 != distance_2) {
    fprintf(stderr, "[doublecheck] MISMATCH: [pk:%d dist:%lf] != "
                    "[pk:%d dist:%lf]\n",
            pk_1, distance_1, pk_2, distance_2);
  }
  return distance_1 == distance_2;
}

/* ------------------------------------------------------------------ */
/*  Cross-validation helper                                           */
/* ------------------------------------------------------------------ */

static int validate_results(Ndb *myNdb,
                            const std::vector<Int32> &pushdown,
                            const std::vector<Int32> &baseline,
                            const char *label) {
  if (pushdown.size() != baseline.size()) {
    fprintf(stderr, "[%s] Size mismatch: pushdown=%zu, baseline=%zu\n",
            label, pushdown.size(), baseline.size());
    return 1;
  }

  for (size_t j = 0; j < pushdown.size(); j++) {
    if (pushdown[j] != baseline[j]) {
      if (!DoubleCheck(myNdb, pushdown[j], baseline[j])) {
        fprintf(stderr, "[%s] Mismatch at index %zu: pushdown pk %d, "
                        "baseline pk %d\n",
                label, j, pushdown[j], baseline[j]);
        return 1;
      }
    }
  }
  return 0;
}

/* ================================================================== */
/*  Test cases                                                        */
/* ================================================================== */

/*
 * Test 1: top_n > table_size
 *   top_n=100, table_size=50  => should return all 50 rows
 */
static int run_test_top_n_gt_table_size(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 100;
  const int table_size = 50;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != (size_t)table_size) {
    fprintf(stderr, "[top_n_gt_table_size] Expected %d results, got %zu\n",
            table_size, pushdown_pks.size());
    return 1;
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks,
                          "top_n_gt_table_size");
}

/*
 * Test 2: top_n = 1
 *   Minimum non-zero top_n
 */
static int run_test_top_n_eq_1(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 1;
  const int table_size = 100;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != 1) {
    fprintf(stderr, "[top_n_eq_1] Expected 1 result, got %zu\n",
            pushdown_pks.size());
    return 1;
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks, "top_n_eq_1");
}

/*
 * Test 3: top_n = table_size
 *   Every row is a candidate; no knockouts
 */
static int run_test_top_n_eq_table_size(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 50;
  const int table_size = 50;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != (size_t)table_size) {
    fprintf(stderr, "[top_n_eq_table_size] Expected %d results, got %zu\n",
            table_size, pushdown_pks.size());
    return 1;
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks,
                          "top_n_eq_table_size");
}

/*
 * Test 4: Empty table
 *   Zero rows to scan; both paths should return 0 results
 */
static int run_test_empty_table(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;

  truncate_table(mysql, myNdb);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (!pushdown_pks.empty()) {
    fprintf(stderr, "[empty_table] Pushdown returned %zu results on empty "
                    "table\n",
            pushdown_pks.size());
    return 1;
  }
  if (!baseline_pks.empty()) {
    fprintf(stderr, "[empty_table] Baseline returned %zu results on empty "
                    "table\n",
            baseline_pks.size());
    return 1;
  }

  return 0;
}

/*
 * Test 5: Index scan, bounds only (no NdbScanFilter)
 *   val in [10000, 100000) => pk in [100, 1000)
 */
static int run_test_index_bounds_only(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;
  const int table_size = 1000;
  const Uint32 low_bound  = 10000;   /* val >= 10000  => pk >= 100 */
  const Uint32 high_bound = 100000;  /* val < 100000  => pk < 1000 */

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (index_scan_vector_search_bounds_only(myNdb, top_n, low_bound,
                                           high_bound, pushdown_pks) != 0)
    return 1;
  if (index_scan_regular_bounds_only(myNdb, top_n, low_bound, high_bound,
                                     baseline_pks) != 0)
    return 1;

  /* All returned pk values should have val in [low_bound, high_bound) */
  for (Int32 pk : pushdown_pks) {
    Int32 val = pk * 100;
    if (val < (Int32)low_bound || val >= (Int32)high_bound) {
      fprintf(stderr, "[index_bounds_only] pk %d has val %d outside "
                      "bounds [%u, %u)\n",
              pk, val, low_bound, high_bound);
      return 1;
    }
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks,
                          "index_bounds_only");
}

/*
 * Test 6: Identical vectors
 *   All rows have the same vector => all distances are equal.
 *   Both paths should return the same number of results (top_n).
 *   We only validate sizes since ordering among ties is undefined.
 */
static int run_test_identical_vectors(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;
  const int table_size = 100;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size, /* identical_vectors= */ true);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != top_n) {
    fprintf(stderr, "[identical_vectors] Pushdown: expected %u, got %zu\n",
            top_n, pushdown_pks.size());
    return 1;
  }
  if (baseline_pks.size() != top_n) {
    fprintf(stderr, "[identical_vectors] Baseline: expected %u, got %zu\n",
            top_n, baseline_pks.size());
    return 1;
  }

  /* With identical distances, order among ties is undefined.
   * Only validate that both returned the same count. */
  return 0;
}

/*
 * Test 7: Large top_n + large table
 *   top_n=5000, table_size=50000
 *   Stresses multi-segment candidate allocation.
 */
static int run_test_large_top_n(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 5000;
  const int table_size = 50000;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != top_n) {
    fprintf(stderr, "[large_top_n] Pushdown: expected %u, got %zu\n",
            top_n, pushdown_pks.size());
    return 1;
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks, "large_top_n");
}

/*
 * Test 8: Index scan with bounds + NdbScanFilter
 *   val in [10000, 100000) AND pk < 500
 *   Exercises the combined bounds+filter code path.
 */
static int run_test_index_bounds_filter(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;
  const int table_size = 1000;
  const Uint32 low_bound    = 10000;   /* val >= 10000  => pk >= 100 */
  const Uint32 high_bound   = 100000;  /* val < 100000  => pk < 1000 */
  const Uint32 pk_filter_lt = 500;     /* pk < 500 */

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (index_scan_vector_search_bounds_filter(myNdb, top_n, low_bound,
                                             high_bound, pk_filter_lt,
                                             pushdown_pks) != 0)
    return 1;
  if (index_scan_regular_bounds_filter(myNdb, top_n, low_bound, high_bound,
                                       pk_filter_lt, baseline_pks) != 0)
    return 1;

  /* All returned pk values should satisfy both bounds and filter */
  for (Int32 pk : pushdown_pks) {
    Int32 val = pk * 100;
    if (val < (Int32)low_bound || val >= (Int32)high_bound ||
        pk >= (Int32)pk_filter_lt) {
      fprintf(stderr,
              "[index_bounds_filter] pk %d (val %d) outside bounds "
              "[%u,%u) or pk >= %u\n",
              pk, val, low_bound, high_bound, pk_filter_lt);
      return 1;
    }
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks,
                          "index_bounds_filter");
}

/*
 * Test 9: Index scan with empty range
 *   Bounds that match zero rows — exercises index scan returning nothing.
 */
static int run_test_index_empty_range(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;
  const int table_size = 100;
  /* val = pk * 100, so val ranges [100, 10000].
   * Setting bounds to [999999, 9999999) matches nothing. */
  const Uint32 low_bound  = 999999;
  const Uint32 high_bound = 9999999;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (index_scan_vector_search_bounds_only(myNdb, top_n, low_bound,
                                           high_bound, pushdown_pks) != 0)
    return 1;
  if (index_scan_regular_bounds_only(myNdb, top_n, low_bound, high_bound,
                                     baseline_pks) != 0)
    return 1;

  if (!pushdown_pks.empty()) {
    fprintf(stderr,
            "[index_empty_range] Pushdown returned %zu on empty range\n",
            pushdown_pks.size());
    return 1;
  }
  if (!baseline_pks.empty()) {
    fprintf(stderr,
            "[index_empty_range] Baseline returned %zu on empty range\n",
            baseline_pks.size());
    return 1;
  }

  return 0;
}

/*
 * Test 10: Single row table
 *   table_size=1, top_n=1 — absolute minimum data.
 */
static int run_test_single_row(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 1;
  const int table_size = 1;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);
  init_target();

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != 1) {
    fprintf(stderr, "[single_row] Pushdown: expected 1, got %zu\n",
            pushdown_pks.size());
    return 1;
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks, "single_row");
}

/*
 * Test 11: Zero vector target
 *   Query vector is all zeros — degenerate distance computation.
 */
static int run_test_zero_target(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;
  const int table_size = 100;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);

  /* Set target to all zeros instead of random */
  memset(g_target_vec, 0, sizeof(g_target_vec));

  std::vector<Int32> pushdown_pks, baseline_pks;

  if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) return 1;
  if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) return 1;

  if (pushdown_pks.size() != top_n) {
    fprintf(stderr, "[zero_target] Pushdown: expected %u, got %zu\n",
            top_n, pushdown_pks.size());
    return 1;
  }

  return validate_results(myNdb, pushdown_pks, baseline_pks, "zero_target");
}

/*
 * Test 12: Multiple iterations
 *   Same table, 3 different random targets — verifies no stale state.
 */
static int run_test_multi_iteration(Ndb *myNdb, MYSQL &mysql) {
  const uint32_t top_n = 10;
  const int table_size = 200;

  truncate_table(mysql, myNdb);
  insert_rows(mysql, table_size);

  for (int iter = 0; iter < 3; iter++) {
    init_target();

    std::vector<Int32> pushdown_pks, baseline_pks;

    if (scan_vector_search(myNdb, top_n, pushdown_pks) != 0) {
      fprintf(stderr, "[multi_iteration] Pushdown failed at iter %d\n", iter);
      return 1;
    }
    if (table_scan_regular(myNdb, top_n, baseline_pks) != 0) {
      fprintf(stderr, "[multi_iteration] Baseline failed at iter %d\n", iter);
      return 1;
    }

    if (pushdown_pks.size() != top_n) {
      fprintf(stderr,
              "[multi_iteration] iter %d: expected %u, got %zu\n",
              iter, top_n, pushdown_pks.size());
      return 1;
    }

    char label[64];
    snprintf(label, sizeof(label), "multi_iteration[%d]", iter);
    if (validate_results(myNdb, pushdown_pks, baseline_pks, label) != 0) {
      return 1;
    }
  }

  return 0;
}

/* ================================================================== */
/*  Test runner                                                       */
/* ================================================================== */

struct TestCase {
  const char *name;
  int (*run_fn)(Ndb *, MYSQL &);
};

static TestCase test_cases[] = {
    {"top_n_gt_table_size",  run_test_top_n_gt_table_size},
    {"top_n_eq_1",           run_test_top_n_eq_1},
    {"top_n_eq_table_size",  run_test_top_n_eq_table_size},
    {"empty_table",          run_test_empty_table},
    {"index_bounds_only",    run_test_index_bounds_only},
    {"identical_vectors",    run_test_identical_vectors},
    {"large_top_n",          run_test_large_top_n},
    {"index_bounds_filter",  run_test_index_bounds_filter},
    {"index_empty_range",    run_test_index_empty_range},
    {"single_row",           run_test_single_row},
    {"zero_target",          run_test_zero_target},
    {"multi_iteration",      run_test_multi_iteration},
};

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr,
            "Usage: %s <ndb_connectstring> "
            "[--mysql_host=127.0.0.1] [--mysql_port=3308] "
            "[--mysql_user=root] [--mysql_pwd=STRING]\n",
            argv[0]);
    return 1;
  }

  const char *connectstring = argv[1];

  Config cfg;
  parse_args(argc, argv, cfg);

  /* Fixed seed for deterministic results */
  g_rng.seed(42);

  /* MySQL connection */
  MYSQL mysql;
  mysql_init(&mysql);
  if (!mysql_real_connect(&mysql, cfg.mysql_host.c_str(),
                          cfg.mysql_user.c_str(), cfg.mysql_pwd.c_str(),
                          nullptr, cfg.mysql_port, nullptr, 0)) {
    fprintf(stderr, "mysql_real_connect failed: %s\n",
            mysql_error(&mysql));
    return 1;
  }

  /* Create schema and table */
  ensure_schema_and_table(mysql);

  /* NDB connection — scoped so objects are destroyed before ndb_end() */
  ndb_init();

  int failures = 0;
  {
    Ndb_cluster_connection cluster_connection(connectstring);
    if (cluster_connection.connect(4, 5, 1)) {
      fprintf(stderr, "Unable to connect to cluster within 30 secs.\n");
      ndb_end(0);
      mysql_close(&mysql);
      return 1;
    }
    if (cluster_connection.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster was not ready within 30 secs.\n");
      ndb_end(0);
      mysql_close(&mysql);
      return 1;
    }

    Ndb myNdb(&cluster_connection, DB_NAME);
    if (myNdb.init(1024) == -1) {
      APIERROR(myNdb.getNdbError());
    }

    /* Run test cases */
    int n_tests = sizeof(test_cases) / sizeof(test_cases[0]);

    for (int i = 0; i < n_tests; i++) {
      TestCase &tc = test_cases[i];
      fprintf(stderr, "\n=== Running: %s ===\n", tc.name);

      int result = tc.run_fn(&myNdb, mysql);

      /* Output to stdout for MTR result matching */
      printf("[TEST] %s: %s\n", tc.name, result == 0 ? "PASS" : "FAIL");
      fflush(stdout);

      if (result != 0) failures++;
    }
  } /* Ndb and cluster_connection destroyed here */

  /* Cleanup */
  {
    std::string sql = std::string("DROP DATABASE IF EXISTS `") +
                      DB_NAME + "`";
    mysql_query(&mysql, sql.c_str());
  }

  ndb_end(0);
  mysql_close(&mysql);

  return failures > 0 ? 1 : 0;
}
