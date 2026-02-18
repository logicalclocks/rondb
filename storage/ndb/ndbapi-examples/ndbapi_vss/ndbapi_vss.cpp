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

#ifdef _WIN32
#include <winsock2.h>
#else
#include <unistd.h> // sleep
#endif

#include <mysql.h>
#include <mysqld_error.h>
#include <NdbApi.hpp>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <ndb_config.h>
#include <random>
#include <string>
#include <vector>
#include <queue>

#include <AttributeHeader.hpp>
#include <NdbSleep.h>

#include <simsimd/simsimd.h>

/**
 * Helper debugging macros
 */
#define PRINT_ERROR(code,msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
  << ", code: " << code \
  << ", msg: " << msg << "." << std::endl

#define MYSQLERROR(mysql) { \
  PRINT_ERROR(mysql_errno(&(mysql)),mysql_error(&(mysql))); \
  exit(-1); }

#define APIERROR(error) { \
  PRINT_ERROR(error.code,error.message); \
  exit(-1); }

#define DIMS       1024
#define VEC_TOP_N  1000
static const int BYTES_PER_VEC = DIMS * sizeof(float);

std::vector<Int32> res_vs;
std::vector<Int32> res_scan;

static std::mt19937 g_rng;
static bool g_has_seed = false;
static uint64_t g_seed_value = 0;
static float g_target_vec[DIMS];
static uint32_t g_top_n = 10;

static std::string get_env_or_default(const char *name,
                                      const char *def_value) {
  const char *v = std::getenv(name);
  if (v == nullptr || *v == '\0')
    return std::string(def_value);
  return std::string(v);
}

static bool starts_with(const std::string &s, const std::string &prefix) {
  return s.size() >= prefix.size() &&
         std::equal(prefix.begin(), prefix.end(), s.begin());
}

struct Config {
  bool do_load = true;
  int iters = 1;
  bool has_seed = false;
  uint64_t seed = 0;
  int32_t top_n = 10;
	std::string mysql_host = "127.0.0.1";
  std::string mysql_user = "root";
  std::string mysql_pwd = "";
  int mysql_port = 3308;
  int table_size = 10000;
};

static void parse_args(int argc, char **argv, Config &cfg) {
  // argv[0] = prog
  // argv[1] = ndb connectstring
  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (starts_with(arg, "--load=")) {
      std::string v = arg.substr(strlen("--load="));
      cfg.do_load = (v != "0");

    } else if (starts_with(arg, "--iters=")) {
      std::string v = arg.substr(strlen("--iters="));
      cfg.iters = std::stoi(v);
      if (cfg.iters < 1) cfg.iters = 1;

    } else if (starts_with(arg, "--seed=")) {
      std::string v = arg.substr(strlen("--seed="));
      cfg.has_seed = true;
      cfg.seed = static_cast<uint64_t>(std::stoull(v));

    } else if (starts_with(arg, "--top_n=")) {
      std::string v = arg.substr(strlen("--top_n="));
      cfg.top_n = std::stoi(v);
      if (cfg.top_n < 0) cfg.top_n = 0;

		} else if (starts_with(arg, "--mysql_host=")) {
			cfg.mysql_host = arg.substr(strlen("--mysql_host="));

		} else if (starts_with(arg, "--mysql_user=")) {
			cfg.mysql_user = arg.substr(strlen("--mysql_user="));

		} else if (starts_with(arg, "--mysql_pwd=")) {
			cfg.mysql_pwd = arg.substr(strlen("--mysql_pwd="));

		} else if (starts_with(arg, "--mysql_port=")) {
			cfg.mysql_port = std::stoi(arg.substr(strlen("--mysql_port=")));

		} else if (starts_with(arg, "--table_size=")) {
			cfg.table_size = std::stoi(arg.substr(strlen("--table_size=")));
			if (cfg.table_size < 1) cfg.table_size = 1;
		} else {
			std::cerr << "[warn] Unknown argument: " << arg << std::endl;
		}
  }
}

static void init_rng(const Config &cfg) {
  uint64_t seed = 0;

  if (cfg.has_seed) {
    seed = cfg.seed;
    std::cerr << "[rng] Using seed from CLI: " << seed << std::endl;
  } else {
    std::string seed_env = get_env_or_default("VEC_SEED", "");
    if (!seed_env.empty()) {
      seed = static_cast<uint64_t>(std::stoull(seed_env));
      std::cerr << "[rng] Using seed from env VEC_SEED: " << seed << std::endl;
    } else {
      std::random_device rd;
      seed = (static_cast<uint64_t>(rd()) << 32) ^ rd();
      std::cerr << "[rng] Using random_device seed: " << seed << std::endl;
    }
  }

	g_top_n = cfg.top_n;
  g_has_seed = true;
  g_seed_value = seed;
  g_rng.seed(seed);
}

static void init_target() {
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  for (int i = 0; i < DIMS; ++i) {
    g_target_vec[i] = dist(g_rng);
    // g_target_vec[i] = 0.5;
  }

  std::cerr << "[rng] Target vector initialized with dim=" << DIMS
            << ", range approx [-1,1)" << std::endl;
}

static void ensure_schema_and_table(MYSQL &mysql, Config &cfg) {
  std::string db_name = get_env_or_default("MYSQL_DB", "test_ndb_vec");

  // CREATE DATABASE IF NOT EXISTS `DB_NAME`
  {
    std::string sql = "CREATE DATABASE IF NOT EXISTS `" + db_name + "`";
    if (mysql_query(&mysql, sql.c_str()) != 0) {
      MYSQLERROR(mysql);
    }
  }

  // USE DB_NAME
  {
    std::string sql = "USE `" + db_name + "`";
    if (mysql_query(&mysql, sql.c_str()) != 0) {
      MYSQLERROR(mysql);
    }
  }

  // CREATE TABLE IF NOT EXISTS vec_tbl (...)
  const char *create_sql =
      "CREATE TABLE IF NOT EXISTS `vec_tbl` ("
      "  `pk`  INT NOT NULL,"
      "  `val` INT NOT NULL,"
      "  `vec` VARBINARY(4096) NOT NULL,"
      "  PRIMARY KEY (`pk`),"
      "  KEY `index_val`(`val`)"
      ") ENGINE=NDBCLUSTER";

  if (mysql_query(&mysql, create_sql) != 0) {
    MYSQLERROR(mysql);
  }

  std::cerr << "[loader] Schema and table ensured (DB="
            << db_name << ", TABLE=vec_tbl, TABLE_SIZE="
            << cfg.table_size << ", VECTOR DIMS=1024)" << std::endl;
}

static void gen_random_vec(std::vector<char> &out) {
  out.resize(BYTES_PER_VEC);
  float *f = reinterpret_cast<float *>(out.data());
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

  for (int i = 0; i < DIMS; i++) {
    f[i] = dist(g_rng);
  }
}

static void insert_rows(MYSQL &mysql, int n = 10000, int batch_size = 1000)
{
    std::string db_name = get_env_or_default("MYSQL_DB", "test_ndb_vec");

    std::string sql = "USE `" + db_name + "`";
    if (mysql_query(&mysql, sql.c_str()) != 0) MYSQLERROR(mysql);

    mysql_autocommit(&mysql, 0);

    mysql_query(&mysql, "DELETE FROM vec_tbl");

    const char *ins_sql =
        "INSERT INTO vec_tbl (pk,val,vec) VALUES (?,?,?)";

    MYSQL_STMT *stmt = mysql_stmt_init(&mysql);
    mysql_stmt_prepare(stmt, ins_sql, strlen(ins_sql));

    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));

    int pk, val;

    static std::vector<char> vec_buf(BYTES_PER_VEC);
    unsigned long vec_len = BYTES_PER_VEC;

    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer      = &pk;

    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer      = &val;

    bind[2].buffer_type     = MYSQL_TYPE_BLOB;
    bind[2].buffer          = vec_buf.data();
    bind[2].buffer_length   = vec_len;
    bind[2].length          = &vec_len;

    mysql_stmt_bind_param(stmt, bind);

    for (pk = 1; pk <= n; pk++) {
        val = pk * 100;

        gen_random_vec(vec_buf);

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

    fprintf(stderr, "[loader] done\n");
}

int scan_vector_search(Ndb * myNdb, MYSQL& mysql)
{
  const int retryMax = 10;
  NdbError  err;

  const NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Table *myTable = myDict->getTable("vec_tbl");
  if (myTable == NULL)
    APIERROR(myDict->getNdbError());

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      std::cout << e.message << std::endl;
      return -1;
    }

    NdbScanOperation *myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead,
                             0, 0, 0) != 0) {
      APIERROR (myTrans->getNdbError());
    }

    NdbRecAttr* myRecAttr[3];
    myRecAttr[0] = myScanOp->getValue("pk");
    myRecAttr[1] = myScanOp->getValue("val");
    myRecAttr[2] = myScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr ||
        myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    NdbAggregator aggregator(myTable);
    bool ret = aggregator.VectorSearch("vec",
                                       g_target_vec,
                                       DIMS,
                                       g_top_n);
    assert(ret);
    if (myScanOp->setAggregationCode(&aggregator) == -1) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    auto start = std::chrono::high_resolution_clock::now();
    if (myScanOp->DoVectorSearch(myRecAttr, 3) == -1) {
      err = myTrans->getNdbError();
      std::cout << "DoVectorSearch failed: " << err.message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }
    auto end = std::chrono::high_resolution_clock::now();

    fprintf(stderr, "------FINAL RESULT (pushdown)------\n");
    res_vs.clear();
    while (aggregator.VecFetchNextResult()) {
      fprintf(stderr, "pk: %d, val: %d\n",
              myRecAttr[0]->int32_value(),
              myRecAttr[1]->int32_value());
      res_vs.push_back(myRecAttr[0]->int32_value());
    }

    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "Time cost (pushdown): " << elapsed.count() << " ms"
              << std::endl;

    myNdb->closeTransaction(myTrans);
    return 1;
  }

  std::cout << "ERROR: scan_vector_search retried " << retryMax
            << " times, failing." << std::endl;
  return -1;
}

int scan_index_vector_search(Ndb *myNdb, MYSQL& mysql) {
  NdbError err;
  NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex =
      myDict->getIndex("index_val", "vec_tbl");
  if (myPIndex == NULL) {
    APIERROR(myDict->getNdbError());
  }

  NdbTransaction *myTrans = myNdb->startTransaction();
  if (myTrans == NULL) {
    APIERROR(myNdb->getNdbError());
  }

  NdbIndexScanOperation *myIndexScanOp =
      myTrans->getNdbIndexScanOperation(myPIndex);

  Uint32 scanFlags = NdbScanOperation::SF_OrderBy |
                     NdbScanOperation::SF_MultiRange;

  if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                scanFlags) != 0) {
    APIERROR (myTrans->getNdbError());
  }

  Uint32 low  = 10000;
  Uint32 high = 100000;

  if (myIndexScanOp->setBound("val",
                              NdbIndexScanOperation::BoundLE,
                              (char*)&low)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->setBound("val",
                              NdbIndexScanOperation::BoundGT,
                              (char*)&high)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->end_of_bound(0)) {
    APIERROR(myIndexScanOp->getNdbError());
  }

  Uint32 val = 500;
  NdbScanFilter filter(myIndexScanOp);
  if (filter.begin(NdbScanFilter::AND) < 0  ||
      filter.cmp(NdbScanFilter::COND_LT, 0, &val, sizeof(val)) < 0 ||
      filter.end() < 0) {
    std::cout <<  myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  NdbRecAttr* myRecAttr[2];
  myRecAttr[0] = myIndexScanOp->getValue("pk");
  myRecAttr[1] = myIndexScanOp->getValue("val");
  if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr) {
    std::cout << myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  const NdbDictionary::Table *myTable = myDict->getTable("vec_tbl");
  if (myTable == NULL) {
    APIERROR(myDict->getNdbError());
  }

  NdbAggregator aggregator(myTable);
  bool ret = aggregator.VectorSearch("vec",
                                     g_target_vec,
                                     DIMS,
                                     g_top_n);
  assert(ret);
  if (myIndexScanOp->setAggregationCode(&aggregator) == -1) {
    std::cout << myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  auto start = std::chrono::high_resolution_clock::now();
  if (myIndexScanOp->DoVectorSearch(myRecAttr, 2) == -1) {
    err = myTrans->getNdbError();
    std::cout << "DoVectorSearch failed: " << err.message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }
  auto end = std::chrono::high_resolution_clock::now();

  fprintf(stderr, "------FINAL RESULT (index pushdown)------\n");
  res_vs.clear();
  while (aggregator.VecFetchNextResult()) {
    fprintf(stderr, "pk: %d, val: %d\n",
            myRecAttr[0]->int32_value(),
            myRecAttr[1]->int32_value());
    res_vs.push_back(myRecAttr[0]->int32_value());
  }
  std::chrono::duration<double, std::milli> elapsed = end - start;
  std::cout << "Time cost (index pushdown): " << elapsed.count() << " ms"
            << std::endl;

  myNdb->closeTransaction(myTrans);
  return 1;
}

int table_scan_regular_vector_search(Ndb * myNdb, MYSQL& mysql)
{
  const int retryMax = 10;
  NdbError  err;

  const NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Table *myTable = myDict->getTable("vec_tbl");
  if (myTable == NULL)
    APIERROR(myDict->getNdbError());

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      std::cout << e.message << std::endl;
      return -1;
    }

    NdbScanOperation *myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead) != 0) {
      APIERROR (myTrans->getNdbError());
    }

    NdbRecAttr* myRecAttr[3];
    myRecAttr[0] = myScanOp->getValue("pk");
    myRecAttr[1] = myScanOp->getValue("val");
    myRecAttr[2] = myScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr ||
        myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    auto start = std::chrono::high_resolution_clock::now();
    if (myTrans->execute(NdbTransaction::NoCommit) != 0) {
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    std::priority_queue<NdbAggregator::VectorSearchResult*,
                        std::vector<NdbAggregator::VectorSearchResult*>,
                        NdbAggregator::ByDistance> vec_results;
    int    check    = -1;
    double distance = 0;

    while ((check = myScanOp->nextResult(true)) == 0) {
      float* current =
        (float*)((char*)(myRecAttr[2]->aRef()) + 2);

      simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance);
      if (g_top_n != 0 &&
          (vec_results.size() < g_top_n ||
          distance < vec_results.top()->distance_)) {
        if (vec_results.size() == g_top_n) {
          NdbAggregator::VectorSearchResult* kickout = vec_results.top();
          // fprintf(stderr, "Kickout: [%d, %lf] -> [%d, %lf]\n",
          //         kickout->attrs_[0]->int32_value(), kickout->distance_,
          //         myRecAttr[0]->int32_value(), distance);
          vec_results.pop();
          delete kickout;
        }
        NdbAggregator::VectorSearchResult* candidate =
          new NdbAggregator::VectorSearchResult(distance, 2, myRecAttr);
        vec_results.push(candidate);
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;

    fprintf(stderr, "------FINAL RESULT (non-pushdown)------\n");
    std::vector<NdbAggregator::VectorSearchResult*> vec_results_final;
    while (!vec_results.empty()) {
      vec_results_final.push_back(vec_results.top());
      vec_results.pop();
    }

    res_scan.clear();
    std::for_each(vec_results_final.rbegin(),
                  vec_results_final.rend(),
                  [](NdbAggregator::VectorSearchResult* candidate) {
                    res_scan.push_back(candidate->attrs_[0]->int32_value());
                    std::cout << "pk: " << candidate->attrs_[0]->int32_value();
                    std::cout << ", val: "
                              << candidate->attrs_[1]->int32_value()
                              << std::endl;
                    delete candidate;
                  });

    std::cout << "Time cost (non-pushdown): " << elapsed.count() << " ms"
              << std::endl;

    myNdb->closeTransaction(myTrans);
    return 1;
  }

  std::cout << "ERROR: table_scan_regular_vector_search retried " << retryMax
            << " times, failing." << std::endl;
  return -1;
}

int index_scan_regular_vector_search(Ndb * myNdb, MYSQL& mysql)
{
  const int retryMax = 10;
  NdbError  err;

  NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex =
      myDict->getIndex("index_val", "vec_tbl");
  if (myPIndex == NULL) {
    APIERROR(myDict->getNdbError());
  }

  for (int attempt = 0; attempt < retryMax; ++attempt) {
    NdbTransaction *myTrans = myNdb->startTransaction();
    if (myTrans == NULL) {
      const NdbError e = myNdb->getNdbError();
      if (e.status == NdbError::TemporaryError) {
        NdbSleep_MilliSleep(50);
        continue;
      }
      std::cout << e.message << std::endl;
      return -1;
    }

    NdbIndexScanOperation *myIndexScanOp =
        myTrans->getNdbIndexScanOperation(myPIndex);

    Uint32 scanFlags = NdbScanOperation::SF_OrderBy |
                       NdbScanOperation::SF_MultiRange;

    if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                  scanFlags) != 0) {
      APIERROR (myTrans->getNdbError());
    }

    Uint32 low  = 10000;
    Uint32 high = 100000;

    if (myIndexScanOp->setBound("val",
                                NdbIndexScanOperation::BoundLE,
                                (char*)&low)) {
      APIERROR(myTrans->getNdbError());
    }
    if (myIndexScanOp->setBound("val",
                                NdbIndexScanOperation::BoundGT,
                                (char*)&high)) {
      APIERROR(myTrans->getNdbError());
    }
    if (myIndexScanOp->end_of_bound(0)) {
      APIERROR(myIndexScanOp->getNdbError());
    }

    Uint32 val = 500;
    NdbScanFilter filter(myIndexScanOp);
    if (filter.begin(NdbScanFilter::AND) < 0  ||
        filter.cmp(NdbScanFilter::COND_LT, 0, &val, sizeof(val)) < 0 ||
        filter.end() < 0) {
      std::cout <<  myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    NdbRecAttr* myRecAttr[3];
    myRecAttr[0] = myIndexScanOp->getValue("pk");
    myRecAttr[1] = myIndexScanOp->getValue("val");
    myRecAttr[2] = myIndexScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr ||
        myRecAttr[1] == nullptr ||
        myRecAttr[2] == nullptr) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    auto start = std::chrono::high_resolution_clock::now();
    if (myTrans->execute(NdbTransaction::NoCommit) != 0) {
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    std::priority_queue<NdbAggregator::VectorSearchResult*,
                        std::vector<NdbAggregator::VectorSearchResult*>,
                        NdbAggregator::ByDistance> vec_results;
    int    check    = -1;
    double distance = 0;

    while ((check = myIndexScanOp->nextResult(true)) == 0) {
      float* current =
        (float*)((char*)(myRecAttr[2]->aRef()) + 2);

      simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance);
      if (g_top_n != 0 &&
          (vec_results.size() < g_top_n ||
          distance < vec_results.top()->distance_)) {
        if (vec_results.size() == g_top_n) {
          NdbAggregator::VectorSearchResult* kickout = vec_results.top();
          // fprintf(stderr, "Kickout: [%d, %lf] -> [%d, %lf]\n",
          //         kickout->attrs_[0]->int32_value(), kickout->distance_,
          //         myRecAttr[0]->int32_value(), distance);
          vec_results.pop();
          delete kickout;
        }
        NdbAggregator::VectorSearchResult* candidate =
          new NdbAggregator::VectorSearchResult(distance, 2, myRecAttr);
        vec_results.push(candidate);
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;

    fprintf(stderr, "------FINAL RESULT (non-pushdown)------\n");
    std::vector<NdbAggregator::VectorSearchResult*> vec_results_final;
    while (!vec_results.empty()) {
      vec_results_final.push_back(vec_results.top());
      vec_results.pop();
    }

    res_scan.clear();
    std::for_each(vec_results_final.rbegin(),
                  vec_results_final.rend(),
                  [](NdbAggregator::VectorSearchResult* candidate) {
                    res_scan.push_back(candidate->attrs_[0]->int32_value());
                    std::cout << "pk: " << candidate->attrs_[0]->int32_value();
                    std::cout << ", val: "
                              << candidate->attrs_[1]->int32_value()
                              << std::endl;
                    delete candidate;
                  });

    std::cout << "Time cost (Index non-pushdown): " << elapsed.count() << " ms"
              << std::endl;

    myNdb->closeTransaction(myTrans);
    return 1;
  }

  std::cout << "ERROR: index_scan_regular_vector_search retried " << retryMax
            << " times, failing." << std::endl;
  return -1;
}


bool DoubleCheck(Ndb* myNdb, Int32 pk_1, Int32 pk_2) {
  double distance_1 = 0;
  double distance_2 = 0;
  const NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Table *myTable = myDict->getTable("vec_tbl");
  std::cout << "Double check " << pk_1 << ", " << pk_2 << std::endl;
  if (myTable == NULL) APIERROR(myDict->getNdbError());

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
    float* current = (float*)((char*)(myRecAttr->aRef()) + 2);
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
    float* current = (float*)((char*)(myRecAttr->aRef()) + 2);
    simsimd_l2sq_f32(current, g_target_vec, DIMS, &distance_2);
    myNdb->closeTransaction(myTransaction);
  }

  if (distance_1 != distance_2) {
    fprintf(stderr, "[pk: %d, distance: %lf] != [pk: %d, distance: %lf]\n",
            pk_1, distance_1, pk_2, distance_2);
  }
  return distance_1 == distance_2;
}

void ndb_run_scan(const char * connectstring, MYSQL& mysql,
                  int iters)
{
  Ndb_cluster_connection cluster_connection(connectstring);
  if (cluster_connection.connect(4, 5, 1))
  {
    std::cout << "Unable to connect to cluster within 30 secs." << std::endl;
    exit(-1);
  }

  if (cluster_connection.wait_until_ready(30,0) < 0)
  {
    std::cout << "Cluster was not ready within 30 secs.\n";
    exit(-1);
  }

  std::string db_name = get_env_or_default("MYSQL_DB", "test_ndb_vec");
  Ndb myNdb(&cluster_connection, db_name.c_str());
  if (myNdb.init(1024) == -1) {      // Set max 1024 parallel transactions
    APIERROR(myNdb.getNdbError());
    exit(-1);
  }

  for (int i = 0; i < iters; ++i) {
    std::cout << "\n========== Iteration " << (i+1)
              << " / " << iters << " ==========\n";
    init_target();

    fprintf(stderr, "1. Pushdown Vector Search via TABLE Scan\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 ORDER BY embedding <-> "
                    "'[target_vec]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", g_top_n);
    if (scan_vector_search(&myNdb, mysql) > 0) {
      std::cout << "Query 1: success!" << std::endl  << std::endl;
    }

    fprintf(stderr, "2. Non-pushdown Vector Search via TABLE Scan\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 ORDER BY embedding <-> "
                    "'[target_vec]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", g_top_n);
    if (table_scan_regular_vector_search(&myNdb, mysql) > 0) {
      std::cout << "Query 2: success!" << std::endl  << std::endl;
      std::cout << "Validating [TABLE]: " << std::endl;
      if (res_vs.size() == res_scan.size()) {
        std::cout << "Result sizes matche, both are "
                  << res_vs.size() << std::endl;
      } else {
        std::cout << "Results size mismatches, vs: " << res_vs.size()
                  << ", scan: " << res_scan.size() << std::endl;
        exit(0);
      }
      for (size_t j = 0; j < res_vs.size(); j++) {
        if (res_vs[j] != res_scan[j]) {
          if (!DoubleCheck(&myNdb, res_vs[j], res_scan[j])) {
            std::cout << "Result mismatches: pushdown pk " << res_vs[j]
                      << ", non-pushdown pk: " << res_scan[j] << std::endl;
            exit(0);
          }
        }
      }
      std::cout << "All results are identical between pushdown "
                << "and non-pushdown vector search."
                << std::endl;
      std::cout << "Validation [TABLE] passed" << std::endl;
    }

    fprintf(stderr, "3. Pushdown Vector Search via Index Scan with Lower–Upper Bounds and Filter\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 WHERE val >= 10000 AND val < 100000 AND pk < 500\n");
    fprintf(stderr, "                 ORDER BY embedding <-> '[target_vec]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", VEC_TOP_N);
    if (scan_index_vector_search(&myNdb, mysql) > 0) {
      std::cout << "Query 3 success!" << std::endl;
    }

    fprintf(stderr, "4. Non-pushdown Vector Search via Index Scan with Lower–Upper Bounds and Filter\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 WHERE val >= 10000 AND val < 100000 AND pk < 500\n");
    fprintf(stderr, "                 ORDER BY embedding <-> '[target]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", VEC_TOP_N);
    if (index_scan_regular_vector_search(&myNdb, mysql) > 0) {
      std::cout << "Query 4: success!" << std::endl  << std::endl;
      std::cout << "Validating [INDEX]: " << std::endl;
      if (res_vs.size() == res_scan.size()) {
        std::cout << "Result sizes matche, both are "
                  << res_vs.size() << std::endl;
      } else {
        std::cout << "Results size mismatches, vs: " << res_vs.size()
                  << ", scan: " << res_scan.size() << std::endl;
        exit(0);
      }
      for (size_t j = 0; j < res_vs.size(); j++) {
        if (res_vs[j] != res_scan[j]) {
          if (!DoubleCheck(&myNdb, res_vs[j], res_scan[j])) {
            std::cout << "Result mismatches: pushdown pk " << res_vs[j]
                      << ", non-pushdown pk: " << res_scan[j] << std::endl;
            exit(0);
          }
        }
      }
      std::cout << "All results are identical between pushdown "
                << "and non-pushdown vector search."
                << std::endl;
      std::cout << "Validation [INDEX] passed" << std::endl;
    }

#ifdef _WIN32
    Sleep(1000);
#else
    sleep(1);
#endif
  }
}

int main(int argc, char** argv)
{
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <ndb_connectstring> \n"
                 "                                             "
                 "[--load=0|1] [--iters=1] [--seed=NUM] [--top_n=10]\n"
                 "                                             "
                 "[--table_size=10000] [--mysql-host=127.0.0.1]\n"
                 "                                             "
                 "[--mysql-port=3308] [--mysql-user=root] [--mysql-pwd=STRING]\n";
    return 1;
  }

  const char *connectstring = argv[1];

  Config cfg;
  parse_args(argc, argv, cfg);

  MYSQL mysql;
  std::cout << "SIMD dynamic dispatch: " << simsimd_uses_dynamic_dispatch() << std::endl;
  std::cout << "SIMD capabilities: " << simsimd_capabilities() << std::endl;
  std::cout << "     NEON " << simsimd_uses_neon()
            << ", SVE " << simsimd_uses_sve()
            << ", HASWELL " << simsimd_uses_haswell() << std::endl;
  std::cout << "     SKYLAKE " << simsimd_uses_skylake()
            << ", ICE " << simsimd_uses_ice()
            << ", GENOA " << simsimd_uses_genoa() << std::endl;
  std::cout << "     SAPPHIRE " << simsimd_uses_sapphire()
            << ", TURIN " << simsimd_uses_turin()
            << ", SIERRA " << simsimd_uses_sierra() << std::endl;

  mysql_init(&mysql);

  if (!mysql_real_connect(&mysql,
                          cfg.mysql_host.c_str(),
                          cfg.mysql_user.c_str(),
                          cfg.mysql_pwd.c_str(),
                          nullptr,
                          cfg.mysql_port,
                          nullptr,
                          0)) {
      std::cerr << "mysql_real_connect failed: "
                << mysql_error(&mysql) << std::endl;
      return 1;
  }

  init_rng(cfg);
  if (cfg.do_load) {
    ensure_schema_and_table(mysql, cfg);
    insert_rows(mysql, cfg.table_size, 1000);
  } else {
    std::cerr << "[main] Skip loading data (--load=0). "
              << "Assuming vec_tbl already exists and is populated.\n";
  }

  ndb_init();
  ndb_run_scan(connectstring, mysql, cfg.iters);
  ndb_end(0);

  mysql_close(&mysql);

  return 0;
}
