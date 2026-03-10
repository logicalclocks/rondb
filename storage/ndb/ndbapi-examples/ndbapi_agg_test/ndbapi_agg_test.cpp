/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

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
 * Comprehensive Pushdown Aggregation Test Program
 *
 * Tests charset-aware GROUP BY comparison for utf8mb4_general_ci collation.
 * Validates that GBHashEntryCmp::operator() correctly uses collation-aware
 * comparison instead of raw memcmp.
 *
 * Usage:
 *   ndb_ndbapi_agg_test <socket> <connectstring> [load:true/false]
 *                                                 [validate:true/false]
 */

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <mysqld_error.h>
#include <NdbApi.hpp>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#define PRINT_ERROR(code, msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
            << ", code: " << code \
            << ", msg: " << msg << "." << std::endl
#define MYSQLERROR(mysql) { \
  PRINT_ERROR(mysql_errno(&mysql), mysql_error(&mysql)); \
  exit(-1); }
#define APIERROR(error) { \
  PRINT_ERROR(error.code, error.message); \
  exit(-1); }

/* Ensure side-effectful calls are not stripped by NDEBUG */
#ifdef NDEBUG
#define VERIFY(expr) ((void)(expr))
#else
#define VERIFY(expr) assert(expr)
#endif

static const char *DB_NAME = "agg_test";

/* ----------------------------------------------------------------
 * Table creation
 * ---------------------------------------------------------------- */

static void create_tables(MYSQL &mysql) {
  /* Drop existing tables */
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test");
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test_inno");

  /* NDB table */
  if (mysql_query(&mysql,
        "CREATE TABLE pa_test ("
        "id INT NOT NULL,"
        "name VARCHAR(64) NOT NULL,"
        "category VARCHAR(32) NOT NULL,"
        "val_int INT NOT NULL,"
        "val_bigint BIGINT NOT NULL,"
        "val_uint INT UNSIGNED NOT NULL,"
        "val_double DOUBLE NOT NULL,"
        "val_float FLOAT NOT NULL,"
        "val_small SMALLINT NOT NULL,"
        "val_tiny TINYINT NOT NULL,"
        "val_dec DECIMAL(10,2) NOT NULL,"
        "val_dec2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "nullable_int INT NULL,"
        "val_biguint BIGINT UNSIGNED NOT NULL,"
        "PRIMARY KEY USING HASH (id)"
        ") ENGINE=NDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
    MYSQLERROR(mysql);

  if (mysql_query(&mysql,
        "CREATE INDEX idx_val_small ON pa_test (val_small)"))
    MYSQLERROR(mysql);

  /* InnoDB reference table */
  if (mysql_query(&mysql,
        "CREATE TABLE pa_test_inno ("
        "id INT NOT NULL,"
        "name VARCHAR(64) NOT NULL,"
        "category VARCHAR(32) NOT NULL,"
        "val_int INT NOT NULL,"
        "val_bigint BIGINT NOT NULL,"
        "val_uint INT UNSIGNED NOT NULL,"
        "val_double DOUBLE NOT NULL,"
        "val_float FLOAT NOT NULL,"
        "val_small SMALLINT NOT NULL,"
        "val_tiny TINYINT NOT NULL,"
        "val_dec DECIMAL(10,2) NOT NULL,"
        "val_dec2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "nullable_int INT NULL,"
        "val_biguint BIGINT UNSIGNED NOT NULL,"
        "PRIMARY KEY (id)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
    MYSQLERROR(mysql);

  if (mysql_query(&mysql,
        "CREATE INDEX idx_val_small ON pa_test_inno (val_small)"))
    MYSQLERROR(mysql);

  /* NDB table with Blob/Text columns for validation tests */
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test_blob");
  if (mysql_query(&mysql,
        "CREATE TABLE pa_test_blob ("
        "id INT NOT NULL,"
        "val_int INT NOT NULL,"
        "val_blob BLOB,"
        "val_text TEXT,"
        "PRIMARY KEY USING HASH (id)"
        ") ENGINE=NDB"))
    MYSQLERROR(mysql);
}

static void create_large_tables(MYSQL &mysql) {
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test_large");
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test_large_inno");

  if (mysql_query(&mysql,
        "CREATE TABLE pa_test_large ("
        "id INT NOT NULL,"
        "name VARCHAR(64) NOT NULL,"
        "category VARCHAR(32) NOT NULL,"
        "val_int INT NOT NULL,"
        "val_bigint BIGINT NOT NULL,"
        "val_uint INT UNSIGNED NOT NULL,"
        "val_double DOUBLE NOT NULL,"
        "val_float FLOAT NOT NULL,"
        "val_small SMALLINT NOT NULL,"
        "val_tiny TINYINT NOT NULL,"
        "val_dec DECIMAL(10,2) NOT NULL,"
        "val_dec2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "nullable_int INT NULL,"
        "val_biguint BIGINT UNSIGNED NOT NULL,"
        "PRIMARY KEY USING HASH (id)"
        ") ENGINE=NDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
    MYSQLERROR(mysql);

  if (mysql_query(&mysql,
        "CREATE TABLE pa_test_large_inno ("
        "id INT NOT NULL,"
        "name VARCHAR(64) NOT NULL,"
        "category VARCHAR(32) NOT NULL,"
        "val_int INT NOT NULL,"
        "val_bigint BIGINT NOT NULL,"
        "val_uint INT UNSIGNED NOT NULL,"
        "val_double DOUBLE NOT NULL,"
        "val_float FLOAT NOT NULL,"
        "val_small SMALLINT NOT NULL,"
        "val_tiny TINYINT NOT NULL,"
        "val_dec DECIMAL(10,2) NOT NULL,"
        "val_dec2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "nullable_int INT NULL,"
        "val_biguint BIGINT UNSIGNED NOT NULL,"
        "PRIMARY KEY (id)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
    MYSQLERROR(mysql);
}

static void create_nullagg_tables(MYSQL &mysql) {
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test_nullagg");
  mysql_query(&mysql, "DROP TABLE IF EXISTS pa_test_nullagg_inno");

  if (mysql_query(&mysql,
        "CREATE TABLE pa_test_nullagg ("
        "id INT NOT NULL,"
        "grp INT NOT NULL,"
        "val INT NULL,"
        "PRIMARY KEY USING HASH (id)"
        ") ENGINE=NDB"))
    MYSQLERROR(mysql);

  if (mysql_query(&mysql,
        "CREATE TABLE pa_test_nullagg_inno ("
        "id INT NOT NULL,"
        "grp INT NOT NULL,"
        "val INT NULL,"
        "PRIMARY KEY (id)"
        ") ENGINE=InnoDB"))
    MYSQLERROR(mysql);
}

/* ----------------------------------------------------------------
 * Data population — small dataset (22 rows)
 * ---------------------------------------------------------------- */

static void populate_data(MYSQL &mysql) {
  /*
   * Build batch INSERT with actual UTF-8 literals.
   * We use direct UTF-8 byte sequences in the SQL string.
   */
  const char *names[] = {
    "Hello", "hello", "HELLO",
    "cafe",
    "caf\xC3\xA9",             /* café */
    "CAF\xC3" "\x89",          /* CAFÉ — split to avoid \x89S hex parse */
    "uber",
    "\xC3\xBC" "ber",          /* über — split to avoid \xBCb hex parse */
    "\xC3\x9C" "BER",          /* ÜBER — split to avoid \x9CB hex parse */
    "olsen",
    "\xC3\x98lsen",            /* Ølsen */
    "\xC3\x98LSEN",            /* ØLSEN */
    "resume",
    "r\xC3\xA9sum\xC3\xA9",   /* résumé */
    "R\xC3" "\x89" "SUM\xC3" "\x89",  /* RÉSUMÉ */
    "mas",
    "ma\xC3\x9F",              /* maß */
    "naive",
    "na\xC3\xAF" "ve",         /* naïve — split to avoid \xAFv parse */
    "NA\xC3\x8F" "VE",         /* NAÏVE — split to avoid \x8FV parse */
    "angstrom",
    "\xC3\x85ngstr\xC3\xB6m"   /* Ångström */
  };

  const char *categories[] = {
    "Alpha", "Beta", "Gamma", "Delta",
    "Alpha", "Beta", "Gamma", "Delta",
    "Alpha", "Beta", "Gamma", "Delta",
    "Alpha", "Beta", "Gamma", "Delta",
    "Alpha", "Beta", "Gamma", "Delta",
    "Alpha", "Beta"
  };

  /* Build one big INSERT for all 22 rows */
  std::string sql = "INSERT INTO pa_test VALUES ";
  std::string sql_inno = "INSERT INTO pa_test_inno VALUES ";

  for (int id = 0; id < 22; id++) {
    if (id > 0) {
      sql += ",";
      sql_inno += ",";
    }

    int val_int = id * 7 - 500;
    long long val_bigint = (long long)id * 100003 - 5000000;
    unsigned int val_uint = id * 13;
    double val_double = id * 3.14 - 100.0;
    float val_float = id * 1.5f;
    int val_small = (id % 20) - 10;
    int val_tiny = (id % 5) - 2;

    /* Escape name for SQL */
    char escaped_name[256];
    mysql_real_escape_string(&mysql, escaped_name, names[id],
                             (unsigned long)strlen(names[id]));

    char nullable_str[32];
    if (id % 3 != 0) {
      snprintf(nullable_str, sizeof(nullable_str), "%d", id);
    } else {
      snprintf(nullable_str, sizeof(nullable_str), "NULL");
    }

    unsigned long long val_biguint = (unsigned long long)id * 13;

    char row_buf[512];
    snprintf(row_buf, sizeof(row_buf),
             "(%d,'%s','%s',%d,%lld,%u,%.2f,%.1f,%d,%d,"
             "CAST(%.2f AS DECIMAL(10,2)),CAST(%u AS DECIMAL(10,0)),%s,%llu)",
             id, escaped_name, categories[id],
             val_int, val_bigint, val_uint, val_double, val_float,
             val_small, val_tiny,
             val_double, val_uint, nullable_str, val_biguint);

    sql += row_buf;
    sql_inno += row_buf;
  }

  if (mysql_real_query(&mysql, sql.c_str(), (unsigned long)sql.length()))
    MYSQLERROR(mysql);
  if (mysql_real_query(&mysql, sql_inno.c_str(),
                       (unsigned long)sql_inno.length()))
    MYSQLERROR(mysql);

  fprintf(stderr, "  Inserted 22 rows into pa_test and pa_test_inno\n");
}

/* ----------------------------------------------------------------
 * Data population — large dataset (2000 rows)
 * ---------------------------------------------------------------- */

static void populate_large_data(MYSQL &mysql) {
  const char *cat_variants[] = {
    "Alpha", "ALPHA", "Beta", "beta", "Gamma", "GAMMA", "Delta", "DELTA"
  };
  const int TOTAL = 2000;
  const int BATCH = 100;

  for (int batch_start = 0; batch_start < TOTAL; batch_start += BATCH) {
    int batch_end = batch_start + BATCH;
    if (batch_end > TOTAL) batch_end = TOTAL;

    std::string sql = "INSERT INTO pa_test_large VALUES ";
    std::string sql_inno = "INSERT INTO pa_test_large_inno VALUES ";

    for (int id = batch_start; id < batch_end; id++) {
      if (id > batch_start) {
        sql += ",";
        sql_inno += ",";
      }

      int grp_idx = id % 300;
      char name_buf[64];
      switch (id % 3) {
        case 0:
          snprintf(name_buf, sizeof(name_buf), "grp_%03d", grp_idx);
          break;
        case 1:
          snprintf(name_buf, sizeof(name_buf), "GRP_%03d", grp_idx);
          break;
        case 2:
          snprintf(name_buf, sizeof(name_buf), "Grp_%03d", grp_idx);
          break;
      }

      const char *cat = cat_variants[id % 8];

      int val_int = id * 7 - 500;
      long long val_bigint = (long long)id * 100003 - 5000000;
      unsigned int val_uint = id * 13;
      double val_double = id * 3.14 - 100.0;
      float val_float = id * 1.5f;
      int val_small = (id % 20) - 10;
      int val_tiny = (id % 5) - 2;

      char nullable_str[32];
      if (id % 3 != 0) {
        snprintf(nullable_str, sizeof(nullable_str), "%d", id);
      } else {
        snprintf(nullable_str, sizeof(nullable_str), "NULL");
      }

      unsigned long long val_biguint = (unsigned long long)id * 13;

      char row_buf[512];
      snprintf(row_buf, sizeof(row_buf),
               "(%d,'%s','%s',%d,%lld,%u,%.2f,%.1f,%d,%d,"
               "CAST(%.2f AS DECIMAL(10,2)),CAST(%u AS DECIMAL(10,0)),%s,%llu)",
               id, name_buf, cat,
               val_int, val_bigint, val_uint, val_double, val_float,
               val_small, val_tiny,
               val_double, val_uint, nullable_str, val_biguint);

      sql += row_buf;
      sql_inno += row_buf;
    }

    if (mysql_real_query(&mysql, sql.c_str(), (unsigned long)sql.length()))
      MYSQLERROR(mysql);
    if (mysql_real_query(&mysql, sql_inno.c_str(),
                         (unsigned long)sql_inno.length()))
      MYSQLERROR(mysql);
  }

  fprintf(stderr, "  Inserted %d rows into pa_test_large and "
                  "pa_test_large_inno\n", TOTAL);
}

/* ----------------------------------------------------------------
 * Data population — nullagg dataset (300 rows)
 * ---------------------------------------------------------------- */

static void populate_nullagg_data(MYSQL &mysql) {
  const int TOTAL = 300;
  const int BATCH = 100;

  for (int batch_start = 0; batch_start < TOTAL; batch_start += BATCH) {
    int batch_end = batch_start + BATCH;
    if (batch_end > TOTAL) batch_end = TOTAL;

    std::string sql = "INSERT INTO pa_test_nullagg VALUES ";
    std::string sql_inno = "INSERT INTO pa_test_nullagg_inno VALUES ";

    for (int id = batch_start; id < batch_end; id++) {
      if (id > batch_start) {
        sql += ",";
        sql_inno += ",";
      }

      int grp = (id / 100) + 1;  /* grp 1: ids 0-99, grp 2: 100-199, grp 3: 200-299 */
      char row_buf[64];
      if (grp == 1) {
        /* Group 1: all non-NULL, val = id */
        snprintf(row_buf, sizeof(row_buf), "(%d,%d,%d)", id, grp, id);
      } else if (grp == 2) {
        /* Group 2: all NULL */
        snprintf(row_buf, sizeof(row_buf), "(%d,%d,NULL)", id, grp);
      } else {
        /* Group 3: even ids non-NULL, odd ids NULL */
        if (id % 2 == 0) {
          snprintf(row_buf, sizeof(row_buf), "(%d,%d,%d)", id, grp, id);
        } else {
          snprintf(row_buf, sizeof(row_buf), "(%d,%d,NULL)", id, grp);
        }
      }

      sql += row_buf;
      sql_inno += row_buf;
    }

    if (mysql_real_query(&mysql, sql.c_str(), (unsigned long)sql.length()))
      MYSQLERROR(mysql);
    if (mysql_real_query(&mysql, sql_inno.c_str(),
                         (unsigned long)sql_inno.length()))
      MYSQLERROR(mysql);
  }

  fprintf(stderr, "  Inserted %d rows into pa_test_nullagg and "
                  "pa_test_nullagg_inno\n", TOTAL);
}

/* ----------------------------------------------------------------
 * Validation helpers
 * ---------------------------------------------------------------- */

/*
 * Extract the string value from a VARCHAR or LONGVARCHAR group-by column.
 * - Varchar (max_byte_len <= 255): 1-byte length prefix
 * - Longvarchar (max_byte_len > 255): 2-byte length prefix
 * For VARCHAR(64) utf8mb4, max_byte_len = 256 → Longvarchar (2-byte prefix).
 * For VARCHAR(32) utf8mb4, max_byte_len = 128 → Varchar (1-byte prefix).
 */
static std::string extract_varchar(NdbAggregator::Column &col) {
  if (col.type() == NdbDictionary::Column::Longvarchar) {
    /* 2-byte little-endian length prefix */
    Uint32 prefix_len = 2;
    Uint32 data_len = col.byte_size() - prefix_len;
    return std::string(col.data() + prefix_len, data_len);
  } else {
    /* Varchar: 1-byte length prefix */
    Uint32 prefix_len = 1;
    Uint32 data_len = col.byte_size() - prefix_len;
    return std::string(col.data() + prefix_len, data_len);
  }
}

static bool compare_value(double pa_val, const char *sql_str,
                          bool is_integer) {
  if (sql_str == nullptr) return false;
  if (is_integer) {
    /* For integer results (SUM of ints, COUNT), compare exactly */
    long long pa_ll = (long long)pa_val;
    long long sql_ll = std::stoll(sql_str);
    return pa_ll == sql_ll;
  } else {
    /* For double results, allow ±1.0 tolerance */
    double sql_val = std::stod(sql_str);
    return fabs(pa_val - sql_val) <= 1.0;
  }
}

/*
 * Run a SQL query and return the single result row's values as doubles.
 * Returns false if query fails or returns no rows.
 */
static bool run_sql_and_fetch(MYSQL &mysql, const std::string &sql,
                              std::vector<double> &out_vals,
                              const std::vector<bool> &is_integer) {
  if (mysql_real_query(&mysql, sql.c_str(), (unsigned long)sql.length())) {
    fprintf(stderr, "  SQL error: %s\n  Query: %s\n",
            mysql_error(&mysql), sql.c_str());
    return false;
  }
  MYSQL_RES *res = mysql_store_result(&mysql);
  if (!res) return false;

  MYSQL_ROW row = mysql_fetch_row(res);
  if (!row) {
    mysql_free_result(res);
    return false;
  }

  unsigned int n_fields = mysql_num_fields(res);
  out_vals.resize(n_fields);
  for (unsigned int i = 0; i < n_fields; i++) {
    if (row[i] == nullptr) {
      out_vals[i] = 0.0;
    } else if (i < is_integer.size() && is_integer[i]) {
      out_vals[i] = (double)std::stoll(row[i]);
    } else {
      out_vals[i] = std::stod(row[i]);
    }
  }

  mysql_free_result(res);
  return true;
}

/*
 * Like run_sql_and_fetch but also tracks which columns are SQL NULL.
 * out_nulls[i] is true when row[i] is NULL.
 */
static bool run_sql_and_fetch_nullable(
    MYSQL &mysql, const std::string &sql,
    std::vector<double> &out_vals,
    std::vector<bool> &out_nulls,
    const std::vector<bool> &is_integer) {
  if (mysql_real_query(&mysql, sql.c_str(), (unsigned long)sql.length())) {
    fprintf(stderr, "  SQL error: %s\n  Query: %s\n",
            mysql_error(&mysql), sql.c_str());
    return false;
  }
  MYSQL_RES *res = mysql_store_result(&mysql);
  if (!res) return false;

  MYSQL_ROW row = mysql_fetch_row(res);
  if (!row) {
    mysql_free_result(res);
    return false;
  }

  unsigned int n_fields = mysql_num_fields(res);
  out_vals.resize(n_fields);
  out_nulls.resize(n_fields);
  for (unsigned int i = 0; i < n_fields; i++) {
    if (row[i] == nullptr) {
      out_vals[i] = 0.0;
      out_nulls[i] = true;
    } else {
      out_nulls[i] = false;
      if (i < is_integer.size() && is_integer[i]) {
        out_vals[i] = (double)std::stoll(row[i]);
      } else {
        out_vals[i] = std::stod(row[i]);
      }
    }
  }

  mysql_free_result(res);
  return true;
}

/* ----------------------------------------------------------------
 * Generic test runner helpers
 * ---------------------------------------------------------------- */

struct TestContext {
  Ndb *ndb;
  MYSQL *mysql;
  bool validate;
  const char *ndb_table;
  const char *inno_table;
};

/* ----------------------------------------------------------------
 * Test 1: Charset-Aware GROUP BY — Table Scan
 * SELECT name, SUM(val_int), COUNT(val_int), MIN(val_bigint),
 *        MAX(val_double) FROM pa_test_inno GROUP BY name
 * ---------------------------------------------------------------- */

static bool run_test_1(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 1: Charset-Aware GROUP BY — Table Scan ===\n");
  fprintf(stderr, "  SELECT name, SUM(val_int), COUNT(val_int), "
                  "MIN(val_bigint), MAX(val_double)\n"
                  "  FROM pa_test GROUP BY name\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.LoadColumn("val_bigint", kReg1));
  VERIFY(agg.Min(2, kReg1));
  VERIFY(agg.LoadColumn("val_double", kReg1));
  VERIFY(agg.Max(3, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    while (!col.end()) {
      name_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[4];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [name='%s']: SUM=%lld COUNT=%lld MIN=%lld "
                    "MAX=%.2f\n",
            name_val.c_str(), (long long)pa_vals[0], (long long)pa_vals[1],
            (long long)pa_vals[2], pa_vals[3]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int), MIN(val_bigint), "
          "MAX(val_double) FROM ") + DB_NAME + "." + ctx.inno_table +
          " WHERE name='" + escaped + "' GROUP BY name";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true, true, false};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 4; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             is_int[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 9)\n", n_groups);
  if (n_groups != 9) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 2: Multi-Column GROUP BY (String + Numeric) — Table Scan
 * SELECT name, val_small, SUM(val_int), COUNT(val_bigint)
 *   FROM pa_test_inno GROUP BY name, val_small
 * ---------------------------------------------------------------- */

static bool run_test_2(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 2: Multi-Column GROUP BY — Table Scan ===\n");
  fprintf(stderr, "  SELECT name, val_small, SUM(val_int), COUNT(val_bigint)\n"
                  "  FROM pa_test GROUP BY name, val_small\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.GroupBy("val_small"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_bigint", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    int small_val = 0;
    int col_idx = 0;
    while (!col.end()) {
      if (col_idx == 0) {
        name_val = extract_varchar(col);
      } else {
        small_val = col.data_int16();
      }
      col_idx++;
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [name='%s', val_small=%d]: SUM=%lld COUNT=%lld\n",
            name_val.c_str(), small_val,
            (long long)pa_vals[0], (long long)pa_vals[1]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_bigint) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE name='" + escaped + "' AND val_small=" +
          std::to_string(small_val) + " GROUP BY name, val_small";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             is_int[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d\n", n_groups);
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 3: Numeric-Only GROUP BY — Table Scan (Regression)
 * SELECT val_tiny, SUM(val_bigint), MIN(val_int), MAX(val_uint),
 *        COUNT(val_int) FROM pa_test_inno GROUP BY val_tiny
 * ---------------------------------------------------------------- */

static bool run_test_3(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 3: Numeric-Only GROUP BY — Table Scan ===\n");
  fprintf(stderr, "  SELECT val_tiny, SUM(val_bigint), MIN(val_int), "
                  "MAX(val_uint), COUNT(val_int)\n"
                  "  FROM pa_test GROUP BY val_tiny\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("val_tiny"));
  VERIFY(agg.LoadColumn("val_bigint", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Min(1, kReg1));
  VERIFY(agg.LoadColumn("val_uint", kReg1));
  VERIFY(agg.Max(2, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(3, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    int tiny_val = 0;
    while (!col.end()) {
      tiny_val = col.data_int8();
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[4];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [val_tiny=%d]: SUM=%lld MIN=%lld MAX=%lld "
                    "COUNT=%lld\n",
            tiny_val, (long long)pa_vals[0], (long long)pa_vals[1],
            (long long)pa_vals[2], (long long)pa_vals[3]);

    if (ctx.validate) {
      std::string sql = std::string(
          "SELECT SUM(val_bigint), MIN(val_int), MAX(val_uint), "
          "COUNT(val_int) FROM ") + DB_NAME + "." + ctx.inno_table +
          " WHERE val_tiny=" + std::to_string(tiny_val) +
          " GROUP BY val_tiny";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true, true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 4; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             is_int[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 5)\n", n_groups);
  if (n_groups != 5) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 4: Aggregation Without GROUP BY — Table Scan
 * SELECT SUM(val_int), COUNT(val_int), MIN(val_double),
 *        MAX(val_bigint) FROM pa_test_inno
 * ---------------------------------------------------------------- */

static bool run_test_4(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 4: Aggregation Without GROUP BY ===\n");
  fprintf(stderr, "  SELECT SUM(val_int), COUNT(val_int), MIN(val_double), "
                  "MAX(val_bigint)\n  FROM pa_test\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  /* No GroupBy */
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.LoadColumn("val_double", kReg1));
  VERIFY(agg.Min(2, kReg1));
  VERIFY(agg.LoadColumn("val_bigint", kReg1));
  VERIFY(agg.Max(3, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  bool all_valid = true;
  double pa_vals[4] = {};

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  if (!record.end()) {
    /* No group-by columns to fetch */
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Result: SUM=%lld COUNT=%lld MIN=%.2f MAX=%lld\n",
            (long long)pa_vals[0], (long long)pa_vals[1],
            pa_vals[2], (long long)pa_vals[3]);
  }

  if (ctx.validate) {
    std::string sql = std::string(
        "SELECT SUM(val_int), COUNT(val_int), MIN(val_double), "
        "MAX(val_bigint) FROM ") + DB_NAME + "." + ctx.inno_table;

    std::vector<double> sql_vals;
    std::vector<bool> is_int = {true, true, false, true};
    if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
      bool ints[] = {true, true, false, true};
      for (int i = 0; i < 4; i++) {
        if (!compare_value(pa_vals[i],
                           std::to_string(sql_vals[i]).c_str(),
                           ints[i])) {
          fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                  i, pa_vals[i], sql_vals[i]);
          all_valid = false;
        }
      }
    } else {
      fprintf(stderr, "    SQL validation query failed\n");
      all_valid = false;
    }
  }

  ctx.ndb->closeTransaction(trans);
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 5: Aggregation With Filter — Table Scan
 * SELECT name, SUM(val_int), COUNT(val_int)
 *   FROM pa_test WHERE val_tiny = 1 GROUP BY name
 * ---------------------------------------------------------------- */

static bool run_test_5(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 5: Aggregation With Filter — Table Scan ===\n");
  fprintf(stderr, "  SELECT name, SUM(val_int), COUNT(val_int)\n"
                  "  FROM pa_test WHERE val_tiny = 1 GROUP BY name\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  /* Filter: val_tiny (col 9) = 1 */
  Int8 filter_val = 1;
  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 9, &filter_val,
                 sizeof(filter_val)) < 0 ||
      filter.end() < 0) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    while (!col.end()) {
      name_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [name='%s']: SUM=%lld COUNT=%lld\n",
            name_val.c_str(), (long long)pa_vals[0], (long long)pa_vals[1]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE val_tiny=1 AND name='" + escaped +
          "' GROUP BY name";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             is_int[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d\n", n_groups);
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 6: Arithmetic Expressions — Table Scan
 * SELECT category, SUM(val_int + val_bigint),
 *        MAX(val_double * 2.5), MIN(val_uint - val_small)
 *   FROM pa_test_inno GROUP BY category
 * ---------------------------------------------------------------- */

static bool run_test_6(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 6: Arithmetic Expressions — Table Scan ===\n");
  fprintf(stderr, "  SELECT category, SUM(val_int + val_bigint), "
                  "MAX(val_double * 2.5), MIN(val_uint - val_small)\n"
                  "  FROM pa_test GROUP BY category\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("category"));
  /* SUM(val_int + val_bigint) */
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.LoadColumn("val_bigint", kReg2));
  VERIFY(agg.Add(kReg1, kReg2));
  VERIFY(agg.Sum(0, kReg1));
  /* MAX(val_double * 2.5) */
  VERIFY(agg.LoadColumn("val_double", kReg1));
  VERIFY(agg.LoadDouble(2.5, kReg2));
  VERIFY(agg.Mul(kReg1, kReg2));
  VERIFY(agg.Max(1, kReg1));
  /* MIN(val_uint - val_small) */
  VERIFY(agg.LoadColumn("val_uint", kReg1));
  VERIFY(agg.LoadColumn("val_small", kReg2));
  VERIFY(agg.Minus(kReg1, kReg2));
  VERIFY(agg.Min(2, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string cat_val;
    while (!col.end()) {
      cat_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[3];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [category='%s']: SUM=%lld MAX=%.2f MIN=%lld\n",
            cat_val.c_str(), (long long)pa_vals[0], pa_vals[1],
            (long long)pa_vals[2]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, cat_val.c_str(),
                               (unsigned long)cat_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int + val_bigint), MAX(val_double * 2.5), "
          "MIN(CAST(val_uint AS SIGNED) - val_small) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE category='" + escaped + "' GROUP BY category";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, false, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        bool ints[] = {true, false, true};
        for (int i = 0; i < 3; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             ints[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 4)\n", n_groups);
  if (n_groups != 4) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 7: DECIMAL Aggregation — Table Scan
 * SELECT val_tiny, SUM(val_dec), MAX(val_dec2)
 *   FROM pa_test_inno GROUP BY val_tiny
 * ---------------------------------------------------------------- */

static bool run_test_7(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 7: DECIMAL Aggregation — Table Scan ===\n");
  fprintf(stderr, "  SELECT val_tiny, SUM(val_dec), MAX(val_dec2)\n"
                  "  FROM pa_test GROUP BY val_tiny\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("val_tiny"));
  VERIFY(agg.LoadColumn("val_dec", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_dec2", kReg1));
  VERIFY(agg.Max(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    int tiny_val = 0;
    while (!col.end()) {
      tiny_val = col.data_int8();
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [val_tiny=%d]: SUM(val_dec)=%.2f "
                    "MAX(val_dec2)=%.0f\n",
            tiny_val, pa_vals[0], pa_vals[1]);

    if (ctx.validate) {
      std::string sql = std::string(
          "SELECT SUM(val_dec), MAX(val_dec2) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE val_tiny=" + std::to_string(tiny_val) +
          " GROUP BY val_tiny";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {false, false};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             false)) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 5)\n", n_groups);
  if (n_groups != 5) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 8: NULL Handling in COUNT — Table Scan
 * SELECT name, COUNT(nullable_int), SUM(val_int)
 *   FROM pa_test_inno GROUP BY name
 * ---------------------------------------------------------------- */

static bool run_test_8(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 8: NULL Handling in COUNT — Table Scan ===\n");
  fprintf(stderr, "  SELECT name, COUNT(nullable_int), SUM(val_int)\n"
                  "  FROM pa_test GROUP BY name\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.LoadColumn("nullable_int", kReg1));
  VERIFY(agg.Count(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    while (!col.end()) {
      name_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [name='%s']: COUNT(nullable_int)=%lld "
                    "SUM(val_int)=%lld\n",
            name_val.c_str(), (long long)pa_vals[0], (long long)pa_vals[1]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT COUNT(nullable_int), SUM(val_int) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE name='" + escaped + "' GROUP BY name";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             true)) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 9)\n", n_groups);
  if (n_groups != 9) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 9: Charset-Aware GROUP BY — Index Scan
 * SELECT name, SUM(val_int), COUNT(val_int)
 *   FROM pa_test WHERE val_small >= -5 AND val_small < 5
 *   GROUP BY name
 * ---------------------------------------------------------------- */

static bool run_test_9(TestContext &ctx) {
  fprintf(stderr, "\n=== Test 9: Charset-Aware GROUP BY — Index Scan ===\n");
  fprintf(stderr, "  SELECT name, SUM(val_int), COUNT(val_int)\n"
                  "  FROM pa_test WHERE val_small >= -5 AND val_small < 5\n"
                  "  GROUP BY name\n");

  NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Index *idx =
      dict->getIndex("idx_val_small", ctx.ndb_table);
  if (!idx) APIERROR(dict->getNdbError());

  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbIndexScanOperation *indexOp = trans->getNdbIndexScanOperation(idx);
  if (!indexOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  Uint32 scanFlags = NdbScanOperation::SF_OrderBy |
                     NdbScanOperation::SF_MultiRange;
  if (indexOp->readTuples(NdbOperation::LM_CommittedRead, scanFlags) != 0)
    APIERROR(trans->getNdbError());

  /* Bounds: val_small >= -5 AND val_small < 5 */
  Int16 low = -5;
  Int16 high = 5;
  if (indexOp->setBound("val_small",
                        NdbIndexScanOperation::BoundLE, (char *)&low))
    APIERROR(trans->getNdbError());
  if (indexOp->setBound("val_small",
                        NdbIndexScanOperation::BoundGT, (char *)&high))
    APIERROR(trans->getNdbError());
  if (indexOp->end_of_bound(0))
    APIERROR(indexOp->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.Finalize());

  if (indexOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (indexOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    while (!col.end()) {
      name_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [name='%s']: SUM=%lld COUNT=%lld\n",
            name_val.c_str(), (long long)pa_vals[0], (long long)pa_vals[1]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE val_small >= -5 AND val_small < 5 AND name='" +
          escaped + "' GROUP BY name";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             is_int[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d\n", n_groups);
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 10: Index Scan + Filter + Arithmetic
 * SELECT category, SUM(val_int + val_bigint), MIN(val_double)
 *   FROM pa_test
 *   WHERE val_small >= -5 AND val_small < 5 AND val_tiny = 1
 *   GROUP BY category
 *
 * Note: Uses val_int + val_bigint (both signed) instead of
 * val_int + val_uint (signed + unsigned). MySQL's integer addition
 * forces the result unsigned when either operand is unsigned,
 * so a negative result like -444 + 104 = -340 correctly triggers
 * ERROR 1690 "BIGINT UNSIGNED value is out of range".
 * ---------------------------------------------------------------- */

static bool run_test_10(TestContext &ctx) {
  fprintf(stderr,
          "\n=== Test 10: Index Scan + Filter + Arithmetic ===\n");
  fprintf(stderr, "  SELECT category, SUM(val_int + val_bigint), "
                  "MIN(val_double)\n"
                  "  FROM pa_test WHERE val_small >= -5 AND val_small < 5\n"
                  "  AND val_tiny = 1 GROUP BY category\n");

  NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Index *idx =
      dict->getIndex("idx_val_small", ctx.ndb_table);
  if (!idx) APIERROR(dict->getNdbError());

  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbIndexScanOperation *indexOp = trans->getNdbIndexScanOperation(idx);
  if (!indexOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  Uint32 scanFlags = NdbScanOperation::SF_OrderBy |
                     NdbScanOperation::SF_MultiRange;
  if (indexOp->readTuples(NdbOperation::LM_CommittedRead, scanFlags) != 0)
    APIERROR(trans->getNdbError());

  /* Bounds: val_small >= -5 AND val_small < 5 */
  Int16 low = -5;
  Int16 high = 5;
  if (indexOp->setBound("val_small",
                        NdbIndexScanOperation::BoundLE, (char *)&low))
    APIERROR(trans->getNdbError());
  if (indexOp->setBound("val_small",
                        NdbIndexScanOperation::BoundGT, (char *)&high))
    APIERROR(trans->getNdbError());
  if (indexOp->end_of_bound(0))
    APIERROR(indexOp->getNdbError());

  /* Filter: val_tiny (col 9) = 1 */
  Int8 filter_val = 1;
  NdbScanFilter filter(indexOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 9, &filter_val,
                 sizeof(filter_val)) < 0 ||
      filter.end() < 0) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("category"));
  /* SUM(val_int + val_bigint) */
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.LoadColumn("val_bigint", kReg2));
  VERIFY(agg.Add(kReg1, kReg2));
  VERIFY(agg.Sum(0, kReg1));
  /* MIN(val_double) */
  VERIFY(agg.LoadColumn("val_double", kReg1));
  VERIFY(agg.Min(1, kReg1));
  VERIFY(agg.Finalize());

  if (indexOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (indexOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string cat_val;
    while (!col.end()) {
      cat_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [category='%s']: SUM=%lld MIN=%.2f\n",
            cat_val.c_str(), (long long)pa_vals[0], pa_vals[1]);

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, cat_val.c_str(),
                               (unsigned long)cat_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int + val_bigint), "
          "MIN(val_double) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE val_small >= -5 AND val_small < 5 AND val_tiny = 1"
          " AND category='" + escaped + "' GROUP BY category";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, false};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        bool ints[] = {true, false};
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             ints[i])) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.2f SQL=%.2f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d\n", n_groups);
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 11: Large Dataset Charset-Aware GROUP BY — Table Scan
 * SELECT name, SUM(val_int), COUNT(val_int)
 *   FROM pa_test_large GROUP BY name
 * Tests API-side merge of intermediate results from multiple
 * fragments/flushes with different case variants of the same group.
 * ---------------------------------------------------------------- */

static bool run_test_11(TestContext &ctx) {
  fprintf(stderr,
          "\n=== Test 11: Large Dataset API-Side Merge — Table Scan ===\n");
  fprintf(stderr, "  SELECT name, SUM(val_int), COUNT(val_int)\n"
                  "  FROM pa_test_large GROUP BY name\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable("pa_test_large");
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;
  int n_validated = 0;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    while (!col.end()) {
      name_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    /* Print first few and last few groups */
    if (n_groups < 3 || (n_groups >= 297 && n_groups < 300)) {
      fprintf(stderr, "  Group [name='%s']: SUM=%lld COUNT=%lld\n",
              name_val.c_str(), (long long)pa_vals[0], (long long)pa_vals[1]);
    } else if (n_groups == 3) {
      fprintf(stderr, "  ... (showing first 3 and last 3 groups)\n");
    }

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int) FROM ") +
          DB_NAME + ".pa_test_large_inno WHERE name='" +
          escaped + "' GROUP BY name";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             is_int[i])) {
            fprintf(stderr, "    MISMATCH group '%s' agg[%d]: "
                            "PA=%.2f SQL=%.2f\n",
                    name_val.c_str(), i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
        n_validated++;
      } else {
        fprintf(stderr, "    SQL validation query failed for '%s'\n",
                name_val.c_str());
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 300)\n", n_groups);
  if (ctx.validate) {
    fprintf(stderr, "  Validated %d groups against InnoDB\n", n_validated);
  }
  if (n_groups != 300) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 13: Bug B — unsigned minus unsigned underflow
 *
 * Bug: In RegMinusReg, when both operands are unsigned and val0 < val1,
 * PA incorrectly sets res_unsigned = true. MySQL keeps res_unsigned
 * false, so check_integer_overflow detects the overflow:
 *   (unsigned_flag=true && !res_unsigned=true && value<0) → overflow
 * But PA's wrong res_unsigned makes the check:
 *   (true && !true && ...) → no overflow → silently wraps.
 *
 * Example: CAST(0 AS UNSIGNED) - CAST(1000 AS UNSIGNED)
 *   MySQL:  ERROR 1690 (overflow) ✓
 *   PA bug: returns 18446744073709550616 (wrapped)
 *
 * SELECT SUM(val_biguint - CAST(1000 AS UNSIGNED))
 *   FROM pa_test WHERE id = 1
 *
 * Uses BIGINT UNSIGNED column (not INT UNSIGNED) so PA and MySQL
 * see the same types. Single row (id=1, val_biguint=13) so Sum's
 * multi-row accumulation can't mask the Minus bug.
 *   Minus(13, 1000) both BIGINT UNSIGNED → should overflow
 *   Bug: wraps to 18446744073709550629, Sum returns it as-is
 *   Fix: Minus correctly returns overflow error
 *
 * This is a NEGATIVE test — it expects overflow.
 * Before fix: PA silently wraps, returns huge wrong value → FAILS
 * After fix:  PA correctly detects overflow              → PASSES
 * ---------------------------------------------------------------- */

static bool run_test_13(TestContext &ctx) {
  fprintf(stderr,
          "\n=== Test 13: Bug B — unsigned - unsigned underflow ===\n");
  fprintf(stderr, "  SELECT SUM(val_biguint - CAST(1000 AS UNSIGNED))\n"
                  "  FROM pa_test WHERE id = 1\n"
                  "  (single row: val_biguint=13, expects overflow)\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  /* Filter: id (col 0) = 1 — single row with val_uint = 13 */
  Int32 filter_val = 1;
  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 0, &filter_val,
                 sizeof(filter_val)) < 0 ||
      filter.end() < 0) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  NdbAggregator agg(table);
  /* SUM(val_biguint - CAST(1000 AS UNSIGNED)) */
  VERIFY(agg.LoadColumn("val_biguint", kReg1));
  VERIFY(agg.LoadUint64(1000, kReg2));
  VERIFY(agg.Minus(kReg1, kReg2));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  bool pa_overflowed = (scanOp->DoAggregation() == -1);

  if (pa_overflowed) {
    fprintf(stderr, "  PA overflow correctly detected (expected)\n");
  }

  /* Validate against MySQL using the same BIGINT UNSIGNED column
   * and per-row query (NOT SUM, which uses DECIMAL accumulation). */
  bool mysql_overflowed = false;
  if (ctx.validate) {
    std::string sql = std::string(
        "SELECT val_biguint - CAST(1000 AS UNSIGNED) FROM ") +
        DB_NAME + "." + ctx.inno_table + " WHERE id = 1";
    if (mysql_real_query(ctx.mysql, sql.c_str(),
                         (unsigned long)sql.length())) {
      fprintf(stderr, "  MySQL also errors (expected): %s\n",
              mysql_error(ctx.mysql));
      mysql_overflowed = true;
    } else {
      MYSQL_RES *r = mysql_store_result(ctx.mysql);
      if (r) {
        MYSQL_ROW row = mysql_fetch_row(r);
        if (row && row[0]) {
          fprintf(stderr, "  MySQL returned: %s (no overflow)\n", row[0]);
        }
        mysql_free_result(r);
      } else if (mysql_errno(ctx.mysql)) {
        /* Error surfaced at store_result time (row evaluation) */
        fprintf(stderr, "  MySQL also errors (expected): %s\n",
                mysql_error(ctx.mysql));
        mysql_overflowed = true;
      }
    }
  }

  if (pa_overflowed) {
    if (ctx.validate && !mysql_overflowed) {
      fprintf(stderr, "  MISMATCH: PA overflowed but MySQL did not\n");
      ctx.ndb->closeTransaction(trans);
      return false;
    }
    ctx.ndb->closeTransaction(trans);
    return true;  /* PASS — both PA and MySQL overflow */
  }

  /* PA did NOT overflow — bug exists. Read the wrong result. */
  Uint64 pa_val_u = 0;
  Int64 pa_val_s = 0;
  bool got_unsigned = false;
  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  if (!record.end()) {
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigunsigned:
          pa_val_u = result.data_uint64();
          got_unsigned = true;
          break;
        case NdbDictionary::Column::Bigint:
          pa_val_s = result.data_int64();
          break;
        default:
          break;
      }
      result = record.FetchAggregationResult();
    }
  }

  if (got_unsigned) {
    fprintf(stderr, "  BUG: No overflow detected! PA returned unsigned: "
                    "%llu (expected overflow)\n",
            (unsigned long long)pa_val_u);
  } else {
    fprintf(stderr, "  BUG: No overflow detected! PA returned signed: "
                    "%lld (expected overflow)\n",
            (long long)pa_val_s);
  }
  if (ctx.validate && mysql_overflowed) {
    fprintf(stderr, "  MySQL correctly overflowed but PA did not\n");
  }
  ctx.ndb->closeTransaction(trans);
  return false;  /* FAIL — overflow was not detected */
}

/* ----------------------------------------------------------------
 * Test 15: Bug D — INT_MIN64 * n (n != 1), missing overflow in multiply
 *
 * Bug: RegMulReg handles INT_MIN64 * 1 correctly, but when the other
 * operand is not 1, it falls through to val0 = -val0 (or val1 = -val1),
 * which is UNDEFINED BEHAVIOR because -INT_MIN64 overflows signed Int64.
 * MySQL always returns overflow for INT_MIN64 * n where n != 1.
 *
 * Test case: LLONG_MIN * val_tiny(id=4) = -9223372036854775808 * 2
 *   MySQL: overflow ✓
 *   PA bug: undefined behavior (may crash or return wrong result)
 *
 * SELECT SUM(CAST(-9223372036854775808 AS SIGNED) * val_tiny)
 *   FROM pa_test WHERE id = 4
 *
 * This is a NEGATIVE test — it expects overflow.
 * Before fix: UB (wrong result or crash) → FAILS
 * After fix:  Overflow correctly detected  → PASSES
 * ---------------------------------------------------------------- */

static bool run_test_15(TestContext &ctx) {
  fprintf(stderr,
          "\n=== Test 15: Bug D — INT_MIN64 * n overflow ===\n");
  fprintf(stderr, "  SELECT SUM(LLONG_MIN * val_tiny)\n"
                  "  FROM pa_test WHERE id = 4\n"
                  "  (LLONG_MIN * 2, expects overflow)\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  /* Filter: id (col 0) = 4 — single row with val_tiny = 2 */
  Int32 filter_val = 4;
  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 0, &filter_val,
                 sizeof(filter_val)) < 0 ||
      filter.end() < 0) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  NdbAggregator agg(table);
  /* SUM(LLONG_MIN * val_tiny)
   * LLONG_MIN = -9223372036854775808 = -9223372036854775807 - 1 */
  VERIFY(agg.LoadInt64(-9223372036854775807LL - 1, kReg1));
  VERIFY(agg.LoadColumn("val_tiny", kReg2));
  VERIFY(agg.Mul(kReg1, kReg2));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  bool pa_overflowed = (scanOp->DoAggregation() == -1);

  if (pa_overflowed) {
    fprintf(stderr, "  PA overflow correctly detected (expected)\n");
  }

  /* Validate against MySQL */
  bool mysql_overflowed = false;
  if (ctx.validate) {
    std::string sql = std::string(
        "SELECT CAST('-9223372036854775808' AS SIGNED) * val_tiny FROM ") +
        DB_NAME + "." + ctx.inno_table + " WHERE id = 4";
    if (mysql_real_query(ctx.mysql, sql.c_str(),
                         (unsigned long)sql.length())) {
      fprintf(stderr, "  MySQL also errors (expected): %s\n",
              mysql_error(ctx.mysql));
      mysql_overflowed = true;
    } else {
      MYSQL_RES *r = mysql_store_result(ctx.mysql);
      if (r) {
        MYSQL_ROW row = mysql_fetch_row(r);
        if (row && row[0]) {
          fprintf(stderr, "  MySQL returned: %s (no overflow)\n", row[0]);
        }
        mysql_free_result(r);
      } else if (mysql_errno(ctx.mysql)) {
        fprintf(stderr, "  MySQL also errors (expected): %s\n",
                mysql_error(ctx.mysql));
        mysql_overflowed = true;
      }
    }
  }

  if (pa_overflowed) {
    if (ctx.validate && !mysql_overflowed) {
      fprintf(stderr, "  MISMATCH: PA overflowed but MySQL did not\n");
      ctx.ndb->closeTransaction(trans);
      return false;
    }
    ctx.ndb->closeTransaction(trans);
    return true;  /* PASS — both PA and MySQL overflow */
  }

  /* PA did NOT overflow — bug exists */
  Uint64 pa_val_u = 0;
  Int64 pa_val_s = 0;
  bool got_unsigned = false;
  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  if (!record.end()) {
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigunsigned:
          pa_val_u = result.data_uint64();
          got_unsigned = true;
          break;
        case NdbDictionary::Column::Bigint:
          pa_val_s = result.data_int64();
          break;
        default:
          break;
      }
      result = record.FetchAggregationResult();
    }
  }

  if (got_unsigned) {
    fprintf(stderr, "  BUG: No overflow detected! PA returned unsigned: "
                    "%llu (expected overflow)\n",
            (unsigned long long)pa_val_u);
  } else {
    fprintf(stderr, "  BUG: No overflow detected! PA returned signed: "
                    "%lld (expected overflow)\n",
            (long long)pa_val_s);
  }
  if (ctx.validate && mysql_overflowed) {
    fprintf(stderr, "  MySQL correctly overflowed but PA did not\n");
  }
  ctx.ndb->closeTransaction(trans);
  return false;  /* FAIL — overflow was not detected */
}

/* ----------------------------------------------------------------
 * Test 14: Bug C — signed minus signed, missing overflow check
 *
 * Bug: RegMinusReg has no else-branch for (!a.is_unsigned && !b.is_unsigned).
 * MySQL's Item_func_minus::int_op checks:
 *   if (val0 >= 0 && val1 < 0) res_unsigned = true;
 *   else if (val0 < 0 && val1 > 0 && res >= 0) goto err;  // overflow
 * PA is missing both checks.
 *
 * Test case: val_bigint(id=0) - LLONG_MAX = -5000000 - 9223372036854775807
 * Mathematical result: -9223372036859775807 (< LLONG_MIN → overflow)
 * Wrapped result: 9223372036849775809 (positive! val0<0, val1>0, res>=0)
 *
 *   MySQL: overflow detected ✓
 *   PA bug: returns 9223372036849775809 (wrong)
 *
 * SELECT SUM(val_bigint - CAST(9223372036854775807 AS SIGNED))
 *   FROM pa_test WHERE id = 0
 *
 * This is a NEGATIVE test — it expects overflow.
 * Before fix: PA returns wrong positive value → FAILS
 * After fix:  PA correctly detects overflow    → PASSES
 * ---------------------------------------------------------------- */

static bool run_test_14(TestContext &ctx) {
  fprintf(stderr,
          "\n=== Test 14: Bug C — signed - signed overflow ===\n");
  fprintf(stderr, "  SELECT SUM(val_bigint - CAST(9223372036854775807 "
                  "AS SIGNED))\n"
                  "  FROM pa_test WHERE id = 0\n"
                  "  (val_bigint=-5000000, expects overflow)\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  /* Filter: id (col 0) = 0 — single row with val_bigint = -5000000 */
  Int32 filter_val = 0;
  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, 0, &filter_val,
                 sizeof(filter_val)) < 0 ||
      filter.end() < 0) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  NdbAggregator agg(table);
  /* SUM(val_bigint - LLONG_MAX)
   * Both operands are signed BIGINT. */
  VERIFY(agg.LoadColumn("val_bigint", kReg1));
  VERIFY(agg.LoadInt64(9223372036854775807LL, kReg2));
  VERIFY(agg.Minus(kReg1, kReg2));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  bool pa_overflowed = (scanOp->DoAggregation() == -1);

  if (pa_overflowed) {
    fprintf(stderr, "  PA overflow correctly detected (expected)\n");
  }

  /* Validate against MySQL: signed - signed overflow */
  bool mysql_overflowed = false;
  if (ctx.validate) {
    std::string sql = std::string(
        "SELECT val_bigint - CAST(9223372036854775807 AS SIGNED) FROM ") +
        DB_NAME + "." + ctx.inno_table + " WHERE id = 0";
    if (mysql_real_query(ctx.mysql, sql.c_str(),
                         (unsigned long)sql.length())) {
      fprintf(stderr, "  MySQL also errors (expected): %s\n",
              mysql_error(ctx.mysql));
      mysql_overflowed = true;
    } else {
      MYSQL_RES *r = mysql_store_result(ctx.mysql);
      if (r) {
        MYSQL_ROW row = mysql_fetch_row(r);
        if (row && row[0]) {
          fprintf(stderr, "  MySQL returned: %s (no overflow)\n", row[0]);
        }
        mysql_free_result(r);
      } else if (mysql_errno(ctx.mysql)) {
        fprintf(stderr, "  MySQL also errors (expected): %s\n",
                mysql_error(ctx.mysql));
        mysql_overflowed = true;
      }
    }
  }

  if (pa_overflowed) {
    if (ctx.validate && !mysql_overflowed) {
      fprintf(stderr, "  MISMATCH: PA overflowed but MySQL did not\n");
      ctx.ndb->closeTransaction(trans);
      return false;
    }
    ctx.ndb->closeTransaction(trans);
    return true;  /* PASS — both PA and MySQL overflow */
  }

  /* PA did NOT overflow — bug exists */
  Uint64 pa_val_u = 0;
  Int64 pa_val_s = 0;
  bool got_unsigned = false;
  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  if (!record.end()) {
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigunsigned:
          pa_val_u = result.data_uint64();
          got_unsigned = true;
          break;
        case NdbDictionary::Column::Bigint:
          pa_val_s = result.data_int64();
          break;
        default:
          break;
      }
      result = record.FetchAggregationResult();
    }
  }

  if (got_unsigned) {
    fprintf(stderr, "  BUG: No overflow detected! PA returned unsigned: "
                    "%llu (expected overflow)\n",
            (unsigned long long)pa_val_u);
  } else {
    fprintf(stderr, "  BUG: No overflow detected! PA returned signed: "
                    "%lld (expected overflow)\n",
            (long long)pa_val_s);
  }
  if (ctx.validate && mysql_overflowed) {
    fprintf(stderr, "  MySQL correctly overflowed but PA did not\n");
  }
  ctx.ndb->closeTransaction(trans);
  return false;  /* FAIL — overflow was not detected */
}

/* ----------------------------------------------------------------
 * Test 12: Bug E — Modulo unsigned_flag (signed % unsigned)
 *
 * Bug: PA sets unsigned_flag = (a.is_unsigned | b.is_unsigned) for
 * modulo, but MySQL uses unsigned_flag = args[0]->unsigned_flag
 * (dividend only). When a signed negative dividend is modded by an
 * unsigned divisor, PA's wrong unsigned_flag causes a false overflow.
 *
 * Example: (-500) % 13 = -6
 *   res_unsigned = false (val0 is negative)
 *   PA:  unsigned_flag = (false | true) = true   → WRONG
 *        check: (true && !false && -6<0) → false overflow!
 *   Fix: unsigned_flag = a.is_unsigned = false
 *        check: (!false && false && ...) → no overflow → returns -6
 *
 * SELECT SUM(val_int % val_uint) FROM pa_test WHERE val_uint > 0
 *
 * Before fix: DoAggregation fails (false overflow) → FAILS
 * After fix:  Correct result matches InnoDB       → PASSES
 * ---------------------------------------------------------------- */

static bool run_test_12(TestContext &ctx) {
  fprintf(stderr,
          "\n=== Test 12: Bug E — Modulo signed %% unsigned ===\n");
  fprintf(stderr, "  SELECT SUM(val_int %% val_uint)\n"
                  "  FROM pa_test WHERE val_uint > 0\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  /* Filter: val_uint (col 5) > 0 to avoid division by zero */
  Uint32 filter_val = 0;
  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_GT, 5, &filter_val,
                 sizeof(filter_val)) < 0 ||
      filter.end() < 0) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  NdbAggregator agg(table);
  /* No GroupBy — aggregate all qualifying rows */
  /* SUM(val_int % val_uint) */
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.LoadColumn("val_uint", kReg2));
  VERIFY(agg.Mod(kReg1, kReg2));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed (Bug E: false overflow): "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  bool all_valid = true;
  double pa_val = 0;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  if (!record.end()) {
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_val = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_val = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_val = result.data_double();
          break;
        default:
          pa_val = 0;
      }
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Result: SUM(val_int %% val_uint) = %lld\n",
            (long long)pa_val);
  }

  if (ctx.validate) {
    std::string sql = std::string(
        "SELECT SUM(val_int % val_uint) FROM ") +
        DB_NAME + "." + ctx.inno_table +
        " WHERE val_uint > 0";

    std::vector<double> sql_vals;
    std::vector<bool> is_int = {true};
    if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
      if (!compare_value(pa_val,
                         std::to_string(sql_vals[0]).c_str(), true)) {
        fprintf(stderr, "    MISMATCH: PA=%lld SQL=%lld\n",
                (long long)pa_val, (long long)sql_vals[0]);
        all_valid = false;
      }
    } else {
      fprintf(stderr, "    SQL validation query failed\n");
      all_valid = false;
    }
  }

  ctx.ndb->closeTransaction(trans);
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 16: Multi-Column Numeric GROUP BY — Table Scan
 *          (exercises all_binary_cmp fast path with multiple columns)
 * SELECT val_tiny, val_small, SUM(val_int), COUNT(val_int)
 *   FROM pa_test GROUP BY val_tiny, val_small
 * ---------------------------------------------------------------- */

static bool run_test_16(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 16: Multi-Column Numeric GROUP BY — Table Scan ===\n");
  fprintf(stderr, "  SELECT val_tiny, val_small, SUM(val_int), COUNT(val_int)\n"
                  "  FROM pa_test GROUP BY val_tiny, val_small\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("val_tiny"));
  VERIFY(agg.GroupBy("val_small"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    int tiny_val = 0;
    int small_val = 0;
    int col_idx = 0;
    while (!col.end()) {
      if (col_idx == 0)
        tiny_val = col.data_int8();
      else
        small_val = col.data_int16();
      col_idx++;
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [val_tiny=%d, val_small=%d]: SUM=%lld COUNT=%lld\n",
            tiny_val, small_val, (long long)pa_vals[0], (long long)pa_vals[1]);

    if (ctx.validate) {
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int) FROM ") + DB_NAME + "." +
          ctx.inno_table + " WHERE val_tiny=" + std::to_string(tiny_val) +
          " AND val_small=" + std::to_string(small_val) +
          " GROUP BY val_tiny, val_small";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(), true)) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.0f SQL=%.0f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d\n", n_groups);
  if (n_groups == 0) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 17: Nullable Numeric GROUP BY — Table Scan
 *          (exercises all_binary_cmp fast path with NULL values)
 * SELECT nullable_int, SUM(val_int), COUNT(val_int)
 *   FROM pa_test GROUP BY nullable_int
 * ---------------------------------------------------------------- */

static bool run_test_17(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 17: Nullable Numeric GROUP BY — Table Scan ===\n");
  fprintf(stderr, "  SELECT nullable_int, SUM(val_int), COUNT(val_int)\n"
                  "  FROM pa_test GROUP BY nullable_int\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("nullable_int"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    bool is_null = false;
    int nullable_val = 0;
    while (!col.end()) {
      if (col.is_null()) {
        is_null = true;
      } else {
        nullable_val = col.data_int32();
      }
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[2];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    if (is_null) {
      fprintf(stderr, "  Group [nullable_int=NULL]: SUM=%lld COUNT=%lld\n",
              (long long)pa_vals[0], (long long)pa_vals[1]);
    } else {
      fprintf(stderr, "  Group [nullable_int=%d]: SUM=%lld COUNT=%lld\n",
              nullable_val, (long long)pa_vals[0], (long long)pa_vals[1]);
    }

    if (ctx.validate) {
      std::string sql;
      if (is_null) {
        sql = std::string(
            "SELECT SUM(val_int), COUNT(val_int) FROM ") + DB_NAME + "." +
            ctx.inno_table + " WHERE nullable_int IS NULL";
      } else {
        sql = std::string(
            "SELECT SUM(val_int), COUNT(val_int) FROM ") + DB_NAME + "." +
            ctx.inno_table + " WHERE nullable_int=" +
            std::to_string(nullable_val);
      }

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (int i = 0; i < 2; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(), true)) {
            fprintf(stderr, "    MISMATCH agg[%d]: PA=%.0f SQL=%.0f\n",
                    i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed\n");
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  /* 22 rows: ids 0,3,6,9,12,15,18,21 are NULL (8 rows), rest are unique
     non-null values (14 rows) => 15 groups */
  fprintf(stderr, "  Total groups: %d (expected 15)\n", n_groups);
  if (n_groups != 15) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 18: Reject Blob/Text as GROUP BY columns
 *          (validates that GroupBy() returns false for unsupported types)
 * ---------------------------------------------------------------- */

static bool run_test_18(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 18: Reject Blob/Text as GROUP BY columns ===\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable("pa_test_blob");
  if (!table) {
    fprintf(stderr, "  SKIP: pa_test_blob table not found\n");
    return false;
  }

  bool all_valid = true;

  /* Test 1: GroupBy(name) with BLOB column should fail */
  {
    NdbAggregator agg(table);
    bool ret = agg.GroupBy("val_blob");
    if (ret) {
      fprintf(stderr, "  FAIL: GroupBy(\"val_blob\") should have returned "
                      "false but returned true\n");
      all_valid = false;
    } else {
      const AggregationError &err = agg.GetError();
      if (err.errno_ != kErrUnSupportedColumn) {
        fprintf(stderr, "  FAIL: Expected error %u (kErrUnSupportedColumn) "
                        "but got %u\n", kErrUnSupportedColumn, err.errno_);
        all_valid = false;
      } else {
        fprintf(stderr, "  PASS: GroupBy(\"val_blob\") correctly rejected "
                        "with: %s\n", err.err_msg_);
      }
    }
  }

  /* Test 2: GroupBy(name) with TEXT column should fail */
  {
    NdbAggregator agg(table);
    bool ret = agg.GroupBy("val_text");
    if (ret) {
      fprintf(stderr, "  FAIL: GroupBy(\"val_text\") should have returned "
                      "false but returned true\n");
      all_valid = false;
    } else {
      const AggregationError &err = agg.GetError();
      if (err.errno_ != kErrUnSupportedColumn) {
        fprintf(stderr, "  FAIL: Expected error %u (kErrUnSupportedColumn) "
                        "but got %u\n", kErrUnSupportedColumn, err.errno_);
        all_valid = false;
      } else {
        fprintf(stderr, "  PASS: GroupBy(\"val_text\") correctly rejected "
                        "with: %s\n", err.err_msg_);
      }
    }
  }

  /* Test 3: GroupBy(col_id) with BLOB column should fail */
  {
    const NdbDictionary::Column *blob_col = table->getColumn("val_blob");
    assert(blob_col != nullptr);
    Int32 blob_col_id = blob_col->getAttrId();

    NdbAggregator agg(table);
    bool ret = agg.GroupBy(blob_col_id);
    if (ret) {
      fprintf(stderr, "  FAIL: GroupBy(%d) [val_blob] should have returned "
                      "false but returned true\n", blob_col_id);
      all_valid = false;
    } else {
      fprintf(stderr, "  PASS: GroupBy(%d) [val_blob] correctly rejected\n",
              blob_col_id);
    }
  }

  /* Test 4: GroupBy with INT column should succeed (sanity check) */
  {
    NdbAggregator agg(table);
    bool ret = agg.GroupBy("val_int");
    if (!ret) {
      fprintf(stderr, "  FAIL: GroupBy(\"val_int\") should have succeeded "
                      "but returned false\n");
      all_valid = false;
    } else {
      fprintf(stderr, "  PASS: GroupBy(\"val_int\") correctly accepted\n");
    }
  }

  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 19: Repeated PA Scan Lifecycle
 *   Run the same GROUP BY query 3 times in sequence to verify that
 *   the factory create / Init / Destruct cycle works correctly and
 *   the bump allocator resets between scans.
 * ---------------------------------------------------------------- */

static bool run_test_19(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 19: Repeated PA Scan Lifecycle ===\n");
  fprintf(stderr, "  SELECT category, SUM(val_int), COUNT(val_int)\n"
                  "  FROM pa_test GROUP BY category   (x3 iterations)\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  const int N_ITERS = 3;
  /* category -> {sum, count} */
  std::map<std::string, std::vector<double>> run_results[N_ITERS];

  for (int iter = 0; iter < N_ITERS; iter++) {
    NdbTransaction *trans = ctx.ndb->startTransaction();
    if (!trans) APIERROR(ctx.ndb->getNdbError());

    NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
    if (!scanOp) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
      APIERROR(trans->getNdbError());

    NdbAggregator agg(table);
    VERIFY(agg.GroupBy("category"));
    VERIFY(agg.LoadColumn("val_int", kReg1));
    VERIFY(agg.Sum(0, kReg1));
    VERIFY(agg.LoadColumn("val_int", kReg1));
    VERIFY(agg.Count(1, kReg1));
    VERIFY(agg.Finalize());

    if (scanOp->setAggregationCode(&agg) == -1) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    if (scanOp->DoAggregation() == -1) {
      std::cout << "DoAggregation failed (iter " << iter << "): "
                << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    NdbAggregator::ResultRecord record = agg.FetchResultRecord();
    while (!record.end()) {
      NdbAggregator::Column col = record.FetchGroupbyColumn();
      std::string cat_val;
      while (!col.end()) {
        cat_val = extract_varchar(col);
        col = record.FetchGroupbyColumn();
      }

      std::vector<double> vals;
      NdbAggregator::Result result = record.FetchAggregationResult();
      while (!result.end()) {
        switch (result.type()) {
          case NdbDictionary::Column::Bigint:
            vals.push_back((double)result.data_int64());
            break;
          case NdbDictionary::Column::Bigunsigned:
            vals.push_back((double)result.data_uint64());
            break;
          default:
            vals.push_back(0);
        }
        result = record.FetchAggregationResult();
      }

      run_results[iter][cat_val] = vals;
      record = agg.FetchResultRecord();
    }

    ctx.ndb->closeTransaction(trans);
    fprintf(stderr, "  Iteration %d: %d groups\n",
            iter, (int)run_results[iter].size());
  }

  /* Compare all 3 runs — they must be identical */
  bool all_valid = true;
  if (run_results[0].size() != run_results[1].size() ||
      run_results[0].size() != run_results[2].size()) {
    fprintf(stderr, "  FAIL: group counts differ across iterations: "
                    "%d / %d / %d\n",
            (int)run_results[0].size(), (int)run_results[1].size(),
            (int)run_results[2].size());
    all_valid = false;
  }

  for (auto &kv : run_results[0]) {
    for (int iter = 1; iter < N_ITERS; iter++) {
      auto it = run_results[iter].find(kv.first);
      if (it == run_results[iter].end()) {
        fprintf(stderr, "  FAIL: group '%s' missing in iteration %d\n",
                kv.first.c_str(), iter);
        all_valid = false;
        continue;
      }
      if (kv.second.size() != it->second.size()) {
        fprintf(stderr, "  FAIL: group '%s' agg count differs in iter %d\n",
                kv.first.c_str(), iter);
        all_valid = false;
        continue;
      }
      for (size_t a = 0; a < kv.second.size(); a++) {
        if ((long long)kv.second[a] != (long long)it->second[a]) {
          fprintf(stderr, "  FAIL: group '%s' agg[%d] iter0=%lld iter%d=%lld\n",
                  kv.first.c_str(), (int)a,
                  (long long)kv.second[a], iter,
                  (long long)it->second[a]);
          all_valid = false;
        }
      }
    }
  }

  /* Validate against InnoDB */
  if (ctx.validate) {
    for (auto &kv : run_results[0]) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, kv.first.c_str(),
                               (unsigned long)kv.first.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int) FROM ") +
          DB_NAME + "." + ctx.inno_table +
          " WHERE category='" + escaped + "' GROUP BY category";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        for (size_t i = 0; i < 2 && i < kv.second.size(); i++) {
          if (!compare_value(kv.second[i],
                             std::to_string(sql_vals[i]).c_str(), true)) {
            fprintf(stderr, "    MISMATCH group '%s' agg[%d]: PA=%lld SQL=%lld\n",
                    kv.first.c_str(), (int)i,
                    (long long)kv.second[i], (long long)sql_vals[i]);
            all_valid = false;
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed for '%s'\n",
                kv.first.c_str());
        all_valid = false;
      }
    }
  }

  fprintf(stderr, "  Total groups (run 0): %d (expected 4)\n",
          (int)run_results[0].size());
  if ((int)run_results[0].size() != 4) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 20: Many Aggregation Operations (4 aggs × 300 groups)
 *   Uses pa_test_large (2000 rows, 300 name-groups) with 4 mixed-type
 *   aggregation ops.  300 groups × 4 result items ≈ 19 KB of result
 *   data, which forces the kernel to call SendAggregationResult()
 *   mid-scan (before the fragment scan completes).  This exercises
 *   PrepareAggResIfNeeded() serialization with val_len() and the
 *   sendBatchedFragmentedSignal large-result path.
 * ---------------------------------------------------------------- */

static bool run_test_20(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 20: Many Aggregation Operations (4 aggs) ===\n");
  fprintf(stderr, "  SELECT name, SUM(val_int), COUNT(val_int), "
                  "MIN(val_double), MAX(val_bigint)\n"
                  "  FROM pa_test_large GROUP BY name\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable("pa_test_large");
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("name"));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val_int", kReg1));
  VERIFY(agg.Count(1, kReg1));
  VERIFY(agg.LoadColumn("val_double", kReg1));
  VERIFY(agg.Min(2, kReg1));
  VERIFY(agg.LoadColumn("val_bigint", kReg1));
  VERIFY(agg.Max(3, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;
  int n_validated = 0;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    std::string name_val;
    while (!col.end()) {
      name_val = extract_varchar(col);
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[4];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          pa_vals[vi] = (double)result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          pa_vals[vi] = (double)result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          pa_vals[vi] = result.data_double();
          break;
        default:
          pa_vals[vi] = 0;
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    /* Print first few and last few groups */
    if (n_groups < 3 || (n_groups >= 297 && n_groups < 300)) {
      fprintf(stderr, "  Group [name='%s']: SUM=%lld COUNT=%lld "
                      "MIN=%.2f MAX=%lld\n",
              name_val.c_str(), (long long)pa_vals[0], (long long)pa_vals[1],
              pa_vals[2], (long long)pa_vals[3]);
    } else if (n_groups == 3) {
      fprintf(stderr, "  ... (showing first 3 and last 3 groups)\n");
    }

    if (ctx.validate) {
      char escaped[256];
      mysql_real_escape_string(ctx.mysql, escaped, name_val.c_str(),
                               (unsigned long)name_val.length());
      std::string sql = std::string(
          "SELECT SUM(val_int), COUNT(val_int), MIN(val_double), "
          "MAX(val_bigint) FROM ") + DB_NAME + ".pa_test_large_inno"
          " WHERE name='" + escaped + "' GROUP BY name";

      std::vector<double> sql_vals;
      std::vector<bool> is_int = {true, true, false, true};
      if (run_sql_and_fetch(*ctx.mysql, sql, sql_vals, is_int)) {
        bool ints[] = {true, true, false, true};
        for (int i = 0; i < 4; i++) {
          if (!compare_value(pa_vals[i],
                             std::to_string(sql_vals[i]).c_str(),
                             ints[i])) {
            fprintf(stderr, "    MISMATCH group '%s' agg[%d]: "
                            "PA=%.2f SQL=%.2f\n",
                    name_val.c_str(), i, pa_vals[i], sql_vals[i]);
            all_valid = false;
          }
        }
        n_validated++;
      } else {
        fprintf(stderr, "    SQL validation query failed for '%s'\n",
                name_val.c_str());
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 300)\n", n_groups);
  if (ctx.validate) {
    fprintf(stderr, "  Validated %d groups against InnoDB\n", n_validated);
  }
  if (n_groups != 300) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 21: All-NULL SUM/MIN/MAX Merge Across Fragments
 *   When all rows in a group have NULL for the aggregated column,
 *   the kernel produces NDB_TYPE_UNDEFINED AggResItems. The API
 *   must correctly merge UNDEFINED with UNDEFINED and with reals.
 * ---------------------------------------------------------------- */

static bool run_test_21(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 21: All-NULL SUM/MIN/MAX Merge ===\n");
  fprintf(stderr, "  SELECT grp, SUM(val), MIN(val), MAX(val), COUNT(val)\n"
                  "  FROM pa_test_nullagg GROUP BY grp\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable("pa_test_nullagg");
  if (!table) APIERROR(dict->getNdbError());

  NdbTransaction *trans = ctx.ndb->startTransaction();
  if (!trans) APIERROR(ctx.ndb->getNdbError());

  NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
  if (!scanOp) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
    APIERROR(trans->getNdbError());

  NdbAggregator agg(table);
  VERIFY(agg.GroupBy("grp"));
  VERIFY(agg.LoadColumn("val", kReg1));
  VERIFY(agg.Sum(0, kReg1));
  VERIFY(agg.LoadColumn("val", kReg1));
  VERIFY(agg.Min(1, kReg1));
  VERIFY(agg.LoadColumn("val", kReg1));
  VERIFY(agg.Max(2, kReg1));
  VERIFY(agg.LoadColumn("val", kReg1));
  VERIFY(agg.Count(3, kReg1));
  VERIFY(agg.Finalize());

  if (scanOp->setAggregationCode(&agg) == -1) {
    std::cout << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  if (scanOp->DoAggregation() == -1) {
    std::cout << "DoAggregation failed: "
              << trans->getNdbError().message << std::endl;
    ctx.ndb->closeTransaction(trans);
    return false;
  }

  int n_groups = 0;
  bool all_valid = true;

  NdbAggregator::ResultRecord record = agg.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    int grp_val = 0;
    while (!col.end()) {
      grp_val = col.data_int32();
      col = record.FetchGroupbyColumn();
    }

    double pa_vals[4];
    bool pa_nulls[4];
    int vi = 0;
    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      pa_nulls[vi] = result.is_null();
      if (pa_nulls[vi]) {
        pa_vals[vi] = 0;
      } else {
        switch (result.type()) {
          case NdbDictionary::Column::Bigint:
            pa_vals[vi] = (double)result.data_int64();
            break;
          case NdbDictionary::Column::Bigunsigned:
            pa_vals[vi] = (double)result.data_uint64();
            break;
          case NdbDictionary::Column::Double:
            pa_vals[vi] = result.data_double();
            break;
          default:
            pa_vals[vi] = 0;
        }
      }
      vi++;
      result = record.FetchAggregationResult();
    }

    fprintf(stderr, "  Group [grp=%d]: SUM=%s MIN=%s MAX=%s COUNT=%lld\n",
            grp_val,
            pa_nulls[0] ? "NULL" : std::to_string((long long)pa_vals[0]).c_str(),
            pa_nulls[1] ? "NULL" : std::to_string((long long)pa_vals[1]).c_str(),
            pa_nulls[2] ? "NULL" : std::to_string((long long)pa_vals[2]).c_str(),
            (long long)pa_vals[3]);

    if (ctx.validate) {
      std::string sql = std::string(
          "SELECT SUM(val), MIN(val), MAX(val), COUNT(val) FROM ") +
          DB_NAME + ".pa_test_nullagg_inno WHERE grp=" +
          std::to_string(grp_val) + " GROUP BY grp";

      std::vector<double> sql_vals;
      std::vector<bool> sql_nulls;
      std::vector<bool> is_int = {true, true, true, true};
      if (run_sql_and_fetch_nullable(*ctx.mysql, sql, sql_vals, sql_nulls,
                                     is_int)) {
        for (int i = 0; i < 4; i++) {
          if (pa_nulls[i] != sql_nulls[i]) {
            fprintf(stderr, "    MISMATCH grp=%d agg[%d]: "
                            "PA_null=%d SQL_null=%d\n",
                    grp_val, i, (int)pa_nulls[i], (int)sql_nulls[i]);
            all_valid = false;
          } else if (!pa_nulls[i]) {
            if (!compare_value(pa_vals[i],
                               std::to_string(sql_vals[i]).c_str(), true)) {
              fprintf(stderr, "    MISMATCH grp=%d agg[%d]: PA=%lld SQL=%lld\n",
                      grp_val, i,
                      (long long)pa_vals[i], (long long)sql_vals[i]);
              all_valid = false;
            }
          }
        }
      } else {
        fprintf(stderr, "    SQL validation query failed for grp=%d\n",
                grp_val);
        all_valid = false;
      }
    }

    n_groups++;
    record = agg.FetchResultRecord();
  }

  ctx.ndb->closeTransaction(trans);
  fprintf(stderr, "  Total groups: %d (expected 3)\n", n_groups);
  if (n_groups != 3) all_valid = false;
  return all_valid;
}

/* ----------------------------------------------------------------
 * Test 22: Empty Result Set With GROUP BY
 *   Verifies the zero-groups path in PrepareAggResIfNeeded() and
 *   cleanup of an AggInterpreter that was initialized but never
 *   received any matching rows.
 * ---------------------------------------------------------------- */

static bool run_test_22(TestContext &ctx) {
  fprintf(stderr,
      "\n=== Test 22: Empty Result Set ===\n");

  const NdbDictionary::Dictionary *dict = ctx.ndb->getDictionary();
  const NdbDictionary::Table *table = dict->getTable(ctx.ndb_table);
  if (!table) APIERROR(dict->getNdbError());

  bool all_valid = true;

  /* Part A: GROUP BY with filter that matches no rows */
  {
    fprintf(stderr, "  Part A: SELECT category, SUM(val_int) FROM pa_test "
                    "WHERE id > 99999 GROUP BY category\n");

    NdbTransaction *trans = ctx.ndb->startTransaction();
    if (!trans) APIERROR(ctx.ndb->getNdbError());

    NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
    if (!scanOp) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
      APIERROR(trans->getNdbError());

    /* Filter: id (col 0) > 99999 — no rows match */
    Int32 filter_val = 99999;
    NdbScanFilter filter(scanOp);
    if (filter.begin(NdbScanFilter::AND) < 0 ||
        filter.cmp(NdbScanFilter::COND_GT, 0, &filter_val,
                   sizeof(filter_val)) < 0 ||
        filter.end() < 0) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    NdbAggregator agg(table);
    VERIFY(agg.GroupBy("category"));
    VERIFY(agg.LoadColumn("val_int", kReg1));
    VERIFY(agg.Sum(0, kReg1));
    VERIFY(agg.Finalize());

    if (scanOp->setAggregationCode(&agg) == -1) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    if (scanOp->DoAggregation() == -1) {
      std::cout << "DoAggregation failed: "
                << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    NdbAggregator::ResultRecord record = agg.FetchResultRecord();
    int n_groups = 0;
    while (!record.end()) {
      n_groups++;
      record = agg.FetchResultRecord();
    }

    ctx.ndb->closeTransaction(trans);
    fprintf(stderr, "  Part A: %d groups (expected 0)\n", n_groups);
    if (n_groups != 0) {
      fprintf(stderr, "  FAIL: expected 0 groups but got %d\n", n_groups);
      all_valid = false;
    }
  }

  /* Part B: No GROUP BY with filter that matches no rows */
  {
    fprintf(stderr, "  Part B: SELECT SUM(val_int) FROM pa_test "
                    "WHERE id > 99999\n");

    NdbTransaction *trans = ctx.ndb->startTransaction();
    if (!trans) APIERROR(ctx.ndb->getNdbError());

    NdbScanOperation *scanOp = trans->getNdbScanOperation(table);
    if (!scanOp) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    if (scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)
      APIERROR(trans->getNdbError());

    /* Filter: id (col 0) > 99999 */
    Int32 filter_val = 99999;
    NdbScanFilter filter(scanOp);
    if (filter.begin(NdbScanFilter::AND) < 0 ||
        filter.cmp(NdbScanFilter::COND_GT, 0, &filter_val,
                   sizeof(filter_val)) < 0 ||
        filter.end() < 0) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    NdbAggregator agg(table);
    /* No GroupBy */
    VERIFY(agg.LoadColumn("val_int", kReg1));
    VERIFY(agg.Sum(0, kReg1));
    VERIFY(agg.Finalize());

    if (scanOp->setAggregationCode(&agg) == -1) {
      std::cout << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    if (scanOp->DoAggregation() == -1) {
      std::cout << "DoAggregation failed: "
                << trans->getNdbError().message << std::endl;
      ctx.ndb->closeTransaction(trans);
      return false;
    }

    NdbAggregator::ResultRecord record = agg.FetchResultRecord();
    bool found_result = false;
    bool sum_is_null = false;
    double sum_val = 0;

    if (!record.end()) {
      found_result = true;
      NdbAggregator::Result result = record.FetchAggregationResult();
      if (!result.end()) {
        sum_is_null = result.is_null();
        if (!sum_is_null) {
          switch (result.type()) {
            case NdbDictionary::Column::Bigint:
              sum_val = (double)result.data_int64();
              break;
            case NdbDictionary::Column::Bigunsigned:
              sum_val = (double)result.data_uint64();
              break;
            default:
              sum_val = 0;
          }
        }
      }
    }

    ctx.ndb->closeTransaction(trans);

    if (found_result) {
      fprintf(stderr, "  Part B: 1 result record, SUM=%s\n",
              sum_is_null ? "NULL" : std::to_string((long long)sum_val).c_str());
      /* Without GROUP BY and no matching rows, SUM should be NULL */
      if (!sum_is_null) {
        fprintf(stderr, "  FAIL: expected NULL SUM but got %lld\n",
                (long long)sum_val);
        all_valid = false;
      }
    } else {
      fprintf(stderr, "  Part B: no result record (expected 1 with NULL)\n");
      /* It's also acceptable to return 0 records for no-GROUP-BY empty set
         in the PA implementation, since MySQL handles this at a higher level */
      fprintf(stderr, "  INFO: no result record returned — acceptable\n");
    }
  }

  return all_valid;
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

int main(int argc, char **argv) {
  if (argc < 3) {
    fprintf(stderr,
            "Usage: %s <socket> <connectstring> "
            "[load:true/false] [validate:true/false]\n",
            argv[0]);
    exit(1);
  }

  const char *socket = argv[1];
  const char *connectstring = argv[2];

  bool do_load = true;
  bool do_validate = true;

  for (int i = 3; i < argc; i++) {
    if (strcmp(argv[i], "load:false") == 0) do_load = false;
    else if (strcmp(argv[i], "load:true") == 0) do_load = true;
    else if (strcmp(argv[i], "validate:false") == 0) do_validate = false;
    else if (strcmp(argv[i], "validate:true") == 0) do_validate = true;
    else {
      fprintf(stderr, "Unknown argument: %s\n", argv[i]);
      exit(1);
    }
  }

  fprintf(stderr, "=== Pushdown Aggregation Charset Test ===\n");
  fprintf(stderr, "  load=%s validate=%s\n",
          do_load ? "true" : "false",
          do_validate ? "true" : "false");

  /* MySQL connection */
  MYSQL mysql;
  mysql_init(&mysql);
  if (!mysql_real_connect(&mysql, "localhost", "root", "",
                          "", 0, socket, 0)) {
    MYSQLERROR(mysql);
  }

  /* Set UTF-8 connection charset */
  if (mysql_set_character_set(&mysql, "utf8mb4")) {
    fprintf(stderr, "Failed to set character set utf8mb4\n");
    MYSQLERROR(mysql);
  }

  /* Create database */
  mysql_query(&mysql, "CREATE DATABASE IF NOT EXISTS agg_test");
  if (mysql_select_db(&mysql, DB_NAME)) {
    MYSQLERROR(mysql);
  }

  if (do_load) {
    fprintf(stderr, "\n--- Loading data ---\n");
    create_tables(mysql);
    create_large_tables(mysql);
    create_nullagg_tables(mysql);
    populate_data(mysql);
    populate_large_data(mysql);
    populate_nullagg_data(mysql);
    fprintf(stderr, "--- Data loading complete ---\n");
  }

  /* NDB connection */
  ndb_init();

  int passed = 0;
  int N_TESTS = 0;
  {
    // Block scope ensures Ndb and Ndb_cluster_connection are destroyed
    // before ndb_end() is called below.
    Ndb_cluster_connection cluster_connection(connectstring);
    if (cluster_connection.connect(4, 5, 1)) {
      std::cout << "Unable to connect to cluster within 30 secs." << std::endl;
      exit(-1);
    }
    if (cluster_connection.wait_until_ready(30, 0) < 0) {
      std::cout << "Cluster was not ready within 30 secs." << std::endl;
      exit(-1);
    }

    Ndb myNdb(&cluster_connection, DB_NAME);
    if (myNdb.init(1024) == -1) {
      APIERROR(myNdb.getNdbError());
    }

    /* Run tests */
    TestContext ctx;
    ctx.ndb = &myNdb;
    ctx.mysql = &mysql;
    ctx.validate = do_validate;
    ctx.ndb_table = "pa_test";
    ctx.inno_table = "pa_test_inno";

    typedef bool (*TestFunc)(TestContext &);
    struct TestEntry {
      const char *name;
      TestFunc func;
    };

    TestEntry tests[] = {
      {"Test  1: Charset-Aware GROUP BY (table scan)",     run_test_1},
      {"Test  2: Multi-Column GROUP BY (table scan)",      run_test_2},
      {"Test  3: Numeric-Only GROUP BY (table scan)",      run_test_3},
      {"Test  4: Aggregation Without GROUP BY",            run_test_4},
      {"Test  5: Aggregation With Filter (table scan)",    run_test_5},
      {"Test  6: Arithmetic Expressions (table scan)",     run_test_6},
      {"Test  7: DECIMAL Aggregation (table scan)",        run_test_7},
      {"Test  8: NULL Handling in COUNT (table scan)",     run_test_8},
      {"Test  9: Charset-Aware GROUP BY (index scan)",     run_test_9},
      {"Test 10: Index Scan + Filter + Arithmetic",        run_test_10},
      {"Test 11: Large Dataset API-Side Merge",            run_test_11},
      {"Test 12: Bug E — Modulo signed % unsigned",        run_test_12},
      {"Test 13: Bug B — unsigned - unsigned underflow",   run_test_13},
      {"Test 14: Bug C — signed - signed overflow",        run_test_14},
      {"Test 15: Bug D — INT_MIN64 * n overflow",          run_test_15},
      {"Test 16: Multi-Column Numeric GROUP BY",            run_test_16},
      {"Test 17: Nullable Numeric GROUP BY",                run_test_17},
      {"Test 18: Reject Blob/Text GROUP BY",                run_test_18},
      {"Test 19: Repeated PA Scan Lifecycle",               run_test_19},
      {"Test 20: Many Aggregation Operations (4 aggs)",     run_test_20},
      {"Test 21: All-NULL SUM/MIN/MAX Merge",               run_test_21},
      {"Test 22: Empty Result Set",                         run_test_22},
    };

    const int n_tests = sizeof(tests) / sizeof(tests[0]);
    N_TESTS = n_tests;
    bool results[n_tests];

    for (int i = 0; i < n_tests; i++) {
      results[i] = tests[i].func(ctx);
      fprintf(stderr, "  >>> %s: %s\n",
              tests[i].name, results[i] ? "PASSED" : "FAILED");
      /* Output to stdout for MTR result matching */
      printf("[TEST] %s: %s\n", tests[i].name, results[i] ? "PASS" : "FAIL");
      fflush(stdout);
    }

    /* Summary */
    fprintf(stderr, "\n========== SUMMARY ==========\n");
    for (int i = 0; i < n_tests; i++) {
      fprintf(stderr, "  %s: %s\n",
              tests[i].name, results[i] ? "PASSED" : "FAILED");
      if (results[i]) passed++;
    }
    fprintf(stderr, "  Result: %d/%d PASSED\n", passed, n_tests);
    fprintf(stderr, "=============================\n");
  }
  // Ndb and Ndb_cluster_connection are now destroyed; safe to tear down.

  mysql_close(&mysql);
  ndb_end(0);

  return (passed == N_TESTS) ? 0 : 1;
}
