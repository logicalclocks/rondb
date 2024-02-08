/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
 *
 * Author: Zhao Song
 */
#include "config.h"

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <mysqld_error.h>
#include <NdbApi.hpp>
// Used for cout
#include<iomanip>
#include <cassert>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <config.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#include <random>
#include <fstream>
#include <AttributeHeader.hpp>

/**
 * Helper sleep function
 */
static void
milliSleep(int milliseconds){
  struct timeval sleeptime;
  sleeptime.tv_sec = milliseconds / 1000;
  sleeptime.tv_usec = (milliseconds - (sleeptime.tv_sec * 1000)) * 1000000;
  select(0, 0, 0, 0, &sleeptime);
}


/**
 * Helper debugging macros
 */
#define PRINT_ERROR(code,msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
  << ", code: " << code \
  << ", msg: " << msg << "." << std::endl
#define MYSQLERROR(mysql) { \
  PRINT_ERROR(mysql_errno(&mysql),mysql_error(&mysql)); \
  exit(-1); }
#define APIERROR(error) { \
  PRINT_ERROR(error.code,error.message); \
  exit(-1); }

struct Row {
  Int32 cint32;
  Int8 cint8;
  Int16 cint16;
  Int32 cint24;
  Int64 cint64;

  Uint8 cuint8;
  Uint16 cuint16;
  Uint32 cuint24;
  Uint32 cuint32;
  Uint64 cuint64;

  float cfloat;
  double cdouble;

  double cdecimal;
  Uint64 cdecimal2;

  char cchar[32];
};

void drop_table(MYSQL &mysql)
{
  if (mysql_query(&mysql, "DROP TABLE IF EXISTS api_scan"))
    MYSQLERROR(mysql);
}

void drop_table_inno(MYSQL &mysql)
{
  if (mysql_query(&mysql, "DROP TABLE IF EXISTS api_scan_inno"))
    MYSQLERROR(mysql);
}

void create_table(MYSQL &mysql)
{
  /* Disk column table
  while (mysql_query(&mysql,
        "CREATE TABLE agg.api_scan ("
        "CINT INT NOT NULL,"
        "CTINYINT TINYINT NOT NULL,"
        "CSMALLINT SMALLINT NOT NULL,"
        "CMEDIUMINT MEDIUMINT NOT NULL,"
        "CBIGINT BIGINT NOT NULL,"
        "CUTINYINT TINYINT UNSIGNED NOT NULL,"
        "CUSMALLINT SMALLINT UNSIGNED NOT NULL,"
        "CUMEDIUMINT MEDIUMINT UNSIGNED NOT NULL,"
        "CUINT INT UNSIGNED NOT NULL,"
        "CUBIGINT BIGINT UNSIGNED NOT NULL,"
        "CFLOAT FLOAT NOT NULL,"
        "CDOUBLE DOUBLE NOT NULL STORAGE DISK,"
        "CCHAR VARCHAR(29) NOT NULL,"
        "CDECIMAL DECIMAL(10,2) NOT NULL,"
        "CDECIMAL2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "PRIMARY KEY USING HASH (CINT)) TABLESPACE ts_1 ENGINE=NDB CHARSET=latin1"))
  */
  while (mysql_query(&mysql,
        "CREATE TABLE agg.api_scan ("
        "CINT INT NOT NULL,"
        "CTINYINT TINYINT NOT NULL,"
        "CSMALLINT SMALLINT NOT NULL,"
        "CMEDIUMINT MEDIUMINT NOT NULL,"
        "CBIGINT BIGINT NOT NULL,"
        "CUTINYINT TINYINT UNSIGNED NOT NULL,"
        "CUSMALLINT SMALLINT UNSIGNED NOT NULL,"
        "CUMEDIUMINT MEDIUMINT UNSIGNED NOT NULL,"
        "CUINT INT UNSIGNED NOT NULL,"
        "CUBIGINT BIGINT UNSIGNED NOT NULL,"
        "CFLOAT FLOAT NOT NULL,"
        "CDOUBLE DOUBLE NOT NULL,"
        "CCHAR VARCHAR(29) NOT NULL,"
        "CDECIMAL DECIMAL(10,2) NOT NULL,"
        "CDECIMAL2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "PRIMARY KEY USING HASH (CINT)) ENGINE=NDB CHARSET=latin1"))
  {
    if (mysql_errno(&mysql) != ER_TABLE_EXISTS_ERROR)
      MYSQLERROR(mysql);
    std::cout << "MySQL Cluster already has example table: api_scan. "
      << "Dropping it..." << std::endl;
    drop_table(mysql);
  }

  if (mysql_query(&mysql,
                  "CREATE INDEX"
                  "  INDEX_CMEDIUMINT"
                  "  ON api_scan"
                  "  (CMEDIUMINT)")) {
    MYSQLERROR(mysql);
  }
}

void create_table_innodb(MYSQL &mysql)
{
  while (mysql_query(&mysql,
        "CREATE TABLE agg.api_scan_inno ("
        "CINT INT NOT NULL,"
        "CTINYINT TINYINT NOT NULL,"
        "CSMALLINT SMALLINT NOT NULL,"
        "CMEDIUMINT MEDIUMINT NOT NULL,"
        "CBIGINT BIGINT NOT NULL,"
        "CUTINYINT TINYINT UNSIGNED NOT NULL,"
        "CUSMALLINT SMALLINT UNSIGNED NOT NULL,"
        "CUMEDIUMINT MEDIUMINT UNSIGNED NOT NULL,"
        "CUINT INT UNSIGNED NOT NULL,"
        "CUBIGINT BIGINT UNSIGNED NOT NULL,"
        "CFLOAT FLOAT NOT NULL,"
        "CDOUBLE DOUBLE NOT NULL,"
        "CCHAR VARCHAR(29) NOT NULL,"
        "CDECIMAL DECIMAL(10,2) NOT NULL,"
        "CDECIMAL2 DECIMAL(10,0) UNSIGNED NOT NULL,"
        "PRIMARY KEY(CINT)) ENGINE=INNODB CHARSET=latin1"))
  {
    if (mysql_errno(&mysql) != ER_TABLE_EXISTS_ERROR)
      MYSQLERROR(mysql);
    std::cout << "MySQL Cluster already has example table: api_scan_inno. "
      << "Dropping it..." << std::endl;
    drop_table_inno(mysql);
  }

  if (mysql_query(&mysql,
                  "CREATE INDEX"
                  "  INDEX_CMEDIUMINT"
                  "  ON api_scan_inno"
                  "  (CMEDIUMINT)")) {
    MYSQLERROR(mysql);
  }
}

std::random_device rd;
std::mt19937 gen(rd());

/*
   std::uniform_int_distribution<int64_t> g_bigint(0xFFFFFFFF, 0x7FFFFFFF);
   std::uniform_int_distribution<uint64_t> g_ubigint(0, 0xFFFFFFFF);
   std::uniform_int_distribution<int32_t> g_int(0xFFFF, 0x7FFF);
   std::uniform_int_distribution<uint32_t> g_uint(0, 0xFFFF);
   std::uniform_int_distribution<int32_t> g_mediumint(0x0FFF, 0x7FF);
   std::uniform_int_distribution<uint32_t> g_umediumint(0, 0xFFF);
   std::uniform_int_distribution<int16_t> g_smallint(0xFF, 0x7F);
   std::uniform_int_distribution<uint16_t> g_usmallint(0, 0xFF);
   std::uniform_int_distribution<int8_t> g_tinyint(0xF, 0x7);
   std::uniform_int_distribution<uint8_t> g_utinyint(0, 0xF);
   std::uniform_real_distribution<float> g_float(0xFFFF, 0x7FFF);
   std::uniform_real_distribution<double> g_double(0xFFFFFFFF, 0x7FFFFFFF);
*/

std::uniform_int_distribution<int64_t> g_bigint(-3147483648, 3147483648);
std::uniform_int_distribution<uint64_t> g_ubigint(0, 5294967295);
std::uniform_int_distribution<int32_t> g_int(-2147483648, 2147483647);
std::uniform_int_distribution<uint32_t> g_uint(0, 4294967295);
// std::uniform_int_distribution<int32_t> g_mediumint(-8388608, 8388607);
std::uniform_int_distribution<int32_t> g_mediumint(-10, 10);
std::uniform_int_distribution<uint32_t> g_umediumint(0, 8388607);
std::uniform_int_distribution<int16_t> g_smallint(-32768, 32767);
std::uniform_int_distribution<uint16_t> g_usmallint(0, 32768);
// std::uniform_int_distribution<int8_t> g_tinyint(-128, 127);
std::uniform_int_distribution<int8_t> g_tinyint(60, 70);
std::uniform_int_distribution<uint8_t> g_utinyint(0, 255);
std::uniform_real_distribution<float> g_float(-32768, 32767);
std::uniform_real_distribution<double> g_double(-8388608, 8388607);

std::uniform_int_distribution<uint8_t> g_zero(0, 19);

#define NUM 10000
int populate(Ndb * myNdb, MYSQL& mysql)
{
  int i;
  Row rows[NUM];

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("api_scan");

  if (myTable == NULL)
    APIERROR(myDict->getNdbError());

  std::fstream fs;
  fs.open("/tmp/agg_data.txt", std::fstream::out | std::ofstream::trunc);

  for (i = 0; i < NUM; i++)
  {
    // rows[i].cint32 = g_int(gen);
    rows[i].cint32 = i;
    rows[i].cint8 = g_tinyint(gen);
    rows[i].cint16 = g_smallint(gen);
    rows[i].cint24 = g_mediumint(gen);
    rows[i].cint64 = g_bigint(gen);

    rows[i].cuint8 = g_utinyint(gen);
    rows[i].cuint16 = g_usmallint(gen);
    if (g_zero(gen) == 6) {
      rows[i].cuint16 = 0;
    }
    rows[i].cuint24 = g_umediumint(gen);
    rows[i].cuint32 = g_uint(gen);
    rows[i].cuint64 = g_ubigint(gen);
    if (g_zero(gen) == 6) {
      rows[i].cuint64 = 0;
    }

    rows[i].cfloat = g_float(gen);
    rows[i].cdouble = g_double(gen);
    if (g_zero(gen) == 6) {
      rows[i].cdouble = 0;
    }

    // Simple for debug
    // rows[i].cint32 = i;
    // rows[i].cint8 = i;
    // rows[i].cint16 = i;
    // rows[i].cint24 = i;
    // rows[i].cint64 = i;
    // rows[i].cuint8 = i * 2;
    // rows[i].cuint16 = i * 2;
    // rows[i].cuint24 = i * 2;
    // rows[i].cuint32 = i * 2;
    // rows[i].cuint64 = i * 2;
    // rows[i].cfloat = i * 1.1;
    // rows[i].cdouble = i * 1.11;


    // Must memset here, otherwise group by this
    // column in aggregation interpreter would be undefined.
    memset(rows[i].cchar, 0, sizeof(rows[i].cchar));

    rows[i].cdecimal = rows[i].cdouble;
    rows[i].cdecimal2 = rows[i].cuint64;

    rows[i].cchar[0] = 10;
    switch (i % 4) {
      case 0:
        sprintf(&(rows[i].cchar[1]), "GROUPxxx_1");
        break;
      case 1:
        sprintf(&(rows[i].cchar[1]), "GROUPxxx_2");
        break;
      case 2:
        sprintf(&(rows[i].cchar[1]), "GROUPxxx_3");
        break;
      case 3:
        sprintf(&(rows[i].cchar[1]), "GROUPxxx_4");
        break;
      default:
        assert(0);
    }
		std::string str = std::to_string(rows[i].cint32) + "," +
                      std::to_string(rows[i].cint8) + "," +
                      std::to_string(rows[i].cint16) + "," +
                      std::to_string(rows[i].cint24) + "," +
                      std::to_string(rows[i].cint64) + "," +
                      std::to_string(rows[i].cuint8) + "," +
                      std::to_string(rows[i].cuint16) + "," +
                      std::to_string(rows[i].cuint24) + "," +
                      std::to_string(rows[i].cuint32) + "," +
                      std::to_string(rows[i].cuint64) + "," +
                      std::to_string(rows[i].cfloat) + "," +
                      std::to_string(rows[i].cdouble) + "," +
                      "'" +
                      std::string(&(rows[i].cchar[1]), 10) +
                      "'" + "," +
                      std::to_string(rows[i].cdecimal) + "," +
                      std::to_string(rows[i].cdecimal2);
    std::string insert_sql = "INSERT INTO agg.api_scan_inno VALUES(" + str + ")";
    if (mysql_real_query(&mysql, insert_sql.data(), insert_sql.length())) {
      MYSQLERROR(mysql);
    }
    std::string insert_sql_ndb = "INSERT INTO agg.api_scan VALUES(" + str + ")";
    if (mysql_real_query(&mysql, insert_sql_ndb.data(), insert_sql_ndb.length())) {
      MYSQLERROR(mysql);
    }
    fs << str;
    fs << std::endl;
  }
  return 1;

  /*
   * Moz
   * Since we're populating a table with DECIMAL columns, which NDBAPI doesn't
   * support.
   * Here we use mysql client instead.
   *
  NdbTransaction* myTrans = myNdb->startTransaction();
  if (myTrans == NULL)
    APIERROR(myNdb->getNdbError());

  for (i = 0; i < NUM; i++)
  {
    NdbOperation* myNdbOperation = myTrans->getNdbOperation(myTable);
    if (myNdbOperation == NULL)
      APIERROR(myTrans->getNdbError());
    myNdbOperation->insertTuple();
#ifdef NDEBUG
    myNdbOperation->equal("CINT", rows[i].cint32);
    myNdbOperation->setValue("CTINYINT", rows[i].cint8);
    myNdbOperation->setValue("CSMALLINT", rows[i].cint16);
    myNdbOperation->setValue("CMEDIUMINT", rows[i].cint24);
    myNdbOperation->setValue("CBIGINT", rows[i].cint64);

    myNdbOperation->setValue("CUTINYINT", rows[i].cuint8);
    myNdbOperation->setValue("CUSMALLINT", rows[i].cuint16);
    myNdbOperation->setValue("CUMEDIUMINT", rows[i].cuint24);
    myNdbOperation->setValue("CUINT", rows[i].cuint32);
    myNdbOperation->setValue("CUBIGINT", rows[i].cuint64);

    myNdbOperation->setValue("CFLOAT", rows[i].cfloat);
    myNdbOperation->setValue("CDOUBLE", rows[i].cdouble);

    myNdbOperation->setValue("CCHAR", rows[i].cchar);
#else
    assert(myNdbOperation->equal("CINT", rows[i].cint32) != -1);
    assert(myNdbOperation->setValue("CTINYINT", rows[i].cint8) != -1);
    assert(myNdbOperation->setValue("CSMALLINT", rows[i].cint16) != -1);
    assert(myNdbOperation->setValue("CMEDIUMINT", rows[i].cint24) != -1);
    assert(myNdbOperation->setValue("CBIGINT", rows[i].cint64) != -1);

    assert(myNdbOperation->setValue("CUTINYINT", rows[i].cuint8) != -1);
    assert(myNdbOperation->setValue("CUSMALLINT", rows[i].cuint16) != -1);
    assert(myNdbOperation->setValue("CUMEDIUMINT", rows[i].cuint24) != -1);
    assert(myNdbOperation->setValue("CUINT", rows[i].cuint32) != -1);
    assert(myNdbOperation->setValue("CUBIGINT", rows[i].cuint64) != -1);

    assert(myNdbOperation->setValue("CFLOAT", rows[i].cfloat) != -1);
    assert(myNdbOperation->setValue("CDOUBLE", rows[i].cdouble) != -1);

    assert(myNdbOperation->setValue("CCHAR", rows[i].cchar) != -1);
#endif // NDEBUG
  }

  int check = myTrans->execute(NdbTransaction::Commit);
  if (check != 0) {
    std::cout <<  myTrans->getNdbError().message << std::endl;
  }

  myTrans->close();

  return check != -1;
  */
}

#define sint3korr(A)  ((int32_t) ((((uint8_t) (A)[2]) & 128) ? \
                                  (((uint32_t) 255L << 24) | \
                                  (((uint32_t) (uint8_t) (A)[2]) << 16) |\
                                  (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                   ((uint32_t) (uint8_t) (A)[0])) : \
                                 (((uint32_t) (uint8_t) (A)[2]) << 16) |\
                                 (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                  ((uint32_t) (uint8_t) (A)[0])))

#define uint3korr(A)  (uint32_t) (((uint32_t) ((uint8_t) (A)[0])) +\
                                  (((uint32_t) ((uint8_t) (A)[1])) << 8) +\
                                  (((uint32_t) ((uint8_t) (A)[2])) << 16))

int scan_aggregation(Ndb * myNdb, MYSQL& mysql, bool validation)
{
  // Scan all records exclusive and update
  // them one by one
  int                  retryAttempt = 0;
  const int            retryMax = 10;
  NdbError              err;
  NdbTransaction	*myTrans;
  NdbScanOperation	*myScanOp;

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("api_scan");

  if (myTable == NULL)
    APIERROR(myDict->getNdbError());

  /**
   * Loop as long as :
   *  retryMax not reached
   *  failed operations due to TEMPORARY errors
   *
   * Exit loop;
   *  retyrMax reached
   *  Permanent error (return -1)
   */
  while (true)
  {

    if (retryAttempt >= retryMax)
    {
      std::cout << "ERROR: has retried this operation " << retryAttempt
        << " times, failing!" << std::endl;
      return -1;
    }

    myTrans = myNdb->startTransaction();
    if (myTrans == NULL)
    {
      const NdbError err = myNdb->getNdbError();

      if (err.status == NdbError::TemporaryError)
      {
        milliSleep(50);
        retryAttempt++;
        continue;
      }
      std::cout << err.message << std::endl;
      return -1;
    }
    /*
     * Define a scan operation.
     * NDBAPI.
     */
    myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL)
    {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead) != 0) {
      APIERROR (myTrans->getNdbError());
    }

    /* Filter CTINYINT = 66 */
    uint8_t val = 66;
    NdbScanFilter filter(myScanOp);
    if (filter.begin(NdbScanFilter::AND) < 0  ||
        filter.cmp(NdbScanFilter::COND_EQ, 1, &val, sizeof(val)) < 0 ||
        filter.end() < 0) {
      std::cout <<  myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    /*
     * Define an aggregator
     */
    NdbAggregator aggregator(myTable);
#ifdef NDEBUG
    aggregator.GroupBy("CCHAR");
    aggregator.GroupBy("CMEDIUMINT");
    aggregator.LoadColumn("CUBIGINT", kReg1);
    aggregator.LoadColumn("CUTINYINT", kReg2);
    aggregator.Add(kReg1, kReg2);
    aggregator.LoadUint64(6666, kReg2);
    aggregator.Add(kReg1, kReg2);
    aggregator.Sum(0, kReg1);
    aggregator.LoadColumn("CDOUBLE", kReg1);
    aggregator.LoadInt64(-8888, kReg2);
    aggregator.Minus(kReg1, kReg2);
    aggregator.Min(1, kReg1);
    aggregator.LoadColumn("CUMEDIUMINT", kReg1);
    aggregator.LoadDouble(6.6, kReg2);
    aggregator.Mul(kReg1, kReg2);
    aggregator.Max(2, kReg1);
    aggregator.LoadColumn("CDECIMAL", kReg1);
    aggregator.LoadColumn("CDECIMAL2", kReg2);
    aggregator.Add(kReg1, kReg2);
    aggregator.Max(3, kReg1);
#else
    assert(aggregator.GroupBy("CCHAR"));
    assert(aggregator.GroupBy("CMEDIUMINT"));
    assert(aggregator.LoadColumn("CUBIGINT", kReg1));
    assert(aggregator.LoadColumn("CUTINYINT", kReg2));
    assert(aggregator.Add(kReg1, kReg2));
    assert(aggregator.LoadUint64(6666, kReg2));
    assert(aggregator.Add(kReg1, kReg2));
    assert(aggregator.Sum(0, kReg1));
    assert(aggregator.LoadColumn("CDOUBLE", kReg1));
    assert(aggregator.LoadInt64(-8888, kReg2));
    assert(aggregator.Minus(kReg1, kReg2));
    assert(aggregator.Min(1, kReg1));
    assert(aggregator.LoadColumn("CUMEDIUMINT", kReg1));
    assert(aggregator.LoadDouble(6.6, kReg2));
    assert(aggregator.Mul(kReg1, kReg2));
    assert(aggregator.Max(2, kReg1));
    assert(aggregator.LoadColumn("CDECIMAL", kReg1));
    assert(aggregator.LoadColumn("CDECIMAL2", kReg2));
    assert(aggregator.Add(kReg1, kReg2));
    assert(aggregator.Max(3, kReg1));
#endif // NDEBUG

    /* Example of how to catch an error
    int ret = aggregator.Sum(0, kReg1);
    if (!ret) {
      fprintf(stderr, "Error: %u, %s\n",
                      aggregator.GetError().errno_,
                      aggregator.GetError().err_msg_);
    }
    */

#ifdef NDEBUG
    aggregator.Finalize();
#else
    assert(aggregator.Finalize());
#endif // NDEBUG
    if (myScanOp->setAggregationCode(&aggregator) == -1) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->DoAggregation() == -1) {
      err = myTrans->getNdbError();
      if(err.status == NdbError::TemporaryError) {
        std::cout << myTrans->getNdbError().message << std::endl;
        myNdb->closeTransaction(myTrans);
        milliSleep(50);
        continue;
      }
      std::cout << "DoAggregation failed: " << err.message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (validation) {
      bool valid = true;
      fprintf(stderr, "Num of groups: %lu\n", aggregator.gb_map()->size());
      for (auto iter = aggregator.gb_map()->begin();
          iter != aggregator.gb_map()->end(); iter++) {
        std::string value_cchar = std::string(iter->first.ptr +
            sizeof(AttributeHeader) + 1, 10);
        assert(iter->first.ptr + sizeof(AttributeHeader) + 12 +
            sizeof(AttributeHeader) + 4 ==
            iter->second.ptr);
        int32_t cmedium =
          sint3korr(iter->first.ptr + 2 * sizeof(AttributeHeader) + 12);
        std::string value_cmedium = std::to_string(
            sint3korr(iter->first.ptr + 2 * sizeof(AttributeHeader) + 12));

        AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
        uint64_t agg_1 = item[0].value.val_uint64;
        double agg_2 = item[1].value.val_double;
        double agg_3 = item[2].value.val_double;
        double agg_4 = item[3].value.val_double;
        {
          MYSQL_RES *res;
          MYSQL_ROW row;
          std::string sql = std::string("SELECT SUM(CUBIGINT+CUTINYINT+6666),MIN(CDOUBLE-(-8888)), MAX(CUMEDIUMINT*6.6), MAX(CDECIMAL+CDECIMAL2) FROM agg.api_scan_inno WHERE CTINYINT = 66 AND CCHAR=") +
            "'" +
            value_cchar +
            "'" +
            " and CMEDIUMINT = " +
            value_cmedium +
            " GROUP BY CCHAR, CMEDIUMINT";
          if (mysql_real_query(&mysql, sql.data(), sql.length())) {
          } else {
            res = mysql_store_result(&mysql);
            assert(res != nullptr);
            assert(mysql_num_fields(res) == 4);
            assert(mysql_num_rows(res) == 1);
            while ((row = mysql_fetch_row(res))) {
              // unsigned long *lengths = mysql_fetch_lengths(res);
              // fprintf(stderr, "row length: %ld\n, data: %s, data: %lu", *lengths, row[0], atol(row[0]));
              if (std::stoul(row[0]) != agg_1 ||
                  (agg_2 - std::stod(row[1]) > 1.0 ||
                   agg_2 - std::stod(row[1]) < -1.0) ||
                  (agg_3 - std::stod(row[2]) > 1.0 ||
                   agg_3 - std::stod(row[2]) < -1.0) ||
                  (agg_4 - std::stod(row[3]) > 1.0 ||
                   agg_4 - std::stod(row[3]) < -1.0)) {
                fprintf(stderr, "Catch [%s, %d] -> %lu, %lf, %lf, %lf : %lu, %lf, %lf, %lf\n",
                    value_cchar.c_str(), cmedium,
                    agg_1, agg_2, agg_3, agg_4,
                    std::stoul(row[0]), std::stod(row[1]), std::stod(row[2]), std::stod(row[3]));
                valid = false;
              }
            }
          }
          mysql_free_result(res);
        }
      }
      if (valid) {
        fprintf(stderr, "Results validation: PASSED\n");
      } else {
        fprintf(stderr, "Results validation: FAILED\n");
      }
    }

    fprintf(stderr, "---FINAL RESULT---\n");
    NdbAggregator::ResultRecord record = aggregator.FetchResultRecord();
    while (!record.end()) {
      NdbAggregator::Column column = record.FetchGroupbyColumn();
      int n = 0;
      std::string value_cchar;
      std::string value_cmedium;
      while (!column.end()) {
        if (n == 0) {
        fprintf(stderr,
            "group [id: %u, type: %u, byte_size: %u, is_null: %u, data: %s]:",
            column.id(), column.type(), column.byte_size(),
            column.is_null(), &column.data()[1]);
        value_cchar = std::string(&column.data()[1], column.byte_size() - 1);
        } else {
        fprintf(stderr,
            "group [id: %u, type: %u, byte_size: %u, is_null: %u, data: %d]:",
            column.id(), column.type(), column.byte_size(),
            column.is_null(), column.data_medium());
        }
        value_cmedium = std::to_string(column.data_medium());
        n++;
        column = record.FetchGroupbyColumn();
      }

      NdbAggregator::Result result = record.FetchAggregationResult();
      while (!result.end()) {
        switch (result.type()) {
          case NdbDictionary::Column::Bigint:
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %ld)",
                result.type(), result.is_null(), result.data_int64());
            break;
          case NdbDictionary::Column::Bigunsigned:
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %lu)",
                result.type(), result.is_null(), result.data_uint64());
            break;
          case NdbDictionary::Column::Double:
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %lf)",
                result.type(), result.is_null(), result.data_double());
            break;
          case NdbDictionary::Column::Undefined:
            // Aggregation on empty table or all rows are filtered out.
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %ld)",
                result.type(), result.is_null(), result.data_int64());
            break;
          default:
            assert(0);
        }
        result = record.FetchAggregationResult();
      }
      fprintf(stderr, "\n");
      record = aggregator.FetchResultRecord();
    }

    myNdb->closeTransaction(myTrans);
    return 1;
  }
  return -1;
}

int scan_index_aggregation(Ndb *myNdb, MYSQL& mysql, bool validation) {
  NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex = myDict->getIndex("INDEX_CMEDIUMINT", "api_scan");
  if (myPIndex == NULL) {
    APIERROR(myDict->getNdbError());
  }

  NdbTransaction *myTrans = myNdb->startTransaction();
  if (myTrans == NULL) {
    APIERROR(myNdb->getNdbError());
  }

  NdbIndexScanOperation *myIndexScanOp = myTrans->getNdbIndexScanOperation(myPIndex);


  /* Index Scan */
  Uint32 scanFlags= NdbScanOperation::SF_OrderBy |
                    NdbScanOperation::SF_MultiRange;
  /**
   * Read without locks, without being placed in lock queue
   */
  if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                scanFlags
                                /*(Uint32) 0 // batch */
                                /*(Uint32) 0 // parallel */
                                ) != 0) {
    APIERROR (myTrans->getNdbError());
  }

  /* Index range: CMEDIUMINT >= 6 and CMEDIUMINT < 8 */
  Uint32 low=6;
  Uint32 high=8;

  if (myIndexScanOp->setBound("CMEDIUMINT", NdbIndexScanOperation::BoundLE, (char*)&low)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->setBound("CMEDIUMINT", NdbIndexScanOperation::BoundGT, (char*)&high)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->end_of_bound(0)) {
    APIERROR(myIndexScanOp->getNdbError());
  }

  /* Filter: CTINYINT = 66 */
  uint8_t val = 66;
  NdbScanFilter filter(myIndexScanOp);
  if (filter.begin(NdbScanFilter::AND) < 0  ||
      filter.cmp(NdbScanFilter::COND_EQ, 1, &val, sizeof(val)) < 0 ||
      filter.end() < 0) {
    std::cout <<  myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  /* Aggregation program */
  const NdbDictionary::Table *myTable= myDict->getTable("api_scan");

  if (myTable == NULL) {
    APIERROR(myDict->getNdbError());
  }

  NdbAggregator aggregator(myTable);
#ifdef NDEBUG
  aggregator.GroupBy("CCHAR");
  aggregator.GroupBy("CMEDIUMINT");
  aggregator.LoadColumn("CUBIGINT", kReg1);
  aggregator.LoadColumn("CUTINYINT", kReg2);
  aggregator.Add(kReg1, kReg2);
  aggregator.LoadUint64(6666, kReg2);
  aggregator.Add(kReg1, kReg2);
  aggregator.Sum(0, kReg1);
  aggregator.LoadColumn("CDOUBLE", kReg1);
  aggregator.LoadInt64(-8888, kReg2);
  aggregator.Minus(kReg1, kReg2);
  aggregator.Min(1, kReg1);
  aggregator.LoadColumn("CUMEDIUMINT", kReg1);
  aggregator.LoadDouble(6.6, kReg2);
  aggregator.Mul(kReg1, kReg2);
  aggregator.Max(2, kReg1);
  aggregator.LoadColumn("CDECIMAL", kReg1);
  aggregator.LoadColumn("CDECIMAL2", kReg2);
  aggregator.Add(kReg1, kReg2);
  aggregator.Max(3, kReg1);
#else
  assert(aggregator.GroupBy("CCHAR"));
  assert(aggregator.GroupBy("CMEDIUMINT"));
  assert(aggregator.LoadColumn("CUBIGINT", kReg1));
  assert(aggregator.LoadColumn("CUTINYINT", kReg2));
  assert(aggregator.Add(kReg1, kReg2));
  assert(aggregator.LoadUint64(6666, kReg2));
  assert(aggregator.Add(kReg1, kReg2));
  assert(aggregator.Sum(0, kReg1));
  assert(aggregator.LoadColumn("CDOUBLE", kReg1));
  assert(aggregator.LoadInt64(-8888, kReg2));
  assert(aggregator.Minus(kReg1, kReg2));
  assert(aggregator.Min(1, kReg1));
  assert(aggregator.LoadColumn("CUMEDIUMINT", kReg1));
  assert(aggregator.LoadDouble(6.6, kReg2));
  assert(aggregator.Mul(kReg1, kReg2));
  assert(aggregator.Max(2, kReg1));
  assert(aggregator.LoadColumn("CDECIMAL", kReg1));
  assert(aggregator.LoadColumn("CDECIMAL2", kReg2));
  assert(aggregator.Add(kReg1, kReg2));
  assert(aggregator.Max(3, kReg1));
#endif // NDEBUG

#ifdef NDEBUG
  aggregator.Finalize();
#else
  assert(aggregator.Finalize());
#endif // NDEBUG
  if (myIndexScanOp->setAggregationCode(&aggregator) == -1) {
    std::cout << myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  NdbError err;
  if (myIndexScanOp->DoAggregation() == -1) {
    err = myTrans->getNdbError();
    std::cout << "DoAggregation failed: " << err.message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }

  if (validation) {
    bool valid = true;
    fprintf(stderr, "Num of groups: %lu\n", aggregator.gb_map()->size());
    for (auto iter = aggregator.gb_map()->begin();
        iter != aggregator.gb_map()->end(); iter++) {
      std::string value_cchar = std::string(iter->first.ptr +
          sizeof(AttributeHeader) + 1, 10);
      assert(iter->first.ptr + sizeof(AttributeHeader) + 12 +
          sizeof(AttributeHeader) + 4 ==
          iter->second.ptr);
      int32_t cmedium =
        sint3korr(iter->first.ptr + 2 * sizeof(AttributeHeader) + 12);
      std::string value_cmedium = std::to_string(
          sint3korr(iter->first.ptr + 2 * sizeof(AttributeHeader) + 12));

      AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
      uint64_t agg_1 = item[0].value.val_uint64;
      double agg_2 = item[1].value.val_double;
      double agg_3 = item[2].value.val_double;
      double agg_4 = item[3].value.val_double;
      {
        MYSQL_RES *res;
        MYSQL_ROW row;
        std::string sql = std::string("SELECT SUM(CUBIGINT+CUTINYINT+6666),MIN(CDOUBLE-(-8888)), MAX(CUMEDIUMINT*6.6), MAX(CDECIMAL+CDECIMAL2) FROM agg.api_scan_inno WHERE CTINYINT = 66 AND CMEDIUMINT >= 6 AND CMEDIUMINT < 8 AND CCHAR=") +
          "'" +
          value_cchar +
          "'" +
          " and CMEDIUMINT = " +
          value_cmedium +
          " GROUP BY CCHAR, CMEDIUMINT";
        if (mysql_real_query(&mysql, sql.data(), sql.length())) {
        } else {
          res = mysql_store_result(&mysql);
          assert(res != nullptr);
          assert(mysql_num_fields(res) == 4);
          assert(mysql_num_rows(res) == 1);
          while ((row = mysql_fetch_row(res))) {
            // unsigned long *lengths = mysql_fetch_lengths(res);
            // fprintf(stderr, "row length: %ld\n, data: %s, data: %lu", *lengths, row[0], atol(row[0]));
            if (std::stoul(row[0]) != agg_1 ||
                (agg_2 - std::stod(row[1]) > 1.0 ||
                 agg_2 - std::stod(row[1]) < -1.0) ||
                (agg_3 - std::stod(row[2]) > 1.0 ||
                 agg_3 - std::stod(row[2]) < -1.0) ||
                (agg_4 - std::stod(row[3]) > 1.0 ||
                 agg_4 - std::stod(row[3]) < -1.0)) {
              fprintf(stderr, "Catch [%s, %d] -> %lu, %lf, %lf, %lf: %lu, %lf, %lf, %lf\n",
                  value_cchar.c_str(), cmedium,
                  agg_1, agg_2, agg_3, agg_4,
                  std::stoul(row[0]), std::stod(row[1]), std::stod(row[2]), std::stod(row[3]));
              valid = false;
            }
          }
        }
        mysql_free_result(res);
      }
    }
    if (valid) {
      fprintf(stderr, "Results validation: PASSED\n");
    } else {
      fprintf(stderr, "Results validation: FAILED\n");
    }
  }

  fprintf(stderr, "---FINAL RESULT---\n");
  NdbAggregator::ResultRecord record = aggregator.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column column = record.FetchGroupbyColumn();
    int n = 0;
    while (!column.end()) {
      if (n == 0) {
      fprintf(stderr,
          "group [id: %u, type: %u, byte_size: %u, is_null: %u, data: %s]:",
          column.id(), column.type(), column.byte_size(),
          column.is_null(), &column.data()[1]);
      } else {
      fprintf(stderr,
          "group [id: %u, type: %u, byte_size: %u, is_null: %u, data: %d]:",
          column.id(), column.type(), column.byte_size(),
          column.is_null(), column.data_medium());
      }
      n++;
      column = record.FetchGroupbyColumn();
    }

    NdbAggregator::Result result = record.FetchAggregationResult();
    while (!result.end()) {
      switch (result.type()) {
        case NdbDictionary::Column::Bigint:
          fprintf(stderr,
              " (type: %u, is_null: %u, data: %ld)",
              result.type(), result.is_null(), result.data_int64());
          break;
        case NdbDictionary::Column::Bigunsigned:
          fprintf(stderr,
              " (type: %u, is_null: %u, data: %lu)",
              result.type(), result.is_null(), result.data_uint64());
          break;
        case NdbDictionary::Column::Double:
          fprintf(stderr,
              " (type: %u, is_null: %u, data: %lf)",
              result.type(), result.is_null(), result.data_double());
          break;
        case NdbDictionary::Column::Undefined:
          // Aggregation on empty table or all rows are filtered out.
          fprintf(stderr,
              " (type: %u, is_null: %u, data: %ld)",
              result.type(), result.is_null(), result.data_int64());
          break;
        default:
          assert(0);
      }
      result = record.FetchAggregationResult();
    }
    fprintf(stderr, "\n");
    record = aggregator.FetchResultRecord();
  }

  myNdb->closeTransaction(myTrans);
  return 1;
}

void populate_table_from_dataset(MYSQL& mysql) {
  std::fstream fs;
  fs.open("/tmp/agg_data.txt", std::fstream::in);
  std::string str;
  while (getline(fs, str)) {
    std::string insert_sql = "INSERT INTO agg.api_scan VALUES(" + str + ")";
    if (mysql_real_query(&mysql, insert_sql.data(), insert_sql.length())) {
      MYSQLERROR(mysql);
    }
    insert_sql = "INSERT INTO agg.api_scan_inno VALUES(" + str + ")";
    if (mysql_real_query(&mysql, insert_sql.data(), insert_sql.length())) {
      MYSQLERROR(mysql);
    }
  }
}

void mysql_connect_and_create(MYSQL & mysql, const char *socket, bool load)
{
  bool ok;

  ok = mysql_real_connect(&mysql, "localhost", "root", "", "", 0, socket, 0);
  if(ok) {
    mysql_query(&mysql, "CREATE DATABASE agg");
    ok = ! mysql_select_db(&mysql, "agg");
  }
  if(ok && load) {
    fprintf(stderr, "Creating 2 tables...\n");
    create_table(mysql);
    create_table_innodb(mysql);
    fprintf(stderr, "Create 2 tables done\n");
  }

  if(! ok) MYSQLERROR(mysql);
}

void ndb_run_scan(const char * connectstring, MYSQL& mysql,
                  bool load, bool populate_data, bool validation)
{

  /**************************************************************
   * Connect to ndb cluster                                     *
   **************************************************************/

  Ndb_cluster_connection cluster_connection(connectstring);
  if (cluster_connection.connect(4, 5, 1))
  {
    std::cout << "Unable to connect to cluster within 30 secs." << std::endl;
    exit(-1);
  }
  // Optionally connect and wait for the storage nodes (ndbd's)
  if (cluster_connection.wait_until_ready(30,0) < 0)
  {
    std::cout << "Cluster was not ready within 30 secs.\n";
    exit(-1);
  }

  Ndb myNdb(&cluster_connection,"agg");
  if (myNdb.init(1024) == -1) {      // Set max 1024  parallel transactions
    APIERROR(myNdb.getNdbError());
    exit(-1);
  }

  if (load) {
    if (populate_data) {
      fprintf(stderr, "populating 2 tables with a random datasets...\n");
      for (int i = 0; i < 1; i++) {
        if (populate(&myNdb, mysql) != 1) {
          std::cout << "populate: Failed!" << std::endl;
        }
      }
    } else {
      fprintf(stderr, "populating 2 tables with the datasets /tmp/agg_data.txt...\n");
      populate_table_from_dataset(mysql);
    }
    fprintf(stderr, "populate 2 tables done\n");
  }

  fprintf(stderr, "-----------------------START PUSHDOWN AGGREGATION--------------------\n");

  fprintf(stderr, "1. TABLE SCAN:\n");
  fprintf(stderr, "SELECT CCHAR, CMEDIUMINT, "
                  "SUM(CUBIGINT+CUTINYINT+6666), "
                  "MIN(CDOUBLE-(-8888)), MAX(CUMEDIUMINT*6.6) "
                  "FROM agg.api_scan "
                  "WHERE CTINYINT = 66 "                      // Filter
                  "GROUP BY CCHAR, CMEDIUMINT;\n");
  if(scan_aggregation(&myNdb, mysql, validation) > 0) {
    std::cout << "Table scan aggregation Success!" << std::endl  << std::endl;
  }

  fprintf(stderr, "2. INDEX SCAN:\n");
  fprintf(stderr, "SELECT CCHAR, CMEDIUMINT, "
                  "SUM(CUBIGINT+CUTINYINT+6666), "
                  "MIN(CDOUBLE-(-8888)), MAX(CUMEDIUMINT*6.6) "
                  "FROM agg.api_scan "
                  "WHERE CMEDIUMINT >= 6 AND CMEDIUMINT < 8 " // Index range scan
                  " AND CTINYINT = 66 "                       // Filter
                  "GROUP BY CCHAR, CMEDIUMINT;\n");
  if(scan_index_aggregation(&myNdb, mysql, validation) > 0) {
    std::cout << "Index scan aggregation Success!" << std::endl  << std::endl;
  }
}

int main(int argc, char** argv)
{
  // Usage: binary <socket mysqld> <connect_string cluster> <load> <new dataset> <validation>
  //        <load>(true)        create table and load data before running aggregation
  //        <new dataset>(true) populate table with new random dataset or reuse /tmp/agg_data.txt
  //        <validation>(true)  for each pushdown aggregation result, validate it with InnoDB.
  char * mysqld_sock  = argv[1];
  const char *connectstring = argv[2];
  MYSQL mysql;

  bool load = true;
  if (argc >= 4) {
    if (strcmp(argv[3], "false") == 0) {
      load = false;
    } else if (strcmp(argv[3], "true") != 0) {
      fprintf(stderr, "WRONG arguments[load]:(true / false)\n");
      exit(-1);
    }
  }
  bool populate = true;
  if (argc >= 5) {
    if (strcmp(argv[4], "false") == 0) {
      populate = false;
    } else if (strcmp(argv[4], "true") != 0) {
      fprintf(stderr, "WRONG arguments[populate]:(true / false)\n");
      exit(-1);
    }
    std::fstream tmp("/tmp/agg_data.txt");
    if ((populate &&!load) || !tmp.good()) {
      fprintf(stderr, "populate==true can only work with load mode(load==true). "
                      "If populate==false in load mode, it requires that "
                      "/tmp/agg_data.txt file must"
                      "exists");
      exit(-1);
    }
  }

  bool validation = true;
  if (argc == 6) {
    if (strcmp(argv[5], "false") == 0) {
      validation = false;
    } else if (strcmp(argv[5], "true") != 0) {
      fprintf(stderr, "WRONG arguments[validation]:(true / false)\n");
      exit(-1);
    }
  }

  mysql_init(& mysql);
  mysql_connect_and_create(mysql, mysqld_sock, load);

  ndb_init();
  ndb_run_scan(connectstring, mysql, load, populate, validation);
  ndb_end(0);

  mysql_close(&mysql);

  return 0;
}
