/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
 *
 * Author: Zhao Song
 */
#include <mysql.h>
#include <mysqld_error.h>
#include <random>
#include <cassert>
#include <iostream>

struct Row {
  int32_t cint32;
  int8_t cint8;
  int16_t cint16;
  int32_t cint24;
  int64_t cint64;

  uint8_t cuint8;
  uint16_t cuint16;
  uint32_t cuint24;
  uint32_t cuint32;
  uint64_t cuint64;

  float cfloat;
  double cdouble;

  char cchar[32];
};

std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<int64_t> g_bigint(-3147483648, 3147483648);
std::uniform_int_distribution<uint64_t> g_ubigint(0, 5294967295);
std::uniform_int_distribution<int32_t> g_int(0, 1000);
std::uniform_int_distribution<uint32_t> g_uint(0, 4294967295);
std::uniform_int_distribution<int32_t> g_mediumint(-10, 10);
std::uniform_int_distribution<uint32_t> g_umediumint(0, 8388607);
std::uniform_int_distribution<int16_t> g_smallint(-32768, 32767);
std::uniform_int_distribution<uint16_t> g_usmallint(0, 32768);
std::uniform_int_distribution<int8_t> g_tinyint(60, 70);
std::uniform_int_distribution<uint8_t> g_utinyint(0, 255);
std::uniform_real_distribution<float> g_float(-32768, 32767);
std::uniform_real_distribution<double> g_double(-8388608, 8388607);

int main() {
  const char *mysqld_sock = "/tmp/mysql.sock";
  MYSQL mysql;
  Row row;
  mysql_init(&mysql);
  assert(mysql_real_connect(&mysql, "localhost", "root", "", "", 0, mysqld_sock, 0));
  while (true) {
    row.cint32 = g_int(gen);
    row.cint8 = g_tinyint(gen);
    row.cint16 = g_smallint(gen);
    row.cint24 = g_mediumint(gen);
    row.cint64 = g_bigint(gen);
    row.cuint8 = g_utinyint(gen);
    row.cuint16 = g_usmallint(gen);
    row.cuint24 = g_umediumint(gen);
    row.cuint32 = g_uint(gen);
    row.cuint64 = g_ubigint(gen);
    row.cfloat = g_float(gen);
    row.cdouble = g_double(gen);
    
    std::string update_sql = "UPDATE agg.api_scan SET ctinyint = ";
    update_sql += std::to_string(row.cint8);
    update_sql += ", cmediumint = ";
    update_sql += std::to_string(row.cint24);
    update_sql += ", cutinyint = ";
    update_sql += std::to_string(row.cuint8);
    update_sql += ", cumediumint = ";
    update_sql += std::to_string(row.cuint24);
    update_sql += ", cubigint = ";
    update_sql += std::to_string(row.cuint64);
    update_sql += ", cfloat = ";
    update_sql += std::to_string(row.cfloat);
    update_sql += ", cdouble = ";
    update_sql += std::to_string(row.cdouble);
    update_sql += " WHERE cint = ";
    update_sql += std::to_string(row.cint32);

    std::cout << update_sql << std::endl;
    if (mysql_real_query(&mysql, update_sql.data(), update_sql.length())) {
      std::cout << "Got error, " << mysql_error(&mysql) << std::endl;
      break;
    }
  }
  mysql_close(&mysql);
}
